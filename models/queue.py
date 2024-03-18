import pickle
import datetime
import json
import logging
import base64
import uuid

import requests
import slackdown

import odoo
from odoo import models, fields, api
from odoo.tools.translate import _
from odoo.tools.safe_eval import safe_eval

import boto3

from ..api import send_message

_logger = logging.getLogger('IMQ.queue')

QUEUE_PROVIDERS = [
    ('aws_sqs', "AWS SQS"),
    ('pgsql', "PostgreSQL"),
]

QUEUE_TYPES = [
    ('std', "Standard"),
    ('fifo', "FIFO"),
]

class IMQQueue(models.Model):
    _name = 'imq.queue'
    _description = "IMQ - Queue"
    _order = 'name'
    
    # fields
    name = fields.Char(size=40, index=True, required=True)
    sqs_name = fields.Char("SQS Name", compute='compute_sqs_name', store=True)
    sqs_queue_url = fields.Char(
        "SQS Queue URL",
        help="URL of the Q on AWS SQS"
    )
    provider = fields.Selection(QUEUE_PROVIDERS, required=True)
    q_type = fields.Selection(QUEUE_TYPES, string="Queue Type", default='std', required=True)
    database_bound_q = fields.Boolean("Database Bound Queue", default=True)
    visibility_timeout = fields.Integer(
        help="Number of seconds a message will stay invisible once delivered. Once expired message "
             "will become visible again and may be consumed by workers. (Don't use 0 as it may leads"
             " to unpredictable results.",
        default=30
    )
    deduplication_interval_s = fields.Integer(
        "Deduplication Interval",
        default=300,
        help="Time interval in seconds during which message duplicates are searched for FIFO queues."
    )
    @api.onchange('provider')
    def onchange_provider(self):
        if self.provider or '' in ('pgsql'):
            self.database_bound_q = True
            self.key = None
            self.secret = None
            self.region = None

    @api.depends('name', 'q_type', 'database_bound_q')
    def compute_sqs_name(self):
        for record in self:
            if record.provider == 'aws_sqs':
                record.sqs_name = "{}{}{}".format(
                    record.name,
                    '_%s' % record.env.cr.dbname if record.database_bound_q else '',
                    ".fifo" if record.q_type=='fifo' else ''
                )
            else:
                record.sqs_name = None

    provider = fields.Selection(QUEUE_PROVIDERS, required=True)
    active = fields.Boolean(
        default=True,
        help="Inactive queues are not processed by workers."
    )
    region = fields.Char()
    key = fields.Char()
    secret = fields.Char()
    description = fields.Text()

    test_message_name = fields.Char(default="Test Message Name")
    test_payload = fields.Text()
    test_result = fields.Text()
    test_message_selector = fields.Char(
        "Message Selector",
        default="TestMessage",
        help="Selector is used by IMQ to figure out the processor that will "
             "process a message."
    )
    test_message_group = fields.Char(
        "Message Group",
        help="With FIFO Queues, message ordering is warranty for all messages "
             "having the same message_group."
    )
    test_message_deduplication_id = fields.Char(
        "Message Deduplication Id",
        help="On FIFO Queues when 'Content-Based Deduplication' is not set, a "
             "'MessageDeduplicationId' must be passed which each sent message."
    )
    _sql_constraints = [
        (
            'name_queue_uniq', 
            'UNIQUE(name)', 
            _("Queue name must be unique among all queue providers.")
        )
    ]

    admin_secret = fields.Char(
        compute='_compute_admin_secret', 
        help="This field returns sqs secret when user is a member of imq admin group."
    )

    def _compute_admin_secret(self):
        for record in self:
            if self.user_has_groups('inouk_message_queue.group_admin'):
                record.admin_secret = self.secret
            else:
                record.admin_secret = None
            
    def copy(self, default=None):
        self.ensure_one()
        chosen_name = default.get('name') if default else ''
        new_name = chosen_name or _('%s (copy)') % self.name
        default = dict(default or {}, name=new_name)
        return super(IMQQueue, self).copy(default)    

    def get_formview_id(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_queue__form_view').id

    def get_default_action(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_queue__act_window')

    def get_form_url(self):
        self.ensure_one()
        web_base_url = self.env['ir.config_parameter'].get_param('web.base.url')
        action_dict = self.get_formview_action()
        action_dict['action_id'] = self.get_default_action().id
        action_dict['web_base_url'] = web_base_url
        # target:
        # https://xsid-dev.inouk.ovh/web?debug#id=1&action=257&model=imq.test_launcher&view_type=form&menu_id=140
        url_str = "{web_base_url}/web#id={res_id}&action={action_id}&model="\
                  "{res_model}&view_type={view_type}".format(**action_dict)
        _logger.debug("URL for %s = > %q", self, url_str)
        return url_str

    @api.model
    def generate_message_group(self, prefix=None, provider='aws_sqs', q_type='fifo'):
        """ Generate a random 'message_group' compatible with queue provider and type
        """
        MAX_LEN = 128
        mgid = str(uuid.uuid4())
        if self.provider == 'aws_sqs' and self.q_type in ('fifo'):
            if prefix:
                return "%s-%s" % (prefix[:127-len(mgid)], mgid)
        elif self.provider == 'pgsql':
            if prefix:
                return "%s-%s" % (prefix, mgid)
        return mgid

    def get_message_group(self, prefix=None):
        """ Generate a random 'message_group' compatible with current queue.
        """
        MAX_LEN = 128
        mgid = str(uuid.uuid4())
        if self.provider == 'aws_sqs' and self.q_type in ('fifo'):
            if prefix:
                return "%s-%s" % (prefix[:127-len(mgid)], mgid)
        elif self.provider == 'pgsql':
            if prefix:
                return "%s-%s" % (prefix, mgid)
        return mgid

    @api.model
    def generate_message_id(self):
        """ Generate a message id wich is a uuid """
        msgid = str(uuid.uuid4())
        return msgid

    def btn_send_simple_message(self):
        """ Sends a simple message"""
        self.ensure_one()
        _payload = safe_eval(self.test_payload or '{}')
        result = send_message(
            self.env,
            self.name,
            self.test_message_selector,  # Selector
            _payload,  # payload
            message_group=self.test_message_group or None,
            message_deduplication_id=self.test_message_deduplication_id or None,
            message_name="This is a test Message name",  # message_name
        )
        result_str = json.dumps(result, sort_keys=True, indent=4)
        self.test_result = result_str

    def send_notification(self, message_type, message_title, message, icon=None, message_obj=None):
        """ Send message to all 'channels' (slack, sms) of all queues in recordset

        :param message_type: "danger", "warning", "success" or "info". This defines the overall aspect of the notification.
        :param message_title: The title of the notification
        :param message: The message text. HTML content is supported.
        :pram icon: any of :bear:, success, info, warning, danger"
        """        
        self.send_odoo_notification(
            message_type, 
            message_title, 
            message, 
            icon=icon, 
            #sticky=True,
            message_obj=message_obj, 
            user_obj=message_obj.user_id if message_obj else self.env.user
        )
        self.send_slack_notification(message_type, message_title, message, icon=icon, message_obj=message_obj)

        if message_obj:
            message = "%s<br/>%s" % (message_obj.name, message,)
        self.send_teams_notification(message_type, message_title, message, icon=icon, message_obj=message_obj)

    def aws_sqs__create_queue(self):
        self.ensure_one()
        sqs_resource = boto3.resource(
            'sqs',
            region_name=self.region,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret
        )        
        q_attr_dict = {
            "DelaySeconds": "1",
            "ReceiveMessageWaitTimeSeconds": "20",
        }

        if self.q_type=='fifo':
            q_attr_dict.update({
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true"
            })

        sqs_queue = sqs_resource.create_queue(
            QueueName=self.sqs_name,
            Attributes=q_attr_dict
        )
        self.sqs_queue_url = sqs_queue.url

    def aws_sqs__delete_queue(self):
        self.ensure_one()
        sqs_resource = boto3.resource(
            'sqs',
            region_name=self.region,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.secret
        )
        sqs_queue = sqs_resource.get_queue_by_name(QueueName=self.sqs_name)
        _resp = sqs_queue.delete()   
        self.sqs_queue_url = None
        self.test_result = _resp