import pickle
import datetime
import json
import logging
import base64
import uuid

import requests
import slackdown

from odoo import models, fields, api
import odoo
from odoo.exceptions import except_orm
from odoo.tools.translate import _

import boto3

from ..api import send_message

_logger = logging.getLogger('IMQ.queue')

QUEUE_PROVIDERS = [
    ('aws_sqs', "AWS SQS"),
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
    name = fields.Char(size=20, index=True, required=True)
    sqs_name = fields.Char(compute='_compute_sqs_name', store=True)
    provider = fields.Selection(QUEUE_PROVIDERS, required=True)
    q_type = fields.Selection(QUEUE_TYPES, string="Queue Type", default='std', required=True)
    active = fields.Boolean(
        default=True,
        help="Inactive queues are not processed by workers."
    )
    region = fields.Char()
    key = fields.Char()
    secret = fields.Char()
    description = fields.Text()
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

    slack_team = fields.Char()
    slack_access_token = fields.Char()
    slack_webhook_channel = fields.Char()
    slack_webhook_url = fields.Char("Slack webhook URL")
    slack_webhook_config_url = fields.Char()
    slack_oauth_access_response = fields.Text()

    use_odoo_notifications = fields.Boolean()

    
    @api.depends('name', 'q_type')
    def _compute_sqs_name(self):
        for record in self:
            record.sqs_name = "{}_{}{}".format(
                record.name,
                record.env.cr.dbname,
                ".fifo" if record.q_type=='fifo' else ''
            )

    
    def copy(self, default=None):
        self.ensure_one()
        chosen_name = default.get('name') if default else ''
        new_name = chosen_name or _('%s (copy)') % self.name
        default = dict(default or {}, name=new_name)
        return super(IMQQueue, self).copy(default)    

    # TODO: Move to a mixin and update message.py which share the same code
    
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
        #if self.provider in ('aws_sqs') and self.q_type in ('fifo'):
        # 128 chars max for AWS SQS Fifo Queue
        mgid = str(uuid.uuid4())
        if prefix:
            return "%s-%s" % (prefix[:127-len(mgid)], mgid)
        return mgid

    
    def btn_send_simple_message(self):
        """ Sends a simple message"""
        self.ensure_one()
        result = send_message(
            self.env,
            self.name,
            self.test_message_selector,  # Selector
            None,  # payload
            message_group=self.test_message_group or None,
            message_deduplication_id=self.test_message_deduplication_id or None,
            message_name="This is a test Message name",  # message_name
        )
        result_str = json.dumps(result, sort_keys=True, indent=4)
        self.test_result = result_str

    
    def btn_add_to_slack(self):
        """ Launch Slack oauth."""
        self.ensure_one()
        icp_model = self.env['ir.config_parameter'] 
        base_url = icp_model.sudo().get_param('web.base.url')
        SLACK_APP_CLIENT_ID = icp_model.get_param('imq.SLACK_APP_CLIENT_ID')
        SLACK_OAUTH_CALLBACK = icp_model.get_param('imq.SLACK_OAUTH_CALLBACK')  # Cloudflare worker

        imq_slack_oauth_ctrl = "%s/imq/v1/socb" % base_url  # socb = Slack Oauth Call-Back
        state_param_b = bytearray("%s/%s" % (imq_slack_oauth_ctrl, self.name), 'utf-8')
        state_param_b = base64.urlsafe_b64encode(state_param_b)
        state_param_str = state_param_b.decode('utf-8')
        
        # eg. https://slack.com/oauth/v2/authorize?client_id=5006003237.928870046194&scope=incoming-webhook
        slack_auth_uri = "https://slack.com/oauth/v2/authorize?client_id=%s&scope=incoming-webhook"\
                         "&redirect_uri=%s&state=%s" % (
                             SLACK_APP_CLIENT_ID, 
                             SLACK_OAUTH_CALLBACK, 
                             state_param_str,)

        _logger.debug("slack_auth_uri=%s", slack_auth_uri)
        return {
            "type": "ir.actions.act_url",
            "url": slack_auth_uri,
            "target": "self",
        }

    
    def btn_test_slack_notifications(self):
        """ Sends a Slack test notifications."""
        self.ensure_one()
        self.send_slack_notification(":bear:")
        message = "Queue: *%s* is ready to send notifications." % self.name
        self.send_slack_notification(message)
        return
    
    
    def btn_test_odoo_notifications(self):
        """ Sends an Odoo test notifications."""
        self.ensure_one()
        self.send_odoo_notification(":bear:")
        message = "Queue: *%s* is ready to send notifications." % self.name
        return self.send_odoo_notification(message)


    def render_slack_to_fontawesome_part1(self, message):
        r_message = message.replace(':white_check_mark:', 'XXXWHITECHECKMARKXXX')
        r_message = r_message.replace(':bear:', 'XXXBEARXXX')
        r_message = r_message.replace(':bangbang:', 'XXXBANGBANGXXX')
        r_message = r_message.replace(':warning:', 'XXXWARNINGXXX')
        r_message = r_message.replace(':x:', 'XXXXXXX')
        return r_message

    def render_slack_to_fontawesome_part2(self, message):
        r_message = message.replace('XXXWHITECHECKMARKXXX', '<i class="fa fa-check-square"></i>')
        r_message = r_message.replace('XXXBEARXXX', '<i class="fa fa-paw"></i>')
        r_message = r_message.replace('XXXBANGBANGXXX', '<i class="fa fa-exclamation"></i>')
        r_message = r_message.replace('XXXWARNINGXXX', '<i class="fa fa-exclamation-triangle"></i>')
        r_message = r_message.replace('XXXXXXX', '<i class="fa fa-times-circle"></i>')
        return r_message

    def send_odoo_notification(self, message=None, raw=None, obj=None):
        """ Send message to Odoo #IMQ channels of all queues in record set. 
        :param message: when formatted, must use slack markdown
        """
        icp_model = self.env['ir.config_parameter'] 
        imqbot_partner_obj = self.env.ref('inouk_message_queue.partner_imq')
        for record in self:
            if record.use_odoo_notifications:
                if obj:
                    notification_text = message.replace(
                        '{object_link}', 
                        "*<%s|%s>*" % (obj.get_form_url(), obj.name,)
                    )
                    payload = notification_text
                else:
                    payload = message
                if message:
                    body_html = self.render_slack_to_fontawesome_part1(payload)
                    body_html = slackdown.render(body_html)
                    body_html = self.render_slack_to_fontawesome_part2(body_html)
                elif raw:
                    body_html = raw
                channel_obj = self.env.ref('inouk_message_queue.imq_mail_channel')
                channel_obj.message_post(body=body_html, 
                                         author_id=imqbot_partner_obj.id, 
                                         subtype='mail.mt_comment')

    
    def send_slack_notification(self, message, obj=None):
        """ Send message to slack channels of all queues in record set """
        for record in self:
            if record.slack_webhook_url:
                if obj:
                    notification_text = message.replace(
                        '{object_link}', 
                        "*<%s|%s>*" % (obj.get_form_url(), obj.name,)
                    )
                    payload = { "text": notification_text }
                else:
                    payload = { "text": message }
                    
                headers = {'Content-type': 'application/json'}
                result = requests.post(record.slack_webhook_url, 
                                       data=json.dumps(payload), 
                                       headers=headers)
                _logger.debug("requests.post(%s, data=%s, headers=%s) => %s", 
                    record.slack_webhook_url, 
                    json.dumps(payload), 
                    headers, 
                    result
                )
                _logger.info("result.text => %s", result.text)

    
    def send_notification(self, message, obj=None):
        """ Send message to all 'channels' (slack, sms) of all queues in recordset """
        self.send_slack_notification(message, obj=obj)
        self.send_odoo_notification(message, obj=obj)
    
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

        q_obj = sqs_resource.create_queue(
            QueueName=self.sqs_name,
            Attributes=q_attr_dict
        )
        self.test_result = q_obj