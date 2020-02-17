# -*- coding: utf-8 -*-
import datetime
import logging
import json
import timeit

import boto3
import jsonpickle

import odoo
from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError
from odoo.tools.translate import _
from .message_processor import IMQ_MESSAGE_PROCESSOR_TYPES, IMQ_MESSAGE_PROCESSOR_LOG_LEVEL, MAX_ATTEMPTS

"""Stores all odoo's processed messages for user inspection."""

_logger = logging.getLogger("IMQ.message")

IMQ_MESSAGE_STATES = [
    ('new', "New"),
    ('wip', "In progress"),
    ('retry', "Retry"),
    ('done', "Done"),
    ('terminated', "Terminated"),  # For message that processing decided to stop
    ('failed', "Failed"),
    ('archived', "Archived"),
    ('reset', "Reset"),
]

IMQ_MESSAGE_TYPES = [
    ('rpc', "RPC"),
    ('simple', "Simple"),
]


class IMQMessage(models.Model):
    _name = 'imq.message'
    _description = "IMQ - Message"
    _order = 'id DESC'

    # fields
    queue_id = fields.Many2one('imq.queue', _("Queue"))
    processor_id = fields.Many2one('imq.message_processor', string="Processor")
    group = fields.Char(_("Group"), index=True, readonly=True)
    name = fields.Char()
    message_type = fields.Selection(IMQ_MESSAGE_PROCESSOR_TYPES, 
                                    related='processor_id.type', 
                                    readonly=True)
    queue_message_id = fields.Char("Queue Message Id",
                                    index=True,
                                    help="id of message on cloud queue.",
                                   readonly=True)
    parent_message_id = fields.Char("Parent Message Id", 
                                    help="id of parent message on cloud queue.",
                                    readonly=True,
                                    index=True)
    target_children_count = fields.Integer("Expected Children Items")
    queue_message_id_history = fields.Text()
    user_id = fields.Many2one('res.users', _("User"),
                              default = lambda o: o.env.user.id,
                              help=_("User owner of the Message. This defines "
                                     "the security restriction of executed "
                                     "processing."))
    code = fields.Char(help=_("Python expression that will be executed to "
                              "launch message processing. This is informational "
                              "only. Use fields in 'Exec. params. tab to "
                              "manually create messages."))
    context = fields.Text(help=_("pickled context dict"))
    payload = fields.Text(help=_("dict {'args': ..., 'kwargs': ...} pickled."))
    raw_message_body = fields.Text()
    processing_id = fields.Many2one('imq.message_processing', 
                                    'message_id',
                                    index=True)
    processing_ids = fields.One2many('imq.message_processing', 'message_id')
    attempt = fields.Integer(default=0)
    attempt_as_text = fields.Char(_("Attempt / max"), 
                                  compute='_calc_attempt_vs_max_as_text')

    enqueued_time = fields.Datetime(help=_("Timestamp when message has been "
                                           "sent to queue."),
                                    readonly=True)

    start_time = fields.Datetime(help=_("Time when processing has started on "
                                        "this message"),
                                 readonly=True)
    start_time_microseconds = fields.Integer()
    end_time = fields.Datetime(help=_("Processing end or failure time."),
                               readonly=True)
    end_time_microseconds = fields.Integer()
    result = fields.Text(readonly=True)
    operator_comment = fields.Text()

    capture_console = fields.Boolean(default=False)
    logging_activated = fields.Boolean(readonly=True, default=False)
    log_ids = fields.One2many('imq.message_processing_log', 'active_message_id')

    state = fields.Selection(IMQ_MESSAGE_STATES, default='new')

    max_number_of_attempts = fields.Integer(default=MAX_ATTEMPTS)
    ikpdb_debug = fields.Boolean("IKPdb debug",
                                 default=False,
                                 help=_("Will open IKPdb in post mortem mode "
                                        "if an exception is raised."))
    _sql_constraints = [
        (
            'remote_id_uniq', 
            "UNIQUE(queue_id,queue_message_id)", 
            "Message ID must be unique per Queue.")
    ]

    # TODO: Move to a mixin and update queue.py which share the same code
    @api.multi
    def get_formview_id(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_message__form_view').id

    @api.multi
    def get_default_action(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_message__act_window')

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
        _logger.debug("get_form_url(%s) => %s", self,  url_str)
        return url_str

    @api.multi
    def set_work_progress(self, current=None, target=None):
        if not current and not target:
            return
        values = {}
        if current:
            pass
        if target:
            values['target_children_count'] = target
        self.write(values)
        self.env.cr.commit()
        return

    @api.multi
    @api.depends('attempt','max_number_of_attempts')
    def _calc_attempt_vs_max_as_text(self):
        for record in self:
            record.attempt_as_text = "%s / %s" % (record.attempt, 
                                                  record.max_number_of_attempts)

    @api.multi
    def refresh(self):
        pass

    @api.multi
    def do_retry_processing(self):
        for record in self:
            if record.state == 'new':
                continue
            sqs_resource = boto3.resource(
                'sqs',
                region_name=record.queue_id.region,
                aws_access_key_id=record.queue_id.key, 
                aws_secret_access_key=record.queue_id.secret,
            )
        
            sqs_queue = sqs_resource.get_queue_by_name(QueueName=record.queue_id.sqs_name)
            message_body_values = {
                'type': 'rpc',
                'logging_activated': record.logging_activated,
                'capture_console': record.capture_console,
                'module_name': record.processor_id.module,
                'function_name': record.processor_id.function,
                'is_method': record.processor_id.is_method,
                'context': json.loads(record.context),
                'payload': json.loads(record.payload),
                'user_id': record.user_id.id,
            }
            send_message_kwargs = {
                'MessageBody': json.dumps(message_body_values),
                # We want SQS to wait 10s before IMQ Workers can read this message.
                # We need this time to commit the message id change.
                'DelaySeconds': 10,  
                'MessageAttributes': {
                    'name': {
                        'DataType': 'String',
                        'StringValue': record.name,
                    },
                    'code': {
                        'DataType': 'String',
                        'StringValue': "%s" % (record.code),
                    }
                }
            }
            if record.group:
                send_message_kwargs['MessageGroupId'] = record.group
            response = sqs_queue.send_message(**send_message_kwargs)
            _logger.debug("response={resp}".format(resp=response))
            queue_message_id_history = record.queue_message_id_history or ''
            queue_message_id_history = "%s %s\n" % (
                datetime.datetime.now(),
                record.queue_message_id,
            ) + queue_message_id_history
            update_dict = {
                'queue_message_id_history': queue_message_id_history,
                'state': 'retry'
            } 
            if response:
                update_dict['queue_message_id'] = response['MessageId']
            record.write(update_dict)
        return        

    @api.multi
    def do_archive(self):
        self.write({'state': 'archived'})

    @api.multi
    def create_processing_object(self):
        self.ensure_one()
        new_processing_obj = self.env['imq.message_processing'].sudo().create({
            'message_id': self.id,
            'attempt': self.attempt,
            'start_time': self.start_time,
            'start_time_microseconds': self.start_time_microseconds,
            'end_time': None,
            'end_time_microseconds': None,
            'result': None,
            'state': self.state,
            'queue_id': self.queue_id.id,
            'user_id': self.user_id.id,
            'processor_id': self.processor_id.id,
        })
        if self.processing_id:
            self.env['imq.message_processing_log'].search([
                ('processing_id','=',self.processing_id.id)
            ]).write({
                'active_message_id': None,
            })
            if self.processing_id.state == 'wip':
                self.processing_id.write({
                    'state': 'reset',
                    'end_time': None,
                })
        self.write({
            'processing_id': new_processing_obj.id,
        })
        return new_processing_obj

    @api.model
    def purge_message_history(self):
        """ Purge messages a batch of x messages older that y hours
        With:
            - x is defined by system parameter: imq.MESSAGE_PURGE_BATCH_SIZE or 2000 by default.
            - y is defined by system parameter: imq.MESSAGE_PURGE_OLDER_THAN_HOURS or 72 by default     
        """
        icp_model = self.env["ir.config_parameter"].sudo()
        STOP_MESSAGES_PURGE = icp_model.get_param("imq.STOP_MESSAGES_PURGE", None)
        if STOP_MESSAGES_PURGE:
            _logger.info("Messages Purge deactivated. System parameter imq.STOP_MESSAGES_PURGE is defined.")
            return
        
        MESSAGE_PURGE_BATCH_SIZE = int(
            icp_model.get_param("imq.MESSAGE_PURGE_BATCH_SIZE", '2000')
        )        
        MESSAGE_PURGE_OLDER_THAN_HOURS = int(
            icp_model.get_param("imq.MESSAGE_PURGE_OLDER_THAN_HOURS", '72')
        )        
        PURGE_QUERY = """
DELETE FROM imq_message WHERE id IN (
    SELECT id
    FROM imq_message AS im
    WHERE im.end_time < (NOW() - INTERVAL '%s hours')
    AND state NOT IN ('failed', 'retry', 'reset', 'wip', 'archived')
    LIMIT %s
);""" % (MESSAGE_PURGE_OLDER_THAN_HOURS, MESSAGE_PURGE_BATCH_SIZE,)

        _logger.info("Starting to delete %s messages older than %s hours",
            MESSAGE_PURGE_BATCH_SIZE, 
            MESSAGE_PURGE_OLDER_THAN_HOURS
        )
        start_ts = timeit.default_timer()
        self.env.cr.execute(PURGE_QUERY)
        self.env.cr.commit()
        end_ts = timeit.default_timer()
        _logger.info("Deleted %s messages older than %s hours in %.3fs.",
            self.env.cr.rowcount, 
            MESSAGE_PURGE_OLDER_THAN_HOURS,
            end_ts-start_ts
        )    