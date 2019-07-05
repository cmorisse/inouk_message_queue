# -*- coding: utf-8 -*-
import datetime
import logging
import json

import boto3
import jsonpickle

import odoo
from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError
from odoo.tools.translate import _
from .message_processor import IMQ_MESSAGE_PROCESSOR_TYPES, IMQ_MESSAGE_PROCESSOR_LOG_LEVEL

"""Stores all odoo's processed messages for user inspection."""

_logger = logging.getLogger("IMQ")

MAX_ATTEMPTS = 3  # TODO make this a system parameter or a configuration


IMQ_MESSAGE_STATES = [
    ('new', "New"),
    ('wip', "In progress"),
    ('done', "Done"),
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
    queue_message_id = fields.Char("Queue Message id",
                                    help="id of message on cloud queue.",
                                   readonly=True)
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
    processing_id = fields.Many2one('imq.message_processing', 'message_id')
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
        self.ensure_one()
        sqs_resource = boto3.resource(
            'sqs',
            region_name=self.queue_id.region,
            aws_access_key_id=self.queue_id.key, 
            aws_secret_access_key=self.queue_id.secret,
        )
    
        sqs_queue = sqs_resource.get_queue_by_name(QueueName=self.queue_id.sqs_name)
        message_body_values = {
            'type': 'rpc',
            'logging_activated': self.logging_activated,
            'capture_console': self.capture_console,
            'module_name': self.processor_id.module,
            'function_name': self.processor_id.function,
            'is_method': self.processor_id.is_method,
            'context': json.loads(self.context),
            'payload': json.loads(self.payload),
            'user_id': self.user_id.id,
        }
        send_message_kwargs = {
            'MessageBody': json.dumps(message_body_values),
            # We want SQS to wait 10s before IMQ Workers can read this message.
            # We need this time to commit the message id change.
            'DelaySeconds': 10,  
            'MessageAttributes': {
                'name': {
                    'DataType': 'String',
                    'StringValue': self.name,
                },
                'code': {
                    'DataType': 'String',
                    'StringValue': "%s" % (self.code),
                }
            }
        }
        if self.group:
            send_message_kwargs['MessageGroupId'] = self.group
        response = sqs_queue.send_message(**send_message_kwargs)
        _logger.debug("response={resp}".format(resp=response))
        queue_message_id_history = self.queue_message_id_history or ''
        queue_message_id_history = "%s %s\n" % (
            datetime.datetime.now(),
            self.queue_message_id,
        ) + queue_message_id_history
        self.queue_message_id_history = queue_message_id_history
        if response:
            self.queue_message_id = response['MessageId']
        return        

    @api.multi
    def do_archive(self):
        self.state = 'archived'

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
