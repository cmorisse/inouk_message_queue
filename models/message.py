# -*- coding: utf-8 -*-
import datetime
import logging

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

    logging_activated = fields.Boolean(readonly=True, default=False)
    log_ids = fields.One2many('imq.message_processing_log', 'active_message_id')

    state = fields.Selection(IMQ_MESSAGE_STATES, default='new')

    max_number_of_attempts = fields.Integer(default=MAX_ATTEMPTS)
    ikpdb_debug = fields.Boolean("IKPdb debug",
                                 default=False,
                                 help=_("Will open IKPdb in post mortem mode "
                                        "if an exception is raised."))
    @api.multi
    @api.depends('attempt','max_number_of_attempts')
    def _calc_attempt_vs_max_as_text(self):
        for record in self:
            self.attempt_as_text = "%s / %s" % (self.attempt, 
                                                self.max_number_of_attempts)

    @api.multi
    def refresh(self):
        pass


    @api.multi
    def do_retry_processing(self):
        self.ensure_one()
        raise UserError("Not implemented.")

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
