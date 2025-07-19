# -*- coding: utf-8 -*-
import pickle
import datetime

import odoo
from odoo import models, fields, api
from odoo.exceptions import UserError
from odoo.tools.translate import _


from .message import IMQ_MESSAGE_STATES
from .message_processor import IMQ_MESSAGE_PROCESSOR_LOG_LEVEL

"""Tracks message processing history"""




class IMQMessageProcessing(models.Model):
    _name = 'imq.message_processing'
    _description = "IMQ - Message Processing"
    _order = "message_id DESC, attempt DESC"

    message_id = fields.Many2one('imq.message', 
                                 _("Message"),
                                 index=True,
                                 ondelete='cascade')
    attempt = fields.Integer(default=1)


    start_time = fields.Datetime(
        help=_("Timestamp processing of message started."),
        readonly=True
    )
    start_time_microseconds = fields.Integer(readonly=True)

    end_time = fields.Datetime(
        help=_("Timestamp processing of message finished."),
        readonly=True)
    end_time_microseconds = fields.Integer(readonly=True)

    result = fields.Text(readonly=True)
    log_ids = fields.One2many('imq.message_processing_log', 'processing_id')
    state = fields.Selection(IMQ_MESSAGE_STATES, default='new')
    
    worker_type = fields.Selection([
        ('cron-workerv2', 'Cron Worker v2'),
        ('sa-workerv3', 'Standalone Worker v3'),
    ], string='Worker Type', default='cron-workerv2', readonly=True,
       help='Type of worker that processed this message')

    # duplicated to ease analysis
    queue_id = fields.Many2one('imq.queue', 
                               _("Queue"), 
                               related='message_id.queue_id',
                               store=True,
                               readonly=True)
    user_id = fields.Many2one('res.users',
                              _("User"),
                              related='message_id.user_id',
                              store=True,
                              readonly=True)
    processor_id = fields.Many2one('imq.message_processor',
                                   _("Processor"),
                                   related='message_id.processor_id',
                                   store=True,
                                   readonly=True)
    # statistics
    processing_time = fields.Float(compute='_calc_processing_time', store=True)

    
    @api.depends('start_time','end_time')
    def _calc_processing_time(self):
        for record in self:
            if record.start_time and record.end_time:
                record.processing_time = (
                    self.end_time - self.start_time
                ).total_seconds()
            else:
                record.processing_time = None

    
    def refresh(self):
        pass


class IMQMessageProcessingLog(models.Model):
    _name = 'imq.message_processing_log'
    _description = "IMQ - Message Processing Log"
    _order = "processing_id, id"

    processing_id = fields.Many2one('imq.message_processing', _("Processing"), ondelete='cascade', index=True)
    message_id = fields.Many2one('imq.message', _("Message"), ondelete='cascade', index=True)
    active_message_id = fields.Many2one('imq.message', _("Active Message"), ondelete='cascade', index=True)

    logger_name = fields.Char()
    log_level = fields.Selection(IMQ_MESSAGE_PROCESSOR_LOG_LEVEL)
    log_message = fields.Text()
