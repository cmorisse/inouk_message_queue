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
    ('pending', "Pending"),
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


PURGE_MESSAGE_HISTORY_SQL = """
    DELETE FROM imq_message 
    WHERE 
        end_time < (NOW() - INTERVAL '%s hours')
    AND state NOT IN ('failed', 'retry', 'reset', 'wip', 'archived');"""

class IMQMessage(models.Model):
    _name = 'imq.message'
    _description = "IMQ - Message"
    _order = 'id DESC'

    # fields
    queue_id = fields.Many2one('imq.queue', "Queue")
    queue_provider = fields.Selection(related='queue_id.provider', readonly=True)
    queue_type = fields.Selection(related='queue_id.q_type', readonly=True)
    processor_id = fields.Many2one('imq.message_processor', string="Processor")
    group = fields.Char("Group", index=True, readonly=True)
    name = fields.Char()
    message_type = fields.Selection(related='processor_id.type', readonly=True)
    queue_message_id = fields.Char("Queue Message Id",
                                   index=True,
                                   help="id of message on cloud queue.",
                                   readonly=True)
    message_deduplication_id = fields.Char(
        "Message Dedup. Id",
        index=True,
        help="Message hash used to uniquely identify message and avoid duplicates.",
        readonly=True
    )
    parent_message_id = fields.Char("Parent Message Id", 
                                    help="id of parent message on cloud queue.",
                                    readonly=True,
                                    index=True)
    target_children_count = fields.Integer("Expected Children Items")
    queue_message_id_history = fields.Text()
    user_id = fields.Many2one('res.users', "User",
                              default = lambda o: o.env.user.id,
                              help="User owner of the Message. This defines "
                                     "the security restriction of executed "
                                     "processing.")
    code = fields.Char(help="Python expression that will be executed to "
                            "launch message processing. This is informational "
                            "only. Use fields in 'Exec. params. tab to "
                            "manually create messages.")
    context = fields.Text(help="pickled context dict")
    payload = fields.Text(help="dict {'args': ..., 'kwargs': ...} pickled.")
    raw_message_body = fields.Text()
    processing_id = fields.Many2one('imq.message_processing', 
                                    "Message Processing",
                                    index=True)
    processing_ids = fields.One2many('imq.message_processing', 'message_id')
    attempt = fields.Integer(default=0)
    attempt_as_text = fields.Char("Attempt / max", 
                                  compute='_calc_attempt_vs_max_as_text')

    planned_time = fields.Datetime(
        help="When should this message be processed."
    )
    
    enqueued_time = fields.Datetime(
        help="Timestamp when message has been sent to queue.",
        readonly=True
    )
    enqueued_time_microseconds = fields.Integer()

    start_time = fields.Datetime(
        help="Time when processing has started on this message",
        readonly=True
    )
    start_time_microseconds = fields.Integer()
    end_time = fields.Datetime(
        help="Processing end or failure time.",
        readonly=True
    )
    end_time_microseconds = fields.Integer()
    result = fields.Text(readonly=True)
    operator_comment = fields.Text()

    capture_console = fields.Boolean(default=False)
    logging_activated = fields.Boolean(readonly=True, default=False)
    log_ids = fields.One2many('imq.message_processing_log', 'active_message_id')

    visibility_time = fields.Datetime(
        help="When this message will become visible again."
    )

    state = fields.Selection(IMQ_MESSAGE_STATES, default='new')

    max_number_of_attempts = fields.Integer(default=MAX_ATTEMPTS)
    ikpdb_debug = fields.Boolean(
        string="IKPdb debug",
        default=False,
        help="Will open IKPdb in post mortem mode if an exception is raised."
    )
    _sql_constraints = [
        (
            'remote_id_uniq', 
            "UNIQUE(queue_id,queue_message_id)", 
            "Message ID must be unique per Queue.")
    ]

    def get_formview_id(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_message__formview').id

    def get_default_action(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_message__act_window')

    def get_form_url(self):
        self.ensure_one()
        web_base_url = self.env['ir.config_parameter'].sudo().get_param('web.base.url')
        action_id = self.get_default_action().id
        # target:
        # https://xsid-dev.inouk.ovh/odoo#id=1&action=257&model=imq.message&view_type=form
        url_str = "{web_base_url}/odoo#id={res_id}&action={action_id}&model={res_model}&view_type=form".format(
            web_base_url=web_base_url,
            res_id=self.id,
            action_id=action_id,
            res_model=self._name
        )
        _logger.debug("get_form_url(%s) => %s", self,  url_str)
        return url_str

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
    
    @api.depends('attempt','max_number_of_attempts')
    def _calc_attempt_vs_max_as_text(self):
        for record in self:
            record.attempt_as_text = "%s / %s" % (record.attempt, 
                                                  record.max_number_of_attempts)
    
    def refresh(self):
        pass
    
    def btn_retry_processing(self):
        self.ensure_one()
        self.do_retry_processing()

    def do_retry_processing(self):
        """Interactive method which call Q specific method to retry processing 
        of a message record set.
        """
        _method_name = "do_retry_processing__%s" % self.queue_id.provider
        _method = getattr(self, _method_name)
        _method()

    def do_archive(self):
        self.write({'state': 'archived'})
    
    def create_processing_object(self, worker_type='cron-workerv2'):
        self.ensure_one()
        new_processing_obj = self.env['imq.message_processing'].sudo().create({
            'message_id': self.id,
            'attempt': self.attempt,
            'start_time': self.start_time,
            'start_time_microseconds': self.start_time_microseconds,
            'end_time': None,
            'end_time_microseconds': None,
            'worker_type': worker_type,
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
    def purge_messages_history(self):
        """ Purge imq.messages based on the the system parameter 'imq.messages_retention_period_in_hours'
        When imq.messages_retention_period_in_hours is undefined, a default value of 720 hours (30 days) 
        is used.
        Purge is deactivated if system parameter imq.STOP_MESSAGES_PURGE exists.
        """
        icp_model = self.env["ir.config_parameter"].sudo()
        STOP_MESSAGES_PURGE = icp_model.get_param("imq.STOP_MESSAGES_PURGE", None)
        if STOP_MESSAGES_PURGE:
            _logger.info("Messages Purge deactivated. System parameter imq.STOP_MESSAGES_PURGE is defined.")
            return
        
        MESSAGES_RETENTION_PERIOD_IN_HOURS = int(
            icp_model.get_param("imq.messages_retention_period_in_hours", '720')
        )        

        _logger.info("Starting to delete messages older than %s hours",
                     MESSAGES_RETENTION_PERIOD_IN_HOURS)

        start_ts = timeit.default_timer()
        self.env.cr.execute(PURGE_MESSAGE_HISTORY_SQL, 
                            (MESSAGES_RETENTION_PERIOD_IN_HOURS,))
        self.env.cr.commit()
        end_ts = timeit.default_timer()
        _logger.info("Deleted %s messages older than %s hours in %.3fs.",
            self.env.cr.rowcount, 
            MESSAGES_RETENTION_PERIOD_IN_HOURS,
            end_ts-start_ts
        )    