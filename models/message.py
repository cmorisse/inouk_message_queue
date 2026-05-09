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
    ('cancelled', "Cancelled"),
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
    requesting_user_id = fields.Many2one(
        'res.users',
        string="Requesting User",
        index=True,
        help="User who requested this operation. May differ from user_id when "
             "operations are executed by a system user on behalf of another user."
    )
    company_id = fields.Many2one(
        'res.company',
        string="Company",
        readonly=True,
        index=True,
        help="Tenant company at message submission time. Frozen at create from "
             "requesting_user_id.company_id. Consumers may use this field for "
             "tenant-scoped record rules; IMQ itself enforces no ACL on it."
    )
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

    @api.model_create_multi
    def create(self, vals_list):
        # company_id is always derived from requesting_user_id, never declared
        # by the caller. Letting a caller supply company_id would open a
        # cross-tenant injection vector (see muppy_manganese threat model V9).
        # Sudo the User lookup so callers without res.users read access
        # (portal users, minimal-permission contexts) can still create messages.
        User = self.env['res.users'].sudo()
        for vals in vals_list:
            if vals.pop('company_id', None) is not None:
                _logger.warning(
                    "imq.message.create(): caller-supplied company_id ignored "
                    "— tenancy is always derived from requesting_user_id."
                )
            ruid = vals.get('requesting_user_id')
            vals['company_id'] = User.browse(ruid).company_id.id if ruid else False
        return super().create(vals_list)

    def get_formview_id(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_message__formview').id

    def get_default_action(self, access_uid=None):
        self.ensure_one()
        return self.env.ref('inouk_message_queue.imq_message__act_window')

    def get_form_url(self):
        """Return URL to open this record in backend form view (Odoo 18 format)."""
        self.ensure_one()
        base_url = self.get_base_url()
        action = self.get_default_action()
        url_str = f"{base_url}/odoo/action-{action.id}/{self.id}"
        _logger.debug("get_form_url(%s) => %s", self, url_str)
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
    
    def btn_refresh(self):
        """Button action. GUI feedback only — calls refresh()."""
        return self.refresh()

    def refresh(self):
        pass

    @api.model
    def get_task_status(self, message_ids):
        """Get task status with elapsed time and next-step hints.

        When a message has child tasks (linked via parent_message_id),
        the response includes children_summary and children fields.

        Args:
            message_ids: list of imq.message IDs

        Returns:
            list of dicts with status info per task
        """
        messages = self.browse(message_ids).exists()
        now = fields.Datetime.now()
        result = []
        for msg in messages:
            elapsed = None
            if msg.start_time:
                end = msg.end_time or now
                elapsed = round((end - msg.start_time).total_seconds())

            hints = {
                'pending': "Task queued. If still pending after 2min, IMQ worker may not be running.",
                'wip': "Executing. Poll again in 30 seconds.",
                'done': "Completed. Read the target record for results.",
                'failed': (
                    "Failed. Read imq.message_processing_log (filter message_id=<id>) "
                    "for details. If this task is in a FIFO queue group, subsequent "
                    "pending tasks in the same group are blocked until this one is "
                    "archived — call archive() once you have captured the diagnostics."
                ),
                'terminated': "Manually terminated.",
                'retry': "Will be retried automatically.",
                'cancelled': "Cancelled by user.",
            }
            entry = {
                'id': msg.id,
                'name': msg.name or '',
                'state': msg.state,
                'elapsed_seconds': elapsed,
                'hint': hints.get(msg.state, f"State: {msg.state}"),
            }

            # Enrich with children status if this message has child tasks
            if msg.queue_message_id:
                children_objs = self.search([
                    ('parent_message_id', '=', msg.queue_message_id)
                ])
                if children_objs:
                    # Build per-state counters
                    summary = {}
                    children_list = []
                    for child in children_objs:
                        state = child.state
                        summary[state] = summary.get(state, 0) + 1
                        child_elapsed = None
                        if child.start_time:
                            child_end = child.end_time or now
                            child_elapsed = round((child_end - child.start_time).total_seconds())
                        children_list.append({
                            'id': child.id,
                            'name': child.name or '',
                            'state': state,
                            'elapsed_seconds': child_elapsed,
                        })
                    summary['total'] = len(children_objs)
                    entry['children_summary'] = summary
                    entry['children'] = children_list

            result.append(entry)
        return result

    def btn_retry_processing(self):
        """Button action. GUI wrapper — calls retry_processing()."""
        self.ensure_one()
        self.retry_processing()

    def btn_retry_recovery(self):
        """Button action for recovery retry on a 'wip' message.

        Reserved for support staff: a worker restart left the message stuck in
        'wip'. Re-injecting it before the visibility timeout expires avoids
        AWS-style auto-redelivery side effects. Visible only in developer mode.
        """
        self.ensure_one()
        self.retry_processing(force_wip=True)

    def retry_processing(self, force_wip=False):
        """Re-inject a message in the retry pipeline.

        Dispatches to the queue provider's retry_processing__{provider}
        implementation (pgsql, aws_sqs, ...). Each provider applies its own
        constraints (e.g., aws_sqs skips FIFO and non-RPC messages).

        Refuses 'wip' unless force_wip=True (recovery path used by
        btn_retry_recovery, gated to developer mode).
        """
        if not force_wip and any(rec.state == 'wip' for rec in self):
            raise UserError(_(
                "Retry on 'wip' state is a recovery action. "
                "Use the 'Retry (recovery)' button (developer mode required)."
            ))
        _method_name = "retry_processing__%s" % self.queue_id.provider
        _method = getattr(self, _method_name)
        _method()

    def btn_archive(self):
        """Button action. GUI wrapper — calls archive()."""
        return self.archive()

    def archive(self):
        # Primary use: unblock a FIFO group stuck on a failed task.
        # Reject in-flight states (new, wip, retry, reset) to avoid racing the worker.
        ARCHIVABLE_STATES = ('pending', 'failed', 'terminated', 'done', 'cancelled')
        forbidden = self.filtered(lambda m: m.state not in ARCHIVABLE_STATES)
        if forbidden:
            raise UserError(_(
                "Only messages in %s state can be archived. Found: %s"
            ) % (', '.join(ARCHIVABLE_STATES), ', '.join(set(forbidden.mapped('state')))))
        self.write({'state': 'archived'})

    def btn_cancel(self):
        """Button action to cancel message(s). Calls cancel()."""
        return self.cancel()

    def cancel(self):
        """Cancel pending/retry PGSQL messages. Sets state to 'cancelled' and end_time."""
        non_pgsql = self.filtered(lambda m: m.queue_provider != 'pgsql')
        if non_pgsql:
            raise UserError(_("Cancel is only supported for PostgreSQL queue messages."))
        forbidden = self.filtered(lambda m: m.state not in ('pending', 'retry'))
        if forbidden:
            raise UserError(_(
                "Only messages in 'pending' or 'retry' state can be cancelled. "
                "Found: %s") % ', '.join(set(forbidden.mapped('state'))))
        self.write({
            'state': 'cancelled',
            'end_time': fields.Datetime.now(),
        })
    
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