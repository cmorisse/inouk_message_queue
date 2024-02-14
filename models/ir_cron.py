import logging
import datetime
import pytz

import odoo
from odoo import api, fields, models, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

from odoo.addons.base.ir.ir_cron import _intervalTypes

class ir_cron(models.Model):
    """ Patch to allow sub minute CRONs
    """
    _inherit = "ir.cron"
    interval_type = fields.Selection(selection_add=[('seconds', 'Seconds')])
    imq_is_worker = fields.Boolean("Is IMQ Worker?")

    @api.onchange('imq_is_worker')
    def onchange__imq_is_worker(self):
        if self.imq_is_worker:
            self.nextcall = '1990-01-01 00:00:00'
            self.interval_type = 'seconds'
            self.interval_number = 1
            self.doall = False
 
    @classmethod
    def _process_job(cls, job_cr, job, cron_cr):
        """ Run a given job taking care of the repetition.

        :param job_cr: cursor to use to execute the job, safe to commit/rollback
        :param job: job to be run (as a dictionary).
        :param cron_cr: cursor holding lock on the cron job row, to use to update the next exec date,
            must not be committed/rolled back!
        """
        _logger.debug("Entering imq::ir.cron._process_job()")
        if not job.get('imq_is_worker'):
            super(ir_cron, cls)._process_job(job_cr, job, cron_cr)
            return
        cls._imq_process_job(job_cr, job, cron_cr)
        return

    # @api.model
    # def _callback(self, cron_name, server_action_id, job_id):
    #     """ Overload to pass cron_id """
    #     self = self.with_context(cron_id=job_id)
    #     return super()._callback(cron_name, server_action_id, job_id)

    @api.model
    def _callback(self, model_name, method_name, args, job_id):
        """ Overload to pass cron_id """
        self = self.with_context(cron_id=job_id)
        return super(ir_cron, self)._callback(model_name, method_name, args, job_id)

    @classmethod
    def _imq_process_job(cls, job_cr, job, cron_cr):
        """ Run a given job taking care of the repetition.

        :param job_cr: cursor to use to execute the job, safe to commit/rollback
        :param job: job to be run (as a dictionary).
        :param cron_cr: cursor holding lock on the cron job row, to use to update the next exec date,
            must not be committed/rolled back!
        """
        _logger.debug("Entering _imq_process_job()")
        with api.Environment.manage():
            try:
                cron = api.Environment(job_cr, job['user_id'], {})[cls._name]
                # Use the user's timezone to compare and compute datetimes,
                # otherwise unexpected results may appear. For instance, adding
                # 1 month in UTC to July 1st at midnight in GMT+2 gives July 30
                # instead of August 1st!
                now = fields.Datetime.context_timestamp(cron, datetime.datetime.now())
                nextcall = fields.Datetime.context_timestamp(cron, fields.Datetime.from_string(job['nextcall']))
                numbercall = job['numbercall']

                ok = False
                while nextcall < now and numbercall:
                    if numbercall > 0:
                        numbercall -= 1
                    if not ok or job['doall']:
                        cron._callback(job['model'], job['function'], job['args'], job['id'])
                    if numbercall:
                        nextcall += _intervalTypes[job['interval_type']](job['interval_number'])
                    ok = True
                addsql = ''
                if not numbercall:
                    addsql = ', active=False'
                cron_cr.execute("UPDATE ir_cron SET nextcall=%s, numbercall=%s"+addsql+" WHERE id=%s",
                                (fields.Datetime.to_string(nextcall.astimezone(pytz.UTC)), numbercall, job['id']))
                cron.invalidate_cache()

            finally:
                job_cr.commit()
                cron_cr.commit()
