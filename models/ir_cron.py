import logging
import datetime
import pytz

import odoo
from odoo import api, fields, models, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

from odoo.addons.base.models.ir_cron import _intervalTypes

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
        _logger.critical("xxxxxxxxxxxxxxxxxxxxxxxxxxxx yeaaaaaaaaaaaaaaaaaaaaahhhh")
        if not job.get('imq_is_worker'):
            super()._process_job(job_cr, job, cron_cr)
            return
        cls._imq_process_job(job_cr, job, cron_cr)
        return

    @classmethod
    def _imq_process_job(cls, job_cr, job, cron_cr):
        """ Run a given job taking care of the repetition.

        :param job_cr: cursor to use to execute the job, safe to commit/rollback
        :param job: job to be run (as a dictionary).
        :param cron_cr: cursor holding lock on the cron job row, to use to update the next exec date,
            must not be committed/rolled back!
        """
        _logger.critical("Entering _imq_process_job()")
        with api.Environment.manage():
            try:
                cron = api.Environment(
                    job_cr, 
                    job['user_id'], 
                    {
                        'lastcall': fields.Datetime.from_string(job['lastcall'])
                    }
                )[cls._name]
                
                now = fields.Datetime.context_timestamp(cron, datetime.datetime.now())
                numbercall = job['numbercall']

                cron._callback(job['cron_name'], job['ir_actions_server_id'], job['id'])
                if numbercall > 0:
                    numbercall -= 1
                if not numbercall:
                    addsql = ', active=False'
                else:
                    addsql = ''

                cron_cr.execute(
                    "UPDATE ir_cron SET numbercall=%s, lastcall=%s"+addsql+" WHERE id=%s",(
                    numbercall,
                    fields.Datetime.to_string(now.astimezone(pytz.UTC)),
                    job['id']
                ))
                cron.flush()
                cron.invalidate_cache()

            finally:
                job_cr.commit()
                cron_cr.commit()
