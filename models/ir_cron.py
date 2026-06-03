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
    interval_type = fields.Selection(
        selection_add=[('seconds', 'Seconds')],
        ondelete={"seconds": 'cascade'}
    )
    imq_is_worker = fields.Boolean("Is IMQ Worker?")

    @api.onchange('imq_is_worker')
    def onchange__imq_is_worker(self):
        if self.imq_is_worker:
            self.nextcall = '1990-01-01 00:00:00'
            self.interval_type = 'seconds'
            self.interval_number = 1
            self.doall = False
 
    @classmethod
    def _process_job(cls, cron_cr, job):
        """ Run a given job taking care of the repetition.

        :param cron_cr: cursor holding lock on the cron job row, to use to update the next exec date,
            must not be committed/rolled back!
        :param job: job to be run (as a dictionary).
        """
        _logger.debug("Entering imq::ir.cron._process_job()")
        if not job.get('imq_is_worker'):
            super()._process_job(cron_cr, job)
            return
        cls._imq_process_job(cron_cr, job)
        return

    @classmethod
    def _imq_process_job(cls, cron_cr, job):
        """ Run a given job taking care of the repetition.

        :param job_cr: cursor to use to execute the job, safe to commit/rollback
        :param job: job to be run (as a dictionary).
        :param cron_cr: cursor holding lock on the cron job row, to use to update the next exec date,
            must not be committed/rolled back!
        """
        _logger.debug("Entering _imq_process_job()")

        with cls.pool.cursor() as job_cr:
            lastcall = fields.Datetime.to_datetime(job['lastcall'])
            #interval = _intervalTypes[job['interval_type']](job['interval_number'])
            env = api.Environment(job_cr, job['user_id'], {
                'lastcall': job['lastcall'],
                'cron_id': job['id'],
            })
            cron = env[cls._name].browse(job['id'])

            # Use the user's timezone to compare and compute datetimes,
            # otherwise unexpected results may appear. For instance, adding
            # 1 month in UTC to July 1st at midnight in GMT+2 gives July 30
            # instead of August 1st!
            now = fields.Datetime.now()
            cron._callback(job['cron_name'], job['ir_actions_server_id'],)

            # Odoo 18 removed numbercall
            # numbercall = job['numbercall']
            # if numbercall > 0:
            #     numbercall -= 1
            # if not numbercall:
            #     addsql = ', active=False'
            # else:
            #     addsql = ''
            addsql = ''

            cron_cr.execute(
                "UPDATE ir_cron SET lastcall=%s"+addsql+" WHERE id=%s",(
                fields.Datetime.to_string(now.astimezone(pytz.UTC)),
                job['id']
            ))
            cron_cr.commit()
