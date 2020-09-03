import logging

import odoo
from odoo import api, fields, models, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class ir_cron(models.Model):
    """ Patch to allow sub minute CRONs
    """
    _inherit = "ir.cron"
    interval_type = fields.Selection(selection_add=[('seconds', 'Seconds')])
 