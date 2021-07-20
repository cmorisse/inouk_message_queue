import os
import sys
import logging
import pathlib

from dateutil.relativedelta import relativedelta

from odoo.service import server
from odoo.addons.base.models import ir_cron
"""
Patch ir_cron and server.py to allow sub minute intervals.
"""

_logger = logging.getLogger('ir_cron_patcher')

def patch_ir_cron():
    ir_cron._intervalTypes['seconds'] = lambda interval: relativedelta(seconds=interval)
    _logger.warning("Added 'seconds' to ir_cron.py::_intervalTypes")

    server.SLEEP_INTERVAL = 10
    _logger.warning("server.py::SLEEP_INTERVAL=%s", server.SLEEP_INTERVAL)
