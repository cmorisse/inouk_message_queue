import os
import multiprocessing
import threading
import sys
import traceback
from io import StringIO
import importlib
import logging
import json
import jsonpickle
import threading
import datetime
import math
import socket

from dateutil.relativedelta import relativedelta
import psycopg2
import time
import boto3

import odoo
from odoo import _, api, fields, models
from odoo.api import Environment
from odoo.exceptions import MissingError, UserError
#from odoo.addons.inouk_message_queue.api import unwrap_odoo_model
from ..api import unwrap_odoo_model, IMQError, IMQRetryableError, IMQTerminateException
from .message_processor import MAX_ATTEMPTS


TLS = threading.local()


_logger = logging.getLogger('IMQWorker')


PGSQL_GET_MESSAGE_SQL_std = """
UPDATE imq_messqge 
SET 
    state = 'wip',
    start_time = now()
WHERE id = (
    SELECT id
    FROM imq_message
    WHERE 
            state='pending'
        AND (planned_time IS NOT NULL OR planned_time > NOW())
    ORDER BY enqueued_time  
    FOR UPDATE SKIP LOCKED LIMIT 1;
)
    next_delivery <= %s
"""

PGSQL_RESET_VISIBLITY_TIMEOUT = """
UPDATE imq_message
SET state='pending', visibility_time=NULL
WHERE
    state='wip'
AND visibility_time <= NOW();
"""


class IMQWorkerSQS(models.AbstractModel):
    """Processes messages in imq.queue"""
    _inherit = 'imq.worker'

    def get_message__pgsql(self, queue_obj, wait_time=0):
        """Query Q for next message to process.

        :param queue_obj: required imq.queue object to query.
        :param wait_time: number of seconds to block on queue. Must be 0 with ir.cron IMQ Worker
        :return: a SQS message object or None
        """
        return None

    def change_message_visibility__pgsql(self, message_obj, _message):
        """ Update message visibility with timeout defined in processor. """
        m_timeout = message_obj.processor_id.visibility_timeout
        _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=m_timeout)
        message_obj.write({
            "visibility_time": _visibility_time
        })

