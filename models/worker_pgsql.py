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
UPDATE imq_message 
SET 
    state = 'wip',
    start_time = now()
WHERE id = (
    SELECT id
    FROM imq_message
    WHERE 
            state='pending'
        AND ( planned_time IS NULL OR planned_time > NOW() )
    ORDER BY enqueued_time  
    FOR UPDATE SKIP LOCKED LIMIT 1
)
RETURNING id;"""

PGSQL_GET_MESSAGE_SQL_fifo = """
UPDATE imq_message
SET
    state = 'wip',
    start_time = now()
WHERE id = (
    SELECT id
    FROM imq_message
    WHERE id IN (
        SELECT im1.id
            --, (im1.enqueued_time::varchar || '.' || im1.enqueued_time_microseconds::varchar) AS enqueued_ts
            --, (im2.enqueued_time::varchar || '.' || im2.enqueued_time_microseconds::varchar) AS previous_enqueued_ts
            --, im2.state                                                                      AS previous_state
        FROM imq_message AS im1
            LEFT JOIN imq_message AS im2 ON (
                im1."group" = im2."group"
                AND im2.create_date <= im1.create_date
                AND im2.id < im1.id
            )
        WHERE im1.state = 'pending'
        AND (im2.state IN ('done', 'terminated', 'archived') OR im2.state IS NULL)
    )
    LIMIT 1
    FOR UPDATE SKIP LOCKED
) RETURNING id;
COMMIT;
"""



PGSQL_RESET_MESSAGE_SQL_std = """
UPDATE imq_message
SET state = 'pending', start_time = NULL
WHERE id = %s;
"""


PGSQL_RESET_VISIBLITY_TIMEOUT_SQL = """
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
        if queue_obj.q_type == 'std':
            self.env.cr.execute(PGSQL_GET_MESSAGE_SQL_std)
            _row = self.env.cr.fetchone()
            _msg_id = _row and _row[0] or None
            self.env.cr.commit()

        elif queue_obj.q_type == 'fifo':
            self.env.cr.execute(PGSQL_GET_MESSAGE_SQL_fifo)
            _row = self.env.cr.fetchone()
            _msg_id = _row and _row[0] or None
            self.env.cr.commit()
#            raise Exception("get_message__pgsql() not implemented for Queue type:'%s'" % queue_obj.q_type)
        else:
            raise Exception("Unsupported Queue type:'%s' for get_message__pgsql()" % queue_obj.q_type)

        if _msg_id:
            return self.env['imq.message'].browse(_msg_id)
        return None

    def store_message__pgsql(self, queue_obj, message, start_timestamp=None):
        """ Stores SQS message in `imq.message`
        :returns: 'imq.message' model or None
        """
        # For pgsql message already exists in db
        message_obj = message
        body = jsonpickle.decode(message.raw_message_body)

        # search for processor
        processor_obj = self.env['imq.message_processor'].upsert_processor_from_message(body)
    
        message_values_dict = {
            'state': 'wip',
            'start_time': start_timestamp,
            'start_time_microseconds': start_timestamp and start_timestamp.microsecond,
        }

        context = body.get('context', {})
        # Add only id present
        if '_imq_parent_message_id' in context:
            message_values_dict['parent_message_id'] = context['_imq_parent_message_id']
        if '_imq_target_children_count' in context:
            message_values_dict['target_children_count'] = context['_imq_target_children_count']

        if processor_obj:
            if processor_obj.force_visibility_timeout:
                _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=queue_obj.visibility_timeout)
            else:
                _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=processor_obj.visibility_timeout)

            message_values_dict.update({
                'processor_id': processor_obj.id,
                'logging_activated': processor_obj.logging_activated,
                'capture_console': processor_obj.capture_console,
                'max_number_of_attempts': processor_obj.max_attempt,
                'visibility_time': _visibility_time
            })
        else:
            _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=queue_obj.visibility_timeout)
            message_values_dict.update({
                'processor_id': None,
                'logging_activated': False,
                'capture_console': False,
                'max_number_of_attempts': MAX_ATTEMPTS,
                'visibility_time': _visibility_time
            })

        message_obj.write(message_values_dict)
        return message_obj

    def change_message_visibility__pgsql(self, queue_obj, message_obj, _message):
        """ Update message visibility with timeout defined in processor. """
        if message_obj.processor_id.force_visibility_timeout:
            m_timeout = message_obj.processor_id.visibility_timeout
            _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=m_timeout)
            message_obj.write({
                "visibility_time": _visibility_time
            })

    def terminate_message__pgsql(self, queue_obj, sqs_message):
        """ On pgsql we do nothing for now. Later we may move the message and his
        history on another storage.
        """
        return True

    @api.model
    def process_pgsql_messqges_visibility_timeout_daemon(self):
        """ Reset state of messages that are 'wip' while visibility_time is 
        over to 'pending' 
        """
        self.env.cr.execute(PGSQL_RESET_VISIBLITY_TIMEOUT_SQL)
        _rc = self.env.cr.rowcount
        _logger.info("Visibility Timeout reset on %s messages.", _rc)
        return