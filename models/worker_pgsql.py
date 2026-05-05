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
import psycopg2.errors
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
            queue_id = %s
        AND state in ('pending', 'retry')
        AND ( planned_time IS NULL OR planned_time < NOW() )
        AND (%s IS NULL OR id = %s OR queue_message_id = %s)
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
    WITH sq1 AS (
        SELECT
        im.id,
        im.name,
        im.state,
        planned_time,
        -- Previous row
        LAG(im.state) OVER ( PARTITION BY im."group" ORDER BY im.id ) AS prev_state,
        LAG(im.id) OVER ( PARTITION BY im."group" ORDER BY im.id ) AS prev_id
        FROM imq_message AS im
        WHERE im.queue_id = %s
            AND (%s IS NULL OR im.id = %s OR im.queue_message_id = %s)
        GROUP BY im."group", im.id, im.name, im.state
    )
    SELECT id FROM sq1
    WHERE sq1.state in ('pending', 'retry') AND ( planned_time IS NULL OR planned_time < NOW() ) AND (sq1.prev_state IN ('done', 'terminated', 'archived') OR sq1.prev_state IS NULL )
    FOR UPDATE SKIP LOCKED
    LIMIT 1
) RETURNING id;
"""

PGSQL_RESET_MESSAGE_SQL_std = """
UPDATE imq_message
SET state = 'pending', start_time = NULL
WHERE id = %s;
"""


PGSQL_RESET_VISIBLITY_TIMEOUT_SQL = """
UPDATE imq_message
SET state=CASE WHEN attempt < max_number_of_attempts THEN 'pending' ELSE 'failed' END,
    visibility_time=NULL
WHERE
    state='wip'
AND visibility_time <= NOW() AT TIME ZONE 'utc';
"""


class IMQWorkerSQS(models.AbstractModel):
    """Processes messages in imq.queue"""
    _inherit = 'imq.worker'

    def get_message__pgsql(self, queue_obj, wait_time=0, target_message_id=None):
        """Query Q for next message to process.

        :param queue_obj: required imq.queue object to query.
        :param wait_time: number of seconds to block on queue. Must be 0 with ir.cron IMQ Worker
        :param target_message_id: optional specific message ID to target
        :return: a SQS message object or None
        """
        # Convert target_message_id for SQL parameters
        msg_id_param = None
        if target_message_id:
            msg_id_param = int(target_message_id) if target_message_id.isdigit() else None
        
        # Debug logging for message targeting
        if target_message_id:
            _logger.debug("Target message filtering: target_message_id=%s, msg_id_param=%s", 
                         target_message_id, msg_id_param)
        
        _db_cnx = odoo.sql_db.db_connect(self.env.cr.dbname)
        with _db_cnx.cursor() as cr:
            try:
                if queue_obj.q_type == 'std':
                    sql_params = (
                        queue_obj.id,                    # queue_id
                        target_message_id,               # message filter check (NULL or value)
                        msg_id_param,                    # numeric ID match
                        target_message_id                # MessageId UUID match
                    )
                    _logger.debug("Executing SQL query with params: %s", sql_params)

                    cr.execute(PGSQL_GET_MESSAGE_SQL_std, sql_params)
                    _row = cr.fetchone()
                    cr.commit()
                    _msg_id = _row and _row[0] or None

                    _logger.debug("SQL result: _row=%s, _msg_id=%s", _row, _msg_id)

                elif queue_obj.q_type == 'fifo':
                    sql_params = (
                        queue_obj.id,                    # queue_id
                        target_message_id,               # message filter check (NULL or value)
                        msg_id_param,                    # numeric ID match
                        target_message_id                # MessageId UUID match
                    )
                    _logger.debug("Polling FIFO queue '%s' for message: %s with params: %s",
                                 queue_obj.id, target_message_id, sql_params)

                    cr.execute(PGSQL_GET_MESSAGE_SQL_fifo, sql_params)
                    _row = cr.fetchone()
                    cr.commit()
                    _msg_id = _row and _row[0] or None

                    _logger.debug("FIFO result: _row=%s, _msg_id=%s", _row, _msg_id)
                else:
                    raise Exception("Unsupported Queue type:'%s' for get_message__pgsql()" % queue_obj.q_type)

            except psycopg2.errors.SerializationFailure:
                _logger.info("Another worker was speedier to catch the next message from queue %s, will try again at next polling interval", queue_obj.name)
                return None

        if _msg_id:
            message_obj = self.env['imq.message'].browse(_msg_id)
            
            # NEW: Commit the main transaction to synchronize
            # with changes made by the autocommit transaction
            self.env.cr.commit()
            
            # Now the message has been modified AND committed to database
            # The main transaction restarts cleanly with up-to-date data
            
            # Optional: Force a re-read to be sure
            # message_obj.read()    
            return message_obj            
        
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
            # 'state': 'wip',  # already set by GET_MESSAGE_QUERY
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
                _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=processor_obj.visibility_timeout)
            else:
                _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=queue_obj.visibility_timeout)

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