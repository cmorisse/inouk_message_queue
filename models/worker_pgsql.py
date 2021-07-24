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

PGSQL_RESET_MESSQGE_SQL_std = """
UPDATE imq_message
SET state = 'pending', start_time = NULL
WHERE id = %s;
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
        if queue_obj.q_type == 'std':
            self.env.cr.execute(PGSQL_GET_MESSAGE_SQL_std)
            _row = self.env.cr.fetchone()
            _msg_id = _row and _row[0] or None
            return _msg_id

        elif queue_obj.q_type == 'fifo':
            pass
        else:
            raise Exception("Unsupported Queue type:'%s'" % queue_obj.type)
        return None

    def store_message__pgsql(self, queue_obj, sqs_message, start_timestamp=None):
        """ Stores SQS message in `imq.message`
        :returns: 'imq.message' model or None
        """
        message_model = self.env['imq.message']

        queue_message_id = sqs_message.message_id
        body = jsonpickle.decode(sqs_message.body)
        message_selector = body.get('selector', None)
        message_module = body.get('module_name', None)
        message_function = body.get('function_name', None)
        user_id = body.get('user_id',
                           self.env.ref('inouk_message_queue.user_imq').id)

        message_attributes = sqs_message.message_attributes or {}
        name = message_attributes.get(
            'name',
            {'StringValue':'Undefined'}
        )['StringValue']
        code = message_attributes.get(
            'code', 
            {'StringValue':'n/a'}
        )['StringValue']

        # search for processor
        processor_obj = self.env['imq.message_processor'].upsert_processor_from_message(sqs_message)

        _logger.debug("Searching for message with queue_message_id='%s'", queue_message_id)
        message_obj = message_model.search(
            [('queue_message_id', '=', queue_message_id)]
        )
        _logger.debug("Found '%s' with queue_message_id=%s", message_obj or 'No Message', queue_message_id)
    
        epoch_s = int(
            sqs_message.attributes['ApproximateFirstReceiveTimestamp']
        ) / 1000
        context = body.get('context', {})
        message_values_dict = {
            'state': 'wip',
            'queue_message_id': sqs_message.message_id,
            'name': name,
            'code': code,
            'enqueued_time': datetime.datetime.utcfromtimestamp(epoch_s),
            'start_time': start_timestamp,
            'start_time_microseconds': start_timestamp and start_timestamp.microsecond,
            'queue_id': queue_obj.id,
            'group': sqs_message.attributes.get('MessageGroupId', None),
            'user_id': user_id,
            'context': jsonpickle.encode(body.get('context', {})),
            'payload': jsonpickle.encode(body.get('payload', {})),
            'raw_message_body': json.dumps(
                json.loads(sqs_message.body),
                sort_keys=True,
                indent=4
            ),
        }
        # Add only id present
        if '_imq_parent_message_id' in context:
            message_values_dict['parent_message_id'] = context['_imq_parent_message_id']
        if '_imq_target_children_count' in context:
            message_values_dict['target_children_count'] = context['_imq_target_children_count']

        if processor_obj:
            message_values_dict.update({
                'processor_id': processor_obj.id,
                'logging_activated': processor_obj.logging_activated,
                'capture_console': processor_obj.capture_console,
                'max_number_of_attempts': processor_obj.max_attempt,
            })
        else:
            message_values_dict.update({
                'processor_id': None,
                'logging_activated': False,
                'capture_console': False,
                'max_number_of_attempts': MAX_ATTEMPTS
            })

        if message_obj:  #update
            message_obj.write(message_values_dict)
        else:  #create
            message_values_dict['attempt'] = 0
            message_obj = message_model.create(message_values_dict)
        return message_obj



    def change_message_visibility__pgsql(self, message_obj, _message):
        """ Update message visibility with timeout defined in processor. """
        if message_obj.processor_id.force_visibility_timeout:
            m_timeout = message_obj.processor_id.visibility_timeout
            _visibility_time = datetime.datetime.now() + datetime.timedelta(seconds=m_timeout)
            message_obj.write({
                "visibility_time": _visibility_time
            })

