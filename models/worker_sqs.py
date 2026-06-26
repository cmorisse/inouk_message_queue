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


# Must be equal to cron workers interval_number and interval_type
IMQ_SLEEP_INTERVAL = 20 
TLS = threading.local()


_logger = logging.getLogger('IMQWorker')


class IMQWorkerSQS(models.AbstractModel):
    """Processes messages in imq.queue"""
    _inherit = 'imq.worker'

    def get_message__aws_sqs(self, queue_obj, wait_time=0, target_message_id=None):
        """Query Q for next message to process.

        :param queue_obj: required imq.queue object to query.
        :param wait_time: number of seconds to block on queue. Must be 0 with ir.cron IMQ Worker
        :param target_message_id: specific message ID to target (not supported for SQS)
        :return: a SQS message object or None
        :raises: UserError if target_message_id is specified (SQS doesn't support message targeting)
        """
        if target_message_id:
            raise UserError(
                f"Message targeting (--message {target_message_id}) is not supported for AWS SQS queues. "
                f"SQS retrieves messages in queue order and cannot select specific messages by ID. "
                f"Use PostgreSQL provider for message targeting functionality."
            )
        sqs_resource = boto3.resource(
            'sqs',
            region_name=queue_obj.region,
            aws_access_key_id=queue_obj.key, 
            aws_secret_access_key=queue_obj.secret, 
        )
        sqs_queue = sqs_resource.get_queue_by_name(QueueName=queue_obj.sqs_name)

        _logger.debug(
            "[WorkerCron=%s,Q='%s',threadid=%s] Querying next message to process (wait_time=%s).",
            queue_obj.name,
            os.getpid(),
            threading.current_thread().ident,
            wait_time
        )
        messages = sqs_queue.receive_messages(
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_time,
        )
        if messages:
            message = messages[0]
            _logger.debug(
                "[WorkerCron=%s,Q=%s,threadid=%s] Got message: %s.",
                os.getpid(),
                queue_obj.name,
                threading.current_thread().ident,
                message
            )
            return message
        return None

    def store_message__aws_sqs(self, queue_obj, sqs_message, start_timestamp=None):
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
        processor_obj = self.env['imq.message_processor'].upsert_processor_from_message(body)

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
        # Reporting axes (see README "Execution Stats")
        if '_imq_stats_category' in context:
            message_values_dict['stats_category'] = context['_imq_stats_category']
        if '_imq_stats_target' in context:
            message_values_dict['stats_target'] = context['_imq_stats_target']

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

    def change_message_visibility__aws_sqs(self, queue_obj, message_obj, sqs_message):
        """ Update message visibility with timeout defined in processor. """
        m_timeout = message_obj.processor_id.visibility_timeout
        sqs_message.change_visibility(VisibilityTimeout=m_timeout)
    
    def terminate_message__aws_sqs(self, queue_obj, sqs_message):
        sqs_message.delete()  # Delete message from Cloud Queue

        return True

        if queue_obj.q_type == 'std':
            sqs_message.delete()  # Delete message from Cloud Queue
        
        elif queue_obj.q_type == 'fifo_':
            VISIBILITY_TIMEOUT_MAX = 12*60*60  # 12 hours
            sqs_message.change_visibility(VisibilityTimeout=VISIBILITY_TIMEOUT_MAX)
