import datetime
import logging
import json
import timeit

import boto3
import jsonpickle

import odoo
from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError
from odoo.tools.translate import _
from .message_processor import IMQ_MESSAGE_PROCESSOR_TYPES, IMQ_MESSAGE_PROCESSOR_LOG_LEVEL, MAX_ATTEMPTS


_logger = logging.getLogger("IMQMessage")


class IMQMessageSQS(models.Model):
    _inherit = 'imq.message'

    def retry_processing__aws_sqs(self):
        for record in self:
            if record.state == 'new':
                _logger.error("Message Retry ignored. Messages in state 'new' can't be retried.")
                continue

            if record.message_type != 'rpc':
                _logger.error("Message Retry ignored. Only RPC messages can be retried.")
                continue

            if record.queue_id.q_type == 'fifo':
                _logger.error("Message Retry ignored. Only message on AWS standard queues can be retried.")
                continue

            sqs_resource = boto3.resource(
                'sqs',
                region_name=record.queue_id.region,
                aws_access_key_id=record.queue_id.key,
                aws_secret_access_key=record.queue_id.secret,
            )

            sqs_queue = sqs_resource.get_queue_by_name(QueueName=record.queue_id.sqs_name)
            message_body_values = {
                'type': 'rpc',
                'logging_activated': record.logging_activated,
                'capture_console': record.capture_console,
                'module_name': record.processor_id.module,
                'function_name': record.processor_id.function,
                'is_method': record.processor_id.is_method,
                'context': jsonpickle.decode(record.context or "{}"),
                'payload': jsonpickle.decode(record.payload or "{}"),
                'user_id': record.user_id.id,
            }
            send_message_kwargs = {
                'MessageBody': jsonpickle.encode(message_body_values),
                # We want SQS to wait 10s before IMQ Workers can read this message.
                # We need this time to commit the message id change.
                #'DelaySeconds': 5,  # do not run.
                'MessageAttributes': {
                    'name': {
                        'DataType': 'String',
                        'StringValue': record.name,
                    },
                    'code': {
                        'DataType': 'String',
                        'StringValue': "%s" % (record.code),
                    }
                }
            }
            if record.group:
                send_message_kwargs['MessageGroupId'] = record.group
            response = sqs_queue.send_message(**send_message_kwargs)
            _logger.debug("response=%s", response)
            queue_message_id_history = record.queue_message_id_history or ''
            queue_message_id_history = "%s %s\n" % (
                datetime.datetime.now(),
                record.queue_message_id,
            ) + queue_message_id_history
            update_dict = {
                'queue_message_id_history': queue_message_id_history,
                'state': 'retry'
            }
            if response:
                update_dict['queue_message_id'] = response['MessageId']
            record.write(update_dict)
        return
