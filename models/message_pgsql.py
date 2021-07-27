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
from ..api import _send_message

_logger = logging.getLogger("IMQMessage")


class IMQMessagePGSQL(models.Model):
    _inherit = 'imq.message'
    
    def do_retry_processing__pgsql(self):
        for record in self:
            if record.state == 'new':
                _logger.error("Message Retry ignored. Messages in state 'new' can't be retried.")
                continue
            if record.message_type != 'rpc':
                _logger.error("Message Retry ignored. Only RPC messages can be retried.")
                continue
        
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

            _message_attributes = {
                'code': {
                    'DataType': 'String',
                    'StringValue': "%s" % (record.code),
                }
            }
        
            response = _send_message(
                record.queue_id, 
                record.name,
                message_body_values, 
                message_group=record.group or None, 
                message_deduplication_id=None, 
                message_attributes=_message_attributes
            )

            queue_message_id_history = record.queue_message_id_history or ''
            queue_message_id_history = "%s %s\n" % (
                datetime.datetime.now(),
                record.queue_message_id,
            ) + queue_message_id_history
            update_dict = {
                'queue_message_id_history': queue_message_id_history,
                'state': 'retry'
            } 
            record.write(update_dict)
        return        
    
