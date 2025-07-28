import os
import sys
import inspect
import types
import threading
import datetime
import logging
import json
import jsonpickle


import odoo
from odoo import fields, api, SUPERUSER_ID
from odoo.tools.translate import _
from odoo.exceptions import MissingError, UserError


_logger = logging.getLogger(__name__)


CHECK_MESSAGE_DUPLICATE_SQL = """SELECT id
FROM imq_message
WHERE 
    state IN ('wip', 'pending', 'retry')
AND enqueued_time > NOW() - '%s seconds'::INTERVAL
AND message_deduplication_id = %s;
"""



def check_message_duplicate(odoo_env, queue_obj, message_deduplication_id, raise_on_duplicate):
    _dedup_interval = queue_obj.deduplication_interval_s
    odoo_env.cr.execute(CHECK_MESSAGE_DUPLICATE_SQL,(_dedup_interval, message_deduplication_id))
    if odoo_env.cr.rowcount:
        _msg = ("Message with Deduplication Id:'%s' is duplicated within a %ss time "
               "interval. (raise_on_duplicate=%s)") % (
                    message_deduplication_id,
                    _dedup_interval,
                    raise_on_duplicate
                )
        
        if raise_on_duplicate:
            raise UserError(_msg)
    
        else:
            _logger.warning("IMQ Ignored Message. %s", _msg)
            return {
                "error_code": "MESSAGE_IS_DUPLICATED",
                "error_message": _msg,
                "id": None,
                "MessageId": None,
            }
    return None

def send_message__pgsql(
    queue_obj, message_name, message_body_values, message_group=None, 
    message_deduplication_id=None, message_attributes=None, raise_on_duplicate:bool=False
):
    """ Send a simple message to .
    :param queue_obj: An Odoo Queue object
    """
    odoo_env = queue_obj.env
    message_model = odoo_env['imq.message']
    if message_attributes is None: message_attributes = {}
    if not message_group or message_group is None: 
        message_group = queue_obj.generate_message_group()

    _body = jsonpickle.encode(message_body_values)
    _raw_message_body = json.dumps(json.loads(_body), indent=4)
    _now = datetime.datetime.now()

    if queue_obj.q_type == 'fifo':
        _r = check_message_duplicate(
            odoo_env, queue_obj, message_deduplication_id, raise_on_duplicate
        )
        if _r: return _r

    message_values = {
        "name": message_name,
        "queue_id": queue_obj.id,
        "group": message_group,
        "queue_message_id": queue_obj.generate_message_id(),
        "user_id": odoo_env.user.id,
        "raw_message_body": _raw_message_body,
        "enqueued_time": _now.isoformat(sep=' ', timespec='seconds'),
        "enqueued_time_microseconds": _now.microsecond,
        "message_deduplication_id": message_deduplication_id,
        "code": message_attributes.get("code", None),
        "context": jsonpickle.encode(message_body_values.get('context', {})),
        "payload": jsonpickle.encode(message_body_values.get('payload', {})),
        "attempt": 0,
        "state": "pending"
    }
    #_logger.debug("Creating message: %s:%s", queue_obj.name, message_name)
    #_logger.debug("   values => %s", message_values)
    message_obj = message_model.create(message_values)
    return {
        "MessageId": message_obj.queue_message_id,
        "id": message_obj.id
    }
    
