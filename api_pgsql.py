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


# `make_interval(secs => %s)` rather than '%s seconds'::INTERVAL: the placeholder used to sit
# INSIDE a string literal, which was tolerable while the value could only come from an Integer
# column. It can now come from a caller, so the interval is built from a typed parameter.
#
# `clock_timestamp()` and NOT `NOW()`. NOW() is the TRANSACTION START time and does not
# advance, while `enqueued_time` is stamped when the row is written -- two different clocks.
# The column also stores whole seconds (the fraction goes to enqueued_time_microseconds), so
# a row written after the transaction crossed a second boundary reads as LATER than NOW(),
# and then `enqueued_time > NOW() - 0` is TRUE: a zero-length window suppresses the message
# the caller has just enqueued. That is not a rounding artefact, it is the documented
# contract of `deduplication_interval_s=0` failing outright, and it grew more likely the
# longer the enclosing transaction had been open.
#
# Measuring from the real clock also makes the window mean what it says: "still in flight in
# the last N seconds", rather than "in the N seconds before this transaction began".
#
# One bias remains and is the safe one: because enqueued_time is truncated DOWN, a message
# looks up to a second older than it is, so a window is effectively up to a second short.
# That errs toward NOT suppressing -- running twice, never silently skipping.
CHECK_MESSAGE_DUPLICATE_SQL = """SELECT id
FROM imq_message
WHERE
    state IN ('wip', 'pending', 'retry')
AND enqueued_time > clock_timestamp() - make_interval(secs => %s)
AND message_deduplication_id = %s;
"""



def check_message_duplicate(odoo_env, queue_obj, message_deduplication_id,
                            raise_on_duplicate, deduplication_interval_s=None):
    """Is an equivalent message still in flight?

    :param deduplication_interval_s: the window to look in. **None inherits the queue's
        `deduplication_interval_s`** — which is where every pre-existing caller lands, so
        their behaviour is unchanged. `0` is NOT "inherit": it is a zero-length window, so
        nothing can ever match and no message is suppressed. Keeping those two distinct is
        what saves a special case here.
    """
    _dedup_interval = (queue_obj.deduplication_interval_s
                       if deduplication_interval_s is None else deduplication_interval_s)
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
    message_deduplication_id=None, message_attributes=None, raise_on_duplicate:bool=False,
    deduplication_interval_s=None
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
            odoo_env, queue_obj, message_deduplication_id, raise_on_duplicate,
            deduplication_interval_s=deduplication_interval_s
        )
        if _r: return _r

    # `_imq_requesting_user_id` carries three cases, and the difference between the
    # last two is load-bearing (docs/multi_tenancy.md):
    #   an id    -> that user requested the work (escalated paths);
    #   None     -> nothing was declared: attribute to the enqueuing user;
    #   False    -> a SYSTEM message. requesting_user_id stays NULL, so company_id
    #               derives to NULL at create and the message is invisible to every
    #               MGX tenant BY CONSTRUCTION -- for vault-internal or
    #               platform-internal findings a tenant must never be shown.
    # A plain `or` chain cannot express the third case: it swallows the explicit
    # False into the fallback and quietly attributes the message to whoever's read
    # happened to trigger it.
    _requesting_ctx = message_body_values.get('context', {}).get('_imq_requesting_user_id')
    message_values = {
        "name": message_name,
        "queue_id": queue_obj.id,
        "group": message_group,
        "queue_message_id": queue_obj.generate_message_id(),
        "user_id": odoo_env.user.id,
        "requesting_user_id": None if _requesting_ctx is False else (
            _requesting_ctx or message_body_values.get('user_id') or odoo_env.user.id),
        # Reporting axes set at ENQUEUE (pgsql creates the record here), so a
        # still-'pending' message already carries its bucket and get_task_status
        # can return a duration hint before a worker even picks it up. The receive
        # site store_message__pgsql re-applies the same values (harmless) and
        # keeps SQS — which has no record until receive — symmetric.
        "stats_category": message_body_values.get('context', {}).get('_imq_stats_category'),
        "stats_target": message_body_values.get('context', {}).get('_imq_stats_target'),
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
    
