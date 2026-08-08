import os
import sys
import inspect
import types
import threading
import datetime
import logging
import jsonpickle
import boto3


import odoo
from odoo import fields, api, SUPERUSER_ID
from odoo.tools.translate import _
from odoo.exceptions import MissingError, UserError


_logger = logging.getLogger(__name__)


def send_message__aws_sqs(
    queue_obj, message_name, message_body_values, message_group=None,
    message_deduplication_id=None, message_attributes=None, raise_on_duplicate:bool=True,
    deduplication_interval_s=None
):
    """ Send a simple message to AWS SQS queue.
    :param raise_on_duplicate: is ignored with SQS
    :param deduplication_interval_s: is ignored with SQS, and says so out loud. AWS fixes
        the FIFO deduplication window at 5 minutes and offers no per-message override, so a
        caller asking for a different one is not getting it. Ignoring that silently would
        leave them believing a guard is in place that is not.
    """
    if deduplication_interval_s is not None:
        _logger.warning(
            "IMQ: _imq_deduplication_interval_s=%s ignored on SQS queue '%s' — AWS fixes "
            "the FIFO deduplication window at 5 minutes and it is not settable per message. "
            "The message is sent; the requested window is NOT applied.",
            deduplication_interval_s, queue_obj.name)
    if message_attributes is None:
        message_attributes = {}

    sqs_resource = boto3.resource(
        'sqs',
        region_name=queue_obj.region,  # os.environ.get('IMQ_SQS_REGION')
        aws_access_key_id=queue_obj.key,  # ex os.environ.get('IMQ_SQS_ACCESS_KEY_ID'),
        aws_secret_access_key=queue_obj.secret  # ex os.environ.get('IMQ_SQS_SECRET_ACCESS_KEY')
    )
    sqs_queue = sqs_resource.get_queue_by_name(QueueName=queue_obj.sqs_name)

    default_message_attributes =  {
        'name': {
            'DataType': 'String',
            'StringValue': message_name,
        },
    }
    final_message_attributes = { **default_message_attributes, **message_attributes}


    send_message_kwargs = {
        'MessageBody': jsonpickle.encode(message_body_values),
        'MessageAttributes': final_message_attributes
    }
    if message_group:
        send_message_kwargs['MessageGroupId'] = message_group
        if message_deduplication_id:
            send_message_kwargs['MessageDeduplicationId'] = message_deduplication_id

    response = sqs_queue.send_message(**send_message_kwargs)
    return response

