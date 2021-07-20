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
    message_deduplication_id=None
):
    """ Send a simple message to AWS SQS queue.
    :param env: A valid Odoo env
    :param queue: Queue name prefix of the queue to use or queue obj
    """
    sqs_resource = boto3.resource(
        'sqs',
        region_name=queue_obj.region,  # os.environ.get('IMQ_SQS_REGION')
        aws_access_key_id=queue_obj.key,  # ex os.environ.get('IMQ_SQS_ACCESS_KEY_ID'),
        aws_secret_access_key=queue_obj.secret  # ex os.environ.get('IMQ_SQS_SECRET_ACCESS_KEY')
    )
    sqs_queue = sqs_resource.get_queue_by_name(QueueName=queue_obj.sqs_name)
    send_message_kwargs = {
        'MessageBody': jsonpickle.encode(message_body_values),
        'MessageAttributes': {
            'name': {
                'DataType': 'String',
                'StringValue': message_name,
            },
        }
    }
    if message_group:
        send_message_kwargs['MessageGroupId'] = message_group
        if message_deduplication_id:
            send_message_kwargs['MessageDeduplicationId'] = message_deduplication_id

    response = sqs_queue.send_message(**send_message_kwargs)
    return response

