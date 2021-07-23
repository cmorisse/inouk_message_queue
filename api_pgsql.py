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

def send_message__pgsql(
    queue_obj, message_name, message_body_values, message_group=None, 
    message_deduplication_id=None, message_attributes=None
):
    """ Send a simple message to .
    :param queue_obj: An Odoo Queue object
    """
    odoo_env = queue_obj.env
    message_model = odoo_env['imq.message']
    message_values = {
        "name": message_name,
        "queue_id": queue_obj.id,
        "group": queue_obj.generate_message_group(),
        "queue_message_id": queue_obj.generate_message_id(),
        "user_id": odoo_env.user.id,
        "raw_message_body": message_body_values,
        "enqueued_time": fields.Datetime.now(),
        "state": "pending"
    }
    message_obj = message_model.create(message_values)
    return {
        "MessageId": message_obj.queue_message_id,
        "id": message_obj.id
    }
    
