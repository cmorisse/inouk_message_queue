# -*- coding: utf-8 -*-
import sys
import datetime
import logging
import time

import odoo
from odoo.exceptions import UserError
from odoo.tools.safe_eval import safe_eval
from odoo import api, fields, models, tools, SUPERUSER_ID, _
from odoo.modules import get_module_resource

from odoo.addons.inouk_message_queue.api import (
    processor,
    processor_method,
    send_message,
)
from odoo.addons.inouk_message_queue.models.message_processor import (
    IMQ_MESSAGE_PROCESSOR_TYPES,
)


_logger = logging.getLogger(__name__)


class IMQTestLauncher(models.Model):
    _name = "imq.test_launcher"
    _description = "IMQ - Test (messages) Launcher"

    type = fields.Selection(IMQ_MESSAGE_PROCESSOR_TYPES)
    name = fields.Char()
    queue_name = fields.Char()
    debug_mode = fields.Boolean(
        help="When checked, RCP code est executed synchronously."
    )
    launch_result = fields.Text()
    process_result = fields.Text()
    message_group = fields.Char(
        help="Sent to SQS as MessageGroupId param for FIFO Queues."
    )
    should_raise_error = fields.Boolean()

    selector = fields.Char()
    payload = fields.Text()

    processing_duration_s = fields.Integer("Task Duration in seconds")
    param = fields.Char()

    @api.multi
    def launch(self):
        """ Launchs a task straight or asynchonously depending on debug_mode """
        self.ensure_one()
        if self.debug_mode:
            a_task(self, self.param)
        else:
            self.launch_result = a_task.message(self, self.param)

    @api.multi
    def send_simple_message(self):
        """ Sends a simple message"""
        self.ensure_one()
        # please note that for send_message() we must provide a complete
        result = send_message(
            self.env,
            self.queue_name,
            self.selector,
            eval(self.payload),
            self.message_group or None,
            self.name or None,
        )
        self.launch_result = result


@processor()
def a_task(an_object, a_param, _imq_logger=None):
    """Run atask #{0}
    This is our task. It simplied inject param in value adding it current time.
    """
    f_logger = _imq_logger or _logger

    # This is a time consuming task ...
    f_logger.info("a_task sleeping for %ss", an_object.processing_duration_s)
    time.sleep(an_object.processing_duration_s)

    print("Processing atask with param=%s" % an_object.param)
    f_logger.debug("Processing atask with param=%s", an_object.param)
    p_result = an_object.process_result or ""
    p_result = p_result + "a_task = %s @ %s\n" % (
        an_object.param,
        datetime.datetime.now(),
    )
    an_object.process_result = p_result
    if an_object.should_raise_error:
        raise Exception("Error raised during processing")
    # We use print to get a trace in celery
    print("Processed: %s" % an_object)
    f_logger.debug("Processed %s", an_object)
    return an_object.process_result  # is stored in queue


def simple_processor(env, payload, _imq_logger=None):
    f_logger = _imq_logger or _logger

    f_logger.info("Hello")
    f_logger.info("payload=%s", payload)
    f_logger.debug("And with DEBUG level => payload=%s", payload)
    if payload.get("should_raise_error"):
        raise Exception("Error raised during simple message processing")
    return "processor returned string"  # is stored in queue
