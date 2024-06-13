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
    IMQError, IMQRetryableError, IMQTerminateException
)
from odoo.addons.inouk_message_queue.models.message_processor import (
    IMQ_MESSAGE_PROCESSOR_TYPES,
)

_logger = logging.getLogger(__name__)


TEST_EXCEPTION_TYPES_LIST = [
    ('exception',"Python Exception"),
    ('usererror',"Odoo UserError"),
    ('imqerror',"IMQError"),
    ('imqretryable',"IMQRetryableError"),
    ('imqterminate',"IMQTerminateException"),
]


class IMQTestLauncher(models.Model):
    _name = "imq.test_launcher"
    _description = "IMQ - Test (messages) Launcher"

    type = fields.Selection(IMQ_MESSAGE_PROCESSOR_TYPES, default="simple", required=True)
    name = fields.Char(required=True)
    queue_id = fields.Many2one('imq.queue')
    queue_type = fields.Selection(related='queue_id.q_type', readonly=True)
    debug_mode = fields.Boolean(
        help="When checked, RPC code is executed synchronously."
    )
    rpc_test_method = fields.Boolean(
        "Test RPC of method",
        help="For RPC Tests, defined wether we will test a class method or a procedure."
    )
    launch_result = fields.Text()
    process_result = fields.Text()
    message_group = fields.Char(
        help="Sent to SQS as MessageGroupId param for FIFO Queues."
    )

    should_raise_exception = fields.Boolean("Raise Exception", help="Will raise a UserError().")
    should_raise_exception_type = fields.Selection(
        selection=TEST_EXCEPTION_TYPES_LIST,
        string="Exception type", 
        help="The type of exception to raise."
    )
    should_raise_exception_stepname = fields.Char(
        "Exception Step", 
        help="Optional Step name that will raise. Must follow form  "
             "'fifo_step{{number}}' Eg. fifo_step9."
    )
    should_raise_exception_latch = fields.Boolean(
        "Exception Latch", 
        help="Used to rearm exception"
    )
    pass_imqerror_value = fields.Boolean("Pass a value to IMQError")

    selector = fields.Char(
        default="TestMessage", 
        help="Use 'TestMessage' to use default Test Processor."
    )
    payload = fields.Text()

    processing_duration_s = fields.Integer("Task Duration in seconds")
    param = fields.Char()
    delay_param = fields.Integer(
        "Delay (IMQRetryableError)",
        help="Optionl delay in s to defer retry when IMQRetryableError is raised"
    )
    def send_rpc_message(self):
        """ Launchs a task straight or asynchonously depending on debug_mode """
        self.ensure_one()

        if self.queue_type == 'fifo':
            _message_group = self.message_group or self.queue_id.get_message_group("TEST-")
        else:
            _message_group = None

        if self.debug_mode:
            if self.rpc_test_method:
                an_object = self.queue_id  # We use the Q as test object
                self.a_task_method(an_object, self.param)
            else:
                a_task_procedure(self, self.param)
        else:
            if self.rpc_test_method:
                an_object = self.queue_id  # We use the Q as test object
                self.launch_result = self.a_task_method.run_async(self, 
                    an_object, 
                    self.param,
                    _imq_queue_name=self.queue_id.name,
                    _imq_message_group=_message_group
                )
            else:
                self.launch_result = a_task_procedure.run_async(
                    self, 
                    self.param, 
                    _imq_queue_name=self.queue_id.name,
                    _imq_message_group=_message_group
                )

    
    def send_simple_message(self):
        """ Sends a simple message"""
        self.ensure_one()

        if self.queue_type == 'fifo':
            _message_group = self.message_group or self.queue_id.get_message_group("TEST-")
        else:
            _message_group = None

        # please note that for send_message() we must provide a complete
        result = send_message(
            self.env,
            self.queue_id.name,
            self.selector,
            eval(self.payload),
            message_group=_message_group,
            message_deduplication_id=None, 
            message_name=self.name,
            message_attributes=None        
        )
        self.launch_result = result
    
    @processor_method()
    def a_task_method(self, an_object, a_param, _imq_logger=None):
        """Run atask #{0}
        This is our task. It simplied inject param in value adding it current time.
        """
        task_logger = _imq_logger or _logger

        # This is a time consuming task ...
        task_logger.info("Task:'%s' started with an_object=%s, param=%s", 
                         self, 
                         an_object,
                         a_param)
        task_logger.info(
            "Task:'%s' will work for %ss", 
            self.name, 
            self.processing_duration_s
        )
        for i in range(self.processing_duration_s):
            #task_logger.info("   %s iteration # %s", an_object.name, i)
            #time.sleep(1)
            task_logger.info("   (q=%s) iteration # %ss (compute)", an_object.name, i)
            s = time.time()
            while time.time() < s + 1:
                j = s / 3.145

        print("Processing task with param=%s" % a_param)
        task_logger.debug("Processing task with param=%s", a_param)
        p_result = self.process_result or ""
        p_result = p_result + "a_task = %s @ %s\n" % (
            self.param,
            datetime.datetime.now(),
        )
        param_str = "Param=%s" % a_param
        self.process_result = p_result
        if self.should_raise_exception and not self.should_raise_exception_latch:

            if not self.should_raise_exception_stepname or (self.should_raise_exception_stepname and self.should_raise_exception_stepname==a_param):
                if self.should_raise_exception_type=='exception':
                    raise Exception(param_str)

                elif self.should_raise_exception_type=='usererror':
                    raise UserError(param_str)

                elif self.should_raise_exception_type=='imqerror':
                    raise IMQError(param_str, results=[{'key': "value"}])

                elif self.should_raise_exception_type=='imqretryable':
                    kwargs = {}
                    if self.pass_imqerror_value:
                        kwargs['results'] = [{'key': "value"}]
                    if self.delay_param:
                        kwargs['delay'] = self.delay_param
                    if kwargs:
                        raise IMQRetryableError(param_str, **kwargs)
                    else:
                        raise IMQRetryableError(param_str)

                elif self.should_raise_exception_type=='imqterminate':
                    raise IMQTerminateException(param_str)


        # We use print to get a trace in celery
        print("Processed: %s" % self)
        task_logger.debug("Processed %s", self)
        return self.process_result  # is stored in queue


    def process_exception(self, step_name):

        if self.should_raise_exception and not self.should_raise_exception_latch:

            exc_name = "Raise at %s" % step_name
            if not self.should_raise_exception_stepname or (self.should_raise_exception_stepname and self.should_raise_exception_stepname==step_name):
                if self.should_raise_exception_type=='exception':
                    raise Exception(exc_name)

                elif self.should_raise_exception_type=='usererror':
                    raise UserError(exc_name)

                elif self.should_raise_exception_type=='imqerror':
                    raise IMQError(exc_name, results=[{'key': "value"}])

                elif self.should_raise_exception_type=='imqretryable':
                    raise IMQRetryableError(exc_name)

                elif self.should_raise_exception_type=='imqterminate':
                    raise IMQTerminateException(exc_name)

    @processor_method()
    def fifo_step(self, step_name, _imq_logger=None):
        task_logger = _imq_logger or _logger
        task_logger.info(
            "Task:'fifo_step:%s' will work for %ss", 
            step_name, 
            self.processing_duration_s
        )

        for i in range(self.processing_duration_s):
            task_logger.info(
                "   q=%s, step_name=%s, iteration #%ss (compute)", 
                self.queue_id.name, 
                step_name,
                i
            )
            time.sleep(1)

        self.process_exception(step_name)

    def launch_fifo_test(self):
        self.fifo_step.run_async(self, "fifo_step1", _imq_message_name="step1", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)
        self.fifo_step.run_async(self, "fifo_step2", _imq_message_name="step2", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)
        self.fifo_step.run_async(self, "fifo_step3", _imq_message_name="step3", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)
        self.fifo_step.run_async(self, "fifo_step4", _imq_message_name="step4", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)
        self.fifo_step.run_async(self, "fifo_step5", _imq_message_name="step5", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)
        self.fifo_step.run_async(self, "fifo_step6", _imq_message_name="step6", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)
        self.fifo_step.run_async(self, "fifo_step7", _imq_message_name="step7", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)
        self.fifo_step.run_async(self, "fifo_step8", _imq_message_name="step8", _imq_queue_name=self.queue_id.name, _imq_message_group=self.message_group)

@processor()
def a_task_procedure(an_object, a_param, _imq_logger=None, _imq_stream=None):
    """Run a_task_procedure #{0}
    This is our task. It simplied inject param in value adding it current time.
    """
    task_logger = _imq_logger or _logger

    # This is a time consuming task ...
    task_logger.info("Task:'%s' started with param=%s", an_object, a_param)
    task_logger.info(
        "Test: %s will work for for %ss", 
        an_object.name, 
        an_object.processing_duration_s
    )
    for i in range(an_object.processing_duration_s):
        task_logger.info("   %s iteration # %ss (compute)", an_object.name, i)
        s = time.time()
        while time.time() < s + 1:
            j = s / 3.145
        #time.sleep(1)

    print("Processing atask with param=%s" % an_object.param)
    task_logger.debug("Processing atask with param=%s", an_object.param)
    p_result = an_object.process_result or ""
    p_result = p_result + "a_task = %s @ %s\n" % (
        an_object.param,
        datetime.datetime.now(),
    )
    param = "Param=%s" % an_object.param
    an_object.process_result = p_result

    if an_object.should_raise_exception:            
        if an_object.should_raise_exception_type=='exception':
            raise Exception(param)

        elif an_object.should_raise_exception_type=='usererror':
            raise UserError(param)

        elif an_object.should_raise_exception_type=='imqerror':
            kwargs = {}
            if an_object.pass_imqerror_value:
                kwargs['results'] = [{'key': "value"}]

            if kwargs:
                raise IMQError(param, **kwargs)
            else:
                raise IMQError(param)

        elif an_object.should_raise_exception_type=='imqretryable':
            kwargs = {}
            if an_object.pass_imqerror_value:
                kwargs['results'] = [{'key': "value"}]
            if self.delay_param:
                kwargs['delay'] = self.delay_param
            if kwargs:
                raise IMQRetryableError(param_str, **kwargs)
            else:
                raise IMQRetryableError(param_str)

        elif an_object.should_raise_exception_type=='imqterminate':
            raise IMQTerminateException(param)

    # We use print to get a trace in celery
    print("Processed: %s" % an_object)
    task_logger.debug("Processed %s", an_object)
    return an_object.process_result  # is stored in queue


def SimpleMessage_processor(env, payload, _imq_logger=None):
    """ Example simple message processor. 
    Simple message processor are pure functions (not methods).
    :param env: an Odoo api.Environment
    :param payload: Optional dict sent by message sender
    :param _imq_logger: logger supplied by IMQ. whose output is captured.
    """
    task_logger = _imq_logger or _logger
    if payload is None:
        payload = {}
    task_logger.info("Hello")
    task_logger.info("payload=%s", payload)
    task_logger.debug("And with DEBUG level => payload=%s", payload)
    time.sleep(5)
    if isinstance(payload, dict) and payload.get("should_raise_error"):
        _msg = payload.get("should_raise_error")
        raise UserError("Error '%s' raised during processing." % _msg)

    return "processor returned string"  # is stored in queue


