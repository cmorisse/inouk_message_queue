import datetime
import jsonpickle
import logging

import odoo
from odoo import models, fields, api
from odoo.exceptions import except_orm
from odoo.tools.translate import _

"""Maps processings to messages."""

_logger = logging.getLogger("IMQ")


IMQ_MESSAGE_PROCESSOR_LOG_LEVEL = [
    ('10', "DEBUG"),
    ('20', "INFO"),
    ('30', "WARNING"),
    ('40', "ERROR"),
    ('50', "CRITICAL"),
]

IMQ_MESSAGE_PROCESSOR_TYPES = [
    ('rpc', "RPC"),
    ('simple', "Simple"),  # A worker has been assigned
]
MAX_ATTEMPTS = 4  # TODO make this a system parameter or a configuration


class IMQMessageProcessor(models.Model):
    _name = 'imq.message_processor'
    _description = "IMQ - Message Processor"
    _order = 'name'

    @api.depends('function', 'type', 'selector')
    def _calc_name(self):
        for record in self:
            record.name = record.function if record.type == 'rpc' else record.selector

    # fields
    name = fields.Char(compute='_calc_name', store=True, readonly=True)
    type = fields.Selection(IMQ_MESSAGE_PROCESSOR_TYPES, required=True)
    selector = fields.Char(help="Message selector for 'simple' message")
    module = fields.Char(
        help="Name of module that contains the function. Eg. for Simple message "
             "test you can use builtin"
             "odoo.addons.inouk_message_queue.models.test_launcher"
    )
    function = fields.Char(
        help="Name of function called by processor. Eg. SimpleMessage_processor"
    )
    max_attempt = fields.Integer(default=MAX_ATTEMPTS)
    is_method = fields.Boolean(default=False,
                               help="Checked if function is a method, unchecked if it is"
                                    " a pure function.") 
    logging_activated = fields.Boolean(default=True,
                                       help="Defines if the function accepts an "
                                            "_imq_logger parameter. Must be set manually "
                                            "for processor of 'simple' messages.")
    capture_log = fields.Boolean(default=True,
                                 help="Check if you want to capture log of processor "
                                      "having 'logging activated'.")
    capture_console = fields.Boolean(
        default=False,
        help="Check if you want to capture console output having 'logging activated'. "
             "If you check this, IMQ will pass an '_imq_stream' parameter to "
             "each called task. This option is only effective for task that "
             "implement it (eg. Muppy)."
    )
    log_level = fields.Selection(IMQ_MESSAGE_PROCESSOR_LOG_LEVEL, 
                                 required=True,
                                 default='20')
    log_format = fields.Char(default="%(asctime)s %(name)s %(levelname)s %(message)s")
    force_visibility_timeout = fields.Boolean(
        string="Force Visibility Timeout",
        help="When checked, 'Visibility Timeout' value is used to control visibility of messages "
             "related to this processor. When unchecked, message visibility in defined using AWS "
             "SQS Queue parameters",
        default=False)
    visibility_timeout = fields.Integer(help="Time in seconds (from processing start) the"
                                             " message will be invisible to other workers. "
                                             "If 0 SQS will use the 'VisibilityTimeout' "
                                             "declared in the Queue. Min=0s, Max=12h.")

    notify_message_processing_start = fields.Boolean(help="Send a notification when message processing starts.")
    notify_message_processing_end = fields.Boolean(help="Send a notification when message processing ends (normally).")
    notify_message_processing_retry = fields.Boolean(help="Send a notification when message processing is restarted (timeout or retryable errors).")
    notify_message_processing_terminate = fields.Boolean(help="Send a notification when message processing ends because of an error or exception.")
    notify_message_processing_fail = fields.Boolean(help="Send a notification when message processing fails.")

    # TODO: add statistics fields
    _sql_constraints =  [
        (
            'processor_uniq', 
            'UNIQUE(type, selector, module, function)', 
            _("Processor must be unique !")
        )
    ]    
    def copy(self, default=None):
        self.ensure_one()
        old_selector = default.get('selector') if default else ''
        new_selector = old_selector or _('%s_copy') % self.selector
        default = dict(default or {}, selector=new_selector)
        return super().copy(default)    

    @api.model
    def upsert_processor_from_message(self, message_body:dict):
        """Find or create an imq.message_processor from a received message 
        body.
        :param message_body: Body of received message.
        """
        message_type = message_body.get('type', None)
        message_selector = message_body.get('selector', None)
        message_module = message_body.get('module_name', None)
        message_function = message_body.get('function_name', None)
        
        if message_type == 'rpc':
            if message_module is None:
                raise Exception("Missing 'module' in received 'rpc' type message.")
            if message_function is None:
                raise Exception("Missing 'function' in Received 'rpc' type message.")
            
            processor_search_domain = [
                ('type', '=', 'rpc'),
                ('module', '=', message_module),
                ('function', '=', message_function),
            ]
            processor_obj = self.env['imq.message_processor'].search(
                processor_search_domain
            )
            if not processor_obj:
                _logger.critical("No processor defined for received 'rpc' type message "
                                "(module=%s, function=%s)." % (message_module,
                                                              message_function,))
            return processor_obj
            
        elif message_type == 'simple':
            if message_selector is None:
                # TODO raise Error "Received 'simple' message without selector"
                raise Exception("Received 'simple' type message without selector.")
            processor_search_domain = [
                ('type', '=', 'simple'),
                ('selector', '=', message_selector),
            ]
            processor_obj = self.env['imq.message_processor'].search(
                processor_search_domain
            )
            if processor_obj:
                return processor_obj
                
        else:
            return None

        # unknown valid 'simple' message received  
        message_processor = self.create({
            'type': 'simple',
            'selector': message_selector,
        })
        if not message_processor:
            raise Exception("Failed to created message_processor for selector: "
                            "'%s'." % message_selector)
        return message_processor



