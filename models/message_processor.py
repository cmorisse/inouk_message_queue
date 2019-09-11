# -*- coding: utf-8 -*-
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
    module = fields.Char(help="Name of module that contains the function.")
    function = fields.Char(help="Name of function called by processor.")
    max_attempt = fields.Integer(default=3)
    is_method = fields.Boolean(default=False,
                               help="Checked if function is a method, unchecked if it is"
                                    " a pure function.") 
    logging_activated = fields.Boolean(default=False,
                                       help="Defines if the function accepts an "
                                            "__imq_logger parameter. Must be set manually"
                                            "for processor of 'simple' messages.")
    capture_log = fields.Boolean(default=False,
                                 help="Check if you want to capture log of processor"
                                      "having 'logging activated'.")
    capture_console = fields.Boolean(
        default=False,
        help="Check if you want to capture console output having 'logging activated'."
             "If you check this, IMQ will pass an '__imq_stream' parameter to "
             "each called task. This option is only effective for task that "
             "implement it (eg. Muppy)."
    )
    log_level = fields.Selection(IMQ_MESSAGE_PROCESSOR_LOG_LEVEL, 
                                 required=True,
                                 default='20')
    log_format = fields.Char(default="%(asctime)s %(name)s %(levelname)s %(message)s")
    visibility_timeout = fields.Integer(help="Time in seconds (from processing start) the"
                                             "message will be invisible to other workers."
                                             "If 0 SQS will use the 'VisibilityTimeout' "
                                             "declared in the Queue. Min=0s, Max=12h.")
    use_visibility_timeout = fields.Boolean(
        help="Use Timeout to Retry. If unchecked message will be deleted as they are received.",
        default=False)

    # TODO: add statistics fields
    _sql_constraints =  [
        (
            'processor_uniq', 
            'UNIQUE(type, selector, module, function)', 
            _("Processor must be unique !")
        )
    ]    

    @api.model
    def upsert_processor_from_message(self, sqs_message):
        """Find or create an imq.message_processor from an sqs message
        :param sqs_message: a received sqs_message
        :type sqs_message: odoo.api.Environment
        """
        body = jsonpickle.decode(sqs_message.body)
        message_type = body.get('type', None)
        message_module = body.get('module_name', None)
        message_function = body.get('function_name', None)
        
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
            message_selector = body.get('selector', None)
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



