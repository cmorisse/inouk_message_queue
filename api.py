import os
import sys
import inspect
import types
import threading
import datetime
import logging
import hashlib
import jsonpickle
import boto3


import odoo
from odoo import fields, api, SUPERUSER_ID
from odoo.tools.translate import _
from odoo.exceptions import MissingError, UserError

from .api_sqs import send_message__aws_sqs
from .api_pgsql import send_message__pgsql

_logger = logging.getLogger(__name__)


class IMQError(UserError):
    """ Commit, statut = "Failed", Pas de retry """
    def __init__(self, message, results=None):
        """ 
        :param results: Any obj pr string
        """
        super().__init__(message)
        self.results = results


class IMQRetryableError(UserError):
    """ Rollback, statut = "Retry", Retry automatique """
    def __init__(self, message, delay=None, results=None):
        """ 
        :param results: Any obj or string
        :param delay: Optional delay in s to defer retry.
        """
        super().__init__(message)
        self.results = results
        self.delay = delay

class IMQTerminateException(UserError):
    """ Rollback, statut = "Terminated", Pas de retry """
    def __init__(self, message, results=None):
        """ 
        :param results: Any obj pr string
        """
        super().__init__(message)
        self.results = results


class OdooModelWrapper:
    """
    Allows to pickle Odoo models.Model
    """
    def __init__(self, obj, type='model'):
        if type == 'model':
            self.type = 'model'
            self.model = obj.__class__._name
            self.method_name = None
            self.ids = obj._ids
        elif type == 'Environment':
            self.type = type
            self.model = None
            self.method_name = None
            self.ids = None
        else:
            self.type = 'method'
            self.model = obj.im_self._name
            self.method_name = obj.im_func.func_name
            self.ids = obj.im_self._ids

    def __str__(self):
        return "%s(%s)" % (self.model, self.ids,)


def wrap_odoo_model(arg):
    """ Returns an OdooModelWrapper if arg is an Odoo Model else returns arg
    if arg is an Odoo returns wrapper else returns arg
    :param arg:
    :return:
    """
    if isinstance(arg, odoo.models.BaseModel):
        return OdooModelWrapper(arg)
    elif isinstance(arg, odoo.api.Environment):
        return OdooModelWrapper(arg, type='Environment')
    elif isinstance(arg, (list, tuple,)):
        #return map(lambda elem: wrap_odoo_model(elem), arg)
        return [wrap_odoo_model(elem) for elem in arg]
    elif isinstance(arg, dict):
        patch = [(key, wrap_odoo_model(value),) for key, value in arg.items()]
        arg.update(patch)
    elif isinstance(arg, types.MethodType):  # For bound methods
        return OdooModelWrapper(arg, type='method')
    return arg

def unwrap_odoo_model(env, obj):
    """
    if arg is an Odoo returns wrapper else returns arg
    :param obj:
    :return:
    """
    if isinstance(obj, OdooModelWrapper):
        if obj.type == 'Environment':
            return env
        elif obj.type == 'model':
            odoo_obj = env[obj.model].with_context(active_test=False).search([('id', 'in', obj.ids)])
            return odoo_obj
        elif obj.type == 'method':
            odoo_obj = env[obj.model].with_context(active_test=False).search([('id', 'in', obj.ids)])
            return getattr(odoo_obj, obj.method_name)
        else:
            raise UserError("Unwrap of Odoo '%s' not implemented.")
    if isinstance(obj, list):
        return list(map(lambda elem: unwrap_odoo_model(env, elem), obj))
    if isinstance(obj, tuple):
        return tuple(map(lambda elem: unwrap_odoo_model(env, elem), obj))
    if isinstance(obj, dict):
        patch = [
            (key, unwrap_odoo_model(env, value),) for key, value in obj.items()
        ]
        obj.update(patch)
    return obj

def extract_message_name(processor, args, kwargs):
    """Extract message name from (in this order):
       * _imq_message_name named parameter
       * processor doc string
    :return: message name as str
    """
    if kwargs.get('_imq_message_name'):
        message_name = kwargs['_imq_message_name']
        return message_name

    if processor.__doc__:
        processor_name = processor.__doc__.split('\n')[0] or ''
        try:
            formatted_message_name = processor_name.format(*args, **kwargs)
        except Exception as e:
            _logger.error("Failed to generate Message name with: '%s', %s, %s", processor_name, args, kwargs)
            formatted_message_name = "Message name extraction failed with "\
                                     "error: %s" % e
        return formatted_message_name

    return "Un-named Processor"

def find_or_create_queue(env, queue_name):
    """Find or create an imq.queue from name.
    :param env: an ORM environment
    :type env: odoo.api.Environment
    :param queue_name: Name of the queue as defined in AWS
    :type queue_name: str
    :return: an imq.queue
    """
    if not queue_name:
        queue_name = 'default'
    queue_obj = env['imq.queue']
    queue = queue_obj.search([('name', '=', queue_name)])
    if not queue:
        return queue_obj.with_user(env.ref('inouk_message_queue.user_imq')).create({
            'name': queue_name,
            'log_level': '20',
        })
    return queue

def find_or_create_processor(caller_env:api.Environment, function_name, module_name, is_method=False, 
                             logging_activated=False, processor_visibility_timeout=0):
    """Find or create an imq.message_processor from module and python function 
    names (for messages of type 'rpc')

    :return: an imq.message_processor or raise an Error
    """
    USER_IMQ_ID = caller_env.ref('inouk_message_queue.user_imq').id
    try:
        # With v13 we are now longer able to use a new environment 
        # as old one content is reset.
        #with api.Environment.manage(), caller_env.registry.cursor() as cr:
            #new_env = api.Environment(cr, USER_IMQ_ID, {})
        #with caller_env.registry.cursor() as new_cr:
                        
            processor_model = caller_env['imq.message_processor']                    
            processor_obj = processor_model.search([
                ('type', '=', 'rpc'),
                ('module', '=', module_name),
                ('function', '=', function_name),
            ])
            if not processor_obj:
                processor_obj = processor_model.with_user(USER_IMQ_ID).create({
                    'type': 'rpc',
                    'module': module_name,
                    'function': function_name,
                    'is_method': is_method,
                    'logging_activated': logging_activated,
                    'force_visibility_timeout': processor_visibility_timeout > 0,
                    'visibility_timeout': processor_visibility_timeout
                })
                if not processor_obj:
                    raise Exception("Failed to created imq.message_processor for "
                                    "module=%s, function=%s" % (
                                        module_name,
                                        function_name
                                    ))
    except:
        _logger.error(
            "Failed to created imq.message_processor for module=%s, function=%s", 
            module_name,
            function_name
        )
        raise

    return processor_obj

def extract_env_from_params(runnable, args, kwargs):
    """extracts and odoo.api.env from parameters.
    First try find a param of type odoo.models.Model
    then tries to find an odoo.api.Environment
    if there is an _imq_ephemeral_env in kwargs is it removed
    :returns: found odoo.api.Environment or None
    """
    env = None
    args_to_scan = args + tuple(kwargs.values())
    for scanned_arg in args_to_scan:
        if isinstance(scanned_arg, odoo.models.Model):
            env = scanned_arg.env
            break
    if env is None:
        for scanned_arg in args_to_scan:
            if isinstance(scanned_arg, odoo.api.Environment):
                env = scanned_arg
                break
    if '_imq_ephemeral_env' in kwargs:
        del kwargs['_imq_ephemeral_env']
    return env

def _send_message(
    queue_obj, message_name, message_body_values, message_group=None, 
    message_deduplication_id=None, message_attributes=None, raise_on_duplicate:bool=True
):    
    """ Low level driver method that sends message to a queue.
    """
    _send_method_name = "send_message__%s" % queue_obj.provider
    
    # compute hash for body
    if queue_obj.q_type == 'fifo' and message_deduplication_id is None:
        _body_str = jsonpickle.encode(message_body_values, indent=0)
        _msg_dedup_id = hashlib.sha256(_body_str.encode('utf-8')).hexdigest()
    else:
        _msg_dedup_id = message_deduplication_id

    response = getattr(sys.modules[__name__], _send_method_name)(
        queue_obj,
        message_name,
        message_body_values,
        message_group=message_group,
        message_deduplication_id=_msg_dedup_id,
        message_attributes=message_attributes,
        raise_on_duplicate=raise_on_duplicate
    )
    _logger.debug("{method} => {resp}".format(
        method=_send_method_name,
        resp=response
    ))
    return response


# See odoo/api.py ligne 789 to create an ORM env from scratch
def enqueue(runnable, *args, **kwargs):
    """ enqueue function call by sending a message to AWS SQS queue
    containing all required information to execute call.

    :param args: args that will be passed to the function when it will run. 
    :param kwargs: passed to the function when it will run
    
    *args or **kwargs must always contain at least one odoo.models.Model or  
    odoo.odoo.api.Environment instance. `enqueue()` picks the first one to 
    retrieve:
       - a context that is saved to be restored when function will be run        

    :return: AWS SQS response
    """
    env = extract_env_from_params(runnable, args, kwargs)
    if env is None:
        raise MissingError("@processor decorated methods must receive at least "
                           "one parameter of type odoo.models.Model or "
                           "odoo.api.Environment")
    is_method = kwargs.get('_imq_is_method', False)
    if '_imq_is_method' in kwargs:
        del kwargs['_imq_is_method']

    # detect whether logging is requested
    function_signature = inspect.getfullargspec(runnable)
    logging_activated = '_imq_logger' in function_signature.args

    parent_message_id = kwargs.get('_imq_parent_message_id', None)
    if '_imq_parent_message_id' in kwargs:
        del kwargs['_imq_parent_message_id']

    target_children_count = kwargs.get('_imq_target_children_count', None)
    if '_imq_target_children_count' in kwargs:
        del kwargs['_imq_target_children_count']

    message_group = kwargs.get('_imq_message_group', None)
    if '_imq_message_group' in kwargs:
        del kwargs['_imq_message_group']

    message_deduplication_id = kwargs.get('_imq_message_deduplication_id', None)
    if '_imq_message_deduplication_id' in kwargs:
        del kwargs['_imq_message_deduplication_id']

    _imq_raise_on_duplicate = kwargs.get('_imq_raise_on_duplicate', None)
    if '_imq_raise_on_duplicate' in kwargs:
        del kwargs['_imq_raise_on_duplicate']

    message_name = extract_message_name(runnable, args, kwargs)
    if '_imq_message_name' in kwargs:
        del kwargs['_imq_message_name']  # We pass all "_imq" params via context

    queue_name_prefix = kwargs.get('_imq_queue_name', 'default')
    queue_obj = env['imq.queue'].search([('name', '=', queue_name_prefix)])
    if not queue_obj:
        raise UserError("Unknown queue:'%s' !!!" % queue_name_prefix)

    if queue_obj.provider == 'aws_sqs':
        queue_name = "%s_%s%s" % (
            queue_name_prefix, 
            env.cr.dbname,
            '.fifo' if queue_obj.q_type == 'fifo' else ''
        )
    else:
        queue_name = queue_name_prefix

    if '_imq_queue_name' in kwargs:
        del kwargs['_imq_queue_name']  # We pass all "_imq" params via context

    if env:
        processor_context = env.context.copy()
        user_id = env.user.id
    else:
        processor_context = {}
        user_id = env.ref('inouk_message_queue.user_imq')
    processor_context['_imq_message_group'] = message_group
    processor_context['_imq_message_deduplication_id'] = message_deduplication_id
    processor_context['_imq_message_name'] = message_name
    processor_context['_imq_parent_message_id'] = parent_message_id
    processor_context['_imq_target_children_count'] = target_children_count

    # We serialize payload differently based on is_method
    if is_method:
        self = args[0]
        args = args[1:]
        module_name = self._name
        function_name = runnable.__name__
    else:
        self=None
        module_name = runnable.__module__
        function_name = runnable.__name__

    # TODO: ensure _imq_logger is a named parameter and raise if not

    processor_visibility_timeout = kwargs.get('_imq_processor_visibility_timeout', 0)
    if '_imq_processor_visibility_timeout' in kwargs:
        del kwargs['_imq_processor_visibility_timeout']

    processor_obj = find_or_create_processor(env,
                                             function_name,
                                             module_name,
                                             is_method,
                                             logging_activated,
                                             processor_visibility_timeout)
    # Manage synchronous execution
    run_synchronously = kwargs.get('_imq_run_synchronously', False)
    if '_imq_run_synchronously' in kwargs:
        del kwargs['_imq_run_synchronously']

    if run_synchronously:
        return runnable(*args, **kwargs)

    payload = {
        'self': wrap_odoo_model(self),
        'args': wrap_odoo_model(args),
        'kwargs': wrap_odoo_model(kwargs),
    }

    message_body_values = {
        'type': 'rpc',
        'logging_activated': logging_activated,
        'module_name': module_name,
        'function_name': function_name,
        'is_method': is_method,
        'context': processor_context,
        'payload': payload,
        'user_id': user_id,
    }

    message_attributes = {
        'code': {
            'DataType': 'String',
            'StringValue': "%s(%s,%s)" % (
                runnable.__name__,
                [arg for arg in args],
                ["%s=%s" % (arg_name, arg_val) for arg_name, arg_val, in kwargs.items()],
            )
        }
    }
    response = _send_message(
        queue_obj, 
        message_name, 
        message_body_values, 
        message_group=message_group, 
        message_deduplication_id=message_deduplication_id, 
        message_attributes=message_attributes,
        raise_on_duplicate=_imq_raise_on_duplicate,
    )
    return response


def processor(queue_name='default', processor_visibility_timeout=0):
    """ Decorator that allows to enqueue a pure function (not method) call.
    See enqueue() help for detail.
    Note that @processor simply call enqueue() that does the real work.
    """
    if not isinstance(queue_name, str):
        raise Exception("Missing @processor's queue_name mandatory parameter.")
    def real_decorator(decorated_function):
        def run_async(*args, **kwargs):
            kwargs['_imq_queue_name'] = kwargs.get('_imq_queue_name', queue_name)
            kwargs['_imq_processor_visibility_timeout'] = processor_visibility_timeout
            return enqueue(decorated_function, *args, **kwargs)
        def message(*args, **kwargs):
            _logger.warning(".message() is deprecated use .run_async() instead.")
            return run_async(*args, **kwargs)
        def delay(*args, **kwargs):
            _logger.warning(".delay() is deprecated use .run_async() instead.")
            return run_async(*args, **kwargs)
        decorated_function.run_async = run_async
        decorated_function.message = message
        decorated_function.delay = delay
        return decorated_function
    return real_decorator


def processor_method(queue_name='default', processor_visibility_timeout=0):
    """ Decorator that allows to enqueue Odoo models.Model method calls.
    Since we are unable to know if a callable is a method or a function we
    rely on developer's declaration, hence both decorators @processor and 
    @processor_method.
    If a callable is annotated with @processor_method we expect args[0] to be 
    self and extract all required information from it.
    Note that @processor_method simply call enqueue() that does the real work.
    """
    if not isinstance(queue_name, str):
        raise Exception("Missing @processor_method's queue_name mandatory "
                        "parameter.")
    def real_method_decorator(decorated_method):
        def run_async(*args, **kwargs):
            assert args and isinstance(args[0], odoo.models.Model), \
                _("First parameter of functions decorated with "
                  "@processor_method decorator is mandatory and must always be "
                  "an odoo.models.Model instance !")
            kwargs['_imq_queue_name'] = kwargs.get('_imq_queue_name', 
                                                    queue_name)
            kwargs['_imq_processor_visibility_timeout'] = processor_visibility_timeout
            kwargs['_imq_is_method'] = True
            return enqueue(decorated_method, *args, **kwargs)
        def message(*args, **kwargs):
            _logger.warning(".message() is deprecated use .run_async() instead.")
            return run_async(*args, **kwargs)
        def delay(*args, **kwargs):
            _logger.warning(".delay() is deprecated use .run_async() instead.")
            return run_async(*args, **kwargs)
        decorated_method.run_async = run_async
        decorated_method.message = message
        decorated_method.delay = delay
        return decorated_method
    return real_method_decorator

def send_message(
    env, queue, selector, payload, message_group=None, message_deduplication_id=None, 
    message_name=None, message_attributes=None
):
    """ Sends a Simple message to any Queue.
    :param env: A valid Odoo env
    :param queue: Queue name prefix of the queue to use or queue obj. Use 'default' or None for default queue.
    """
    if queue is None:
        queue = 'default'

    if isinstance(queue, str):
        queue_obj = env['imq.queue'].search([('name', '=', queue)])
        if not queue_obj:
            raise UserError("Unknown queue:'%s' !!!" % queue)
    else:
        queue_obj = queue

    if message_name is None:
        message_name = selector
    
    if queue_obj.q_type == 'fifo' and not message_group:
        raise IMQError(
            "Missing required 'message_group' parameter to send message to FIFO queue:'%s'."
            % queue_obj.sqs_name
        )

    message_body_values = {
        'type': 'simple',
        'selector': selector,
        'payload': payload,
    }

    response = _send_message(
        queue_obj, 
        message_name, 
        message_body_values, 
        message_group=message_group, 
        message_deduplication_id=message_deduplication_id, 
        message_attributes=message_attributes
    )
    return response

