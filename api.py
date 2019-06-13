# coding: utf-8
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
from odoo.tools.translate import _
from odoo.exceptions import MissingError, UserError
from odoo import fields

_logger = logging.getLogger(__name__)

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
    """ Returns an OdooModelWrapper if arg is an Odoo Mdel else returns arg
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
    elif isinstance(arg, types.MethodType):  # bound method
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
            odoo_obj = env[obj.model].search([('id', 'in', obj.ids)])
            return odoo_obj
        elif obj.type == 'method':
            odoo_obj = env[obj.model].search([('id', 'in', obj.ids)])
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
       * __imq_message_name named parameter
       * processor doc string
    :return: message name as str
    """
    if kwargs.get('__imq_message_name'):
        message_name = kwargs['__imq_message_name']
        return message_name

    if processor.__doc__:
        processor_name = processor.__doc__.split('\n')[0] or ''
        try:
            formatted_message_name = processor_name.format(*args, **kwargs)
        except Exception as e:
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
        return queue_obj.sudo(env.ref('inouk_message_queue.user_imq')).create({
            'name': queue_name,
            'log_level': '20',
        })
    return queue

def find_or_create_processor(env, function_name, module_name, is_method=False, 
                             logging_activated=False, processor_visibility_timeout=0):
    """Find or create an imq.message_processor from module and python function 
    names (for messages of type 'rpc')
    :return: an imq.message_processor or raise an Error
    """
    processor_model = env['imq.message_processor']
    processor_obj = processor_model.search([
        ('type', '=', 'rpc'),
        ('module', '=', module_name),
        ('function', '=', function_name),
    ])
    if not processor_obj:
        processor_obj = processor_model.sudo(
                env.ref('inouk_message_queue.user_imq')
            ).create({
                'type': 'rpc',
                'module': module_name,
                'function': function_name,
                'is_method': is_method,
                'logging_activated': logging_activated,
                'visibility_timeout': processor_visibility_timeout
            })
        if not processor_obj:
            raise Exception("Failed to created imq.message_processor for "
                            "module=%s, function=%s" % (
                                module_name,
                                function_name
                            ))
    return processor_obj
    

def extract_env_from_params(runnable, args, kwargs):
    """extracts and odoo.api.env from parameters.
    First try find a param of type odoo.models.Model
    then tries to find an odoo.api.Environment
    if there is an __imq_ephemeral_env in kwargs is it removed
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
    if '__imq_ephemeral_env' in kwargs:
        del kwargs['__imq_ephemeral_env']
    return env


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

    is_method = kwargs.get('__imq_is_method', False)
    if '__imq_is_method' in kwargs:
        del kwargs['__imq_is_method']

    # detect whether logging is requested
    function_signature = inspect.getargspec(runnable)
    logging_activated = function_signature.args.count('__imq_logger') == 1

    message_group = kwargs.get('__imq_message_group', None)
    if '__imq_message_group' in kwargs:
        del kwargs['__imq_message_group']

    message_name = extract_message_name(runnable, args, kwargs)
    if '__imq_message_name' in kwargs:
        del kwargs['__imq_message_name']  # We pass all "__imq" params via context

    queue_name_prefix = kwargs.get('__imq_queue_name', 'default')
    queue_obj = env['imq.queue'].search([('name', '=', queue_name_prefix)])
    if not queue_obj:
        raise UserError("Unknown queue:'%s' !!!" % queue_name_prefix)

    queue_name = "%s_%s" % (queue_name_prefix, env.cr.dbname,)
    if '__imq_queue_name' in kwargs:
        del kwargs['__imq_queue_name']  # We pass all "__imq" params via context
    
    if env:
        processor_context = env.context.copy()
        user_id = env.user.id
    else:
        processor_context = {}
        user_id = env.ref('inouk_message_queue.user_imq')
        
    processor_context['__imq_message_group'] = message_group
    processor_context['__imq_message_name'] = message_name

    # TODO: ensure __imq_logger is a named parameter and raise if not

    # We serialize payload differently based on is_method
    if is_method:
        self=args[0]
        args=args[1:]
        module_name = self._name
        function_name = runnable.__name__
    else:
        self=None
        module_name = runnable.__module__
        function_name = runnable.__name__

    processor_visibility_timeout = kwargs.get('__imq_processor_visibility_timeout', 0)
    if '__imq_processor_visibility_timeout' in kwargs: 
        del kwargs['__imq_processor_visibility_timeout']

    processor_obj = find_or_create_processor(env, 
                                             function_name, 
                                             module_name, 
                                             is_method,
                                             logging_activated,
                                             processor_visibility_timeout)
    payload = {
        'self': wrap_odoo_model(self),
        'args': wrap_odoo_model(args),
        'kwargs': wrap_odoo_model(kwargs),
    }
    sqs_resource = boto3.resource(
        'sqs',
        region_name=queue_obj.region,   # os.environ.get('IMQ_SQS_REGION'),
        aws_access_key_id=queue_obj.key,  # os.environ.get('IMQ_SQS_ACCESS_KEY_ID'),
        aws_secret_access_key=queue_obj.secret,  # os.environ.get('IMQ_SQS_SECRET_ACCESS_KEY')
    )
    
    sqs_queue = sqs_resource.get_queue_by_name(QueueName=queue_name)
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
    send_message_kwargs = {
        'MessageBody': jsonpickle.encode(message_body_values),
        'MessageAttributes': {
            'name': {
                'DataType': 'String',
                'StringValue': message_name,
            },
            'code': {
                'DataType': 'String',
                'StringValue': "%s(%s,%s)" % (runnable.__name__,
                                              [arg for arg in args],
                                              ["%s=%s" % (arg_name, arg_val) for arg_name, arg_val, in kwargs.items()],)
            }
        }
    }
    if message_group:
        send_message_kwargs['MessageGroupId'] = message_group
    response = sqs_queue.send_message(**send_message_kwargs)
    _logger.debug("response={resp}".format(resp=response))
    return response
    

def processor(queue_name='default', processor_visibility_timeout=0):
    """ Decorator that allows to enqueue a pure function (not method) call.
    See enqueue() help for detail.
    Note that @processor simply call enqueue() that does the real work.
    """
    if not isinstance(queue_name, str):
        raise Exception("Missing @processor's queue_name mandatory parameter.")
    def real_decorator(decorated_function):
        def message(*args, **kwargs):
            kwargs['__imq_queue_name'] = kwargs.get('__imq_queue_name', 
                                                    queue_name)
            kwargs['__imq_processor_visibility_timeout'] = processor_visibility_timeout
            return enqueue(decorated_function, *args, **kwargs)
        def delay(*args, **kwargs):
            _logger.warning(".delay() is deprecated use .message() instead.")
            return message(*args, **kwargs)
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
        def message(*args, **kwargs):
            assert args and isinstance(args[0], odoo.models.Model), \
                _("First parameter of functions decorated with "
                  "@processor_method decorator is mandatory and must always be "
                  "an odoo.models.Model instance !")
            kwargs['__imq_queue_name'] = kwargs.get('__imq_queue_name', 
                                                    queue_name)
            kwargs['__imq_processor_visibility_timeout'] = processor_visibility_timeout
            kwargs['__imq_is_method'] = True
            return enqueue(decorated_method, *args, **kwargs)
        def delay(*args, **kwargs):
            _logger.warning(".delay() is deprecated use .message() instead.")
            return message(*args, **kwargs)
        decorated_method.message = message
        decorated_method.delay = delay
        return decorated_method
    return real_method_decorator


def send_message(env, queue, selector, payload, message_group=None, 
                 message_name=None):
    """ Sending a simple message to AWS SQS queue.
    :param env: A valid Odoo env
    :param queue: Queue name prefix of the queue to use or queue obj
    """
    if isinstance(queue, str):
        queue_obj = env['imq.queue'].search([('name', '=', queue)])
        if not queue_obj:
            raise UserError("Unknown queue:'%s' !!!" % queue)
    else:
        queue_obj = queue

    if message_name is None:
        name = selector
        
    sqs_resource = boto3.resource(
        'sqs',
        region_name=queue_obj.region,  # os.environ.get('IMQ_SQS_REGION')
        aws_access_key_id=queue_obj.key,  # ex os.environ.get('IMQ_SQS_ACCESS_KEY_ID'),
        aws_secret_access_key=queue_obj.secret  # ex os.environ.get('IMQ_SQS_SECRET_ACCESS_KEY')
    )
    sqs_queue_name = "%s_%s" % (queue_obj.name, env.cr.dbname,)
    sqs_queue = sqs_resource.get_queue_by_name(QueueName=sqs_queue_name)
    message_body_values = {
        'type': 'simple',
        'selector': selector,
        'payload': payload,
    }
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
    response = sqs_queue.send_message(**send_message_kwargs)
    _logger.debug("response={resp}".format(resp=response))
    return response

