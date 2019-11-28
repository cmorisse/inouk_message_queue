import os, threading
import sys
import traceback
from io import StringIO
import importlib
import logging
import json
import jsonpickle
import threading
import datetime
import math
import socket

from dateutil.relativedelta import relativedelta
import psycopg2
import time
import boto3

import openerp
from openerp import _, api, fields, models
from openerp.api import Environment
from odoo.exceptions import MissingError, UserError
#from odoo.addons.inouk_message_queue.api import unwrap_odoo_model
from ..api import unwrap_odoo_model, IMQError, IMQRetryableError, IMQTerminateException
from .message_processor import MAX_ATTEMPTS

# Must be equal to cron workers interval_number and interval_type
IMQ_SLEEP_INTERVAL = 60  


_logger = logging.getLogger('IMQWorker')


class IMQWorker(models.Model):
    """Processes messages in imq.queue"""
    _name = 'imq.worker'
    _description = "IMQ - Worker"

    def get_message(self, queue_obj):
        """Query SQS for next message to process.

        :param queue_obj: required openerp.Model of the queue to query.
        :return: a SQS message object 
        """
        
        _logger.debug("Queue[%s] querying next message to process.", 
                      queue_obj.name)
                      
        queue_name_prefix = queue_obj.name
        queue_obj = self.env['imq.queue'].search([('name', '=', queue_name_prefix)])
        if not queue_obj:
            raise UserError("Unknown queue:'%s' !!!" % queue_name_prefix)

        sqs_resource = boto3.resource(
            'sqs',
            region_name=queue_obj.region,   # os.environ.get('IMQ_SQS_REGION'),
            aws_access_key_id=queue_obj.key,   # os.environ.get('IMQ_SQS_ACCESS_KEY_ID'),
            aws_secret_access_key=queue_obj.secret,   # os.environ.get('IMQ_SQS_SECRET_ACCESS_KEY')
        )

        queue_name = "%s_%s" % (queue_name_prefix, self.env.cr.dbname,)
        sqs_queue = sqs_resource.get_queue_by_name(QueueName=queue_name)

        start_time = datetime.datetime.now()
        while (datetime.datetime.now() - start_time).seconds < 60:
            messages = sqs_queue.receive_messages(
                AttributeNames=[ 'All' ],
                MessageAttributeNames=[ 'All' ],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
            )
            if messages:
                message = messages[0]
                _logger.info("Queue[%s] got message: %s", 
                             queue_name_prefix, 
                             message)
                return message
        return None

    def store_sqs_message(self, queue_obj, sqs_message, start_timestamp=None):
        """ Stores SQS message in `imq.message`
        :returns: 'imq.message' model or None
        """
        message_model = self.env['imq.message']
        
        queue_message_id = sqs_message.message_id
        body = jsonpickle.decode(sqs_message.body)
        message_selector = body.get('selector', None)
        message_module = body.get('module_name', None)
        message_function = body.get('function_name', None)
        user_id = body.get('user_id', 
                           self.env.ref('inouk_message_queue.user_imq').id)
        
        message_attributes = sqs_message.message_attributes or {}
        name = message_attributes.get(
            'name', 
            {'StringValue':'Undefined'}
        )['StringValue']
        code = message_attributes.get(
            'code', 
            {'StringValue':'n/a'}
        )['StringValue']

        # search for processor
        processor_obj = self.env['imq.message_processor']\
            .upsert_processor_from_message(sqs_message)

        _logger.debug("Searching for message with queue_message_id='%s'", queue_message_id)
        message_obj = message_model.search(
            [('queue_message_id', '=', queue_message_id)]
        )
        _logger.debug("Found '%s' with queue_message_id=%s", message_obj or 'No Message', queue_message_id)
    
        epoch_s = int(
            sqs_message.attributes['ApproximateFirstReceiveTimestamp']
        ) / 1000
        context = body.get('context', {})
        message_values_dict = {
            'state': 'wip',
            'queue_message_id': sqs_message.message_id,
            'name': name,
            'code': code,
            'enqueued_time': datetime.datetime.utcfromtimestamp(epoch_s),
            'start_time': start_timestamp,
            'start_time_microseconds': start_timestamp and start_timestamp.microsecond,
            'queue_id': queue_obj.id,
            'group': sqs_message.attributes.get('MessageGroupId', None),
            'user_id': user_id,
            'context': jsonpickle.encode(body.get('context', {})),
            'payload': jsonpickle.encode(body.get('payload', {})),
            'raw_message_body': json.dumps(
                json.loads(sqs_message.body),
                sort_keys=True,
                indent=4
            ),
        }
        # Add only id present
        if '_imq_parent_message_id' in context:
            message_values_dict['parent_message_id'] = context['_imq_parent_message_id']
        if '_imq_target_children_count' in context:
            message_values_dict['target_children_count'] = context['_imq_target_children_count']

        if processor_obj:
            message_values_dict.update({
                'processor_id': processor_obj.id,
                'logging_activated': processor_obj.logging_activated,
                'capture_console': processor_obj.capture_console,
                'max_number_of_attempts': processor_obj.max_attempt,
            })
        else:
            message_values_dict.update({
                'processor_id': None,
                'logging_activated': False,
                'capture_console': False,
                'max_number_of_attempts': MAX_ATTEMPTS
            })

        if message_obj:  #update
            message_obj.write(message_values_dict)
        else:  #create
            message_values_dict['attempt'] = 0
            message_obj = message_model.create(message_values_dict)
        return message_obj

    def process_message(self, sqs_message, message_obj, worker_param):
        """ Processes a message.

        :param message_obj:
        :type message_obj: odoo.addons.inouk_message_queue.message.IMQMessage
        :return:
        """
        _logger.debug("Processing message with id=%s (%s)", 
                      message_obj.queue_message_id, 
                      message_obj.name)

        run_context = jsonpickle.decode(message_obj.context)
        run_context['_imq_message_id'] = sqs_message.message_id
        if worker_param:
            run_context['_imq_worker_param'] = worker_param

        raised = None
        try:
            # create an environment dedicated to function execution
            run_cursor = self.env.registry.cursor()
            _logger.debug("run_cursor: %s created.", run_cursor)
            run_env = Environment(run_cursor,
                                  message_obj.user_id.id,
                                  run_context)
    
            if message_obj.message_type == 'rpc':
                # use run_env to rebuild parameters (including openerp.Models)
                payload = unwrap_odoo_model(
                    run_env,
                    jsonpickle.decode(message_obj.payload)
                )
    
                if message_obj.processor_id.is_method:
                    _logger.debug("Executing 'method'.")

                    if message_obj.logging_activated and message_obj.processor_id.capture_log:
                        payload['kwargs']['_imq_logger'] = self.logger
                    
                    if message_obj.capture_console:
                        payload['kwargs']['_imq_stream'] = self._imq_stream 
                    
                    returned_value = getattr(
                        payload['self'], 
                        message_obj.processor_id.function
                    )(*payload['args'], **payload['kwargs'])

                else:
                    _logger.debug("Executing 'function'.")
    
                    if message_obj.logging_activated and message_obj.processor_id.capture_log:
                        payload['kwargs']['_imq_logger'] = self.logger
                    
                    if message_obj.capture_console:
                        payload['kwargs']['_imq_stream'] = self._imq_stream 
                    
                    function_module = importlib.import_module(
                        message_obj.processor_id.module, 
                        package=None
                    )
                    returned_value = getattr(
                        function_module, 
                        message_obj.processor_id.function
                    )(*payload['args'], **payload['kwargs'])

            else:
                if(message_obj.processor_id.module 
                   and message_obj.processor_id.function):
                    payload = json.loads(message_obj.payload)
                    function_module = importlib.import_module(
                        message_obj.processor_id.module, 
                        package=None
                    )
                    kwargs = {
                        '_imq_logger': self.logger
                    }
                    returned_value = getattr(
                        function_module, 
                        message_obj.processor_id.function
                    )(run_env, payload, **kwargs)
                else:
                    error_message = "No python function defined for "\
                                    "selector: %s on queue: %s." % (
                                        message_obj.processor_id.selector,
                                        message_obj.queue_id.name
                                    )
                    raise Exception(error_message)
            _logger.debug("message %s processed.", sqs_message.message_id)
            if run_env.has_todo():
                run_env.recompute()
            sqs_message.delete()  # Delete message from Cloud Queue
            run_cursor.commit()
            state = 'done'
            _logger.debug("run_cursor:%s committed.", run_cursor)

        except IMQError as imq_err:
            raised = imq_err
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            state = 'failed'
            sqs_message.delete()  # Delete message from Cloud Queue
            _logger.info("Deleted message:'%s' on SQS (IMQError)", sqs_message.message_id)
            if run_env.has_todo():
                run_env.recompute()
            run_cursor.commit()

        except IMQTerminateException as imq_err:
            raised = imq_err
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            state = 'terminated'
            sqs_message.delete()  # Delete message from Cloud Queue
            _logger.info("Deleted message:'%s' on SQS (IMQTerminateException)", sqs_message.message_id)
            run_cursor.rollback()
            run_env.clear()  # invalidates and purges todos

        except (IMQRetryableError, psycopg2.extensions.TransactionRollbackError,) as imq_rerr:
            raised = imq_rerr
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            if message_obj.attempt >= message_obj.max_number_of_attempts:
                state = 'failed'
                sqs_message.delete()  # Delete message from Cloud Queue
                _logger.debug("Deleted message:'%s' on SQS after %s failed attempts.", 
                              sqs_message.message_id,
                              message_obj.attempt
                             )
            else:
                state = 'retry' 
                # Task will retry after visibility timeout
            run_cursor.rollback()
            run_env.clear()  # invalidates and purges todos
            
        except Exception as exc:
            raised = exc
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            state = 'failed'
            sqs_message.delete()  # Delete message from Cloud Queue
            _logger.info("Deleted message:'%s' on SQS (%s)", 
                         sqs_message.message_id,
                         exc_type)
            run_cursor.rollback()
            run_env.clear()  # invalidates and purges todos
            
        finally:
            run_cursor.close()
            _logger.debug("run_cursor:%s closed." % run_cursor)

        if raised and message_obj.ikpdb_debug:
            try: 
                import ikp3db; ikp3db.post_mortem(exc_info[2])
            except ImportError:
                _logger.critical("ImportError: module 'ikpdb' or 'ikp3db' "
                                 "is not installed !")

        _logger.debug("Storing function returned_value as message processing "
                     "result: %s", returned_value)
                     
        if isinstance(returned_value, str) and returned_value[:9].lower()=='traceback':
            result = returned_value
        else:
            try:
                result = json.dumps(returned_value, indent=4)
            except:
                result = str(returned_value)
        return {
            'result': result,
            'state': state
        }

    def change_message_visibility(self, message_obj, sqs_message):
        """ Update message visibility with timeout defined in processor
        if any
        """
        m_timeout = message_obj.processor_id.visibility_timeout or 60
        if m_timeout:
            assert  message_obj.attempt>0, "Internal Error: attempt <= 0"
            sqs_message.change_visibility(VisibilityTimeout=m_timeout)

    @api.model
    def process_message_queue(self, queue_name, worker_name=None, worker_param=None):
        """ This is the IMQ Worker entry_point. This method is called regularly 
        by odoo.ir_cron to process message from the queue identified by 
        `queue_name`.
        For each message, creates an `imq.message` object then run code 
        defined in related processor.
        """
        host_name = socket.gethostname()
        stopped_workers_nodes = self.env["ir.config_parameter"].sudo().get_param("imq.STOP_WORKERS", "").split(',')
        if host_name in stopped_workers_nodes or '*' in stopped_workers_nodes:
            _logger.info("Leaving process_message_queue(queue_name=%s, worker_name=%s, "
                         "worker_param=%s) as host_name:%s is present in system "
                         "parameter 'imq.STOP_WORKERS'.",
                         queue_name, worker_name, worker_param, host_name)
            return

        queue_name = queue_name or 'default'
        _logger.debug("process_message_queue(queue_name=%s, worker_name=%s, "
                     "worker_param=%s)", 
                     queue_name, worker_name, worker_param)
        _logger.debug("    %s-%s pid/thread = %s/%x", 
                      queue_name, worker_name,
                      os.getpid(), 
                      threading.current_thread().ident,)
        _logger.debug("    %s-%s threading.current_thread().dbname=%s", 
                      queue_name, worker_name,
                      threading.current_thread().dbname)
        _logger.debug("    %s-%s Processing cursor: %s", 
                      queue_name, worker_name, 
                      self.env.cr)

        # retrieve queue or exit
        queue_model = self.env['imq.queue']
        queue_obj = queue_model.search([
            ('name', '=', queue_name)]
        ) if queue_name else None
        if not queue_obj:
            _logger.error("Queue '%s' does not exists. Exiting.", queue_name)
            return
        
        processing_start_timestamp = datetime.datetime.now()
        while True: 

            # Clear all ORM cache for Environment
            self.invalidate_cache()

            # query SQS for message
            sqs_message = self.get_message(queue_obj)
            
            # store message in log for user monitoring
            start_timestamp = datetime.datetime.now()
            if sqs_message:
                message_obj = self.store_sqs_message(queue_obj, sqs_message, start_timestamp)
                message_obj.write({
                    "attempt": message_obj.attempt + 1,
                })
                processing_obj = message_obj.create_processing_object()
                message_obj.env.cr.commit()

                self.change_message_visibility(message_obj, sqs_message)
                    
                if message_obj.processor_id:
                    self.start_log_capture(message_obj,
                                           processing_obj,
                                           log_level=message_obj.processor_id.log_level,
                                           log_format=message_obj.processor_id.log_format)
                    if message_obj.capture_console:
                        self.start_stream_capture(message_obj, processing_obj)
                    result_dict = self.process_message(sqs_message,
                                                       message_obj, 
                                                       worker_param)
                    self.stop_log_capture()
                    if message_obj.capture_console:
                        self.stop_stream_capture()
    
                else:  
                    result_dict = {
                        'state': 'failed',
                        'result': "No processor defined for message. Will retry in"
                                  " a few seconds."
                    }

                end_timestamp = datetime.datetime.now()
                result_dict['end_time'] = end_timestamp
                result_dict['end_time_microseconds'] = end_timestamp.microsecond
                message_obj.write(result_dict)
                processing_obj.write(result_dict)

                # TODO: Add a parameter to control deletion which is 
                # unnecessary if queue has a Dead Letter Queue mechanism
                if result_dict['state'] != 'done' and message_obj.attempt >= message_obj.max_number_of_attempts:
                    _logger.debug("Queue[%s] deleting message %s after %s "
                                  "failed attempts.",
                                  queue_obj.name,
                                  message_obj.queue_message_id,
                                  message_obj.attempt)
                    sqs_message.delete()

            # Store message log modifications
            self.env.cr.commit()
            _logger.debug("message/cron cursor:%s committed." % self.env.cr)
            
            processing_duration = (
                datetime.datetime.now() - processing_start_timestamp).seconds
            if processing_duration >= IMQ_SLEEP_INTERVAL:
                _logger.debug("Queue[%s] process_message_queue() exiting after "
                              "%ss processing time.", 
                              queue_obj.name, 
                              processing_duration)
                return

    
    def start_log_capture(
        self, message_obj, processing_obj, log_level=None, log_format="%(asctime)s %(name)s %(levelname)s %(message)s"
    ):
        """Start capturing log output to a string buffer.

        See. http://docs.python.org/release/2.6/library/logging.html
        @param new_log_level: Optionally change the global logging level, 
                              e.g. logging.DEBUG
        :param logger_name: name of the logger to setup
        :param log_level: optional requested log_level
        :type log_level: str
        :param log_format:
        :type log_format: str
        :return: nothing
        """
        logger_name = "IMQ_message_%s" % message_obj.id
        self.logger = logging.getLogger(logger_name)
        if log_level:
            self.logger.setLevel(int(log_level))

        self.log_handler = IMQLogHandler(message_obj, processing_obj)

        formatter = logging.Formatter(log_format)
        self.log_handler.setFormatter(formatter)
        self.logger.addHandler(self.log_handler)

    def stop_log_capture(self):
        """ Stop capturing log output.
        """
        self.log_handler.flush()

    def start_stream_capture(self, message_obj, processing_obj):
        """Start capturing log output to a string buffer.
        :return: nothing
        """
        self._imq_stream = MpyStringIO(message_obj, processing_obj)

    def stop_stream_capture(self):
        """ Stop capturing streams output.
        """
        self._imq_stream.stop_capture()
        self._imq_stream = None


class IMQLogHandler(logging.Handler):
    def __init__(self, message_obj, processing_obj):
        super(IMQLogHandler, self).__init__()
        self._env = message_obj.env
        self._message_id = message_obj.id
        self._processing_id = processing_obj.id
    
    def emit(self, record):
        try:
            self._env['imq.message_processing_log'].sudo().create({
                'message_id': self._message_id,
                'active_message_id': self._message_id,
                'processing_id': self._processing_id,
                'logger_name': record.name,
                'log_level': str(record.levelno),
                'log_message': record.msg % record.args
            })
            self._env.cr.commit()
        except:
            _logger.critical("Failed to log:%s with %s", record.msg, record.args)
        
    def flush(self):
        """ We can't commit in flush since flush can be called long after 
        last emit(), at a time where cursor has been released.
        """
        #self._env.cr.commit()
        pass


class MpyStringIO(StringIO):

    def __init__(self, message_obj, processing_obj):
        _logger.info("MpyStringIO(%s, %s)", message_obj, processing_obj)
        self._env = message_obj.env
        self._message_id = message_obj.id
        self._processing_id = processing_obj.id
        self._log_model = message_obj.env['imq.message_processing_log']
        self._mpy_buffer = ''
        super(MpyStringIO, self).__init__()
        
    def write(self, s:str):
        super(MpyStringIO, self).write(s)
        if '\n' in s:
            if s.endswith('\n'):
                new_buffer = ''
                output_str = s
            else:
                new_buffer = s[s.rfind('\n')+1:]
                output_str = s[:s.rfind('\n')+1]
        else:
            new_buffer = s
            output_str = ''

        self._log_model.create({
            'message_id': self._message_id,
            'active_message_id': self._message_id,
            'processing_id': self._processing_id,
            'logger_name': "Console",
            'log_level': None,
            'log_message': "%s%s" % (self._mpy_buffer, output_str)
        })
        self._env.cr.commit()
        #print(">>>>>>>>>>>>>>>>>>>>>>>>")
        #print("%s%s" % (self._mpy_buffer, output_str))
        #print("<<<<<<<<<<<<<<<<<<<<<<<<")
        self._mpy_buffer = new_buffer

    def stop_capture(self):
        self.flush()
        if self._mpy_buffer:
            self._log_model.sudo().create({
                'message_id': self._message_id,
                'active_message_id': self._message_id,
                'processing_id': self._processing_id,
                'logger_name': "Console",
                'log_level': None,
                'log_message': "%s" % (self._mpy_buffer)
            })
            self._env.cr.commit()
        self._mpy_buffer = None

