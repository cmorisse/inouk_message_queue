import os
import multiprocessing
import threading
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

import odoo
from odoo import _, api, fields, models
from odoo.api import Environment
from odoo.exceptions import MissingError, UserError
#from odoo.addons.inouk_message_queue.api import unwrap_odoo_model
from ..api import unwrap_odoo_model, IMQError, IMQRetryableError, IMQTerminateException
from .message_processor import MAX_ATTEMPTS


# Must be equal to cron workers interval_number and interval_type
IMQ_SLEEP_INTERVAL = 20 
TLS = threading.local()


_logger = logging.getLogger('IMQWorker')


class IMQWorker(models.AbstractModel):
    """Processes messages in imq.queue"""
    _name = 'imq.worker'
    _description = "IMQ - Worker"
    
    def get_message(self, queue_obj, wait_time=0):
        """Query Q for next message to process.

        :param queue_obj: required openerp.Model of the queue to query.
        :return: a SQS message object 
        """
        _logger.debug("get_message for Q=%s/%s", queue_obj.name, queue_obj.provider)
        _get_message_method_name = "get_message__%s" % queue_obj.provider
        _get_message_method = getattr(self, _get_message_method_name)
        #_message = _get_message_method(queue_obj, wait_time=wait_time)
        _message = _get_message_method(queue_obj)
        return _message

    def store_message(self, queue_obj, message, start_timestamp=None):
        """ Store message in `imq.message` in a ready to process state
        :returns: 'imq.message' object or None
        """
        _store_message_method_name = "store_message__%s" % queue_obj.provider
        _store_message_method = getattr(self, _store_message_method_name)
        _message = _store_message_method(queue_obj, message, start_timestamp=start_timestamp)
        return _message

    def terminate_message(self, queue_obj, message):
        """ Terminate the message.
        :returns: True if message has been deleted.
        """
        _terminate_message_method_name = "terminate_message__%s" % queue_obj.provider
        _terminate_message_method = getattr(self, _terminate_message_method_name)
        _result = _terminate_message_method(queue_obj, message)
        return _result

    def process_message(self, message_obj, message, worker_param):
        """ Processes a message.
        :param message_obj:
        :type message_obj: odoo.addons.inouk_message_queue.message.IMQMessage
        :return:
        """
        def extract_orm_object(input):
            """ Extract the first ORM obj in given iterable """
            if isinstance(input, list):
                for _o in input:
                    if isinstance(_o, odoo.models.BaseModel):
                        return _o
            elif isinstance(input, dict):
                for _o in input:
                    if isinstance(input[_o], odoo.models.BaseModel):
                        return input[_o]
            else:
                raise Exception("Unsupported input type for extract_orm_object().")
            return None

        def strfdelta(tdelta, fmt):
            d = {"days": tdelta.days}
            d["hours"], rem = divmod(tdelta.seconds, 3600)
            d["minutes"], d["seconds"] = divmod(rem, 60)            
            return fmt.format(**d)        

        _logger.debug("Processing message with id=%s (%s)", 
                      message_obj.queue_message_id, 
                      message_obj.name)

        run_context = jsonpickle.decode(message_obj.context or '{}')
        run_context['_imq_message_id'] = message_obj.queue_message_id
        if worker_param:
            run_context['_imq_worker_param'] = worker_param

        msg_processor_obj = message_obj.processor_id
        queue_obj = message_obj.queue_id

        if msg_processor_obj.notify_message_processing_start:
            message_obj.queue_id.send_notification(
                'info',
                "Start processing",
                "",
                message_obj=message_obj
            )

        start_timestamp = datetime.datetime.now()
        raised = None
        retry_delay_s = None

        try:
            # create a new environment dedicated to function execution
            # thus we gather all information stored in message env
            _uid = message_obj.user_id.id
            _message_type = message_obj.message_type
            _payload = message_obj.payload
            _logging_activated = message_obj.logging_activated
            _capture_console = message_obj.capture_console
            _capture_log = msg_processor_obj.capture_log
            _is_method = msg_processor_obj.is_method
            _module = msg_processor_obj.module
            _function = msg_processor_obj.function
            
            # create a db cursor and Environment dedicated to execution
            run_cursor = self.env.registry.cursor()
            _logger.debug("run_cursor: %s created.", run_cursor)
            run_env = Environment(
                run_cursor,
                message_obj.user_id.id,
                run_context,
                su=run_context.get('_imq_su', False)
            )
    
            if message_obj.message_type == 'rpc':
                # use run_env to rebuild parameters (including openerp.Models)
                payload = unwrap_odoo_model(
                    run_env,
                    jsonpickle.decode(message_obj.payload)
                )

                if message_obj.logging_activated and msg_processor_obj.capture_log:
                    payload['kwargs']['_imq_logger'] = TLS._logger

                if message_obj.capture_console:
                    payload['kwargs']['_imq_stream'] = TLS._imq_stream

                if msg_processor_obj.is_method:
                    _logger.debug("Executing 'method'.")
                    returned_value = getattr(
                        payload['self'],
                        msg_processor_obj.function
                    )(*payload['args'], **payload['kwargs'])

                    # Purge
                    run_env.flush_all()
                    run_cursor.commit()

                else:
                    _logger.debug("Executing 'function'.")                    
                    function_module = importlib.import_module(
                        message_obj.processor_id.module, 
                        package=None
                    )
                    message_obj.flush_recordset()
                    returned_value = getattr(
                        function_module, 
                        msg_processor_obj.function
                    )(*payload['args'], **payload['kwargs'])

                    # Purge
                    orm_object = extract_orm_object(payload['args'])
                    if orm_object:
                        orm_object.flush_recordset()
                    run_cursor.commit()

            else:  # message_type == 'simple'
                if(msg_processor_obj.module and msg_processor_obj.function):
                    # We extract payload from raw_message_body to support simple message
                    # injected into a queue
                    if message_obj.payload:
                        payload = json.loads(message_obj.payload)
                    else:
                        _rmb = json.loads(message_obj.raw_message_body)
                        payload = _rmb['payload']
                    function_module = importlib.import_module(
                        msg_processor_obj.module, 
                        package=None
                    )
                    kwargs = {
                        '_imq_logger': TLS._logger
                    }
                    if message_obj.capture_console:
                        payload['kwargs']['_imq_stream'] = TLS._imq_stream                    
                    message_obj.flush_recordset()
                    returned_value = getattr(
                        function_module, 
                        msg_processor_obj.function
                    )(run_env, payload, **kwargs)
                else:
                    error_message = "No python function defined for "\
                                    "selector: %s on queue: %s." % (
                                        msg_processor_obj.selector,
                                        message_obj.queue_id.name
                                    )
                    raise Exception(error_message)
                run_cursor.commit()
            
            end_timestamp = datetime.datetime.now()
            duration_str = strfdelta(end_timestamp-start_timestamp, "{hours}hours{minutes}min{seconds}s")
            _logger.debug("message %s processed (duration=%s).", 
                          message_obj.queue_message_id, 
                          duration_str)

            _logger.debug("run_cursor:%s committed.", run_cursor)

            self.terminate_message(queue_obj, message)
            state = 'done'

            if msg_processor_obj.notify_message_processing_end:
                message_obj.queue_id.send_notification(
                    'success',
                    "Processing done without error",
                    message="Duration=%s" % duration_str,
                    icon=":white_check_mark:",
                    message_obj=message_obj
                )

        except IMQError as imq_err:  
            # commit, state = Failed, No retry (message removed from queue)
            raised = imq_err
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            try:
                results = json.dumps(imq_err.results, indent=4)
            except:
                results = str(imq_err.results)
            returned_value += "\n" + "-" * 80 + "\n" + results     
            
            state = 'failed'
            self.terminate_message(queue_obj, message)

            _logger.info(
                "Deleted %s on queue '%s' (IMQError)", 
                message_obj,
                message_obj.queue_message_id
            )

            run_cursor.commit()
            run_env.clear()

            if msg_processor_obj.notify_message_processing_fail:
                message_obj.queue_id.send_notification(
                    'danger',
                    "Processing failed!",
                    "*IMQError* has been raised.",
                    icon=":x:",
                    message_obj=message_obj
                )

        except IMQTerminateException as imq_err:
            # rollback, state = Terminated, No retry (message removed from queue)
            raised = imq_err
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type,
                                                        exc_value,
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            try:
                results = json.dumps(imq_err.results, indent=4)
            except:
                results = str(imq_err.results)
            returned_value += "\n" + "-" * 80 + "\n" + results     

            state = 'terminated'
            self.terminate_message(queue_obj, message)
            _logger.info("Deleted message:'%s' on queue (IMQTerminateException)", message_obj.queue_message_id)
            run_cursor.rollback()
            run_env.clear()  # invalidates and purges todos

            if msg_processor_obj.notify_message_processing_terminate:
                message_obj.queue_id.send_notification(
                    'warning',
                    "Processing terminated !",
                    "*IMQTerminateException* has been raised.",
                    icon=":bangbang:",
                    message_obj=message_obj
                )

        except (
            IMQRetryableError,
            psycopg2.extensions.TransactionRollbackError,
            psycopg2.IntegrityError
        ) as imq_rerr:
            # rollback, state = Retry, Retry
            raised = imq_rerr
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)

            returned_value = "\n".join(returned_value)
            if message_obj.attempt >= message_obj.max_number_of_attempts:
                state = 'failed'
                self.terminate_message(queue_obj, message)
                _logger.debug("Deleted message:'%s' on SQS after %s failed attempts.", 
                              message_obj.queue_message_id,
                              message_obj.attempt
                             )
                if msg_processor_obj.notify_message_processing_fail:
                    message_obj.queue_id.send_notification(
                        'danger',
                        "Retryable processing failed",
                        "*IMQRetryableError* has been raised %s times (max attempt)." % message_obj.max_number_of_attempts,
                        icon=":x:",
                        message_obj=message_obj
                    )

            else:
                if msg_processor_obj.notify_message_processing_retry:
                    message_obj.queue_id.send_notification(
                        'warning',
                        "Retry processing",
                        "Retry processing attempt #%s (*IMQRetryableError* has been raised)." % message_obj.attempt,
                        icon=":warning:",
                        message_obj=message_obj
                    )
                state = 'retry'   # Task will retry after visibility timeout or delay
                if hasattr(imq_rerr, 'delay') and imq_rerr.delay:
                    retry_delay_s = imq_rerr.delay
                elif msg_processor_obj.retry_delay_s:
                    retry_delay_s = msg_processor_obj.retry_delay_s

            run_cursor.rollback()
            run_env.clear()  # invalidates and purges todos

        except (psycopg2.OperationalError) as imq_rerr:
            raised = imq_rerr
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            _logger.error(returned_value)
            return {"state": "psycopg2_Error"}
            
        except Exception as exc:
            # rollback, state = Failed, No retry (message removed from queue)
            raised = exc
            exc_type, exc_value, exc_traceback = exc_info = sys.exc_info()
            returned_value = traceback.format_exception(exc_type, 
                                                        exc_value, 
                                                        exc_traceback)
            returned_value = "\n".join(returned_value)
            state = 'failed'
            _logger.error(returned_value)
            self.terminate_message(queue_obj, message)
            _logger.info("Deleted message:'%s' on queue. (%s)", 
                         message_obj.queue_message_id,
                         exc_type)
            run_cursor.rollback()
            run_env.clear()  # invalidates and purges todos
            if msg_processor_obj.notify_message_processing_fail:
                message_obj.queue_id.send_notification(
                    'danger',
                    "Processing failed",
                    "*%s* has been raised." % repr(exc_value),
                    icon=":x:",
                    message_obj=message_obj
                )

        finally:
            run_cursor.close()
            _logger.debug("run_cursor:%s closed." % run_cursor)

        if raised and message_obj.ikpdb_debug:
            try: 
                import ikp3db; ikp3db.post_mortem(exc_info[2])
            except ImportError:
                _logger.critical("ImportError: Failed to import 'ikp3db'.")

        _logger.debug("Storing function returned_value as message processing "
                     "result: %s", returned_value)
                     
        if isinstance(returned_value, str) and returned_value[:9].lower()=='traceback':
            result = returned_value
        else:
            try:
                result = json.dumps(returned_value, indent=4)
            except:
                result = str(returned_value)

        _r = {
            'result': result,
            'state': state
        }
        if retry_delay_s:
            _r['planned_time'] = fields.Datetime.to_string(datetime.datetime.now() + datetime.timedelta(seconds=retry_delay_s))

        return _r

    def change_message_visibility(self, queue_obj, message_obj, _message):
        """ Update message visibility with timeout defined in processor
        :param message_obj: local imq.message for _message
        :param _message: The Q message object (sqs_messqge, ...)
        """
        if message_obj.processor_id.force_visibility_timeout:
            _change_message_visibility_method_name = "change_message_visibility__%s" % message_obj.queue_provider
            _change_message_visibility_method = getattr(self, _change_message_visibility_method_name)
            _change_message_visibility_method(queue_obj, message_obj, _message)

    @api.model
    def process_message_queue(self, queue_name, worker_name=None, worker_param=None):
        """ This is the IMQ Worker entry_point. This method is called regularly
        by odoo.ir_cron to process message from the queue identified by
        `queue_name`.
        For each message, creates an `imq.message` object then run code
        defined in related processor.
        """
        host_name = socket.gethostname()
        queue_name = queue_name or 'default'
        # Check for new parameter first, then fall back to old parameter for backward compatibility
        stopped_workers_nodes = self.env["ir.config_parameter"].sudo().get_param("imq.STOP_CRON_WORKERS", "").split(',')
        
        # Backward compatibility: check old parameter if new one is empty
        if not stopped_workers_nodes or (len(stopped_workers_nodes) == 1 and stopped_workers_nodes[0] == ''):
            old_param = self.env["ir.config_parameter"].sudo().get_param("imq.STOP_WORKERS", "")
            if old_param:
                stopped_workers_nodes = old_param.split(',')
                _logger.warning(
                    "Using deprecated parameter 'imq.STOP_WORKERS'. Please migrate to 'imq.STOP_CRON_WORKERS' for cron workers."
                )

        if host_name in stopped_workers_nodes or '*' in stopped_workers_nodes:
            _logger.debug(
                "[WorkerCron=%s,Q=%s,Wn=%s,Wp=%s,threadid=%s] leaving process_message_queue() since "
                "host_name:%s is present in system parameter 'imq.STOP_CRON_WORKERS' or 'imq.STOP_WORKERS'.",
                os.getpid(),
                queue_name,
                worker_name,
                worker_param,
                threading.current_thread().ident,
                host_name
            )
            return

        _logger.debug(
            "[WorkerCron=%s,Q=%s,Wn=%s,Wp=%s,threadid=%s] Entering process_message_queue() with threading.current_thread().dbname=%s,processing_cursor:%s",
            os.getpid(),
            queue_name, 
            worker_name,
            worker_param,
            threading.current_thread().ident,
            threading.current_thread().dbname,
            self.env.cr
        )

        # retrieve queue or exit
        queue_model = self.env['imq.queue']
        queue_obj = queue_model.with_context(active_test=False).search([
            ('name', '=', queue_name),
        ]) if queue_name else None
        if not queue_obj:
            _logger.error("Queue '%s' does not exists. Exiting.", queue_name)
            return
        if not queue_obj.active:
            _logger.warning("Queue '%s' is not active. Exiting.", queue_name)
            return
        _logger.debug("Polling queue '%s'/%s for new message.", queue_name, queue_obj.id)

        processing_start_timestamp = datetime.datetime.now()

        _message = self.get_message(queue_obj)

        start_timestamp = datetime.datetime.now()
        if _message:
            message_obj = self.store_message(queue_obj, _message, start_timestamp)
            message_obj.write({
                "attempt": message_obj.attempt + 1,
            })
            processing_obj = message_obj.create_processing_object()
            self.change_message_visibility(queue_obj, message_obj, _message)
            message_obj.flush_recordset()
            self.env.cr.commit() 

            processor_obj = message_obj.processor_id
            if processor_obj:
                self.start_log_capture(message_obj,
                                        processing_obj,
                                        log_level=processor_obj.log_level,
                                        log_format=processor_obj.log_format)

                if message_obj.capture_console:
                    self.start_stream_capture(message_obj, processing_obj)

                result_dict = self.process_message(
                    message_obj,
                    _message,
                    worker_param
                )

                if result_dict['state'] == '"psycopg2_Error"':
                    return  # To Abort
                self.stop_log_capture()
                if message_obj.capture_console:
                    self.stop_stream_capture()

            else:
                result_dict = {
                    'state': 'failed',
                    'result': "No processor defined for message. Will retry in a few seconds."
                }

            end_timestamp = datetime.datetime.now()
            result_dict['end_time'] = end_timestamp
            result_dict['end_time_microseconds'] = end_timestamp.microsecond
            message_obj.write(result_dict)
            if 'planned_time' in result_dict:
                del result_dict['planned_time']
            processing_obj.write(result_dict)
            message_obj.flush_recordset() ; self.env.cr.commit()

            # Delete message after MAX_ATTEMPT
            if result_dict['state'] != 'done' and message_obj.attempt >= message_obj.max_number_of_attempts:
                _logger.debug("Queue[%s] deleting message %s after %s "
                                "failed attempts.",
                                queue_obj.name,
                                message_obj.queue_message_id,
                                message_obj.attempt)
                self.terminate_message(queue_obj, _message)
                message_obj.flush_recordset() ; self.env.cr.commit()

        # Store message log modifications
        queue_obj.flush_recordset()
        self.env.cr.commit()
        _logger.debug("[WorkerCron=%s,Q=%s,Wn=%s,Wp=%s,threadid=%s] Processing cursor:%s committed.",
                        os.getpid(),
                        queue_name,
                        worker_name,
                        worker_param,
                        threading.current_thread().ident,
                        self.env.cr)

        processing_duration = (
            datetime.datetime.now() - processing_start_timestamp).seconds

        _logger.debug("[WorkerCron=%s,Q=%s,Wn=%s,Wp=%s,threadid=%s] process_message_queue() exiting"
                        " after %ss processing time.",
                        os.getpid(),
                        queue_name,
                        worker_name,
                        worker_param,
                        threading.current_thread().ident,
                        processing_duration)
        return

    def start_log_capture(
        self, message_obj, processing_obj, log_level=None, 
        log_format="%(asctime)s %(name)s %(levelname)s %(message)s"
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
        TLS._logger = logging.getLogger(logger_name)
        if log_level:
            TLS._logger.setLevel(int(log_level))

        TLS._log_handler = IMQLogHandler(message_obj, processing_obj)

        formatter = logging.Formatter(log_format)
        TLS._log_handler.setFormatter(formatter)
        TLS._logger.addHandler(TLS._log_handler)

    def stop_log_capture(self):
        """ Stop capturing log output.
        """
        TLS._log_handler.flush()

    def start_stream_capture(self, message_obj, processing_obj):
        """Start capturing log output to a string buffer.
        :return: nothing
        """
        TLS.log_cursor = self.env.registry.cursor()
        TLS._imq_stream = MpyStringIO(
            message_obj.id, 
            processing_obj.id, 
            message_obj.user_id.id,
            TLS.log_cursor
        )

    def stop_stream_capture(self):
        """ Stop capturing streams output.
        """
        TLS._imq_stream.stop_capture()
        TLS.log_cursor.close()
        TLS._imq_stream = None


class IMQLogHandler(logging.Handler):
    def __init__(self, message_obj, processing_obj):
        super(IMQLogHandler, self).__init__()
        self._env = message_obj.env
        self._message_id = message_obj.id
        self._processing_id = processing_obj.id
    
    def emit(self, record):
        try:
            if record.args:
                _msg = record.msg % record.args
            else:
                _msg = record.msg
        except Exception as e1: 
            _logger.exception(e1)
            _msg = "Failed to log:%s with %s" % (str(record.msg), str(record.args))
        try:
            self._env['imq.message_processing_log'].sudo().create({
                'message_id': self._message_id,
                'active_message_id': self._message_id,
                'processing_id': self._processing_id,
                'logger_name': record.name,
                'log_level': str(record.levelno),
                'log_message': _msg
            })
            self._env.cr.commit()
        except Exception as e2:
            _logger.exception(e2)
        
    def flush(self):
        """ We can't commit in flush since flush can be called long after 
        last emit(), at a time where cursor has been released.
        """
        #self._env.cr.commit()
        pass


class MpyStringIO(StringIO):
    def __init__(self, message_id, processing_id, uid, log_cr):
        _logger.info("MpyStringIO(%s, %s, %s, %s)", message_id, processing_id, uid, log_cr)
        self._message_id = message_id
        self._processing_id = processing_id
        self._log_cr = log_cr
        self._uid = uid
        self._mpy_buffer = ''
        super().__init__()
        
    def write(self, s):
        _logger.debug("MpyStringIO.write(%s)", repr(s))
        super().write(s)
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

        # self._log_model.create({
        #     'message_id': self._message_id,
        #     'active_message_id': self._message_id,
        #     'processing_id': self._processing_id,
        #     'logger_name': "Console",
        #     'log_level': None,
        #     'log_message': "%s%s" % (self._mpy_buffer, output_str)
        # })
        _now = datetime.datetime.now()
        self._log_cr.execute(
            """INSERT INTO imq_message_processing_log ( 
                    processing_id, message_id, active_message_id, logger_name, log_level, log_message, 
                    create_uid, create_date, write_uid, write_date
                ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );""",
                (
                    self._processing_id,
                    self._message_id,
                    self._message_id,
                    "Console",
                    None,
                    "%s%s" % (self._mpy_buffer, output_str),
                    self._uid,
                    _now,
                    self._uid,
                    _now,
                )
        )
        self._log_cr.commit()
        #print(">>>>>>>>>>>>>>>>>>>>>>>>")
        #print("%s%s" % (self._mpy_buffer, output_str))
        #print("<<<<<<<<<<<<<<<<<<<<<<<<")
        self._mpy_buffer = new_buffer

    #from typing import List
    #def writelines(self, __lines:List[str]) -> None:
    #    return super().writelines(__lines)

    def stop_capture(self):
        self.flush()
        if self._mpy_buffer:
            # self._log_model.sudo().create({
            #     'message_id': self._message_id,
            #     'active_message_id': self._message_id,
            #     'processing_id': self._processing_id,
            #     'logger_name': "Console",
            #     'log_level': None,
            #     'log_message': "%s" % (self._mpy_buffer)
            # })
            # self._env.cr.commit()
            _now = datetime.datetime.now()
            self._log_cr.execute(
                """INSERT INTO imq_message_processing_log ( 
                        processing_id, message_id, active_message_id, logger_name, log_level, log_message, 
                        create_uid, create_date, write_uid, write_date
                    ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );""",
                    (
                        self._processing_id,
                        self._message_id,
                        self._message_id,
                        "Console",
                        None,
                        "%s" % (self._mpy_buffer),
                        self._uid,
                        _now,
                        self._uid,
                        _now,
                    )
            )
            self._log_cr.commit()
        self._mpy_buffer = None

