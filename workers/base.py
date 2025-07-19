#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import traceback
import importlib
import logging
import json
import jsonpickle
import datetime
import threading
from io import StringIO

import psycopg2
import odoo
from odoo import _, api, fields
from odoo.api import Environment
from odoo.exceptions import MissingError, UserError

from ..api import unwrap_odoo_model, IMQError, IMQRetryableError, IMQTerminateException

_logger = logging.getLogger(__name__)

# Thread-local storage for logging
TLS = threading.local()


class BaseWorker:
    """Base worker functionality extracted from imq.worker model"""
    
    def __init__(self):
        self.logger = _logger
    
    def get_message(self, env, queue_obj, wait_time=0, target_message_id=None):
        """Query queue for next message to process
        
        Args:
            env: Odoo environment
            queue_obj: Queue object to query
            wait_time: Time to wait for message (currently unused)
            target_message_id: Optional specific message ID to target
            
        Returns:
            Message object or None
        """
        self.logger.debug("get_message for Q=%s/%s, target_msg=%s", 
                         queue_obj.name, queue_obj.provider, target_message_id)
        
        # Use provider-specific method from worker model
        worker_model = env['imq.worker']
        method_name = f"get_message__{queue_obj.provider}"
        if hasattr(worker_model, method_name):
            method = getattr(worker_model, method_name)
            return method(queue_obj, wait_time, target_message_id)
        else:
            # Fallback to original model method (without target_message_id support)
            if target_message_id:
                self.logger.warning(f"Provider {queue_obj.provider} doesn't support message targeting, ignoring --message parameter")
            return worker_model.get_message(queue_obj, wait_time)
    
    def store_message(self, env, queue_obj, message, start_timestamp=None):
        """Store message in imq.message in a ready to process state
        
        Args:
            env: Odoo environment
            queue_obj: Queue object
            message: Message to store
            start_timestamp: Processing start timestamp
            
        Returns:
            imq.message object or None
        """
        # Use provider-specific method
        method_name = f"store_message__{queue_obj.provider}"
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method(env, queue_obj, message, start_timestamp)
        else:
            # Fallback to original model method
            worker_model = env['imq.worker']
            return worker_model.store_message(queue_obj, message, start_timestamp)
    
    def terminate_message(self, env, queue_obj, message):
        """Terminate the message
        
        Args:
            env: Odoo environment
            queue_obj: Queue object
            message: Message to terminate
            
        Returns:
            True if message has been deleted
        """
        # Use provider-specific method
        method_name = f"terminate_message__{queue_obj.provider}"
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            return method(env, queue_obj, message)
        else:
            # Fallback to original model method
            worker_model = env['imq.worker']
            return worker_model.terminate_message(queue_obj, message)
    
    def process_message(self, env, message_obj, message, worker_param=None):
        """Process a message
        
        Args:
            env: Odoo environment 
            message_obj: imq.message object
            message: Raw message object
            worker_param: Worker parameters
            
        Returns:
            dict: Processing result with state and result
        """
        def extract_orm_object(input_obj):
            """Extract the first ORM obj in given iterable"""
            if isinstance(input_obj, list):
                for obj in input_obj:
                    if isinstance(obj, odoo.models.BaseModel):
                        return obj
            elif isinstance(input_obj, dict):
                for obj in input_obj.values():
                    if isinstance(obj, odoo.models.BaseModel):
                        return obj
            return None

        def strfdelta(tdelta, fmt):
            """Format timedelta"""
            d = {"days": tdelta.days}
            d["hours"], rem = divmod(tdelta.seconds, 3600)
            d["minutes"], d["seconds"] = divmod(rem, 60)
            return fmt.format(**d)

        self.logger.debug("Processing message with id=%s (%s)", 
                         message_obj.queue_message_id, 
                         message_obj.name)

        # Prepare execution context
        run_context = jsonpickle.decode(message_obj.context or '{}')
        run_context['_imq_message_id'] = message_obj.queue_message_id
        if worker_param:
            run_context['_imq_worker_param'] = worker_param

        msg_processor_obj = message_obj.processor_id
        queue_obj = message_obj.queue_id

        # Send start notification if configured
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
        returned_value = None
        state = 'failed'

        # Create dedicated cursor and environment for execution
        run_cursor = env.registry.cursor()
        try:
            self.logger.debug("run_cursor: %s created.", run_cursor)
            run_env = Environment(run_cursor, message_obj.user_id.id, run_context)

            try:
                if message_obj.message_type == 'rpc':
                    # RPC message processing
                    payload = unwrap_odoo_model(
                        run_env,
                        jsonpickle.decode(message_obj.payload)
                    )

                    # Add logger if requested
                    if message_obj.logging_activated and msg_processor_obj.capture_log:
                        payload['kwargs']['_imq_logger'] = getattr(TLS, '_logger', self.logger)

                    # Add console capture if requested
                    if message_obj.capture_console:
                        payload['kwargs']['_imq_stream'] = getattr(TLS, '_imq_stream', None)

                    if msg_processor_obj.is_method:
                        # Execute method
                        self.logger.debug("Executing 'method'.")
                        returned_value = getattr(
                            payload['self'],
                            msg_processor_obj.function
                        )(*payload['args'], **payload['kwargs'])
                        
                        # Flush and commit
                        payload['self'].flush()
                        run_cursor.commit()
                    else:
                        # Execute function
                        self.logger.debug("Executing 'function'.")
                        function_module = importlib.import_module(
                            msg_processor_obj.module, 
                            package=None
                        )
                        message_obj.flush()
                        returned_value = getattr(
                            function_module, 
                            msg_processor_obj.function
                        )(*payload['args'], **payload['kwargs'])
                        
                        # Flush and commit
                        orm_object = extract_orm_object(payload['args'])
                        if orm_object:
                            orm_object.flush()
                        run_cursor.commit()

                else:  # message_type == 'simple'
                    if msg_processor_obj.module and msg_processor_obj.function:
                        # Extract payload
                        if message_obj.payload:
                            payload = json.loads(message_obj.payload)
                        else:
                            rmb = json.loads(message_obj.raw_message_body)
                            payload = rmb['payload']

                        # Execute function
                        function_module = importlib.import_module(
                            msg_processor_obj.module, 
                            package=None
                        )
                        kwargs = {
                            '_imq_logger': getattr(TLS, '_logger', self.logger)
                        }
                        if message_obj.capture_console:
                            kwargs['_imq_stream'] = getattr(TLS, '_imq_stream', None)
                        
                        message_obj.flush()
                        returned_value = getattr(
                            function_module, 
                            msg_processor_obj.function
                        )(run_env, payload, **kwargs)
                    else:
                        error_message = f"No python function defined for " \
                                       f"selector: {msg_processor_obj.selector} " \
                                       f"on queue: {message_obj.queue_id.name}."
                        raise Exception(error_message)
                    
                    run_cursor.commit()

                # Success
                end_timestamp = datetime.datetime.now()
                duration_str = strfdelta(end_timestamp - start_timestamp, 
                                       "{hours}hours{minutes}min{seconds}s")
                self.logger.debug("message %s processed (duration=%s).", 
                                 message_obj.queue_message_id, 
                                 duration_str)

                self.terminate_message(env, queue_obj, message)
                state = 'done'

                # Send success notification if configured
                if msg_processor_obj.notify_message_processing_end:
                    message_obj.queue_id.send_notification(
                        'success',
                        "Processing done without error",
                        message=f"Duration={duration_str}",
                        icon=":white_check_mark:",
                        message_obj=message_obj
                    )

            except IMQError as imq_err:
                # Commit, state = Failed, No retry (message removed from queue)
                raised = imq_err
                exc_type, exc_value, exc_traceback = sys.exc_info()
                returned_value = traceback.format_exception(exc_type, exc_value, exc_traceback)
                returned_value = "\n".join(returned_value)
                
                try:
                    results = json.dumps(imq_err.results, indent=4)
                except:
                    results = str(imq_err.results)
                returned_value += "\n" + "-" * 80 + "\n" + results

                state = 'failed'
                self.terminate_message(env, queue_obj, message)
                
                self.logger.info("Deleted %s on queue '%s' (IMQError)", 
                                message_obj, message_obj.queue_message_id)
                
                run_cursor.commit()
                run_env.clear()

                # Send failure notification if configured
                if msg_processor_obj.notify_message_processing_fail:
                    message_obj.queue_id.send_notification(
                        'danger',
                        "Processing failed!",
                        "*IMQError* has been raised.",
                        icon=":x:",
                        message_obj=message_obj
                    )

            except IMQTerminateException as imq_err:
                # Rollback, state = Terminated, No retry (message removed from queue)
                raised = imq_err
                exc_type, exc_value, exc_traceback = sys.exc_info()
                returned_value = traceback.format_exception(exc_type, exc_value, exc_traceback)
                returned_value = "\n".join(returned_value)
                
                try:
                    results = json.dumps(imq_err.results, indent=4)
                except:
                    results = str(imq_err.results)
                returned_value += "\n" + "-" * 80 + "\n" + results

                state = 'terminated'
                self.terminate_message(env, queue_obj, message)
                self.logger.info("Deleted message:'%s' on queue (IMQTerminateException)", 
                                message_obj.queue_message_id)
                
                run_cursor.rollback()
                run_env.clear()

                # Send terminate notification if configured
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
                # Rollback, state = Retry, Retry
                raised = imq_rerr
                exc_type, exc_value, exc_traceback = sys.exc_info()
                returned_value = traceback.format_exception(exc_type, exc_value, exc_traceback)
                returned_value = "\n".join(returned_value)

                if message_obj.attempt >= message_obj.max_number_of_attempts:
                    state = 'failed'
                    self.terminate_message(env, queue_obj, message)
                    self.logger.debug("Deleted message:'%s' after %s failed attempts.", 
                                     message_obj.queue_message_id, message_obj.attempt)
                    
                    if msg_processor_obj.notify_message_processing_fail:
                        message_obj.queue_id.send_notification(
                            'danger',
                            "Retryable processing failed",
                            f"*IMQRetryableError* has been raised {message_obj.max_number_of_attempts} times (max attempt).",
                            icon=":x:",
                            message_obj=message_obj
                        )
                else:
                    if msg_processor_obj.notify_message_processing_retry:
                        message_obj.queue_id.send_notification(
                            'warning',
                            "Retry processing",
                            f"Retry processing attempt #{message_obj.attempt} (*IMQRetryableError* has been raised).",
                            icon=":warning:",
                            message_obj=message_obj
                        )
                    
                    state = 'retry'
                    if hasattr(imq_rerr, 'delay') and imq_rerr.delay:
                        retry_delay_s = imq_rerr.delay
                    elif msg_processor_obj.retry_delay_s:
                        retry_delay_s = msg_processor_obj.retry_delay_s

                run_cursor.rollback()
                run_env.clear()

            except psycopg2.OperationalError as imq_rerr:
                # Special handling for operational errors
                raised = imq_rerr
                exc_type, exc_value, exc_traceback = sys.exc_info()
                returned_value = traceback.format_exception(exc_type, exc_value, exc_traceback)
                returned_value = "\n".join(returned_value)
                self.logger.error(returned_value)
                return {"state": "psycopg2_Error"}

            except Exception as exc:
                # Rollback, state = Failed, No retry (message removed from queue)
                raised = exc
                exc_type, exc_value, exc_traceback = sys.exc_info()
                returned_value = traceback.format_exception(exc_type, exc_value, exc_traceback)
                returned_value = "\n".join(returned_value)
                
                state = 'failed'
                self.logger.error(returned_value)
                self.terminate_message(env, queue_obj, message)
                self.logger.info("Deleted message:'%s' on queue. (%s)", 
                                message_obj.queue_message_id, exc_type)
                
                run_cursor.rollback()
                run_env.clear()
                
                if msg_processor_obj.notify_message_processing_fail:
                    message_obj.queue_id.send_notification(
                        'danger',
                        "Processing failed",
                        f"*{repr(exc)}* has been raised.",
                        icon=":x:",
                        message_obj=message_obj
                    )

        finally:
            run_cursor.close()
            self.logger.debug("run_cursor:%s closed.", run_cursor)

        # Post-mortem debugging if requested
        if raised and message_obj.ikpdb_debug:
            try:
                import ikp3db
                ikp3db.post_mortem(sys.exc_info()[2])
            except ImportError:
                self.logger.critical("ImportError: Failed to import 'ikp3db'.")

        # Format result
        self.logger.debug("Storing function returned_value as message processing result: %s", 
                         returned_value)

        if isinstance(returned_value, str) and returned_value[:9].lower() == 'traceback':
            result = returned_value
        else:
            try:
                result = json.dumps(returned_value, indent=4)
            except:
                result = str(returned_value)

        # Build response
        response = {
            'result': result,
            'state': state
        }
        if retry_delay_s:
            response['planned_time'] = fields.Datetime.to_string(
                datetime.datetime.now() + datetime.timedelta(seconds=retry_delay_s)
            )

        return response
    
    def change_message_visibility(self, env, queue_obj, message_obj, message):
        """Update message visibility with timeout defined in processor
        
        Args:
            env: Odoo environment
            queue_obj: Queue object
            message_obj: Local imq.message for message
            message: The queue message object
        """
        if message_obj.processor_id.force_visibility_timeout:
            method_name = f"change_message_visibility__{message_obj.queue_provider}"
            if hasattr(self, method_name):
                method = getattr(self, method_name)
                method(env, queue_obj, message_obj, message)
            else:
                # Fallback to original model method
                worker_model = env['imq.worker']
                worker_model.change_message_visibility(queue_obj, message_obj, message)