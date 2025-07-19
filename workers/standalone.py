#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import signal
import logging
import socket
import time
import datetime
import threading
from io import StringIO
from odoo import api, SUPERUSER_ID
from odoo.modules.registry import Registry

from .base import BaseWorker
from .monitoring import MemoryMonitor, MetricsCollector, ObservabilityServer
from ..worker_utils.worker_utils import get_worker_name, validate_queue_pattern

_logger = logging.getLogger(__name__)

# Import shared TLS from base worker to ensure consistent logging context
from .base import TLS


class StandaloneWorker(BaseWorker):
    """Standalone worker that runs outside ir.cron"""
    
    def __init__(self, database, queue_pattern, **kwargs):
        super().__init__()
        
        # Core configuration
        self.database = database
        self.queue_pattern = queue_pattern
        self.max_messages = kwargs.get('max_messages', 0)
        self.max_rss_memory = kwargs.get('max_rss_memory')
        self.worker_name = get_worker_name(kwargs.get('worker_name'))
        
        # Message targeting
        self.target_message_id = kwargs.get('target_message_id')
        
        # State tracking
        self.processed_count = 0
        self.should_stop = False
        self.current_message = None
        self.start_time = time.time()
        
        # Queue management
        self.queue_stats = {}  # Track per-queue statistics
        self.queue_failures = {}  # Track consecutive failures per queue
        self.queue_last_success = {}  # Track last successful processing per queue
        self.max_queue_failures = kwargs.get('max_queue_failures', 10)
        self.queue_failure_timeout = kwargs.get('queue_failure_timeout', 300)  # 5 minutes
        
        # Initialize components
        self.memory_monitor = MemoryMonitor(self.max_rss_memory)
        self.metrics_collector = MetricsCollector(self.worker_name)
        self.observability_server = ObservabilityServer(
            kwargs.get('observability_port', 0),
            self.metrics_collector,
            self.memory_monitor,
            worker_ref=self,
            metrics_path=kwargs.get('metrics_path', '/metrics')
        )
        
        # Setup logging
        log_level = getattr(logging, kwargs.get('log_level', 'INFO').upper())
        self.logger = logging.getLogger(f'IMQWorker.{self.worker_name}')
        self.logger.setLevel(log_level)
        
        # Initialize tracking attributes for health checker
        self.last_message_time = time.time()
        self.last_activity_time = time.time()
        
        # Validate queue pattern
        if not validate_queue_pattern(self.queue_pattern):
            raise ValueError(f"Invalid queue pattern: {self.queue_pattern}")
        
        # Setup signal handlers
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown on SIGTERM and SIGINT"""
        def handle_sigterm(_signum, _frame):
            self.logger.info("Received SIGTERM, initiating graceful shutdown")
            self._initiate_graceful_shutdown("SIGTERM")
            
        def handle_sigint(_signum, _frame):
            self.logger.info("Received SIGINT (Ctrl+C), initiating graceful shutdown")
            self._initiate_graceful_shutdown("SIGINT")
            
        def handle_sigusr1(_signum, _frame):
            self.logger.info("Received SIGUSR1, logging status and queue health")
            self._log_status_summary()
            
        def handle_sigusr2(_signum, _frame):
            self.logger.info("Received SIGUSR2, resetting queue failure counters")
            self._reset_queue_failures()
            
        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigint)
        signal.signal(signal.SIGUSR1, handle_sigusr1)
        signal.signal(signal.SIGUSR2, handle_sigusr2)
    
    def _initiate_graceful_shutdown(self, signal_name):
        """Initiate graceful shutdown process
        
        Args:
            signal_name: Name of the signal that triggered shutdown
        """
        if self.should_stop:
            self.logger.warning(f"Received {signal_name} again, forcing immediate shutdown")
            # Log current state before forcing shutdown
            self._log_status_summary()
            self.should_stop = True
            return
        
        self.logger.info(f"Graceful shutdown initiated by {signal_name}")
        self.should_stop = True
        
        # Log current processing state
        if self.current_message:
            self.logger.info("Currently processing a message, will finish before shutdown")
        else:
            self.logger.info("No message currently being processed")
        
        # Log final statistics
        self._log_status_summary()
    
    def _log_status_summary(self):
        """Log comprehensive status summary"""
        status = self.get_status()
        uptime_hours = status['uptime_seconds'] / 3600
        
        self.logger.info(f"Worker Status Summary:")
        self.logger.info(f"  Worker Name: {status['worker_name']}")
        self.logger.info(f"  Uptime: {uptime_hours:.2f} hours")
        self.logger.info(f"  Messages Processed: {status['processed_count']}")
        self.logger.info(f"  Queue Pattern: {status['queue_pattern']}")
        
        # Queue health summary
        queue_health = status['queue_health']
        self.logger.info(f"  Queue Health: {queue_health['healthy_queues']}/{queue_health['total_queues']} healthy")
        
        # Per-queue statistics
        for queue_name, stats in status['queue_stats'].items():
            if stats['processed_count'] > 0 or stats['failed_count'] > 0:
                self.logger.info(f"  Queue {queue_name}: {stats['processed_count']} processed, {stats['failed_count']} failed, avg {stats['processing_time_avg']:.2f}s")
        
        # Memory info
        memory_info = status['memory_info']
        self.logger.info(f"  Memory Usage: {memory_info['rss_mb']:.1f}MB")
        if memory_info.get('max_rss_mb'):
            self.logger.info(f"  Memory Limit: {memory_info['max_rss_mb']:.1f}MB")
    
    def _reset_queue_failures(self):
        """Reset all queue failure counters"""
        reset_count = 0
        for queue_name in self.queue_failures:
            if self.queue_failures[queue_name] > 0:
                self.queue_failures[queue_name] = 0
                self.queue_last_success[queue_name] = time.time()
                reset_count += 1
        
        self.logger.info(f"Reset failure counters for {reset_count} queues")
        
        # Log queue health after reset
        health_summary = self._get_queue_health_summary()
        self.logger.info(f"Queue health after reset: {health_summary}")
    
    def _find_matching_queues(self, env):
        """Find queues matching the regex pattern
        
        Supports multiple pattern matching modes:
        - Exact match: queue_name
        - Prefix match: queue_prefix*
        - Suffix match: *queue_suffix
        - Wildcard match: queue_*_name
        - Full regex: ^queue_.*_[0-9]+$
        
        Args:
            env: Odoo environment
            
        Returns:
            list: List of matching queue objects
        """
        try:
            all_queues = env['imq.queue'].search([('active', '=', True)])
            if not all_queues:
                self.logger.warning("No active queues found in the system")
                return []
            
            # Convert simple patterns to regex if needed
            pattern_str = self._normalize_queue_pattern(self.queue_pattern)
            
            try:
                pattern = re.compile(pattern_str)
            except re.error as e:
                self.logger.error(f"Invalid regex pattern '{self.queue_pattern}': {e}")
                return []
            
            matching_queues = []
            for q in all_queues:
                try:
                    if pattern.match(q.name):
                        matching_queues.append(q)
                        self.logger.debug(f"Queue '{q.name}' matches pattern '{self.queue_pattern}'")
                except Exception as e:
                    self.logger.warning(f"Error matching queue '{q.name}': {e}")
            
            if not matching_queues:
                self.logger.error(f"No active queues found matching pattern: {self.queue_pattern}")
                available_queues = [q.name for q in all_queues]
                self.logger.info(f"Available queues: {', '.join(available_queues)}")
            else:
                self.logger.info(f"Found {len(matching_queues)} queues matching pattern: {self.queue_pattern}")
                for q in matching_queues:
                    self.logger.info(f"  - {q.name} ({q.provider})")
            
            return matching_queues
            
        except Exception as e:
            self.logger.error(f"Error finding queues: {e}", exc_info=True)
            return []
    
    def _normalize_queue_pattern(self, pattern):
        """Convert simple patterns to regex patterns
        
        Args:
            pattern: Input pattern string
            
        Returns:
            str: Normalized regex pattern
        """
        # If it's already a regex pattern (contains regex chars), return as-is
        if any(char in pattern for char in ['^', '$', '[', ']', '(', ')', '+', '?', '{', '}']):
            return pattern
        
        # Convert simple patterns to regex
        if pattern.startswith('*') and pattern.endswith('*'):
            # *pattern* -> .*pattern.*
            return f".*{re.escape(pattern[1:-1])}.*"
        elif pattern.startswith('*'):
            # *pattern -> .*pattern$
            return f".*{re.escape(pattern[1:])}$"
        elif pattern.endswith('*'):
            # pattern* -> ^pattern.*
            return f"^{re.escape(pattern[:-1])}.*"
        elif '*' in pattern:
            # pattern*with*wildcards -> ^pattern.*with.*wildcards$
            escaped_parts = [re.escape(part) for part in pattern.split('*')]
            return f"^{'.*'.join(escaped_parts)}$"
        else:
            # exact match: pattern -> ^pattern$
            return f"^{re.escape(pattern)}$"
    
    def _initialize_queue_stats(self, queue_info):
        """Initialize statistics for all queues
        
        Args:
            queue_info: List of (queue_id, queue_name, queue_provider) tuples
        """
        for _queue_id, queue_name, queue_provider in queue_info:
            if queue_name not in self.queue_stats:
                self.queue_stats[queue_name] = {
                    'processed_count': 0,
                    'failed_count': 0,
                    'last_processed': None,
                    'last_failed': None,
                    'processing_time_total': 0.0,
                    'processing_time_avg': 0.0,
                    'provider': queue_provider
                }
                self.queue_failures[queue_name] = 0
                self.queue_last_success[queue_name] = time.time()
    
    def _is_queue_healthy(self, queue_name):
        """Check if a queue is healthy for processing
        
        Args:
            queue_name: Name of the queue to check
            
        Returns:
            bool: True if queue is healthy, False otherwise
        """
        # Check if queue has too many consecutive failures
        if self.queue_failures.get(queue_name, 0) >= self.max_queue_failures:
            # Check if enough time has passed since last success
            last_success = self.queue_last_success.get(queue_name, 0)
            if time.time() - last_success < self.queue_failure_timeout:
                return False
            else:
                # Reset failure count after timeout
                self.queue_failures[queue_name] = 0
                self.logger.info(f"Queue {queue_name} failure timeout expired, retrying")
        
        return True
    
    def _select_next_queue(self, queue_info, queue_index):
        """Select the next healthy queue for processing
        
        Args:
            queue_info: List of (queue_id, queue_name, queue_provider) tuples
            queue_index: Current queue index
            
        Returns:
            tuple: (queue_id, queue_name, queue_provider, new_index) or None if no healthy queues
        """
        healthy_queues = 0
        original_index = queue_index
        
        # Try to find a healthy queue, starting from current index
        while healthy_queues < len(queue_info):
            queue_id, queue_name, _queue_provider = queue_info[queue_index % len(queue_info)]
            
            if self._is_queue_healthy(queue_name):
                return queue_id, queue_name, _queue_provider, queue_index + 1
            
            queue_index += 1
            healthy_queues += 1
            
            # Log unhealthy queue only once per cycle
            if queue_index % len(queue_info) == original_index % len(queue_info):
                self.logger.warning(f"Queue {queue_name} is unhealthy, skipping")
        
        # No healthy queues found
        self.logger.error("No healthy queues available for processing")
        return None
    
    def _update_queue_stats(self, queue_name, success, processing_time):
        """Update queue statistics after processing
        
        Args:
            queue_name: Name of the queue
            success: Whether processing was successful
            processing_time: Time taken to process the message
        """
        if queue_name not in self.queue_stats:
            return
        
        stats = self.queue_stats[queue_name]
        
        if success:
            stats['processed_count'] += 1
            stats['last_processed'] = time.time()
            stats['processing_time_total'] += processing_time
            stats['processing_time_avg'] = stats['processing_time_total'] / stats['processed_count']
            
            # Reset failure count on success
            self.queue_failures[queue_name] = 0
            self.queue_last_success[queue_name] = time.time()
        else:
            stats['failed_count'] += 1
            stats['last_failed'] = time.time()
            
            # Increment failure count
            self.queue_failures[queue_name] += 1
            
            if self.queue_failures[queue_name] >= self.max_queue_failures:
                self.logger.warning(f"Queue {queue_name} has {self.queue_failures[queue_name]} consecutive failures, marking as unhealthy")
    
    def _get_queue_health_summary(self):
        """Get summary of queue health status
        
        Returns:
            dict: Queue health summary
        """
        healthy_queues = 0
        unhealthy_queues = 0
        total_processed = 0
        total_failed = 0
        
        for queue_name, stats in self.queue_stats.items():
            if self._is_queue_healthy(queue_name):
                healthy_queues += 1
            else:
                unhealthy_queues += 1
            
            total_processed += stats['processed_count']
            total_failed += stats['failed_count']
        
        return {
            'healthy_queues': healthy_queues,
            'unhealthy_queues': unhealthy_queues,
            'total_queues': len(self.queue_stats),
            'total_processed': total_processed,
            'total_failed': total_failed
        }
    
    def _check_stop_parameter(self, env):
        """Check if standalone workers should stop via system parameter
        
        Args:
            env: Odoo environment
            
        Returns:
            bool: True if worker should stop, False otherwise
        """
        try:
            host_name = socket.gethostname()
            stopped_workers_nodes = env["ir.config_parameter"].sudo().get_param("imq.STOP_STANDALONE_WORKERS", "").split(',')
            
            # Remove empty strings from split
            stopped_workers_nodes = [node.strip() for node in stopped_workers_nodes if node.strip()]
            
            if stopped_workers_nodes and (host_name in stopped_workers_nodes or '*' in stopped_workers_nodes):
                self.logger.info(f"Stopping worker due to imq.STOP_STANDALONE_WORKERS parameter (hostname: {host_name})")
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking stop parameter: {e}")
            # Don't stop on errors - let worker continue
            return False
    
    def run(self):
        """Main processing loop
        
        Returns:
            int: Exit code (0 for success, 1 for error)
        """
        self.logger.info(f"Starting IMQ worker {self.worker_name}")
        self.logger.info(f"Database: {self.database}")
        self.logger.info(f"Queue pattern: {self.queue_pattern}")
        
        if self.max_messages:
            self.logger.info(f"Will exit after processing {self.max_messages} messages")
        if self.max_rss_memory:
            self.logger.info(f"Will exit when RSS memory exceeds {self.max_rss_memory}")
        if self.target_message_id:
            self.logger.info(f"Message-specific mode: targeting message {self.target_message_id}")
        
        # Start observability server if configured
        if self.observability_server.port:
            self.observability_server.start()
        
        try:
            # Get registry and find queue IDs (not queue objects)
            with api.Environment.manage():
                registry = Registry(self.database)
                with registry.cursor() as cr:
                    env = api.Environment(cr, SUPERUSER_ID, {})
                    queues = self._find_matching_queues(env)
                    
                    if not queues:
                        self.logger.error("No queues found to process")
                        return 1
                        
                    # Store queue IDs and names for round-robin processing
                    # Access all fields while cursor is still open
                    queue_info = [(q.id, q.name, q.provider) for q in queues]
                    
                    # Initialize queue statistics
                    self._initialize_queue_stats(queue_info)
            
            # Main processing loop
            queue_index = 0
            consecutive_empty_polls = 0
            consecutive_no_healthy_queues = 0
            
            while not self.should_stop:
                # Check message count limit
                if self.max_messages > 0 and self.processed_count >= self.max_messages:
                    self.logger.info(f"Reached message limit ({self.max_messages}), exiting")
                    break
                
                # Check memory limit
                if not self.memory_monitor.check_memory():
                    memory_info = self.memory_monitor.get_memory_info()
                    self.logger.error(f"RSS memory limit exceeded: {memory_info['rss_mb']:.1f}MB / {memory_info['max_rss_mb']:.1f}MB, exiting")
                    break
                
                # Check stop parameter periodically (every 10 iterations to avoid overhead)
                if self.processed_count % 10 == 0:
                    with api.Environment.manage():
                        with registry.cursor() as cr:
                            env = api.Environment(cr, SUPERUSER_ID, {})
                            if self._check_stop_parameter(env):
                                self.should_stop = True
                                break
                
                # Select next healthy queue
                queue_selection = self._select_next_queue(queue_info, queue_index)
                if not queue_selection:
                    consecutive_no_healthy_queues += 1
                    # If no healthy queues for extended period, log health summary
                    if consecutive_no_healthy_queues % 50 == 0:
                        health_summary = self._get_queue_health_summary()
                        self.logger.warning(f"Queue health summary: {health_summary}")
                    time.sleep(1.0)  # Longer sleep when no healthy queues
                    continue
                
                queue_id, queue_name, _queue_provider, queue_index = queue_selection
                consecutive_no_healthy_queues = 0
                
                # Start waiting timer if we haven't found messages recently
                if consecutive_empty_polls > 0:
                    self.metrics_collector.start_waiting()
                
                # Process one message
                start_time = time.time()
                result = self._process_one_message(registry, queue_id, queue_name)
                processing_time = time.time() - start_time
                
                # Update queue statistics based on result
                if result == 'processed':
                    self._update_queue_stats(queue_name, True, processing_time)
                    self.processed_count += 1
                    consecutive_empty_polls = 0
                    self.observability_server.update_last_activity()
                    
                    # Log progress periodically
                    if self.processed_count % 100 == 0:
                        uptime = time.time() - self.start_time
                        rate = self.processed_count / uptime if uptime > 0 else 0
                        self.logger.info(f"Processed {self.processed_count} messages ({rate:.1f} msg/s)")
                
                elif result == 'empty':
                    # Queue is empty - this is not a failure, so don't update failure stats
                    consecutive_empty_polls += 1
                    # Update activity to show worker is alive and polling
                    self.observability_server.update_last_activity()
                    # No message available, short sleep to avoid busy loop
                    time.sleep(0.5)
                
                elif result == 'failed':
                    # Actual processing failure - update failure stats
                    self._update_queue_stats(queue_name, False, processing_time)
                    consecutive_empty_polls += 1
                    # Short sleep after failure
                    time.sleep(0.5)
                
                # Update metrics periodically
                if self.processed_count % 10 == 0 or consecutive_empty_polls % 50 == 0:
                    self.metrics_collector.update_metrics(self.memory_monitor)
                    # Update activity during metrics update to provide regular heartbeat
                    self.observability_server.update_last_activity()
                
                # Log status summary periodically (every 10 minutes)
                if self.processed_count % 600 == 0 and self.processed_count > 0:
                    self._log_status_summary()
            
            # Final metrics update and status log
            self.metrics_collector.update_metrics(self.memory_monitor)
            self._log_status_summary()
            
            self.logger.info(f"Worker shutting down. Processed {self.processed_count} messages")
            return 0
            
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down")
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error in worker: {e}", exc_info=True)
            return 1
        finally:
            if self.observability_server.port:
                self.observability_server.stop()
    
    def _process_one_message(self, registry, queue_id, queue_name):
        """Process a single message from the queue
        
        Args:
            registry: Odoo registry
            queue_id: Queue ID to process
            queue_name: Queue name for logging
            
        Returns:
            str: 'processed' if message was processed, 'empty' if no message available, 'failed' if processing failed
        """
        with api.Environment.manage():
            with registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                
                # Get queue object in this context
                queue = env['imq.queue'].browse(queue_id)
                if not queue.exists():
                    self.logger.error(f"Queue {queue_name} (ID: {queue_id}) no longer exists")
                    return 'failed'
                
                # Get next message
                message = self.get_message(env, queue, 0, self.target_message_id)
                if not message:
                    return 'empty'
                
                # Store as current message for signal handler
                self.current_message = message
                
                try:
                    # Store and process message
                    start_time = time.time()
                    start_timestamp = datetime.datetime.now()
                    message_obj = self.store_message(env, queue, message, start_timestamp)
                    
                    if message_obj:
                        self.logger.info(f"Starting to process message - ID: {message_obj.id}, Name: '{message_obj.name}', Queue: {queue_name}")
                    
                    if not message_obj:
                        self.logger.warning(f"Failed to store message from queue {queue_name}")
                        return 'failed'
                    
                    # Update message attempt counter
                    message_obj.write({"attempt": message_obj.attempt + 1})
                    
                    # Create processing object (used for logging in advanced configurations)
                    _processing_obj = message_obj.create_processing_object(worker_type='sa-workerv3')
                    
                    # Update message visibility if needed
                    self.change_message_visibility(env, queue, message_obj, message)
                    
                    # Flush and commit message storage
                    message_obj.flush()
                    cr.commit()
                    
                    # Set up logging BEFORE processing (critical timing fix)
                    processor_obj = message_obj.processor_id
                    if message_obj.logging_activated and processor_obj.capture_log:
                        self.start_log_capture(
                            env,
                            message_obj,
                            _processing_obj,
                            log_level=processor_obj.log_level,
                            log_format=processor_obj.log_format
                        )
                    
                    # Set up console capture if configured
                    if message_obj.capture_console:
                        self.start_stream_capture(env, message_obj, _processing_obj)
                    
                    # Process the message (logging now captures execution)
                    result = self.process_message(env, message_obj, message, {})
                    processing_duration = time.time() - start_time
                    
                    # Update message with processing result (this is the missing piece!)
                    end_timestamp = datetime.datetime.now()
                    
                    # Create a copy for message update to avoid modifying the original
                    message_update = result.copy()
                    message_update['end_time'] = end_timestamp
                    message_update['end_time_microseconds'] = end_timestamp.microsecond
                    
                    # Write result back to message object to update state
                    message_obj.write(message_update)
                    
                    # Also update the processing object if it exists
                    if _processing_obj:
                        # Remove planned_time from processing object update
                        processing_result = message_update.copy()
                        if 'planned_time' in processing_result:
                            del processing_result['planned_time']
                        _processing_obj.write(processing_result)
                    
                    # Flush and commit the state update
                    message_obj.flush()
                    cr.commit()
                    
                    # Record metrics
                    success = result.get('state') == 'done'
                    self.metrics_collector.record_message_processed(
                        queue_name, 
                        processing_duration, 
                        success=success
                    )
                    
                    # Log detailed message info
                    self.logger.info(f"Processed message - ID: {message_obj.id}, Name: '{message_obj.name}', Queue: {queue_name}, State: {result.get('state')}")
                    
                    # Update tracking attributes for health checker
                    self.last_message_time = time.time()
                    self.last_activity_time = time.time()
                    
                    # Stop logging capture
                    if message_obj.logging_activated and processor_obj.capture_log:
                        self.stop_log_capture()
                    if message_obj.capture_console:
                        self.stop_stream_capture()
                    
                    return 'processed'
                    
                except Exception as e:
                    # Record failed message
                    processing_duration = time.time() - start_time
                    self.metrics_collector.record_message_processed(
                        queue_name, 
                        processing_duration, 
                        success=False
                    )
                    
                    # Log detailed error information
                    self.logger.error(f"Error processing message from queue {queue_name}: {e}", exc_info=True)
                    if hasattr(e, '__class__'):
                        self.logger.error(f"Exception type: {e.__class__.__name__}")
                    
                    # Try to update message state to failed if possible
                    try:
                        if 'message_obj' in locals() and message_obj:
                            message_obj.write({
                                'state': 'failed',
                                'result': str(e),
                                'end_time': datetime.datetime.now()
                            })
                            message_obj.flush()
                            cr.commit()
                            self.logger.info(f"Updated message {message_obj.id} state to failed")
                    except Exception as e2:
                        self.logger.error(f"Failed to update message state: {e2}")
                    
                    return 'failed'
                finally:
                    self.current_message = None
                    # Always clean up logging
                    try:
                        self.stop_log_capture()
                        self.stop_stream_capture()
                    except:
                        pass
    
    def get_status(self):
        """Get current worker status
        
        Returns:
            dict: Status information
        """
        uptime = time.time() - self.start_time
        memory_info = self.memory_monitor.get_memory_info()
        queue_health = self._get_queue_health_summary()
        
        return {
            'worker_name': self.worker_name,
            'database': self.database,
            'queue_pattern': self.queue_pattern,
            'uptime_seconds': uptime,
            'processed_count': self.processed_count,
            'max_messages': self.max_messages,
            'should_stop': self.should_stop,
            'memory_info': memory_info,
            'average_processing_duration': self.metrics_collector.get_average_duration(),
            'queue_health': queue_health,
            'queue_stats': self.queue_stats,
            'queue_failures': self.queue_failures,
        }
    
    def start_log_capture(self, env, message_obj, processing_obj, log_level=None, 
                         log_format="%(asctime)s %(name)s %(levelname)s %(message)s"):
        """Start capturing log output for a message
        
        Args:
            env: Odoo environment
            message_obj: The message object being processed
            processing_obj: The processing object
            log_level: Log level to use
            log_format: Log format string
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
        """Stop capturing log output"""
        if hasattr(TLS, '_log_handler'):
            TLS._log_handler.flush()
            if hasattr(TLS, '_logger') and hasattr(TLS, '_log_handler'):
                TLS._logger.removeHandler(TLS._log_handler)

    def start_stream_capture(self, env, message_obj, processing_obj):
        """Start capturing console output
        
        Args:
            env: Odoo environment
            message_obj: The message object being processed
            processing_obj: The processing object
        """
        TLS.log_cursor = env.registry.cursor()
        TLS.log_cursor.autocommit(True)
        TLS._imq_stream = MpyStringIO(
            message_obj.id, 
            processing_obj.id, 
            message_obj.user_id.id,
            TLS.log_cursor
        )

    def stop_stream_capture(self):
        """Stop capturing console output"""
        if hasattr(TLS, '_imq_stream'):
            TLS._imq_stream.close()
        if hasattr(TLS, 'log_cursor'):
            TLS.log_cursor.close()


class IMQLogHandler(logging.Handler):
    """Custom log handler for IMQ message processing"""
    
    def __init__(self, message_obj, processing_obj):
        super(IMQLogHandler, self).__init__()
        self._env = message_obj.env
        self._message_id = message_obj.id
        self._processing_id = processing_obj.id if processing_obj else None
    
    def emit(self, record):
        """Emit a log record to the database"""
        try:
            # Format the message
            if record.args:
                _msg = record.msg % record.args
            else:
                _msg = record.msg
        except Exception as e1: 
            _logger.exception(e1)
            _msg = "Failed to log:%s with %s" % (str(record.msg), str(record.args))
        
        try:
            # Create log entry in database
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
        """Flush handler - nothing to do for database logging"""
        pass


class MpyStringIO(StringIO):
    """Custom StringIO that logs console output to database"""
    
    def __init__(self, message_id, processing_id, uid, log_cr):
        super().__init__()
        _logger.info("MpyStringIO(%s, %s, %s, %s)", message_id, processing_id, uid, log_cr)
        self._message_id = message_id
        self._processing_id = processing_id
        self._log_cr = log_cr
        self._uid = uid
        self._mpy_buffer = ''
        
    def write(self, s):
        """Write string to buffer and log complete lines"""
        _logger.debug("MpyStringIO.write(%s)", repr(s))
        super().write(s)
        
        # Buffer management for line-based logging
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
        
        # Update buffer
        if output_str:
            full_output = self._mpy_buffer + output_str
            self._mpy_buffer = new_buffer
            
            # Log to database (when console capture is enabled)
            try:
                with api.Environment.manage():
                    with self._log_cr.registry.cursor() as cr:
                        env = api.Environment(cr, self._uid, {})
                        env['imq.message_processing_log'].sudo().create({
                            'message_id': self._message_id,
                            'active_message_id': self._message_id,
                            'processing_id': self._processing_id,
                            'logger_name': "Console",
                            'log_level': None,
                            'log_message': full_output.rstrip('\n')
                        })
                        cr.commit()
            except Exception as e:
                _logger.exception("Failed to log console output: %s", e)
        else:
            self._mpy_buffer += new_buffer
    
    def close(self):
        """Close and flush any remaining buffer"""
        if self._mpy_buffer:
            try:
                with api.Environment.manage():
                    with self._log_cr.registry.cursor() as cr:
                        env = api.Environment(cr, self._uid, {})
                        env['imq.message_processing_log'].sudo().create({
                            'message_id': self._message_id,
                            'active_message_id': self._message_id,
                            'processing_id': self._processing_id,
                            'logger_name': "Console",
                            'log_level': None,
                            'log_message': self._mpy_buffer
                        })
                        cr.commit()
            except Exception as e:
                _logger.exception("Failed to log final console output: %s", e)
        super().close()