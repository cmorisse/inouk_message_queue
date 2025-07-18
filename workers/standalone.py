#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import signal
import logging
import time
from odoo import api, SUPERUSER_ID
from odoo.modules.registry import Registry

from .base import BaseWorker
from .monitoring import MemoryMonitor, MetricsCollector, ObservabilityServer
from ..worker_utils.worker_utils import get_worker_name, validate_queue_pattern

_logger = logging.getLogger(__name__)


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
        
        # State tracking
        self.processed_count = 0
        self.should_stop = False
        self.current_message = None
        self.start_time = time.time()
        
        # Initialize components
        self.memory_monitor = MemoryMonitor(self.max_rss_memory)
        self.metrics_collector = MetricsCollector(self.worker_name)
        self.observability_server = ObservabilityServer(
            kwargs.get('observability_port', 0),
            self.metrics_collector,
            kwargs.get('metrics_path', '/metrics')
        )
        
        # Setup logging
        log_level = getattr(logging, kwargs.get('log_level', 'INFO').upper())
        self.logger = logging.getLogger(f'IMQWorker.{self.worker_name}')
        self.logger.setLevel(log_level)
        
        # Validate queue pattern
        if not validate_queue_pattern(self.queue_pattern):
            raise ValueError(f"Invalid queue pattern: {self.queue_pattern}")
        
        # Setup signal handlers
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown on SIGTERM"""
        def handle_sigterm(_signum, _frame):
            self.logger.info("Received SIGTERM, initiating graceful shutdown")
            self.should_stop = True
            
        def handle_sigint(_signum, _frame):
            self.logger.info("Received SIGINT (Ctrl+C), initiating graceful shutdown")
            self.should_stop = True
            
        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigint)
    
    def _find_matching_queues(self, env):
        """Find queues matching the regex pattern
        
        Args:
            env: Odoo environment
            
        Returns:
            list: List of matching queue objects
        """
        all_queues = env['imq.queue'].search([('active', '=', True)])
        pattern = re.compile(self.queue_pattern)
        matching_queues = [q for q in all_queues if pattern.match(q.name)]
        
        if not matching_queues:
            self.logger.error(f"No active queues found matching pattern: {self.queue_pattern}")
        else:
            self.logger.info(f"Found {len(matching_queues)} queues matching pattern: {self.queue_pattern}")
            for q in matching_queues:
                self.logger.info(f"  - {q.name} ({q.provider})")
        
        return matching_queues
    
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
            
            # Main processing loop
            queue_index = 0
            consecutive_empty_polls = 0
            
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
                
                # Round-robin queue selection
                queue_id, queue_name, _queue_provider = queue_info[queue_index % len(queue_info)]
                queue_index += 1
                
                # Start waiting timer if we haven't found messages recently
                if consecutive_empty_polls > 0:
                    self.metrics_collector.start_waiting()
                
                # Process one message
                processed = self._process_one_message(registry, queue_id, queue_name)
                
                if processed:
                    self.processed_count += 1
                    consecutive_empty_polls = 0
                    self.observability_server.update_last_activity()
                    
                    # Log progress periodically
                    if self.processed_count % 100 == 0:
                        uptime = time.time() - self.start_time
                        rate = self.processed_count / uptime if uptime > 0 else 0
                        self.logger.info(f"Processed {self.processed_count} messages ({rate:.1f} msg/s)")
                else:
                    consecutive_empty_polls += 1
                    # No message available, short sleep to avoid busy loop
                    time.sleep(0.5)
                
                # Update metrics periodically
                if self.processed_count % 10 == 0 or consecutive_empty_polls % 50 == 0:
                    self.metrics_collector.update_metrics(self.memory_monitor)
            
            # Final metrics update
            self.metrics_collector.update_metrics(self.memory_monitor)
            
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
            bool: True if message was processed, False if no message available
        """
        with api.Environment.manage():
            with registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                
                # Get queue object in this context
                queue = env['imq.queue'].browse(queue_id)
                if not queue.exists():
                    self.logger.error(f"Queue {queue_name} (ID: {queue_id}) no longer exists")
                    return False
                
                # Get next message
                message = self.get_message(env, queue)
                if not message:
                    return False
                
                # Store as current message for signal handler
                self.current_message = message
                
                try:
                    # Store and process message
                    start_time = time.time()
                    message_obj = self.store_message(env, queue, message)
                    
                    if message_obj:
                        self.logger.info(f"Starting to process message - ID: {message_obj.id}, Name: '{message_obj.name}', Queue: {queue_name}")
                    
                    if not message_obj:
                        self.logger.warning(f"Failed to store message from queue {queue_name}")
                        return False
                    
                    # Update message attempt counter
                    message_obj.write({"attempt": message_obj.attempt + 1})
                    
                    # Create processing object (used for logging in advanced configurations)
                    _processing_obj = message_obj.create_processing_object()
                    
                    # Update message visibility if needed
                    self.change_message_visibility(env, queue, message_obj, message)
                    
                    # Flush and commit message storage
                    message_obj.flush()
                    cr.commit()
                    
                    # Process the message
                    result = self.process_message(env, message_obj, message, {})
                    processing_duration = time.time() - start_time
                    
                    # Record metrics
                    success = result.get('state') == 'done'
                    self.metrics_collector.record_message_processed(
                        queue_name, 
                        processing_duration, 
                        success=success
                    )
                    
                    # Log detailed message info
                    self.logger.info(f"Processed message - ID: {message_obj.id}, Name: '{message_obj.name}', Queue: {queue_name}, State: {result.get('state')}")
                    
                    return True
                    
                except Exception as e:
                    # Record failed message
                    processing_duration = time.time() - start_time
                    self.metrics_collector.record_message_processed(
                        queue_name, 
                        processing_duration, 
                        success=False
                    )
                    
                    self.logger.error(f"Error processing message from queue {queue_name}: {e}", exc_info=True)
                    return False
                finally:
                    self.current_message = None
    
    def get_status(self):
        """Get current worker status
        
        Returns:
            dict: Status information
        """
        uptime = time.time() - self.start_time
        memory_info = self.memory_monitor.get_memory_info()
        
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
        }