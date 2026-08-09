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
from .monitoring import MemoryMonitor, MetricsCollector, ThreadMonitor
from .monitoring__observability_server import ObservabilityServer
from .monitoring__textfile_exporter import TextfileExporter
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
        self.max_thread_delta = kwargs.get('max_thread_delta', 0)
        self.thread_warn_percent = kwargs.get('thread_warn_percent', 50)
        self.status_summary_interval_s = kwargs.get('status_summary_interval_s', 300)
        self.worker_name = get_worker_name(kwargs.get('worker_name'))
        
        # Message targeting
        self.target_message_id = kwargs.get('target_message_id')
        
        # Queue depth caching configuration
        self.queue_depth_caching_period_s = kwargs.get('queue_depth_caching_period_s', 30)
        
        # State tracking
        self.processed_count = 0
        self.should_stop = False
        self.current_message = None
        self.start_time = time.time()
        self.last_status_summary_time = self.start_time
        
        # Queue management
        self.queue_stats = {}  # Track per-queue statistics
        self.queue_failures = {}  # Track consecutive failures per queue
        self.queue_last_success = {}  # Track last successful processing per queue
        self.max_queue_failures = kwargs.get('max_queue_failures', 10)
        self.queue_failure_timeout = kwargs.get('queue_failure_timeout', 300)  # 5 minutes
        
        # Initialize components
        self.memory_monitor = MemoryMonitor(self.max_rss_memory)
        self.thread_monitor = ThreadMonitor(
            max_thread_delta=self.max_thread_delta,
            warn_percent=self.thread_warn_percent,
        )
        self.metrics_collector = MetricsCollector(self.worker_name, self.queue_depth_caching_period_s)

        metrics_export_mode = kwargs.get('metrics_export_mode', 'network')
        if metrics_export_mode == 'textfile':
            self.observability_server = None
            self.textfile_exporter = TextfileExporter(
                directory=kwargs.get('textfile_dir'),
                worker_name=self.worker_name,
            )
        else:
            self.textfile_exporter = None
            self.observability_server = ObservabilityServer(
                kwargs.get('observability_port', 0),
                self.metrics_collector,
                self.memory_monitor,
                worker_ref=self,
                metrics_path=kwargs.get('metrics_path', '/metrics'),
                thread_monitor=self.thread_monitor,
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
            self.should_stop = True
            return

        self.logger.info(f"Graceful shutdown initiated by {signal_name}")
        self.should_stop = True

        # Log current processing state
        if self.current_message:
            self.logger.info("Currently processing a message, will finish before shutdown")
        else:
            self.logger.info("No message currently being processed")
        # Final status summary is logged by run() after the main loop exits,
        # to avoid a duplicate log when the loop exits immediately on signal.
    
    def _log_status_summary(self, level=logging.INFO):
        """Log comprehensive status summary at the given level.

        Default INFO (periodic + shutdown). Promoted to WARNING by
        _post_task_thread_check when a per-task thread leak is detected.
        """
        status = self.get_status()
        uptime_hours = status['uptime_seconds'] / 3600

        log = self.logger.log
        log(level, "Worker Status Summary:")
        log(level, f"  Worker Name: {status['worker_name']}")
        log(level, f"  Uptime: {uptime_hours:.2f} hours")
        log(level, f"  Messages Processed: {status['processed_count']}")
        log(level, f"  Queue Pattern: {status['queue_pattern']}")

        # Queue health summary
        queue_health = status['queue_health']
        log(level, f"  Queue Health: {queue_health['healthy_queues']}/{queue_health['total_queues']} healthy")

        # Per-queue statistics
        for queue_name, stats in status['queue_stats'].items():
            if stats['processed_count'] > 0 or stats['failed_count'] > 0:
                log(level, f"  Queue {queue_name}: {stats['processed_count']} processed, {stats['failed_count']} failed, avg {stats['processing_time_avg']:.2f}s")

        # Memory info
        memory_info = status['memory_info']
        log(level, f"  Memory Usage: {memory_info['rss_mb']:.1f}MB")
        if memory_info.get('max_rss_mb'):
            log(level, f"  Memory Limit: {memory_info['max_rss_mb']:.1f}MB")

        # Thread / FD info
        if getattr(self, 'thread_monitor', None) is not None:
            sample = self.thread_monitor.sample()
            log(level,
                f"  Threads: os={sample['os_threads']} py={sample['py_threads']} "
                f"(baseline os={sample['baseline_os_threads']}, "
                f"delta={sample['thread_delta']})"
            )
            log(level,
                f"  FDs: {sample['fds']} (baseline {sample['baseline_fds']}, "
                f"delta={sample['fd_delta']})"
            )
            # System ceilings. user_nproc counts threads across ALL
            # processes of the current user, so we measure that usage
            # here (more expensive — OK at summary cadence).
            sys_limits = self.thread_monitor.system_limits
            os_max = sys_limits.get('os_threads_max')
            user_soft = sys_limits.get('user_nproc_soft')
            user_hard = sys_limits.get('user_nproc_hard')
            user_usage = self.thread_monitor.get_current_user_thread_usage()

            # % of kernel-wide ceiling by this worker alone.
            os_pct = (sample['os_threads'] / os_max * 100) if os_max else None
            log(level,
                f"  System Thread Ceiling: os_max={os_max} "
                f"(this worker: {f'{os_pct:.3f}%' if os_pct is not None else 'n/a'})"
            )
            # Per-user: usage is sum across all user's processes.
            if user_usage is not None:
                user_total = user_usage['total_threads']
                user_pct = (user_total / user_soft * 100) if user_soft else None
                log(level,
                    f"  User Thread Usage: {user_total} threads across "
                    f"{user_usage['proc_count']} procs "
                    f"(skipped={user_usage['skipped']}) "
                    f"vs nproc soft={user_soft}/hard={user_hard} "
                    f"({f'{user_pct:.2f}%' if user_pct is not None else 'n/a'} of soft)"
                )
            else:
                log(level,
                    f"  User Thread Usage: n/a (non-POSIX), "
                    f"nproc soft={user_soft}/hard={user_hard}"
                )
            if self.thread_monitor.max_thread_delta:
                log(level,
                    f"  Thread Delta Limit: +{self.thread_monitor.max_thread_delta} "
                    f"(warn at +{self.thread_monitor.warn_threshold_delta})"
                )
    
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
    
    def _post_task_thread_check(self, queue_name):
        """Sample thread/FD state post-task and emit a log.

        Per-task leak detection: if the OS-thread count grew vs. the
        previous post-task sample (or the baseline for the first task),
        the just-finished task leaked threads. In that case we log a
        WARNING with the per-task delta and dump the full status summary
        at WARNING for operator visibility.

        Otherwise: DEBUG (default) or INFO if the cumulative thread_delta
        has reached warn_threshold_delta. The loop's pre-iteration check
        handles the exit on cumulative hard limit.
        """
        prev_os_threads = (
            self.thread_monitor.last_sample['os_threads']
            if self.thread_monitor.last_sample is not None
            else self.thread_monitor.baseline_os_threads
        )
        sample = self.thread_monitor.sample()
        delta = sample['thread_delta']
        task_delta = (
            sample['os_threads'] - prev_os_threads
            if prev_os_threads is not None else None
        )

        if task_delta is not None and task_delta > 0:
            self.logger.warning(
                "Thread leak detected on last task: queue=%s leaked=+%d "
                "(os_threads %d -> %d, cumulative delta=%s, fd_delta=%s)",
                queue_name, task_delta, prev_os_threads, sample['os_threads'],
                delta, sample['fd_delta'],
            )
            self._log_status_summary(level=logging.WARNING)
            return

        warn = self.thread_monitor.warn_threshold_delta
        if warn and delta is not None and delta >= warn:
            level = logging.INFO
        else:
            level = logging.DEBUG
        self.logger.log(
            level,
            "Post-task threads: os=%d (delta=%s) py=%d fds=%s (fd_delta=%s) queue=%s",
            sample['os_threads'], delta, sample['py_threads'],
            sample['fds'], sample['fd_delta'], queue_name,
        )

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
        if self.max_thread_delta:
            self.logger.info(
                f"Will exit when OS thread delta vs baseline exceeds "
                f"+{self.max_thread_delta} (warn at {self.thread_warn_percent}%)"
            )
        if self.target_message_id:
            self.logger.info(f"Message-specific mode: targeting message {self.target_message_id}")

        # Start metrics export (textfile or HTTP)
        if self.textfile_exporter:
            self.textfile_exporter.start()
        elif self.observability_server and self.observability_server.port:
            self.observability_server.start()

        # Capture thread/FD baseline AFTER observability server is started
        # (so its daemon thread is counted in the baseline) but BEFORE any
        # message is processed.
        self.thread_monitor.capture_baseline()
        
        try:
            # Get registry and find queue IDs (not queue objects)
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
                
                # Store queue information for monitoring/health checker
                # Convert ORM objects to simple data structures that can be accessed outside cursor context
                self.queues = [{'id': q.id, 'name': q.name, 'provider': q.provider} for q in queues]
                
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

                # Check thread-delta limit (symétrique avec max-rss-memory).
                # The supervisor (K8s/systemd) is expected to restart the
                # worker with a clean thread baseline.
                if not self.thread_monitor.check_threads():
                    sample = self.thread_monitor.last_sample or self.thread_monitor.sample()
                    self.logger.warning(
                        "Thread delta limit exceeded: os_threads=%d baseline=%d "
                        "delta=+%d (max_delta=+%d), exiting for recycling",
                        sample['os_threads'], sample['baseline_os_threads'],
                        sample['thread_delta'], self.thread_monitor.max_thread_delta,
                    )
                    break
                
                # Check stop parameter periodically (every 10 iterations to avoid overhead)
                if self.processed_count % 10 == 0:
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

                # Post-task thread/FD sample (Phase 1: observation only).
                # Runs for processed AND failed tasks — both can leak. 'empty'
                # means no message ran, so the sample would be noise.
                if result in ('processed', 'failed'):
                    self._post_task_thread_check(queue_name)

                # Update queue statistics based on result
                if result == 'processed':
                    self._update_queue_stats(queue_name, True, processing_time)
                    self.processed_count += 1
                    consecutive_empty_polls = 0
                    self._signal_activity()

                    # Log progress periodically
                    if self.processed_count % 100 == 0:
                        uptime = time.time() - self.start_time
                        rate = self.processed_count / uptime if uptime > 0 else 0
                        self.logger.info(f"Processed {self.processed_count} messages ({rate:.1f} msg/s)")

                elif result == 'empty':
                    # Queue is empty - this is not a failure, so don't update failure stats
                    consecutive_empty_polls += 1
                    self._signal_activity()
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
                    self.metrics_collector.update_metrics(
                        self.memory_monitor, self,
                        thread_monitor=self.thread_monitor,
                    )
                    self._signal_activity()
                    self._flush_metrics()
                
                # Log status summary periodically (wall-clock based, not
                # message-count based, because tasks range from 500ms to
                # hours — a count-based trigger would be unreliable).
                if self.status_summary_interval_s > 0:
                    elapsed = time.time() - self.last_status_summary_time
                    if elapsed >= self.status_summary_interval_s:
                        self._log_status_summary()
                        self.last_status_summary_time = time.time()
            
            # Final metrics update and status log
            self.metrics_collector.update_metrics(
                self.memory_monitor, self,
                thread_monitor=self.thread_monitor,
            )
            self._flush_metrics()
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
            if self.textfile_exporter:
                self.textfile_exporter.cleanup()
            elif self.observability_server and self.observability_server.port:
                self.observability_server.stop()
    
    def _signal_activity(self):
        """Signal liveness heartbeat to the observability server (network mode only)."""
        if self.observability_server:
            self.observability_server.update_last_activity()

    def _flush_metrics(self):
        """Persist current metrics to the configured export backend.

        In textfile mode: writes a .prom file for node_exporter.
        In network mode: no-op — the HTTP server exposes metrics on pull.
        """
        if self.textfile_exporter:
            self.textfile_exporter.write()

    def _process_one_message(self, registry, queue_id, queue_name):
        """Process a single message from the queue
        
        Args:
            registry: Odoo registry
            queue_id: Queue ID to process
            queue_name: Queue name for logging
            
        Returns:
            str: 'processed' if message was processed, 'empty' if no message available, 'failed' if processing failed
        """
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
                message_obj.flush_recordset()
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
                message_obj.flush_recordset()
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
                        message_obj.flush_recordset()
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
    
    def get_queue_depths(self):
        """Get queue depths for all monitored queues
        
        Returns:
            dict: Queue name -> depth mapping
        """
        queue_depths = {}
        
        try:
            # Get registry and query queue depths
            registry = Registry(self.database)
            with registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                
                # Query queue depths for all queues we're monitoring
                if hasattr(self, 'queues') and self.queues:
                    for queue_info in self.queues:
                        queue_id = queue_info['id']
                        queue_name = queue_info['name']
                        
                        # SQL to count messages matching our queue depth definition
                        sql = """
                            SELECT COUNT(*)
                            FROM imq_message
                            WHERE queue_id = %s
                              AND state IN ('pending', 'retry')
                              AND (planned_time IS NULL OR planned_time <= NOW())
                        """
                        cr.execute(sql, (queue_id,))
                        result = cr.fetchone()
                        queue_depths[queue_name] = result[0] if result else 0
                            
                    self.logger.debug(f"Queue depths: {queue_depths}")
                    
        except Exception as e:
            self.logger.error(f"Error getting queue depths: {e}", exc_info=True)
            # Return empty dict on error
            return {}
            
        return queue_depths
    
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
        TLS._imq_stream = MpyStringIO(
            message_obj.id, 
            processing_obj.id, 
            message_obj.user_id.id,
            TLS.log_cursor
        )

    def stop_stream_capture(self):
        """Stop capturing console output — and DROP the stream, which is the whole point.

        TLS lives as long as the worker THREAD, not as long as the message. A stream left
        behind here is found again by the next message's cleanup, which closes it a second
        time against the cursor closed on the line below. That is how one console-capturing
        message used to poison every message after it: an
        "ERROR Failed to log final console output: Cursor already closed" per message —
        including messages with `capture_console=False`, which never opened a stream and
        were merely re-closing someone else's.

        `hasattr` cannot express "already cleaned up" once the attribute exists, hence the
        `getattr(..., None)` pair. Odoo's `Cursor.close()` is a no-op on a closed cursor, so
        the second call was always harmless; the stream's was not.
        """
        stream = getattr(TLS, '_imq_stream', None)
        if stream is not None:
            stream.close()
        cursor = getattr(TLS, 'log_cursor', None)
        if cursor is not None:
            cursor.close()
        TLS._imq_stream = None
        TLS.log_cursor = None


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
                env = api.Environment(self._log_cr, self._uid, {})
                env['imq.message_processing_log'].sudo().create({
                        'message_id': self._message_id,
                        'active_message_id': self._message_id,
                        'processing_id': self._processing_id,
                        'logger_name': "Console",
                        'log_level': None,
                        'log_message': full_output.rstrip('\n')
                })
                self._log_cr.commit()
            except Exception as e:
                _logger.exception("Failed to log console output: %s", e)
        else:
            self._mpy_buffer += new_buffer
    
    def close(self):
        """Flush what is left of the buffer — once, and only once.

        The buffer is taken and cleared BEFORE the write, which makes this idempotent: a
        second close has nothing left to say. The standalone worker closes the capture
        twice per message (the nominal path, then the `finally` covering the failure
        paths), and without this the second call replayed the same text against the cursor
        the first one had just closed.

        Clearing before rather than after a successful write is deliberate: the write can
        realistically only fail because the cursor is gone, and a retry on a dead cursor
        fails identically. Better to lose one trailing line, loudly, than to loop on it.
        """
        if self._mpy_buffer:
            buffered, self._mpy_buffer = self._mpy_buffer, ''
            try:
                env = api.Environment(self._log_cr, self._uid, {})
                env['imq.message_processing_log'].sudo().create({
                        'message_id': self._message_id,
                        'active_message_id': self._message_id,
                        'processing_id': self._processing_id,
                        'logger_name': "Console",
                        'log_level': None,
                        'log_message': buffered
                })
                self._log_cr.commit()
            except Exception as e:
                _logger.exception("Failed to log final console output: %s", e)
        super().close()