#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psutil
import resource
import threading
import time
import logging
import os
from collections import defaultdict

try:
    from prometheus_client import Counter, Gauge, Histogram, Summary, Info
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

from ..worker_utils.worker_utils import parse_memory_limit, format_memory_size

_logger = logging.getLogger(__name__)


class MemoryMonitor:
    """Monitor RSS memory usage"""
    
    def __init__(self, max_rss_str=None):
        self.max_rss_bytes = parse_memory_limit(max_rss_str)
        self.process = psutil.Process()
        
        if self.max_rss_bytes:
            _logger.info(f"Memory limit set to {format_memory_size(self.max_rss_bytes)}")
    
    def check_memory(self):
        """Check if RSS memory is within limit
        
        Returns:
            bool: True if within limit (or no limit set), False if over limit
        """
        if not self.max_rss_bytes:
            return True
            
        # Get RSS excluding shared memory
        memory_info = self.process.memory_info()
        rss = memory_info.rss
        
        return rss < self.max_rss_bytes
    
    def get_rss_mb(self):
        """Get current RSS in MB
        
        Returns:
            float: RSS memory in MB
        """
        return self.process.memory_info().rss / (1024 * 1024)
    
    def get_rss_bytes(self):
        """Get current RSS in bytes
        
        Returns:
            int: RSS memory in bytes
        """
        return self.process.memory_info().rss
    
    def get_memory_info(self):
        """Get detailed memory information
        
        Returns:
            dict: Memory information
        """
        memory_info = self.process.memory_info()
        return {
            'rss_bytes': memory_info.rss,
            'rss_mb': memory_info.rss / (1024 * 1024),
            'vms_bytes': memory_info.vms,
            'vms_mb': memory_info.vms / (1024 * 1024),
            'max_rss_bytes': self.max_rss_bytes,
            'max_rss_mb': self.max_rss_bytes / (1024 * 1024) if self.max_rss_bytes else None,
            'usage_percent': (memory_info.rss / self.max_rss_bytes * 100) if self.max_rss_bytes else None
        }


class ThreadMonitor:
    """Monitor thread and file-descriptor usage in the worker process.

    The seuil is a DELTA vs. the baseline captured at startup
    (os_threads - baseline_os_threads), not an absolute count. Delta is:
      - portable across environments (different baselines don't matter),
      - a direct measure of what leaked since the worker started,
      - trivially readable in Grafana.

    On check_threads() → False, the caller (StandaloneWorker.run) breaks
    the main loop so the supervisor restarts a fresh worker with a clean
    thread baseline.

    OS threads (psutil.Process.num_threads()) are used as the source of
    truth because paramiko/fabric create threads in native code that
    threading.active_count() may undercount.
    """

    def __init__(self, max_thread_delta=0, warn_percent=50):
        self.max_thread_delta = max_thread_delta  # 0 disables the check
        if max_thread_delta and 0 < warn_percent <= 100:
            self.warn_threshold_delta = int(max_thread_delta * warn_percent / 100)
        else:
            self.warn_threshold_delta = 0
        self.process = psutil.Process()
        self.baseline_os_threads = None
        self.baseline_fds = None
        self.last_sample = None

        # System-level thread limits (read once — they don't change at runtime).
        # os_threads_max: kernel-wide ceiling (/proc/sys/kernel/threads-max).
        # user_nproc_soft/hard: per-user ceiling (RLIMIT_NPROC, aka ulimit -u).
        # The user limit is usually the one that bites first.
        self.system_limits = self._read_system_limits()

        if self.max_thread_delta:
            _logger.info(
                f"Thread delta limit set to +{max_thread_delta} OS threads "
                f"above baseline (INFO-log warn at +{self.warn_threshold_delta})"
            )
        _logger.info(
            f"System thread limits: os_max={self.system_limits['os_threads_max']}, "
            f"user_nproc={self.system_limits['user_nproc_soft']}/"
            f"{self.system_limits['user_nproc_hard']} (soft/hard)"
        )

    @staticmethod
    def _read_system_limits():
        """Read kernel and per-user thread ceilings. None on read errors."""
        os_threads_max = None
        try:
            with open('/proc/sys/kernel/threads-max', 'r') as f:
                os_threads_max = int(f.read().strip())
        except (OSError, ValueError):
            pass

        user_nproc_soft = user_nproc_hard = None
        try:
            soft, hard = resource.getrlimit(resource.RLIMIT_NPROC)
            # resource.RLIM_INFINITY is -1 on Linux; surface that as None.
            user_nproc_soft = soft if soft != resource.RLIM_INFINITY else None
            user_nproc_hard = hard if hard != resource.RLIM_INFINITY else None
        except (ValueError, OSError, AttributeError):
            pass

        return {
            'os_threads_max': os_threads_max,
            'user_nproc_soft': user_nproc_soft,
            'user_nproc_hard': user_nproc_hard,
        }

    def capture_baseline(self):
        """Capture baseline thread/fd counts.

        Must be called after the worker is fully initialized (including
        the observability HTTP server, which spawns a daemon thread) but
        before the first message is processed.
        """
        self.baseline_os_threads = self.process.num_threads()
        self.baseline_fds = self._num_fds_safe()
        _logger.info(
            f"Worker baseline: {self.baseline_os_threads} OS threads, "
            f"{self.baseline_fds} file descriptors"
        )

    def sample(self):
        """Capture current thread/fd state and compute deltas."""
        os_threads = self.process.num_threads()
        fds = self._num_fds_safe()
        thread_delta = (os_threads - self.baseline_os_threads
                        if self.baseline_os_threads is not None else None)
        fd_delta = (fds - self.baseline_fds
                    if fds is not None and self.baseline_fds is not None else None)
        sample = {
            'py_threads': threading.active_count(),
            'os_threads': os_threads,
            'fds': fds,
            'baseline_os_threads': self.baseline_os_threads,
            'baseline_fds': self.baseline_fds,
            'thread_delta': thread_delta,
            'fd_delta': fd_delta,
            'max_thread_delta': self.max_thread_delta,
            'warn_threshold_delta': self.warn_threshold_delta,
        }
        self.last_sample = sample
        return sample

    def check_threads(self):
        """Return True if delta within limit (or no limit set / no baseline),
        False if delta exceeds max_thread_delta."""
        if not self.max_thread_delta or self.baseline_os_threads is None:
            return True
        delta = self.process.num_threads() - self.baseline_os_threads
        return delta <= self.max_thread_delta

    def _num_fds_safe(self):
        """psutil.num_fds() is Linux-only and may raise on permission issues."""
        try:
            return self.process.num_fds()
        except (AttributeError, psutil.AccessDenied, NotImplementedError):
            return None

    def get_current_user_thread_usage(self):
        """Sum threads across all processes owned by the current real UID.

        This is the quantity RLIMIT_NPROC actually limits — the Linux
        kernel counts threads (not processes) for that limit. Expensive
        (iterates /proc), call only at summary time, not per-task.

        Returns (total, proc_count, skipped) — skipped counts processes
        we could not inspect (permission/race). None if we can't get our
        own uid (non-POSIX).
        """
        try:
            my_uid = os.getuid()
        except AttributeError:
            return None
        total = 0
        proc_count = 0
        skipped = 0
        for p in psutil.process_iter(['uids', 'num_threads']):
            try:
                info = p.info
                uids = info.get('uids')
                num_threads = info.get('num_threads')
                if uids is None or num_threads is None:
                    skipped += 1
                    continue
                if uids.real == my_uid:
                    total += num_threads
                    proc_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                skipped += 1
                continue
        return {
            'total_threads': total,
            'proc_count': proc_count,
            'skipped': skipped,
        }


class MetricsCollector:
    """Collect and expose Prometheus metrics"""
    
    def __init__(self, worker_name, queue_depth_caching_period_s=30):
        self.worker_name = worker_name
        self.start_time = time.time()
        self.wait_start_time = None
        self.total_wait_time = 0
        
        # Per-queue average tracking
        self.queue_totals = defaultdict(lambda: {'count': 0, 'total_time': 0})
        
        # Queue depth caching configuration
        self.queue_depth_caching_period_s = queue_depth_caching_period_s
        self.queue_depth_cache = {}
        self.queue_depth_last_update = 0
        
        if not HAS_PROMETHEUS:
            _logger.warning("prometheus_client not available, metrics will not be collected")
            return
        
        # Initialize Prometheus metrics
        self._init_metrics()
    
    def _init_metrics(self):
        """Initialize Prometheus metrics"""
        if not HAS_PROMETHEUS:
            return
            
        # Global metrics
        self.info = Info('imq_worker', 'IMQ Worker information')
        self.info.info({'worker_name': self.worker_name})
        
        self.up_time = Gauge('imq_worker_uptime_seconds', 
                            'Worker uptime in seconds')
        self.wait_time = Gauge('imq_worker_wait_time_seconds', 
                              'Total time spent waiting for messages')
        self.wait_time_pct = Gauge('imq_worker_wait_time_percent', 
                                  'Percentage of time spent waiting')
        self.messages_processed = Counter('imq_worker_messages_processed_total', 
                                        'Total messages processed')
        self.messages_failed = Counter('imq_worker_messages_failed_total', 
                                     'Total messages failed')
        self.processing_duration = Summary('imq_worker_message_duration_seconds', 
                                         'Message processing duration')
        self.rss_memory = Gauge('imq_worker_rss_memory_bytes',
                               'Current RSS memory usage')
        self.max_rss_memory = Gauge('imq_worker_max_rss_memory_bytes',
                                   'Maximum RSS memory configured')

        # Thread / FD observability
        self.thread_count = Gauge('imq_worker_thread_count',
                                  'Python threads active in worker process')
        self.os_thread_count = Gauge('imq_worker_os_thread_count',
                                     'OS threads in worker process (source of truth for leaks)')
        self.baseline_os_thread_count = Gauge('imq_worker_baseline_os_thread_count',
                                              'OS thread count captured at worker startup')
        self.thread_delta = Gauge('imq_worker_thread_delta',
                                  'OS threads delta vs. baseline (current - baseline)')
        self.fd_count = Gauge('imq_worker_fd_count',
                              'File descriptors open in worker process')
        self.baseline_fd_count = Gauge('imq_worker_baseline_fd_count',
                                       'File descriptor count captured at worker startup')
        self.fd_delta = Gauge('imq_worker_fd_delta',
                              'FDs delta vs. baseline (current - baseline)')
        self.max_thread_delta = Gauge('imq_worker_max_thread_delta',
                                      'Configured max OS-thread delta vs baseline (0 = disabled)')
        
        # Per-queue metrics
        self.queue_messages_processed = Counter('imq_worker_queue_messages_processed_total',
                                              'Messages processed per queue', 
                                              ['queue'])
        self.queue_messages_failed = Counter('imq_worker_queue_messages_failed_total',
                                           'Messages failed per queue', 
                                           ['queue'])
        self.queue_processing_duration = Summary('imq_worker_queue_message_duration_seconds',
                                               'Message processing duration per queue',
                                               ['queue'])
        
        # Queue depth metrics
        self.queue_depth = Gauge('imq_worker_queue_depth',
                                'Current queue depth per queue',
                                ['queue'])
    
    def start_waiting(self):
        """Mark start of waiting period"""
        self.wait_start_time = time.time()
    
    def stop_waiting(self):
        """Mark end of waiting period"""
        if self.wait_start_time:
            self.total_wait_time += time.time() - self.wait_start_time
            self.wait_start_time = None
    
    def record_message_processed(self, queue_name, duration, success=True):
        """Record a processed message"""
        if not HAS_PROMETHEUS:
            return
            
        self.stop_waiting()  # Stop waiting timer if running
        
        # Update global metrics
        if success:
            self.messages_processed.inc()
            self.queue_messages_processed.labels(queue=queue_name).inc()
        else:
            self.messages_failed.inc()
            self.queue_messages_failed.labels(queue=queue_name).inc()
        
        self.processing_duration.observe(duration)
        self.queue_processing_duration.labels(queue=queue_name).observe(duration)
        
        # Track for averages
        self.queue_totals[queue_name]['count'] += 1
        self.queue_totals[queue_name]['total_time'] += duration
    
    def update_metrics(self, memory_monitor, worker_ref=None, thread_monitor=None):
        """Update current metric values"""
        if not HAS_PROMETHEUS:
            return

        uptime = time.time() - self.start_time
        self.up_time.set(uptime)

        # Update wait time metrics
        current_wait = self.total_wait_time
        if self.wait_start_time:  # Currently waiting
            current_wait += time.time() - self.wait_start_time

        self.wait_time.set(current_wait)
        self.wait_time_pct.set((current_wait / uptime * 100) if uptime > 0 else 0)

        # Update memory metrics
        self.rss_memory.set(memory_monitor.get_rss_bytes())
        if memory_monitor.max_rss_bytes:
            self.max_rss_memory.set(memory_monitor.max_rss_bytes)

        # Update thread / FD metrics
        if thread_monitor is not None:
            sample = thread_monitor.sample()
            self.thread_count.set(sample['py_threads'])
            self.os_thread_count.set(sample['os_threads'])
            if sample['fds'] is not None:
                self.fd_count.set(sample['fds'])
            if sample['baseline_os_threads'] is not None:
                self.baseline_os_thread_count.set(sample['baseline_os_threads'])
            if sample['baseline_fds'] is not None:
                self.baseline_fd_count.set(sample['baseline_fds'])
            if sample['thread_delta'] is not None:
                self.thread_delta.set(sample['thread_delta'])
            if sample['fd_delta'] is not None:
                self.fd_delta.set(sample['fd_delta'])
            self.max_thread_delta.set(thread_monitor.max_thread_delta)

        # Update queue depth metrics if worker reference provided
        if worker_ref:
            self.update_queue_depth_metrics(worker_ref)
    
    def update_queue_depth_metrics(self, worker_ref):
        """Update queue depth metrics with caching
        
        Args:
            worker_ref: Reference to the worker instance that has get_queue_depths method
        """
        if not HAS_PROMETHEUS:
            return
            
        current_time = time.time()
        
        # Check if we need to update the cache
        if (current_time - self.queue_depth_last_update) >= self.queue_depth_caching_period_s:
            try:
                # Get fresh queue depths from worker
                if hasattr(worker_ref, 'get_queue_depths'):
                    new_queue_depths = worker_ref.get_queue_depths()
                    self.queue_depth_cache = new_queue_depths
                    self.queue_depth_last_update = current_time
                    _logger.debug(f"Updated queue depth cache: {new_queue_depths}")
                else:
                    _logger.warning("Worker does not have get_queue_depths method")
            except Exception as e:
                _logger.error(f"Error updating queue depth metrics: {e}", exc_info=True)
                # Keep using cached values on error
        
        # Update Prometheus metrics with cached values
        for queue_name, depth in self.queue_depth_cache.items():
            self.queue_depth.labels(queue=queue_name).set(depth)
    
    def get_average_duration(self):
        """Get global average message processing duration"""
        total_count = sum(q['count'] for q in self.queue_totals.values())
        total_time = sum(q['total_time'] for q in self.queue_totals.values())
        return total_time / total_count if total_count > 0 else 0
    
    def get_queue_average_duration(self, queue_name):
        """Get average message processing duration for a queue"""
        queue_data = self.queue_totals.get(queue_name, {'count': 0, 'total_time': 0})
        return queue_data['total_time'] / queue_data['count'] if queue_data['count'] > 0 else 0

