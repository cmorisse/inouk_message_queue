#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psutil
import threading
import time
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import defaultdict

try:
    from prometheus_client import Counter, Gauge, Histogram, Summary, Info
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
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


class MetricsCollector:
    """Collect and expose Prometheus metrics"""
    
    def __init__(self, worker_name):
        self.worker_name = worker_name
        self.start_time = time.time()
        self.wait_start_time = None
        self.total_wait_time = 0
        
        # Per-queue average tracking
        self.queue_totals = defaultdict(lambda: {'count': 0, 'total_time': 0})
        
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
    
    def update_metrics(self, memory_monitor):
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
    
    def get_average_duration(self):
        """Get global average message processing duration"""
        total_count = sum(q['count'] for q in self.queue_totals.values())
        total_time = sum(q['total_time'] for q in self.queue_totals.values())
        return total_time / total_count if total_count > 0 else 0
    
    def get_queue_average_duration(self, queue_name):
        """Get average message processing duration for a queue"""
        queue_data = self.queue_totals.get(queue_name, {'count': 0, 'total_time': 0})
        return queue_data['total_time'] / queue_data['count'] if queue_data['count'] > 0 else 0


class ObservabilityServer:
    """HTTP server for liveness probe and metrics"""
    
    def __init__(self, port, metrics_collector, metrics_path='/metrics'):
        self.port = port
        self.metrics_collector = metrics_collector
        self.metrics_path = metrics_path
        self.last_activity = time.time()
        self.server = None
        self.thread = None
        
        if self.port:
            _logger.info(f"Observability server will listen on port {self.port}")
            _logger.info(f"Liveness probe: http://localhost:{self.port}/livez")
            _logger.info(f"Metrics endpoint: http://localhost:{self.port}{self.metrics_path}")
    
    def start(self):
        """Start HTTP server in background thread"""
        if not self.port:
            return
            
        parent = self  # Capture self for inner class
        
        class ObservabilityHandler(BaseHTTPRequestHandler):
            def do_GET(handler_self):
                if handler_self.path == '/livez':
                    # Liveness check - no activity for 5 minutes = dead
                    if time.time() - parent.last_activity < 300:
                        handler_self.send_response(200)
                        handler_self.end_headers()
                        handler_self.wfile.write(b'OK')
                    else:
                        handler_self.send_response(503)
                        handler_self.end_headers()
                        handler_self.wfile.write(b'No recent activity')
                
                elif handler_self.path == parent.metrics_path:
                    # Prometheus metrics endpoint
                    if HAS_PROMETHEUS:
                        handler_self.send_response(200)
                        handler_self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                        handler_self.end_headers()
                        handler_self.wfile.write(generate_latest())
                    else:
                        handler_self.send_response(503)
                        handler_self.end_headers()
                        handler_self.wfile.write(b'Prometheus client not available')
                
                else:
                    handler_self.send_response(404)
                    handler_self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress access logs
        
        try:
            self.server = HTTPServer(('0.0.0.0', self.port), ObservabilityHandler)
            self.thread = threading.Thread(target=self.server.serve_forever)
            self.thread.daemon = True
            self.thread.start()
            _logger.info(f"Observability server started on port {self.port}")
        except Exception as e:
            _logger.error(f"Failed to start observability server: {e}")
            self.server = None
            self.thread = None
    
    def update_last_activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()
    
    def stop(self):
        """Stop HTTP server"""
        if self.server:
            self.server.shutdown()
            _logger.info("Observability server stopped")