#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psutil
import threading
import time
import logging
import json
import os
from datetime import datetime, timezone
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
    
    def update_metrics(self, memory_monitor, worker_ref=None):
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


class HealthChecker:
    """Advanced health checking for IMQ workers"""
    
    def __init__(self, worker_ref, metrics_collector, memory_monitor):
        self.worker = worker_ref
        self.metrics_collector = metrics_collector
        self.memory_monitor = memory_monitor
        self.start_time = time.time()
        self.last_health_check = 0
        self.cached_health = None
        self.health_cache_ttl = 30  # Cache health for 30 seconds
        
    def get_health(self):
        """Get comprehensive health status"""
        now = time.time()
        if self.cached_health and (now - self.last_health_check) < self.health_cache_ttl:
            return self.cached_health
            
        checks = {}
        overall_status = "healthy"
        
        # Database check
        try:
            if hasattr(self.worker, 'test_database_connection'):
                self.worker.test_database_connection()
            checks['database'] = 'ok'
        except Exception as e:
            checks['database'] = f'error: {str(e)[:100]}'
            overall_status = "unhealthy"
        
        # Memory usage check
        memory_info = self.memory_monitor.get_memory_info()
        if memory_info['usage_percent'] and memory_info['usage_percent'] > 90:
            checks['memory_usage'] = 'critical'
            overall_status = "unhealthy"
        elif memory_info['usage_percent'] and memory_info['usage_percent'] > 80:
            checks['memory_usage'] = 'warning'
            if overall_status == "healthy":
                overall_status = "degraded"
        else:
            checks['memory_usage'] = 'ok'
            
        # Queue connection check
        try:
            if hasattr(self.worker, 'queues') and self.worker.queues:
                checks['queue_connection'] = 'ok'
            else:
                checks['queue_connection'] = 'no_queues'
                overall_status = "unhealthy"
        except Exception:
            checks['queue_connection'] = 'error'
            overall_status = "unhealthy"
            
        # Message processing check
        uptime = now - self.start_time
        if uptime > 300:  # After 5 minutes of uptime
            if hasattr(self.worker, 'last_message_time'):
                time_since_last = now - getattr(self.worker, 'last_message_time', now)
                if time_since_last > 600:  # No message in 10 minutes
                    checks['message_processing'] = 'stuck'
                    if overall_status == "healthy":
                        overall_status = "degraded"
                elif time_since_last > 120:  # Slow processing
                    checks['message_processing'] = 'slow'
                else:
                    checks['message_processing'] = 'ok'
            else:
                checks['message_processing'] = 'ok'
        else:
            checks['message_processing'] = 'starting'
            
        uptime_str = self._format_duration(uptime)
        last_activity = getattr(self.worker, 'last_activity_time', self.start_time)
        
        health = {
            "apiVersion": "imq/v1",
            "kind": "WorkerHealth", 
            "metadata": {
                "worker_name": getattr(self.worker, 'worker_name', 'unknown'),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "database": getattr(self.worker, 'database', 'unknown')
            },
            "status": {
                "status": overall_status,
                "checks": checks,
                "uptime": uptime_str,
                "last_activity": datetime.fromtimestamp(last_activity, timezone.utc).isoformat()
            }
        }
        
        self.cached_health = health
        self.last_health_check = now
        return health
        
    def get_readiness(self):
        """Get readiness status for Kubernetes readiness probe"""
        ready = True
        reason = "ready"
        
        # Check if worker is shutting down
        if getattr(self.worker, 'should_stop', False):
            ready = False
            reason = "shutting_down"
        # Check if worker is still starting up
        elif (time.time() - self.start_time) < 30:
            ready = False
            reason = "starting"
        # Check memory limits
        elif not self.memory_monitor.check_memory():
            ready = False
            reason = "overloaded"
            
        queue_status = {
            "connected": True,
            "queues_found": len(getattr(self.worker, 'queues', [])),
            "last_poll": datetime.now(timezone.utc).isoformat()
        }
        
        return {
            "apiVersion": "imq/v1", 
            "kind": "WorkerReadiness",
            "metadata": {
                "worker_name": getattr(self.worker, 'worker_name', 'unknown'),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "database": getattr(self.worker, 'database', 'unknown')
            },
            "status": {
                "ready": ready,
                "reason": reason,
                "queue_status": queue_status
            }
        }
        
    def get_detailed_status(self):
        """Get comprehensive status information"""
        uptime = time.time() - self.start_time
        memory_info = self.memory_monitor.get_memory_info()
        
        # Calculate processing rate
        processed_count = getattr(self.worker, 'processed_count', 0)
        processing_rate = (processed_count / (uptime / 60)) if uptime > 0 else 0
        
        # Get queue depths if available
        queue_depths = {}
        if hasattr(self.worker, 'get_queue_depths'):
            try:
                queue_depths = self.worker.get_queue_depths()
            except Exception as e:
                _logger.warning(f"Failed to get queue depths: {e}")
        
        # Get queue information
        queues_info = []
        if hasattr(self.worker, 'queues'):
            for queue in getattr(self.worker, 'queues', []):
                # Handle both dict and object formats for compatibility
                if isinstance(queue, dict):
                    queue_name = queue.get('name', 'unknown')
                else:
                    queue_name = getattr(queue, 'name', 'unknown')
                    
                avg_duration = self.metrics_collector.get_queue_average_duration(queue_name)
                queue_totals = self.metrics_collector.queue_totals.get(queue_name, {'count': 0, 'total_time': 0})
                
                queues_info.append({
                    "name": queue_name,
                    "processed": queue_totals['count'],
                    "failed": 0,  # TODO: Add failed count tracking
                    "avg_duration": f"{avg_duration:.1f}s",
                    "depth": queue_depths.get(queue_name, 0)  # Add queue depth
                })
        
        return {
            "apiVersion": "imq/v1",
            "kind": "WorkerStatus",
            "metadata": {
                "worker_name": getattr(self.worker, 'worker_name', 'unknown'),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "database": getattr(self.worker, 'database', 'unknown')
            },
            "status": {
                "worker": {
                    "name": getattr(self.worker, 'worker_name', 'unknown'),
                    "version": "v3.0.0",
                    "uptime": self._format_duration(uptime),
                    "pid": os.getpid(),
                    "started_at": datetime.fromtimestamp(self.start_time, timezone.utc).isoformat()
                },
                "configuration": {
                    "queue_pattern": getattr(self.worker, 'queue_pattern', 'unknown'),
                    "max_messages": getattr(self.worker, 'max_messages', 0),
                    "max_rss_memory": getattr(self.worker, 'max_rss_memory', None),
                    "database": getattr(self.worker, 'database', 'unknown')
                },
                "runtime": {
                    "messages_processed": processed_count,
                    "current_memory_mb": memory_info['rss_mb'],
                    "memory_usage_percent": memory_info['usage_percent'],
                    "processing_rate_per_minute": round(processing_rate, 1),
                    "average_processing_time": f"{self.metrics_collector.get_average_duration():.1f}s",
                    "last_message_at": datetime.fromtimestamp(
                        getattr(self.worker, 'last_message_time', self.start_time), timezone.utc
                    ).isoformat()
                },
                "queues": queues_info,
                "limits": {
                    "message_limit_reached": (
                        getattr(self.worker, 'max_messages', 0) > 0 and 
                        processed_count >= getattr(self.worker, 'max_messages', 0)
                    ),
                    "memory_limit_reached": not self.memory_monitor.check_memory(),
                    "shutdown_requested": getattr(self.worker, 'should_stop', False)
                }
            }
        }
        
    def _format_duration(self, seconds):
        """Format duration in human readable format"""
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}h{minutes}m{secs}s"
        elif minutes > 0:
            return f"{minutes}m{secs}s"
        else:
            return f"{secs}s"


class ObservabilityServer:
    """HTTP server for liveness probe and metrics"""
    
    def __init__(self, port, metrics_collector, memory_monitor, worker_ref=None, metrics_path='/metrics'):
        self.port = port
        self.metrics_collector = metrics_collector
        self.memory_monitor = memory_monitor
        self.worker_ref = worker_ref
        self.metrics_path = metrics_path
        self.last_activity = time.time()
        self.server = None
        self.thread = None
        self.health_checker = None
        
        # Initialize health checker if worker reference provided
        if self.worker_ref:
            self.health_checker = HealthChecker(worker_ref, metrics_collector, memory_monitor)
        
        if self.port:
            _logger.info(f"Observability server will listen on port {self.port}")
            _logger.info(f"Liveness probe: http://localhost:{self.port}/livez")
            _logger.info(f"Health check: http://localhost:{self.port}/healthz")
            _logger.info(f"Readiness probe: http://localhost:{self.port}/readyz")
            _logger.info(f"Status endpoint: http://localhost:{self.port}/status")
            _logger.info(f"Metrics endpoint: http://localhost:{self.port}{self.metrics_path}")
    
    def start(self):
        """Start HTTP server in background thread"""
        if not self.port:
            return
            
        parent = self  # Capture self for inner class
        
        class ObservabilityHandler(BaseHTTPRequestHandler):
            def do_GET(handler_self):
                try:
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
                    
                    elif handler_self.path == '/healthz':
                        # Health check endpoint
                        if parent.health_checker:
                            health_data = parent.health_checker.get_health()
                            status_code = 200 if health_data['status']['status'] in ['healthy', 'degraded'] else 503
                            handler_self._send_json_response(status_code, health_data)
                        else:
                            handler_self._send_json_response(503, {
                                "status": "unhealthy",
                                "error": "Health checker not available"
                            })
                    
                    elif handler_self.path == '/readyz':
                        # Readiness probe endpoint
                        if parent.health_checker:
                            readiness_data = parent.health_checker.get_readiness()
                            status_code = 200 if readiness_data['status']['ready'] else 503
                            handler_self._send_json_response(status_code, readiness_data)
                        else:
                            handler_self._send_json_response(503, {
                                "ready": False,
                                "reason": "health_checker_unavailable"
                            })
                    
                    elif handler_self.path == '/status':
                        # Detailed status endpoint
                        if parent.health_checker:
                            status_data = parent.health_checker.get_detailed_status()
                            handler_self._send_json_response(200, status_data)
                        else:
                            handler_self._send_json_response(503, {
                                "error": "Status information not available",
                                "reason": "health_checker_unavailable"
                            })
                    
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
                        handler_self.send_header('Content-Type', 'application/json')
                        handler_self.end_headers()
                        error_response = {
                            "error": "Not Found",
                            "available_endpoints": ["/livez", "/healthz", "/readyz", "/status", parent.metrics_path]
                        }
                        handler_self.wfile.write(json.dumps(error_response).encode('utf-8'))
                        
                except Exception as e:
                    _logger.error(f"Error handling request {handler_self.path}: {e}")
                    handler_self.send_response(500)
                    handler_self.send_header('Content-Type', 'application/json')
                    handler_self.end_headers()
                    error_response = {"error": "Internal server error", "details": str(e)}
                    handler_self.wfile.write(json.dumps(error_response).encode('utf-8'))
            
            def _send_json_response(self, status_code, data):
                """Helper to send JSON responses"""
                self.send_response(status_code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                json_data = json.dumps(data, indent=2)
                self.wfile.write(json_data.encode('utf-8'))
            
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