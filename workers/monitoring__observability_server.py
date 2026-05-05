#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

_logger = logging.getLogger(__name__)


class HealthChecker:
    """Advanced health checking for IMQ workers"""

    def __init__(self, worker_ref, metrics_collector, memory_monitor, thread_monitor=None):
        self.worker = worker_ref
        self.metrics_collector = metrics_collector
        self.memory_monitor = memory_monitor
        self.thread_monitor = thread_monitor
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

        if getattr(self.worker, 'should_stop', False):
            ready = False
            reason = "shutting_down"
        elif (time.time() - self.start_time) < 30:
            ready = False
            reason = "starting"
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

        processed_count = getattr(self.worker, 'processed_count', 0)
        processing_rate = (processed_count / (uptime / 60)) if uptime > 0 else 0

        queue_depths = {}
        if hasattr(self.worker, 'get_queue_depths'):
            try:
                queue_depths = self.worker.get_queue_depths()
            except Exception as e:
                _logger.warning(f"Failed to get queue depths: {e}")

        queues_info = []
        if hasattr(self.worker, 'queues'):
            for queue in getattr(self.worker, 'queues', []):
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
                    "depth": queue_depths.get(queue_name, 0)
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
                "threads": self._threads_block(),
                "limits": {
                    "message_limit_reached": (
                        getattr(self.worker, 'max_messages', 0) > 0 and
                        processed_count >= getattr(self.worker, 'max_messages', 0)
                    ),
                    "memory_limit_reached": not self.memory_monitor.check_memory(),
                    "thread_limit_reached": (
                        self.thread_monitor is not None and
                        not self.thread_monitor.check_threads()
                    ),
                    "shutdown_requested": getattr(self.worker, 'should_stop', False)
                }
            }
        }

    def _threads_block(self):
        """Build threads/FDs sub-block for /status."""
        if not self.thread_monitor:
            return {
                "enabled": False,
                "reason": "thread_monitor_unavailable",
            }
        sample = self.thread_monitor.last_sample or self.thread_monitor.sample()
        return {
            "enabled": True,
            "python_threads": sample['py_threads'],
            "os_threads": sample['os_threads'],
            "thread_delta": sample['thread_delta'],
            "baseline_os_threads": sample['baseline_os_threads'],
            "fds": sample['fds'],
            "fd_delta": sample['fd_delta'],
            "baseline_fds": sample['baseline_fds'],
            "max_thread_delta": sample['max_thread_delta'],  # 0 = no limit
            "warn_threshold_delta": sample['warn_threshold_delta'],
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

    def __init__(self, port, metrics_collector, memory_monitor, worker_ref=None,
                 metrics_path='/metrics', thread_monitor=None):
        self.port = port
        self.metrics_collector = metrics_collector
        self.memory_monitor = memory_monitor
        self.thread_monitor = thread_monitor
        self.worker_ref = worker_ref
        self.metrics_path = metrics_path
        self.last_activity = time.time()
        self.server = None
        self.thread = None
        self.health_checker = None

        if self.worker_ref:
            self.health_checker = HealthChecker(
                worker_ref, metrics_collector, memory_monitor,
                thread_monitor=thread_monitor,
            )

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
                        if time.time() - parent.last_activity < 300:
                            handler_self.send_response(200)
                            handler_self.end_headers()
                            handler_self.wfile.write(b'OK')
                        else:
                            handler_self.send_response(503)
                            handler_self.end_headers()
                            handler_self.wfile.write(b'No recent activity')

                    elif handler_self.path == '/healthz':
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
                        if parent.health_checker:
                            status_data = parent.health_checker.get_detailed_status()
                            handler_self._send_json_response(200, status_data)
                        else:
                            handler_self._send_json_response(503, {
                                "error": "Status information not available",
                                "reason": "health_checker_unavailable"
                            })

                    elif handler_self.path == parent.metrics_path:
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
                self.send_response(status_code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))

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
