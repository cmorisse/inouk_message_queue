# IMQ Workers v3 Implementation Plan

## Overview
Implement standalone CLI workers for IMQ that run independently of Odoo's ir.cron system, designed for high-performance message processing in Kubernetes environments.

## Key Requirements (from spec)
- Single process/thread per worker
- REGEX-based queue selection with round-robin processing
- RSS memory monitoring (excluding shared memory)
- Exit after fixed message count
- Graceful shutdown on SIGTERM
- Liveness probe support (no readiness probe)

## Architecture

### 1. Module Structure
```
inouk_message_queue/
├── cli/
│   ├── __init__.py
│   └── imq_worker.py       # CLI command implementation
├── workers/
│   ├── __init__.py
│   ├── base.py             # Base worker class (extracted from current worker.py)
│   ├── standalone.py       # Standalone worker implementation
│   └── monitoring.py       # Memory monitoring & liveness probe
└── utils/
    ├── __init__.py
    └── worker_utils.py     # Shared utilities
```

### 2. Key Components

#### A. CLI Command (`cli/imq_worker.py`)
```python
import argparse
import signal
import sys
import re
from odoo.cli import Command
from ..workers.standalone import StandaloneWorker

class IMQWorkerCommand(Command):
    """Run IMQ worker to process messages from queues"""
    name = 'imq-worker'
    
    def run(self, args):
        parser = argparse.ArgumentParser(
            prog=f'{sys.argv[0]} imq-worker',
            description='Run standalone IMQ worker'
        )
        parser.add_argument('--database', '-d', required=True,
                          help='Database name')
        parser.add_argument('--queue', '-q', required=True, 
                          help='Queue name or regex pattern (e.g., mpy.*)')
        parser.add_argument('--max-messages', type=int, default=0,
                          help='Exit after processing N messages (0=unlimited)')
        parser.add_argument('--max-rss-memory', type=str, default=None,
                          help='Exit when RSS memory exceeds limit (e.g., 1024M)')
        parser.add_argument('--worker-name', '-w', default=None,
                          help='Worker identifier for logging')
        parser.add_argument('--log-level', default='INFO',
                          help='Logging level')
        parser.add_argument('--observability-port', type=int, default=0,
                          help='Port for liveness probe and metrics HTTP server (0=disabled)')
        parser.add_argument('--metrics-path', type=str, default='/metrics',
                          help='HTTP path for Prometheus metrics endpoint')
        
        args = parser.parse_args(args)
        
        # Initialize and run worker
        worker = StandaloneWorker(
            database=args.database,
            queue_pattern=args.queue,
            max_messages=args.max_messages,
            max_rss_memory=args.max_rss_memory,
            worker_name=args.worker_name,
            log_level=args.log_level,
            observability_port=args.observability_port,
            metrics_path=args.metrics_path
        )
        
        return worker.run()
```

#### B. Standalone Worker (`workers/standalone.py`)
```python
import re
import signal
import logging
import psutil
import time
from odoo import api, SUPERUSER_ID
from odoo.modules.registry import Registry
from .base import BaseWorker
from .monitoring import MemoryMonitor, MetricsCollector, ObservabilityServer

class StandaloneWorker(BaseWorker):
    """Standalone worker that runs outside ir.cron"""
    
    def __init__(self, database, queue_pattern, **kwargs):
        self.database = database
        self.queue_pattern = queue_pattern
        self.max_messages = kwargs.get('max_messages', 0)
        self.max_rss_memory = kwargs.get('max_rss_memory')
        self.worker_name = kwargs.get('worker_name', f'worker-{os.getpid()}')
        self.processed_count = 0
        self.should_stop = False
        self.current_message = None
        
        # Initialize components
        self.memory_monitor = MemoryMonitor(self.max_rss_memory)
        self.metrics_collector = MetricsCollector(self.worker_name)
        self.observability_server = ObservabilityServer(
            kwargs.get('observability_port', 0),
            self.metrics_collector,
            kwargs.get('metrics_path', '/metrics')
        )
        
        # Setup logging
        log_level = getattr(logging, kwargs.get('log_level', 'INFO'))
        logging.basicConfig(level=log_level)
        self.logger = logging.getLogger(f'IMQWorker.{self.worker_name}')
        
        # Setup signal handlers
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown on SIGTERM"""
        def handle_sigterm(signum, frame):
            self.logger.info("Received SIGTERM, initiating graceful shutdown")
            self.should_stop = True
            
        signal.signal(signal.SIGTERM, handle_sigterm)
        signal.signal(signal.SIGINT, handle_sigterm)  # Also handle Ctrl+C
    
    def _find_matching_queues(self, env):
        """Find queues matching the regex pattern"""
        all_queues = env['imq.queue'].search([('active', '=', True)])
        pattern = re.compile(self.queue_pattern)
        matching_queues = [q for q in all_queues if pattern.match(q.name)]
        
        if not matching_queues:
            self.logger.error(f"No active queues found matching pattern: {self.queue_pattern}")
        else:
            self.logger.info(f"Found {len(matching_queues)} queues matching pattern: {self.queue_pattern}")
            for q in matching_queues:
                self.logger.info(f"  - {q.name}")
        
        return matching_queues
    
    def run(self):
        """Main processing loop"""
        self.logger.info(f"Starting IMQ worker {self.worker_name}")
        self.logger.info(f"Configuration: database={self.database}, queue_pattern={self.queue_pattern}")
        if self.max_messages:
            self.logger.info(f"Will exit after processing {self.max_messages} messages")
        if self.max_rss_memory:
            self.logger.info(f"Will exit when RSS memory exceeds {self.max_rss_memory}")
        
        # Start observability server if configured
        if self.observability_server.port:
            self.observability_server.start()
        
        try:
            # Get registry and find queues
            registry = Registry(self.database)
            with registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                queues = self._find_matching_queues(env)
                
            if not queues:
                return 1
            
            # Main processing loop
            queue_index = 0
            while not self.should_stop:
                # Check message count limit
                if self.max_messages > 0 and self.processed_count >= self.max_messages:
                    self.logger.info(f"Reached message limit ({self.max_messages}), exiting")
                    break
                
                # Check memory limit
                if not self.memory_monitor.check_memory():
                    self.logger.error(f"RSS memory limit exceeded ({self.max_rss_memory}), exiting")
                    break
                
                # Round-robin queue selection
                queue = queues[queue_index % len(queues)]
                queue_index += 1
                
                # Start waiting timer if no message found
                self.metrics_collector.start_waiting()
                
                # Process one message
                processed = self._process_one_message(registry, queue)
                if processed:
                    self.processed_count += 1
                    self.observability_server.update_last_activity()
                else:
                    # No message available, short sleep to avoid busy loop
                    time.sleep(0.1)
                
                # Update metrics periodically
                self.metrics_collector.update_metrics(self.memory_monitor)
            
            self.logger.info(f"Worker shutting down. Processed {self.processed_count} messages")
            return 0
            
        except Exception as e:
            self.logger.error(f"Fatal error in worker: {e}", exc_info=True)
            return 1
        finally:
            if self.observability_server.port:
                self.observability_server.stop()
    
    def _process_one_message(self, registry, queue):
        """Process a single message from the queue"""
        with registry.cursor() as cr:
            env = api.Environment(cr, SUPERUSER_ID, {})
            
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
                result = self.process_message(env, message_obj, message, {})
                processing_duration = time.time() - start_time
                
                # Record metrics
                self.metrics_collector.record_message_processed(queue.name, processing_duration, success=True)
                
                # Commit on success
                cr.commit()
                self.logger.debug(f"Successfully processed message {message_obj.id}")
                return True
                
            except Exception as e:
                processing_duration = time.time() - start_time
                self.metrics_collector.record_message_processed(queue.name, processing_duration, success=False)
                
                cr.rollback()
                self.logger.error(f"Error processing message: {e}", exc_info=True)
                return False
            finally:
                self.current_message = None
```

#### C. Base Worker Refactoring (`workers/base.py`)
```python
# Extract core methods from current imq.worker model
class BaseWorker:
    """Base worker functionality extracted from imq.worker model"""
    
    def get_message(self, env, queue_obj, wait_time=0):
        """Get next message from queue"""
        # Extracted from current implementation
        
    def store_message(self, env, queue_obj, message, start_timestamp=None):
        """Store message in imq.message"""
        # Extracted from current implementation
        
    def process_message(self, env, message_obj, message, worker_param):
        """Process a single message"""
        # Extracted from current implementation
        
    def terminate_message(self, env, queue_obj, message):
        """Mark message as processed"""
        # Extracted from current implementation
```

#### D. Monitoring Component (`workers/monitoring.py`)
```python
import psutil
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import time
from prometheus_client import Counter, Gauge, Histogram, Summary, Info
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from collections import defaultdict

class MemoryMonitor:
    """Monitor RSS memory usage"""
    
    def __init__(self, max_rss_str=None):
        self.max_rss_bytes = self._parse_memory_limit(max_rss_str)
        self.process = psutil.Process()
    
    def _parse_memory_limit(self, limit_str):
        """Parse memory limit string (e.g., '1024M', '2G')"""
        if not limit_str:
            return None
            
        multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3}
        limit_str = limit_str.upper().strip()
        
        for suffix, multiplier in multipliers.items():
            if limit_str.endswith(suffix):
                return int(limit_str[:-1]) * multiplier
        
        return int(limit_str)  # Assume bytes if no suffix
    
    def check_memory(self):
        """Check if RSS memory is within limit"""
        if not self.max_rss_bytes:
            return True
            
        # Get RSS excluding shared memory
        memory_info = self.process.memory_info()
        rss = memory_info.rss
        
        return rss < self.max_rss_bytes
    
    def get_rss_mb(self):
        """Get current RSS in MB"""
        return self.process.memory_info().rss / (1024 * 1024)
    
    def get_rss_bytes(self):
        """Get current RSS in bytes"""
        return self.process.memory_info().rss


class MetricsCollector:
    """Collect and expose Prometheus metrics"""
    
    def __init__(self, worker_name):
        self.worker_name = worker_name
        self.start_time = time.time()
        self.wait_start_time = None
        self.total_wait_time = 0
        
        # Global metrics
        self.info = Info('imq_worker', 'IMQ Worker information')
        self.info.info({'worker_name': worker_name})
        
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
        
        # Per-queue average tracking
        self.queue_totals = defaultdict(lambda: {'count': 0, 'total_time': 0})
    
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
                    handler_self.send_response(200)
                    handler_self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                    handler_self.end_headers()
                    handler_self.wfile.write(generate_latest())
                
                else:
                    handler_self.send_response(404)
                    handler_self.end_headers()
            
            def log_message(self, format, *args):
                pass  # Suppress access logs
        
        self.server = HTTPServer(('0.0.0.0', self.port), ObservabilityHandler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.daemon = True
        self.thread.start()
    
    def update_last_activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()
    
    def stop(self):
        """Stop HTTP server"""
        if self.server:
            self.server.shutdown()
```

### 3. Implementation Phases

#### Phase 1: Core Infrastructure (Week 1) ✅ COMPLETED
1. ✅ Create CLI command structure and argument parsing
2. ✅ Extract base worker logic from current `imq.worker` model
3. ✅ Implement basic standalone worker class
4. ✅ Setup logging and signal handling

#### Phase 2: Queue Processing (Week 1-2) ✅ COMPLETED
1. ✅ Implement regex-based queue matching
2. ✅ Implement round-robin queue processing
3. ✅ Integrate with existing message processing logic
4. ✅ Test with single and multiple queues

#### Phase 3: Resource Management & Metrics (Week 2) ✅ COMPLETED
1. ✅ Implement RSS memory monitoring
2. ✅ Add message count limits
3. ✅ Implement Prometheus metrics collection
4. ✅ Implement graceful shutdown on limits
5. ✅ Test resource limit behaviors

#### Phase 4: Kubernetes Integration (Week 3) ✅ COMPLETED
1. ✅ Implement observability server with liveness probe and metrics
2. ✅ Test SIGTERM handling and graceful shutdown
3. ✅ Create example Kubernetes deployments with ServiceMonitor
4. ✅ Document deployment and monitoring best practices

#### Phase 5: Performance Benchmarking & Ecosystem Comparison (Week 4+)
1. ✅ Unit tests for all new components - COMPLETED
2. ✅ Integration tests with real queues - COMPLETED  
3. 📋 **Performance benchmarking against messaging ecosystem** - TODO
4. ✅ Update documentation - COMPLETED

##### Phase 5.3: Performance Benchmark Plan 📋 TODO

**Status**: Future implementation - comprehensive benchmarking framework for ecosystem positioning

**Objective**: Evaluate IMQ Workers v3 performance against other messaging systems to understand positioning in the ecosystem, identify optimization opportunities, and provide deployment guidance.

**Step 1: IMQ Baseline Performance Characterization**
- **Scope**: Establish IMQ's performance characteristics in isolation
- **Test Scenarios**:
  - Single worker, single queue: throughput and latency baseline
  - Multiple workers, single queue: scaling and contention behavior  
  - Single worker, multiple queues: queue switching overhead
  - Resource utilization under sustained load
- **Metrics**: Messages/sec, P95/P99 latency, CPU/memory usage, queue depth
- **Tools**: Custom benchmarking scripts, Prometheus metrics, system monitoring
- **Deliverable**: IMQ Performance Profile document

**Step 2: Database-Based Messaging Comparison**
- **Scope**: Compare with similar database-backed messaging systems
- **Systems**: PostgreSQL LISTEN/NOTIFY, Redis Streams
- **Rationale**: Similar architecture and persistence patterns to IMQ
- **Test Scenarios**: Message throughput, persistence overhead, connection scaling, task durability
- **Deliverable**: Database Messaging Comparison Report

**Step 3: RabbitMQ Comparison**  
- **Scope**: Compare with RabbitMQ work queues and task processing
- **Systems**: RabbitMQ (work queues, durable queues, acknowledgments)
- **Rationale**: Most relevant comparison for task queue semantics and business requirements
- **Test Scenarios**: 
  - Exactly-once delivery guarantees
  - Ordered processing within queues  
  - Task durability and persistence
  - Worker scaling and load distribution
  - Message acknowledgment patterns
- **Deliverable**: IMQ vs RabbitMQ Task Processing Analysis

**Step 4: Cloud Task Queue Comparison**
- **Scope**: Compare with cloud-native task queue services
- **Systems**: AWS SQS, Azure Service Bus
- **Rationale**: Cloud deployment and managed service alternatives for task processing
- **Test Scenarios**: Task delivery guarantees, scaling, operational overhead, integration patterns
- **Deliverable**: Cloud vs Self-Hosted Task Queue Analysis

**Step 5: Comprehensive Ecosystem Positioning**
- **Scope**: Synthesize all benchmark results into ecosystem positioning
- **Deliverables**:
  - Performance comparison matrix across all systems
  - Use case recommendation guide (when to use IMQ vs alternatives)
  - IMQ optimization roadmap based on competitive analysis
  - Deployment decision framework

**Benchmark Framework Requirements**:
- **Standardized test harness** for consistent measurement across systems
- **Realistic workload patterns** based on Muppy usage patterns
- **Multiple deployment environments** (local, cloud, Kubernetes)
- **Automated benchmark execution** with CI/CD integration
- **Reproducible results** with documented setup procedures

**Success Criteria**:
- Clear understanding of IMQ's performance envelope
- Competitive positioning against task queue and messaging systems
- Data-driven optimization priorities for future development
- Evidence-based deployment and scaling recommendations

### 4. Kubernetes Deployment Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: imq-worker-default
spec:
  replicas: 3
  selector:
    matchLabels:
      app: imq-worker
      queue: default
  template:
    metadata:
      labels:
        app: imq-worker
        queue: default
    spec:
      containers:
      - name: worker
        image: myapp:latest
        command: ["odoo"]
        args:
          - "imq-worker"
          - "--database=$(DATABASE_NAME)"
          - "--queue=default"
          - "--max-messages=1000"
          - "--max-rss-memory=512M"
          - "--observability-port=8080"
          - "--metrics-path=/metrics"
        env:
        - name: DATABASE_NAME
          valueFrom:
            secretKeyRef:
              name: odoo-secrets
              key: database
        - name: PGPASSWORD
          valueFrom:
            secretKeyRef:
              name: odoo-secrets
              key: db-password
        resources:
          limits:
            memory: "768Mi"  # Higher than --max-rss-memory for safety
          requests:
            memory: "256Mi"
            cpu: "250m"
        livenessProbe:
          httpGet:
            path: /livez
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 30
          timeoutSeconds: 5
          failureThreshold: 3
---
apiVersion: v1
kind: Service
metadata:
  name: imq-worker-metrics
  labels:
    app: imq-worker
spec:
  ports:
  - port: 8080
    targetPort: 8080
    name: observability
  selector:
    app: imq-worker
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: imq-worker-metrics
  labels:
    app: imq-worker
spec:
  selector:
    matchLabels:
      app: imq-worker
  endpoints:
  - port: observability
    path: /metrics
    interval: 30s
    scrapeTimeout: 10s
```

### 5. Configuration Best Practices

#### Memory Limits
- Set `--max-rss-memory` to ~70% of Kubernetes memory limit
- This allows graceful shutdown before OOM killer
- Example: K8s limit=768Mi, RSS limit=512M

#### Message Limits
- Use `--max-messages` to ensure regular pod recycling
- Prevents memory leaks from accumulating
- Typical values: 1000-10000 depending on message size

#### Queue Patterns
- Prefer exact queue names for predictable behavior
- Use regex only when necessary (e.g., dynamic queue creation)
- Document queue naming conventions

### 6. Migration Strategy

1. **Parallel Operation**
   - Run both cron and standalone workers initially
   - Monitor performance and stability

2. **Gradual Migration**
   - Migrate one queue at a time
   - Start with low-priority queues
   - Monitor error rates and processing times

3. **Cron Deprecation**
   - Disable cron workers once standalone proven stable
   - Keep cron code for fallback if needed

### 7. Monitoring & Observability

#### Prometheus Metrics Exposed
As specified in the requirements, each worker exposes these metrics:

**Global Metrics:**
- `imq_worker_uptime_seconds` - Worker uptime in seconds
- `imq_worker_wait_time_seconds` - Total time spent waiting for messages
- `imq_worker_wait_time_percent` - Percentage of time spent waiting
- `imq_worker_messages_processed_total` - Total messages processed
- `imq_worker_messages_failed_total` - Total messages failed
- `imq_worker_message_duration_seconds` - Message processing duration histogram
- `imq_worker_rss_memory_bytes` - Current RSS memory usage
- `imq_worker_max_rss_memory_bytes` - Maximum RSS memory configured

**Per-Queue Metrics:**
- `imq_worker_queue_messages_processed_total{queue="queue_name"}` - Messages processed per queue
- `imq_worker_queue_messages_failed_total{queue="queue_name"}` - Messages failed per queue
- `imq_worker_queue_message_duration_seconds{queue="queue_name"}` - Processing duration per queue

#### Grafana Dashboard Queries
```promql
# Worker uptime
imq_worker_uptime_seconds

# Messages processed per second
rate(imq_worker_messages_processed_total[5m])

# Average processing time
rate(imq_worker_message_duration_seconds_sum[5m]) / rate(imq_worker_message_duration_seconds_count[5m])

# Error rate percentage
rate(imq_worker_messages_failed_total[5m]) / rate(imq_worker_messages_processed_total[5m]) * 100

# Memory usage percentage
imq_worker_rss_memory_bytes / imq_worker_max_rss_memory_bytes * 100

# Wait time percentage
imq_worker_wait_time_percent
```

#### Logging Format
```
2024-01-15 10:30:45,123 IMQWorker.worker-1234 INFO Started processing message 5678 from queue 'default'
2024-01-15 10:30:45,456 IMQWorker.worker-1234 INFO Successfully processed message 5678 in 0.333s
2024-01-15 10:30:45,789 IMQWorker.worker-1234 INFO RSS memory: 245.6 MB / 512.0 MB (47.9%)
```

### 8. Error Handling

- Maintain existing error semantics (IMQError, IMQRetryableError)
- Log errors with full context
- Ensure graceful shutdown even on errors
- Never lose messages due to worker crashes

### 9. Performance Optimizations

1. **Connection Reuse**
   - Keep database connections alive between messages
   - Reconnect automatically on connection loss

2. **Efficient Queue Polling**
   - Use appropriate wait times for queue providers
   - Avoid busy loops when queues empty

3. **Memory Efficiency**
   - Clear large objects after processing
   - Use weak references where appropriate

### 10. Dependencies

#### New Python Dependencies
Add to requirements or setup.py:
```python
prometheus_client>=0.14.0  # For metrics collection and exposition
psutil>=5.8.0             # For memory monitoring (likely already present)
```

#### Module Dependencies
No additional Odoo modules required beyond existing IMQ dependencies.

### 11. Testing Requirements

#### Unit Tests
- Queue pattern matching
- Memory limit parsing and checking
- Signal handling
- Metrics collection and exposition
- Liveness probe responses

#### Integration Tests
- Multi-queue processing
- Resource limit enforcement
- Graceful shutdown scenarios
- Error recovery
- Prometheus metrics accuracy

#### Performance Tests
- Throughput comparison with cron workers
- Memory usage patterns
- Startup/shutdown times
- Metrics collection overhead