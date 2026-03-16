# Inouk Message Queue (IMQ)

**The production-ready task queue system built for Odoo developers who need reliable asynchronous processing.**

Turn any Odoo method into an async task with just one `@processor` annotation. Get enterprise-grade task processing with exactly-once delivery, automatic retries, comprehensive logging, and transparent scaling from PostgreSQL to AWS SQS.

## What IMQ Is

**IMQ is a task queue system** designed for business applications that require:
- **Exactly-once processing** - No duplicate task execution
- **Ordered task processing** - Sequential execution within queues  
- **Reliable delivery** - Tasks are never lost, even during failures
- **Business transaction safety** - Full integration with Odoo's database transactions
- **Comprehensive audit trails** - Complete logging and monitoring of all task execution

## What IMQ Is NOT

IMQ is **not** a pub/sub system, event streaming platform, or real-time messaging solution. It's specifically designed for **reliable task processing** where business semantics and data consistency matter more than raw throughput.

## Why Choose IMQ?

### 🎯 **Effortless Odoo Integration**
```python
@processor('default')
def my_heavy_task(env, data):
    # Your business logic here
    pass

# That's it! Call anywhere in Odoo:
my_heavy_task.run_async({'key': 'value'})
```

### 📊 **Complete Observability** 
- **Automatic log capture** - Every print, log, and error is captured and stored
- **Comprehensive GUI** - Monitor, debug, and replay tasks through Odoo's interface
- **Processing history** - Full audit trail of all task attempts and outcomes
- **Performance metrics** - Built-in Prometheus metrics and health endpoints

### 🏗️ **Production-Ready Architecture**
- **PostgreSQL-native HA** - Leverage your existing database infrastructure for high availability
- **Transparent scaling** - Start with PostgreSQL, scale to AWS SQS without code changes
- **Kubernetes-ready** - Production deployments with monitoring and autoscaling
- **Battle-tested** - Used in production Odoo environments

### 🛡️ **Robust & Reliable**
- **Exactly-once delivery** - Business-critical tasks execute once and only once
- **Automatic retries** - Configurable retry policies with exponential backoff
- **Graceful error handling** - Distinguish between retryable and permanent failures
- **Database transaction safety** - Tasks integrate seamlessly with Odoo's transaction model

### 🔧 **Developer Experience**
- **Zero configuration** - Works out of the box with PostgreSQL
- **Rich debugging** - Inspect task state, logs, and execution history
- **Flexible routing** - Route tasks to specific queues and workers
- **Open source** - MIT licensed, community-driven development

## Quick Start

### 1. Install
```bash
# Add to your Odoo addons and install through Apps menu
```

### 2. Create a Task
```python
from odoo.addons.inouk_message_queue.api import processor

@processor('default')  # One annotation is all you need
def send_welcome_email(env, user_id):
    user = env['res.users'].browse(user_id)
    # Send email logic here
    return f"Email sent to {user.email}"
```

### 3. Execute Asynchronously
```python
# In any Odoo method:
send_welcome_email.run_async(user.id)
# Task is queued and will be processed by workers
```

### 4. Monitor & Debug
Navigate to **IMQ > Messages** in Odoo to see task execution, logs, and performance metrics.

---

## Technical Features

### 🚀 Dual Queue Architecture
- **PostgreSQL Provider**: Database-native queuing with ACID guarantees and HA support
- **AWS SQS Provider**: Cloud-scale processing with standard and FIFO queues
- **Transparent switching**: Change providers without modifying application code
- **Hybrid deployments**: Mix providers for different queue types

### 🎯 Developer-Friendly API
- **One-line integration**: `@processor('queue_name')` decorator
- **Automatic serialization**: Handles Odoo models, records, and complex Python objects
- **Method processors**: `@processor_method` for class methods and model integration
- **Flexible parameters**: Pass any JSON-serializable data to tasks

### 🛡️ Advanced Error Handling
- **IMQError**: Mark failures as permanent (no retry)
- **IMQRetryableError**: Automatic retries with configurable delays and backoff
- **IMQTerminateException**: Graceful worker termination
- **Transaction safety**: Full integration with Odoo's database transaction model
- **Dead letter handling**: Failed messages are preserved for analysis

### 📊 Enterprise Monitoring
- **Complete log capture**: Every print(), logger call, and exception is stored
- **Processing history**: Full audit trail with timing, attempts, and outcomes  
- **Performance metrics**: Built-in Prometheus metrics for production monitoring
- **GUI integration**: Rich Odoo interface for task management and debugging
- **Notification systems**: Slack, Teams, and Odoo chat integration for alerts

## Installation

1. Add `inouk_message_queue` to your Odoo addons path
2. Install the module through Odoo's Apps menu
3. Configure your queue provider (AWS SQS or PostgreSQL)

## Quick Start

### Basic Function Processing

```python
from odoo.addons.inouk_message_queue.api import processor

@processor('default')
def process_heavy_task(env, data, _imq_logger=None):
    """Process heavy task: {data.get('name')}"""
    _imq_logger.info("Processing started")
    # Your heavy processing here
    return "Success"

# Enqueue the task
process_heavy_task.run_async(env, {'name': 'My Task'})
```

### Model Method Processing

```python
from odoo import models
from odoo.addons.inouk_message_queue.api import processor_method

class MyModel(models.Model):
    _name = 'my.model'
    
    @processor_method('default')
    def process_records(self, _imq_logger=None):
        """Process {0.name} records"""
        for record in self:
            _imq_logger.info(f"Processing {record.name}")
            # Your processing logic
        return f"Processed {len(self)} records"

# Usage
records = env['my.model'].search([])
records.process_records.run_async()
```

## Configuration

### Queue Setup

1. Navigate to **IMQ > Configuration > Queues**
2. Create a new queue:
   - **Name**: Internal identifier (e.g., 'default')
   - **Provider**: AWS SQS or PostgreSQL
   - **Type**: Standard or FIFO
   - **Visibility Timeout**: Time a message stays invisible after delivery

### AWS SQS Configuration

```python
queue.write({
    'provider': 'aws_sqs',
    'region': 'us-east-1',
    'key': 'your-access-key',
    'secret': 'your-secret-key',
    'database_bound_q': True,  # Prefix queue name with Odoo database name
})
```

### Worker Types

IMQ provides two types of workers for processing messages:

#### Legacy Cron Workers (v2)
- **Runs inside Odoo**: Execute as standard Odoo scheduled actions (cron jobs)
- **Easy setup**: No additional infrastructure required - works in any Odoo instance
- **Lower performance**: Limited by Odoo's cron execution model
- **Best for**: Development, testing, and low-volume production environments

#### Standalone Workers (v3) - Recommended
- **Dedicated processes**: Run as separate processes outside of Odoo's web server
- **High performance**: Optimized for throughput with dedicated processing threads
- **Advanced features**: Pattern matching, round-robin processing, health monitoring
- **Production ready**: Kubernetes support, Prometheus metrics, graceful shutdowns
- **Best for**: Production environments requiring reliability and scale

#### Choosing Worker Type

| Feature | Cron Workers (v2) | Standalone Workers (v3) |
|---------|-------------------|------------------------|
| Setup complexity | Simple | Moderate |
| Performance | Lower | Higher |
| Resource usage | Shared with Odoo | Dedicated |
| Monitoring | Basic | Advanced (Prometheus) |
| Queue patterns | Fixed queues | Regex patterns |
| Kubernetes ready | No | Yes |
| Recommended for | Development/Testing | Production |

### Worker Configuration

#### Cron Worker Setup
Workers are configured as Odoo cron jobs:
1. Navigate to **Settings > Technical > Automation > Scheduled Actions**
2. Configure the IMQ worker cron job
3. Set execution frequency based on your needs

#### Standalone Worker Setup
Deploy workers using the CLI:
```bash
# Process a single queue
bin/start_odoo imq-worker --database $PGDATABASE --queue=default

# Process multiple queues with pattern
bin/start_odoo imq-worker --database $PGDATABASE --queue="high_priority_*" --max-messages=1000

# Run with monitoring enabled
bin/start_odoo imq-worker --database $PGDATABASE --queue=default --observability-port=9090
```

### Worker Control via System Parameters

You can control worker execution using system parameters for maintenance or debugging:

#### Stopping Cron Workers

To stop cron workers (traditional Odoo scheduled action workers):

1. Navigate to **Settings > Technical > Parameters > System Parameters**
2. Create or edit parameter:
   - **Key**: `imq.STOP_CRON_WORKERS`
   - **Value**: 
     - `*` to stop all cron workers
     - `hostname1,hostname2` to stop workers on specific servers
     - `server.example.com` to stop workers on a single server

#### Stopping Standalone Workers

To stop standalone workers (IMQ Workers v3):

1. Navigate to **Settings > Technical > Parameters > System Parameters**
2. Create or edit parameter:
   - **Key**: `imq.STOP_STANDALONE_WORKERS`
   - **Value**: 
     - `*` to stop all standalone workers
     - `hostname1,hostname2` to stop workers on specific servers
     - `server.example.com` to stop workers on a single server

#### Examples

```bash
# Stop all cron workers for maintenance
Key: imq.STOP_CRON_WORKERS
Value: *

# Stop standalone workers on production servers only
Key: imq.STOP_STANDALONE_WORKERS  
Value: prod-worker-01.internal,prod-worker-02.internal

# Stop all workers on current server (mixed environment)
Key: imq.STOP_CRON_WORKERS
Value: $(hostname)

Key: imq.STOP_STANDALONE_WORKERS
Value: $(hostname)
```

**Note**: Workers check these parameters periodically and will stop gracefully when detected. This allows for controlled shutdown during deployments or maintenance without killing processes.

## Advanced Usage

### Error Handling

```python
from odoo.addons.inouk_message_queue.api import (
    processor, IMQError, IMQRetryableError
)

@processor('default')
def task_with_error_handling(env, data, _imq_logger=None):
    """Task with custom error handling"""
    try:
        # Your logic here
        if not data.get('required_field'):
            raise IMQError("Missing required field - do not retry")
        
        if external_service_down():
            raise IMQRetryableError(
                "Service temporarily unavailable",
                delay=300  # Retry after 5 minutes
            )
            
    except Exception as e:
        _imq_logger.error(f"Unexpected error: {e}")
        raise
```

### Message Deduplication (FIFO Queues)

```python
# Prevent duplicate processing within time window
records.process_records.run_async(
    _imq_message_group='batch-1',
    _imq_message_deduplication_id='unique-task-id'
)
```

### Custom Visibility Timeout

```python
@processor_method('default', processor_visibility_timeout=300)
def long_running_task(self):
    """Task that needs 5 minutes to complete"""
    # Long processing...
```

### Parent-Child Message Relationships

```python
# Create child tasks that update parent progress
parent_msg_id = env.context.get('_imq_parent_message_id')
for item in items:
    process_item.run_async(
        item,
        _imq_parent_message_id=parent_msg_id,
        _imq_target_children_count=len(items)
    )
```

## Monitoring

### Message States
- **new**: Just created
- **pending**: Queued for processing
- **wip**: Currently being processed
- **done**: Successfully completed
- **failed**: Permanently failed
- **retry**: Waiting for retry
- **terminated**: Stopped by user or system

### Viewing Messages
1. Navigate to **IMQ > Messages**
2. Filter by queue, state, or processor
3. Click on a message to see:
   - Processing history
   - Execution logs
   - Error details
   - Timing information

### Programmatic Status Check (`get_task_status`)

The `get_task_status()` model method provides a simple way to poll task status programmatically (e.g., from MCP agents or external scripts).

```python
# Single task
result = env['imq.message'].get_task_status([message_id])

# Multiple tasks
result = env['imq.message'].get_task_status([id1, id2, id3])
```

Returns a list of dicts:
```python
[
    {
        'id': 42,
        'name': 'Provision dev server...',
        'state': 'wip',
        'elapsed_seconds': 145,
        'hint': 'Executing. Poll again in 30 seconds.',
    }
]
```

**Hints by state:**
| State | Hint |
|-------|------|
| `pending` | Task queued. If still pending after 2min, IMQ worker may not be running. |
| `wip` | Executing. Poll again in 30 seconds. |
| `done` | Completed. Read the target record for results. |
| `failed` | Failed. Read imq.message_processing_log for details. |
| `retry` | Will be retried automatically. |
| `terminated` | Manually terminated. |

## Best Practices

1. **Use meaningful message names**: The first line of the docstring becomes the message name
   ```python
   @processor('default')
   def process_invoice(invoice):
       """Process invoice {invoice.name}"""
   ```

2. **IMQ Logger Propagation Pattern**:
   Methods decorated with `@processor` or `@processor_method` receive an IMQ logger when executed asynchronously. Use this pattern to ensure consistent logging:

   ```python
   import logging
   _logger = logging.getLogger(__name__)

   @processor('my_queue')
   def my_task(env, data, _imq_logger=None):
       """My task description."""
       _task_logger = _imq_logger or _logger

       _task_logger.info("Starting task...")

       # Propagate _imq_logger to dependent methods
       helper_function(data, _imq_logger=_imq_logger)

       _task_logger.info("Task completed")
       return result


   def helper_function(data, _imq_logger=None):
       """Helper that also supports IMQ logging."""
       _task_logger = _imq_logger or _logger

       _task_logger.debug("Helper processing...")
       # ... logic ...
   ```

   **Key rules:**
   - `_imq_logger=None` parameter always **last** (before kwargs)
   - `_task_logger = _imq_logger or _logger` at **method start**
   - Use `_task_logger` throughout your code
   - Pass `_imq_logger=_imq_logger` to dependent methods
   - Each dependent method implements the same pattern

   **Benefits:**
   - **Async execution**: All logs go to the IMQ task journal
   - **Sync execution**: Each method uses its own `_logger`
   - **Traceability**: Centralized logs for debugging

3. **Handle retries appropriately**:
   - Use `IMQError` for permanent failures
   - Use `IMQRetryableError` for transient issues
   - Set reasonable retry delays

4. **Monitor queue depth**: Set up alerts for queue backlogs

5. **Use FIFO queues** when message ordering matters

## Architecture

### Message Flow
1. Task decorated with `@processor` or `@processor_method`
2. `.run_async()` serializes parameters and sends to queue
3. Worker polls queue and retrieves message
4. Message stored in database with processing record
5. Task executed in isolated transaction
6. Results and logs captured and stored
7. Message marked as done/failed/retry

### Key Models
- `imq.queue`: Queue configuration
- `imq.message`: Message records
- `imq.message_processor`: Task definitions
- `imq.message_processing`: Processing history
- `imq.message_processing_log`: Execution logs

## Troubleshooting

### Messages not processing
1. Check worker cron job is active
2. Verify queue is active
3. Check for errors in worker logs

### High retry rates
1. Review error messages in processing logs
2. Check external service availability
3. Adjust retry delays if needed

### Performance issues
1. Add more workers
2. Increase visibility timeout for long tasks
3. Use multiple queues to separate workloads

## Worker Command (`imq-worker`)

The IMQ Workers v3 system provides a standalone worker command that can process messages independently of Odoo's cron system.

### Basic Usage

```bash
# Process messages from default queue
bin/start_odoo imq-worker --database $PGDATABASE --queue default

# Process with queue pattern matching
bin/start_odoo imq-worker --database $PGDATABASE --queue "mpy.*" --max-messages 100

# Process with memory limit and observability
bin/start_odoo imq-worker --database $PGDATABASE --queue default \
  --max-rss-memory 1024M --observability-port 8080
```

**Note**: Use `--database $PGDATABASE` to automatically use the database name from your environment variables.

### Message Targeting

The `--message` parameter allows you to process a specific message by ID or MessageId. This is particularly useful for debugging, testing, or processing stuck messages:

```bash
# Process specific message by numeric ID
bin/start_odoo imq-worker --database $PGDATABASE --queue default --message 49737

# Process specific message by MessageId (UUID)
bin/start_odoo imq-worker --database $PGDATABASE --queue default \
  --message "8f3ec366-68c8-4945-87cc-aaf2cad5dd0f"
```

#### Message Targeting Features

- **Flexible Input**: Accepts both numeric IDs and UUID MessageIds
- **State Validation**: Only processes messages in `pending` or `retry` state
- **Queue Requirement**: The queue parameter is still required for security
- **Elegant Implementation**: Uses existing polling logic with optional message filtering
- **Debug Support**: Combine with `--log-level DEBUG` to see SQL execution details

#### Message Targeting Examples

```bash
# Debug specific message processing
bin/start_odoo imq-worker --database $PGDATABASE --queue default \
  --message 49737 --max-messages 1 --log-level DEBUG

# Process message and exit immediately  
bin/start_odoo imq-worker --database $PGDATABASE --queue default \
  --message 49737 --max-messages 1 --worker-name "debug-worker"

# Process message with observability for monitoring
bin/start_odoo imq-worker --database $PGDATABASE --queue default \
  --message 49737 --observability-port 8080
```

### Command Options

```bash
# Required arguments
--database, -d DATABASE    # Database name to connect to  
--queue, -q PATTERN        # Queue name or regex pattern

# Processing limits
--max-messages N           # Exit after processing N messages (0=unlimited)
--max-rss-memory SIZE      # Exit when RSS memory exceeds limit (e.g., 1024M)

# Message targeting
--message, -m ID           # Process specific message by ID or MessageId

# Worker configuration  
--worker-name, -w NAME     # Worker identifier for logging
--log-level LEVEL          # Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL

# Observability
--observability-port PORT  # Port for liveness probe and metrics (0=disabled)
--metrics-path PATH        # HTTP path for Prometheus metrics (default: /metrics)
--queue-depth-caching-period-s SECONDS  # Queue depth metrics caching period (default: 30)
```

## Advanced Observability

IMQ Workers v3 provides comprehensive observability features designed for modern container environments and operations teams. When you enable observability with `--observability-port`, the worker exposes multiple HTTP endpoints for health checking, monitoring, and debugging.

### Quick Start

```bash
# Start worker with observability on port 8080
bin/start_odoo imq-worker --database $PGDATABASE --queue default \
  --observability-port 8080 --worker-name "production-worker"
```

This enables all observability endpoints:
- **Health Check**: http://localhost:8080/healthz
- **Readiness Probe**: http://localhost:8080/readyz  
- **Detailed Status**: http://localhost:8080/status
- **Liveness Probe**: http://localhost:8080/livez
- **Prometheus Metrics**: http://localhost:8080/metrics

### Health Check Endpoint (`/healthz`)

Provides comprehensive health assessment suitable for monitoring systems and alerting.

```bash
curl http://localhost:8080/healthz
```

**Response Format:**
```json
{
  "apiVersion": "imq/v1",
  "kind": "WorkerHealth",
  "metadata": {
    "worker_name": "production-worker",
    "timestamp": "2025-07-19T15:30:45Z",
    "database": "production_db"
  },
  "status": {
    "status": "healthy",
    "checks": {
      "database": "ok",
      "memory_usage": "ok", 
      "queue_connection": "ok",
      "message_processing": "ok"
    },
    "uptime": "2h15m30s",
    "last_activity": "2025-07-19T15:29:12Z"
  }
}
```

**Health Status Values:**
- `healthy`: All systems operating normally
- `degraded`: Minor issues detected (high memory usage, slow processing)  
- `unhealthy`: Critical issues requiring attention

**Individual Check Status:**
- **database**: `ok` | `error: <details>`
- **memory_usage**: `ok` | `warning` | `critical`
- **queue_connection**: `ok` | `no_queues` | `error`
- **message_processing**: `ok` | `slow` | `stuck` | `starting`

**HTTP Status Codes:**
- `200`: Healthy or degraded
- `503`: Unhealthy

### Readiness Probe Endpoint (`/readyz`)

Indicates whether the worker is ready to process messages. Perfect for Kubernetes readiness probes.

```bash
curl http://localhost:8080/readyz
```

**Response Format:**
```json
{
  "apiVersion": "imq/v1",
  "kind": "WorkerReadiness", 
  "metadata": {
    "worker_name": "production-worker",
    "timestamp": "2025-07-19T15:30:45Z",
    "database": "production_db"
  },
  "status": {
    "ready": true,
    "reason": "ready",
    "queue_status": {
      "connected": true,
      "queues_found": 3,
      "last_poll": "2025-07-19T15:30:45Z"
    }
  }
}
```

**Readiness Reasons:**
- `ready`: Worker can accept new messages
- `starting`: Worker still initializing (first 30 seconds)
- `shutting_down`: Graceful shutdown in progress
- `overloaded`: Memory limit exceeded

**HTTP Status Codes:**
- `200`: Ready to process messages
- `503`: Not ready

### Detailed Status Endpoint (`/status`)

Comprehensive runtime information for operations teams and debugging.

```bash
curl http://localhost:8080/status
```

**Response Format:**
```json
{
  "apiVersion": "imq/v1",
  "kind": "WorkerStatus",
  "metadata": {
    "worker_name": "production-worker", 
    "timestamp": "2025-07-19T15:30:45Z",
    "database": "production_db"
  },
  "status": {
    "worker": {
      "name": "production-worker",
      "version": "v3.0.0",
      "uptime": "2h15m30s",
      "pid": 12345,
      "started_at": "2025-07-19T13:15:15Z"
    },
    "configuration": {
      "queue_pattern": "production.*",
      "max_messages": 1000,
      "max_rss_memory": "512M",
      "database": "production_db"
    },
    "runtime": {
      "messages_processed": 247,
      "current_memory_mb": 245.6,
      "memory_usage_percent": 47.9,
      "processing_rate_per_minute": 12.3,
      "average_processing_time": "2.1s",
      "last_message_at": "2025-07-19T15:29:12Z"
    },
    "queues": [
      {
        "name": "production-orders",
        "depth": 45,
        "processed": 150,
        "failed": 2,
        "avg_duration": "1.8s"
      }
    ],
    "limits": {
      "message_limit_reached": false,
      "memory_limit_reached": false, 
      "shutdown_requested": false
    }
  }
}
```

**Always returns `200` status code**

#### Queue Depth in Status Response

The `queues[].depth` field shows the **real-time backlog** of processable messages per queue:

**Definition**: Messages where:
- `state` is in (`'pending'`, `'retry'`) 
- AND (`planned_time` IS NULL OR `planned_time` <= NOW())

**Operational Significance**:
- **depth = 0**: Queue is current, no backlog
- **depth > 0**: Messages waiting for processing
- **Trending up**: Potential capacity/performance issues
- **High sustained depth**: Scaling trigger

**Example Response with Queue Depth**:
```json
{
  "status": {
    "queues": [
      {
        "name": "high-priority",
        "depth": 12,
        "processed": 1250,
        "failed": 3,
        "avg_duration": "0.8s"
      },
      {
        "name": "background-tasks",
        "depth": 0,
        "processed": 97,
        "failed": 0,
        "avg_duration": "2.1s"
      }
    ]
  }
}
```

### Liveness Probe Endpoint (`/livez`)

Simple endpoint for basic liveness checking. Returns `OK` if worker process has been operational (polling queues, processing messages, or performing normal worker operations) within 5 minutes. This indicates the worker process is alive and functioning, regardless of message availability.

```bash
curl http://localhost:8080/livez
# Response: OK (HTTP 200) or "No recent activity" (HTTP 503)
```

**Activity is updated when the worker:**
- Successfully processes a message
- Polls queues (even when empty)
- Performs periodic metrics updates  
- Executes normal operational tasks

### Prometheus Metrics Endpoint (`/metrics`)

Exports comprehensive metrics in Prometheus format for monitoring and alerting.

```bash
curl http://localhost:8080/metrics
```

**Key Metrics Available:**

**Worker Lifecycle:**
- `imq_worker_uptime_seconds` - Worker uptime
- `imq_worker_wait_time_seconds` - Time spent waiting for messages
- `imq_worker_wait_time_percent` - Percentage of time waiting

**Message Processing:**
- `imq_worker_messages_processed_total` - Total messages processed
- `imq_worker_messages_failed_total` - Total messages failed  
- `imq_worker_message_duration_seconds` - Processing duration histogram

**Memory Usage:**
- `imq_worker_rss_memory_bytes` - Current RSS memory usage
- `imq_worker_max_rss_memory_bytes` - Configured memory limit

**Per-Queue Metrics:**
- `imq_worker_queue_messages_processed_total{queue="default"}` - Messages per queue
- `imq_worker_queue_messages_failed_total{queue="default"}` - Failures per queue
- `imq_worker_queue_message_duration_seconds{queue="default"}` - Duration per queue
- `imq_worker_queue_depth{queue="default"}` - Current queue depth per queue

**Queue Depth Metrics:**
- `imq_worker_queue_depth{queue="default"}` - Current processable message count per queue

**Queue Depth Definition**: Messages where `state ∈ ('pending','retry')` AND `(planned_time IS NULL OR planned_time <= NOW())`

**Note**: Queue depth metrics are cached and updated every 30 seconds by default (configurable via `--queue-depth-caching-period-s`).

### Error Handling

All endpoints provide structured error responses:

```bash
# Invalid endpoint
curl http://localhost:8080/invalid
```

```json
{
  "error": "Not Found",
  "available_endpoints": ["/livez", "/healthz", "/readyz", "/status", "/metrics"]
}
```

**Server errors return 500 with details:**
```json
{
  "error": "Internal server error",
  "details": "Specific error message"
}
```

### Kubernetes Integration

Perfect for Kubernetes deployments with standard probe configuration:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: imq-worker
spec:
  template:
    spec:
      containers:
      - name: worker
        image: myapp:latest
        command: ["bin/start_odoo"]
        args:
          - "imq-worker"
          - "--database=$(DATABASE_NAME)"
          - "--queue=production.*"
          - "--max-messages=1000"
          - "--max-rss-memory=512M"
          - "--observability-port=8080"
        
        # Health checks
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 30
          timeoutSeconds: 5
          failureThreshold: 3
        
        readinessProbe:
          httpGet:
            path: /readyz
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2
        
        # Resource limits
        resources:
          limits:
            memory: "768Mi"  # Higher than --max-rss-memory
          requests:
            memory: "256Mi"
            cpu: "250m"
```

#### Horizontal Pod Autoscaler (HPA)

Scale workers automatically based on queue depth using the new queue depth metrics:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: imq-worker-hpa
  namespace: default
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: imq-worker
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Pods
    pods:
      metric:
        name: imq_worker_queue_depth
      target:
        type: AverageValue
        averageValue: 20  # Scale when avg queue depth > 20 per pod
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60   # Quick scale-up for queue backlogs
      policies:
      - type: Percent
        value: 100  # Double pods when scaling up
        periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300  # Conservative scale-down
      policies:
      - type: Percent
        value: 50   # Reduce by half when scaling down
        periodSeconds: 60
```

**HPA Configuration Notes:**
- **Target Metric**: `imq_worker_queue_depth` - Uses our new queue depth metric
- **Scaling Trigger**: When average queue depth > 20 messages per pod
- **Aggressive Scale-Up**: Quick response to queue backlogs (100% increase)
- **Conservative Scale-Down**: Prevents thrashing (50% decrease, 5min stabilization)
- **Min/Max Replicas**: Always maintain 2 pods, scale up to 10 maximum

### Monitoring Setup

#### Prometheus ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: imq-worker-metrics
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

#### Grafana Dashboard Queries

```promql
# Worker uptime
imq_worker_uptime_seconds

# Messages processed per second
rate(imq_worker_messages_processed_total[5m])

# Average processing time
rate(imq_worker_message_duration_seconds_sum[5m]) / 
rate(imq_worker_message_duration_seconds_count[5m])

# Error rate percentage
rate(imq_worker_messages_failed_total[5m]) / 
rate(imq_worker_messages_processed_total[5m]) * 100

# Memory usage percentage
imq_worker_rss_memory_bytes / imq_worker_max_rss_memory_bytes * 100

# Wait time percentage (worker efficiency)
imq_worker_wait_time_percent

# Queue depth by queue name
imq_worker_queue_depth

# Total queue depth across all queues
sum(imq_worker_queue_depth)

# Queue depth trend (5-minute rate)
increase(imq_worker_queue_depth[5m])

# Queue utilization ratio (depth vs processing capacity)
imq_worker_queue_depth / (rate(imq_worker_messages_processed_total[5m]) * 300)
```

#### Alerting Rules

```yaml
groups:
- name: imq-worker
  rules:
  - alert: IMQWorkerDown
    expr: up{job="imq-worker"} == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "IMQ Worker is down"
      
  - alert: IMQWorkerUnhealthy
    expr: probe_success{job="imq-worker-health"} == 0
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "IMQ Worker health check failing"
      
  - alert: IMQWorkerHighErrorRate  
    expr: rate(imq_worker_messages_failed_total[5m]) / rate(imq_worker_messages_processed_total[5m]) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "IMQ Worker error rate above 10%"
      
  - alert: IMQWorkerHighMemoryUsage
    expr: (imq_worker_rss_memory_bytes / imq_worker_max_rss_memory_bytes) > 0.9
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "IMQ Worker memory usage above 90%"
      
  - alert: IMQQueueDepthHigh
    expr: imq_worker_queue_depth > 100
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "IMQ Queue depth high: {{ $labels.queue }}"
      description: "Queue {{ $labels.queue }} has {{ $value }} pending messages"
      
  - alert: IMQQueueDepthCritical
    expr: imq_worker_queue_depth > 1000
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "IMQ Queue depth critical: {{ $labels.queue }}"
      description: "Queue {{ $labels.queue }} has {{ $value }} pending messages - immediate attention required"
      
  - alert: IMQQueueDepthTrending
    expr: increase(imq_worker_queue_depth[10m]) > 50
    for: 3m
    labels:
      severity: warning
    annotations:
      summary: "IMQ Queue depth rapidly increasing"
      description: "Queue {{ $labels.queue }} depth increased by {{ $value }} messages in 10 minutes"
```

### Operational Workflows

#### Health Check Script

```bash
#!/bin/bash
# health_check.sh - Simple health monitoring script

WORKER_URL="http://localhost:8080"
ENDPOINTS=("healthz" "readyz" "status")

for endpoint in "${ENDPOINTS[@]}"; do
  response=$(curl -s -w "%{http_code}" "${WORKER_URL}/${endpoint}")
  http_code="${response: -3}"
  
  if [[ "$http_code" =~ ^2[0-9]{2}$ ]]; then
    echo "✅ /$endpoint: OK ($http_code)"
  else
    echo "❌ /$endpoint: FAILED ($http_code)"
    exit 1
  fi
done

echo "🎉 All health checks passed!"
```

#### Status Monitoring

```bash
#!/bin/bash
# monitor_worker.sh - Real-time worker monitoring

watch -n 5 "
echo 'IMQ Worker Status:'
curl -s http://localhost:8080/status | jq '
  .status.runtime | 
  \"Messages: \(.messages_processed) | Rate: \(.processing_rate_per_minute)/min | Memory: \(.current_memory_mb)MB | Avg: \(.average_processing_time)\"
'
"
```

#### Load Testing with Observability

```bash
#!/bin/bash
# load_test.sh - Load test with monitoring

# Start monitoring in background
./monitor_worker.sh &
MONITOR_PID=$!

# Create load test messages
bin/start_odoo imq-test --database $PGDATABASE --simple \
  --queue production --count 1000 --delay 0.1

# Monitor processing
echo "Monitoring worker performance..."
while true; do
  pending=$(curl -s http://localhost:8080/status | jq '.status.runtime.messages_processed')
  if [ "$pending" -ge 1000 ]; then
    break
  fi
  sleep 5
done

# Stop monitoring
kill $MONITOR_PID

echo "Load test completed!"
curl -s http://localhost:8080/status | jq '.status.runtime'
```

### Best Practices

1. **Always enable observability** in production environments
2. **Use readiness probes** for Kubernetes deployments
3. **Monitor error rates** and set up alerting for > 5% failure rates
4. **Track memory usage** trends to optimize resource allocation
5. **Set up dashboards** for queue depth, processing rates, and worker health
6. **Use /status endpoint** for debugging performance issues
7. **Monitor wait time percentage** to optimize worker scaling

### Troubleshooting

#### Worker Not Responding to Health Checks

```bash
# Check if observability port is accessible
nc -zv localhost 8080

# Check worker logs for startup errors
docker logs <worker-container>

# Verify worker is still running
ps aux | grep imq-worker
```

#### High Memory Usage Alerts

```bash
# Get detailed memory information
curl -s http://localhost:8080/status | jq '.status.runtime.current_memory_mb'

# Check memory limit configuration
curl -s http://localhost:8080/status | jq '.status.configuration.max_rss_memory'

# Monitor memory trend
for i in {1..10}; do
  echo "$(date): $(curl -s http://localhost:8080/status | jq '.status.runtime.current_memory_mb')MB"
  sleep 30
done
```

#### Processing Slowdown Investigation

```bash
# Check processing rate and average time
curl -s http://localhost:8080/status | jq '.status.runtime | {rate: .processing_rate_per_minute, avg_time: .average_processing_time}'

# Check queue-specific performance  
curl -s http://localhost:8080/status | jq '.status.queues[]'

# Check last activity time
curl -s http://localhost:8080/status | jq '.status.runtime.last_message_at'
```

## Kubernetes Deployment (Beta)

> ⚠️ **Beta Warning**: The Kubernetes deployment examples and configurations are currently in **beta**. While they follow best practices and have been tested, they may require adjustments for your specific production environment. Please thoroughly test in non-production environments first.

### Overview

IMQ Workers v3 are designed to run natively in Kubernetes environments, providing:
- Horizontal scaling with HPA (Horizontal Pod Autoscaler)
- Native health checks and observability
- Prometheus metrics integration
- Graceful shutdown handling
- Resource management and limits

### Deployment Examples

We provide comprehensive Kubernetes manifests in the [`k8s/examples/`](k8s/examples/) directory:

- **[Basic Deployment](k8s/examples/deployment-basic.yaml)**: Simple 2-replica deployment for getting started
- **[Production Deployment](k8s/examples/deployment-production.yaml)**: Full production setup with autoscaling, monitoring, and security
- **[Multi-Queue Deployment](k8s/examples/deployment-multiqueue.yaml)**: Separate worker pools for different queue priorities
- **[Jobs & CronJobs](k8s/examples/job-examples.yaml)**: One-time and scheduled batch processing

### Quick Start

```bash
# Create namespace
kubectl create namespace muppy-workers

# Create secrets (edit the template first!)
kubectl apply -f k8s/examples/secrets-template.yaml

# Deploy basic workers
kubectl apply -f k8s/examples/deployment-basic.yaml

# Check deployment
kubectl get pods -n muppy-workers
kubectl logs -l app=imq-worker -n muppy-workers
```

### Monitoring Integration

Deploy Prometheus ServiceMonitor for automatic metrics discovery:

```bash
kubectl apply -f k8s/examples/servicemonitor.yaml
```

Available metrics endpoints:
- `/metrics` - Prometheus metrics
- `/livez` - Liveness probe
- `/readyz` - Readiness probe  
- `/statusz` - Detailed worker status

### Key Features

1. **Container-native design**: No dependency on cron, runs as long-lived processes
2. **Observability built-in**: Prometheus metrics, structured logging, health endpoints
3. **Resource aware**: Memory and CPU limits with graceful shutdown
4. **Queue flexibility**: Process multiple queues with regex patterns
5. **Security**: Non-root user, dropped capabilities, security contexts

### Documentation

For complete Kubernetes deployment documentation, see:
- 📚 **[Kubernetes Deployment Guide](k8s/README.md)** - Comprehensive guide with examples
- 🔧 **[RBAC Configuration](k8s/examples/rbac.yaml)** - Security and access control
- 📊 **[Monitoring Setup](k8s/examples/servicemonitor.yaml)** - Prometheus and alerting
- 🔐 **[Secrets Management](k8s/examples/secrets-template.yaml)** - Secure configuration

### Beta Considerations

While the Kubernetes deployment is functional and follows best practices, please note:

- Test thoroughly in your environment before production use
- Resource limits and requests may need tuning for your workload
- Network policies should be adjusted for your security requirements
- Consider using GitOps tools (Flux, ArgoCD) for production deployments
- Monitor closely during initial rollout

For questions or issues with Kubernetes deployments, please open an issue on our repository.

## IMQ Control Command (`imq-ctl`)

The `imq-ctl` command provides a kubectl-style management tool for IMQ objects, allowing you to examine messages, queues, processors, and processing records in structured YAML or JSON format.

### Basic Usage

```bash
# Describe message information in YAML format (default)
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737

# Describe message with logs in JSON format
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737 --include-logs --output json

# Describe queue information
bin/start_odoo imq-ctl --database $PGDATABASE describe queue default

# Describe processor by selector
bin/start_odoo imq-ctl --database $PGDATABASE describe processor TestMessage
```

### Object Types

#### Messages (`--message`)

Dump detailed message information including processing history and logs:

```bash
# By numeric ID
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737

# By MessageId (UUID)  
bin/start_odoo imq-ctl --database $PGDATABASE describe message "8f3ec366-68c8-4945-87cc-aaf2cad5dd0f"

# Include processing logs
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737 --include-logs
```

#### Queues (`--queue`)

Dump queue configuration and statistics:

```bash
# Queue information with message counts by state
bin/start_odoo imq-ctl --database $PGDATABASE describe queue default
```

#### Processors (`--processor`)

Dump message processor configuration:

```bash
# By numeric ID
bin/start_odoo imq-ctl --database $PGDATABASE describe processor 1

# By selector name
bin/start_odoo imq-ctl --database $PGDATABASE describe processor TestMessage
```

#### Processing Records (`--processing`)

Dump individual processing attempt information:

```bash
# Processing record with logs
bin/start_odoo imq-ctl --database $PGDATABASE describe processing 47495 --include-logs
```

#### Processing Logs (`--logs`)

Dump processing logs in streaming format, similar to `kubectl logs`:

```bash
# Stream format - human-readable log output
bin/start_odoo imq-ctl --database $PGDATABASE logs processing 47497

# JSON format - structured log data
bin/start_odoo imq-ctl --database $PGDATABASE logs processing 47497 --json

# Save logs to file for analysis
bin/start_odoo imq-ctl --database $PGDATABASE logs processing 47497 --output processing_47497.log
```

### Output Formats

#### YAML Format (Default)

```yaml
apiVersion: imq/v1
kind: Message
metadata:
  id: 49737
  messageId: 8f3ec366-68c8-4945-87cc-aaf2cad5dd0f
  name: Message Targeting Test 2
  createdAt: '2025-07-19T10:45:02.508176'
spec:
  queue:
    id: 1
    name: default
    provider: pgsql
    type: std
  processor:
    id: 1
    name: TestMessage
    selector: TestMessage
    function: SimpleMessage_processor
status:
  state: done
  attempt: 1
  startTime: '2025-07-19T10:45:16.272511'
  endTime: '2025-07-19T10:45:21.301887'
  processing:
  - id: 47495
    workerType: sa-workerv3
    state: done
    result: '"processor returned string"'
```

#### JSON Format

```bash
# Output in JSON format
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737 --json
```

```json
{
  "apiVersion": "imq/v1",
  "kind": "Message",
  "metadata": {
    "id": 49737,
    "messageId": "8f3ec366-68c8-4945-87cc-aaf2cad5dd0f",
    "name": "Message Targeting Test 2"
  },
  "status": {
    "state": "done",
    "workerType": "sa-workerv3"
  }
}
```

#### Processing Logs Format

The `--logs` command provides a specialized streaming format for processing logs:

```bash
# Stream format output
bin/start_odoo imq-ctl --database $PGDATABASE logs processing 47497
```

```
# Processing Logs for ID: 47497
# Message: Run atask #imq.test_launcher(10,) (ID: 49739)
# Worker Type: sa-workerv3
# State: done
# Attempt: 1
# Start Time: 2025-07-19T10:58:08.090016
# End Time: 2025-07-19T10:58:10.134718
# Result: "a_task = test_param @ 2025-07-19 10:58:10.115141\n"
# Total Log Entries: 3
#
# Log Stream:
# -----------
2025-07-19 10:58:08.090 IMQ_message_49739 INFO Task started with param=test_param
2025-07-19 10:58:08.500 IMQ_message_49739 INFO Processing iteration #1
2025-07-19 10:58:09.200 IMQ_message_49739 INFO Task completed successfully
```

```bash
# JSON format provides structured data
bin/start_odoo imq-ctl --database $PGDATABASE logs processing 47497 --json
```

```json
{
  "apiVersion": "imq/v1",
  "kind": "ProcessingLogs", 
  "metadata": {
    "processingId": 47497,
    "messageId": 49739,
    "messageName": "Run atask #imq.test_launcher(10,)",
    "workerType": "sa-workerv3"
  },
  "spec": {
    "processing": {
      "state": "done",
      "attempt": 1,
      "startTime": "2025-07-19T10:58:08.090016",
      "endTime": "2025-07-19T10:58:10.134718"
    }
  },
  "logs": [
    {
      "id": 12345,
      "timestamp": "2025-07-19T10:58:08.090016",
      "loggerName": "IMQ_message_49739",
      "level": "20",
      "message": "Task started with param=test_param"
    }
  ]
}
```

### Command Options

```bash
# Required arguments
--database, -d DATABASE    # Database name to connect to

# Object type (choose one)
--message, -m ID          # Dump message by ID or MessageId
--queue, -q NAME          # Dump queue by name  
--processor, -p REF       # Dump processor by ID or selector
--processing ID           # Dump processing record by ID
--logs PROCESSING_ID      # Dump processing logs stream for processing ID

# Output options
--json                    # Output in JSON format (default: YAML)
--output, -o FILE         # Output file path (default: stdout)
--include-logs            # Include processing logs for messages/processing
--verbose, -v             # Verbose output with debug information
```

### Use Cases

#### Debugging Message Processing

```bash
# Check message state and processing history
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737

# Examine detailed logs
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737 --include-logs

# Stream processing logs for detailed debugging
bin/start_odoo imq-ctl --database $PGDATABASE logs processing 47497

# Check what processor handles the message
bin/start_odoo imq-ctl --database $PGDATABASE describe processor TestMessage
```

#### Monitoring and Operations

```bash
# Export message data for analysis
bin/start_odoo imq-ctl --database $PGDATABASE describe message 49737 --json \
  --output message_49737.json

# Check queue health
bin/start_odoo imq-ctl --database $PGDATABASE describe queue default

# Audit processing attempts
bin/start_odoo imq-ctl --database $PGDATABASE describe processing 47495 --include-logs

# Tail processing logs for monitoring
bin/start_odoo imq-ctl --database $PGDATABASE logs processing 47497 --output /var/log/imq/processing_47497.log
```

#### CI/CD Integration

```bash
#!/bin/bash
# Verify message processing in pipeline

MESSAGE_ID=$(bin/start_odoo imq-test --database $PGDATABASE --simple --json-output | jq -r '.[0].id')

# Process the message
bin/start_odoo imq-worker --database $PGDATABASE --queue default --message $MESSAGE_ID --max-messages 1

# Verify it completed successfully
FINAL_STATE=$(bin/start_odoo imq-ctl --database $PGDATABASE describe message $MESSAGE_ID --output json | jq -r '.status.state')

if [ "$FINAL_STATE" != "done" ]; then
  echo "Message processing failed: $FINAL_STATE"
  bin/start_odoo imq-ctl --database $PGDATABASE describe message $MESSAGE_ID --include-logs
  exit 1
fi

echo "Message processed successfully!"
```

## Testing

### IMQ Test CLI Command (`imq-test`)

The `imq-test` command provides a powerful CLI interface for creating and testing IMQ messages. It offers complete feature parity with the GUI test launcher and is perfect for debugging, automation, and load testing.

#### Quick Start

```bash
# List available test processors
bin/start_odoo imq-test --database $PGDATABASE --list-processors

# Create a simple test message
bin/start_odoo imq-test --database $PGDATABASE --simple --queue default --verbose

# Create an RPC method test with custom parameters
bin/start_odoo imq-test --database $PGDATABASE --rpc-method --queue default --param "test_data" --duration 10

# Create multiple messages with JSON output
bin/start_odoo imq-test --database $PGDATABASE --simple --queue default --count 5 --json-output
```

#### Command Syntax

```bash
bin/start_odoo imq-test [OPTIONS] TEST_TYPE
```

#### Required Arguments

- `--database`, `-d`: Database name to connect to
- **Test Type** (choose one):
  - `--simple`: Create simple message test using SimpleMessage_processor
  - `--rpc-method`: Create RPC message test using a_task_method
  - `--rpc-function`: Create RPC message test using a_task_procedure  
  - `--fifo-test`: Create FIFO test sequence
  - `--list-processors`: List available test processors

#### Message Configuration Options

```bash
--queue QUEUE, -q QUEUE          # Queue name (default: default)
--count COUNT, -c COUNT          # Number of messages to create (default: 1)
--name NAME, -n NAME             # Custom message name (auto-generated if not provided)
--selector SELECTOR, -s SELECTOR # Processor selector for simple messages (default: TestMessage)
--payload PAYLOAD, -p PAYLOAD    # JSON payload for simple messages (default: {})
```

#### RPC Test Options

```bash
--param PARAM                    # Parameter for RPC test methods (default: test_param)
--duration DURATION              # Processing duration in seconds (default: 5)
```

#### Exception Testing Options

```bash
--raise-exception                # Make test raise an exception
--exception-type TYPE            # Type of exception to raise:
                                # - exception: Python Exception
                                # - usererror: Odoo UserError  
                                # - imqerror: IMQError
                                # - imqretryable: IMQRetryableError
                                # - imqterminate: IMQTerminateException
--exception-step STEP            # Step name for FIFO test exceptions (e.g., fifo_step3)
--delay-param DELAY              # Delay in seconds for IMQRetryableError (default: 0)
--pass-imqerror-value           # Pass a value to IMQError/IMQRetryableError
```

#### FIFO Queue Options

```bash
--message-group GROUP            # Message group ID for FIFO queues (auto-generated if not provided)
```

#### Advanced Options

```bash
--debug-mode                     # Run in debug mode (synchronous execution)
--enable-logging                # Enable logging for test messages
--enable-console                # Enable console capture for test messages
--user-id USER_ID               # User ID to run test as (default: system user)
--context CONTEXT               # Additional context for message (JSON format)
--delay DELAY                   # Delay between creating messages in seconds
```

#### Output Options

```bash
--verbose, -v                   # Verbose output showing progress
--json-output                   # Output results in JSON format
--log-level LEVEL               # Log level: DEBUG, INFO, WARNING, ERROR
```

#### Examples

##### Basic Simple Message Test

```bash
# Create a simple test message
bin/start_odoo imq-test --database $PGDATABASE --simple --queue default --verbose

# Output:
# Created simple message 1/1: ID=49682, MessageID=f2f1b01a-8559-4058-9e69-e16b8ec288fa
# Successfully created 1 simple message(s) in queue 'default'
#   - Message ID: 49682, Name: 'CLI Test Simple 1'
```

##### RPC Method Test with Custom Parameters

```bash
# Test RPC method with 30-second duration and custom parameter
bin/start_odoo imq-test --database $PGDATABASE --rpc-method \
  --queue default --param "production_data" --duration 30 --verbose
```

##### Exception Testing

```bash
# Test IMQRetryableError with 60-second delay
bin/start_odoo imq-test --database $PGDATABASE --rpc-method \
  --queue default --raise-exception --exception-type imqretryable \
  --delay-param 60 --pass-imqerror-value
```

##### FIFO Test Sequence

```bash
# Create FIFO test sequence with exception on step 3
bin/start_odoo imq-test --database $PGDATABASE --fifo-test \
  --queue fifo_queue --raise-exception --exception-step fifo_step3 \
  --exception-type usererror --message-group "test-batch-001"
```

##### Batch Message Creation

```bash
# Create 10 simple messages with 2-second delay between each
bin/start_odoo imq-test --database $PGDATABASE --simple \
  --queue default --count 10 --delay 2 --json-output > test_results.json
```

##### Custom Payload Testing

```bash
# Test with custom JSON payload
bin/start_odoo imq-test --database $PGDATABASE --simple \
  --queue default --payload '{"customer_id": 12345, "action": "process_order"}' \
  --selector "OrderProcessor" --name "Order Processing Test"
```

##### Load Testing

```bash
# Create 100 messages quickly for load testing
bin/start_odoo imq-test --database $PGDATABASE --simple \
  --queue default --count 100 --json-output | jq '.[].id'
```

#### JSON Output Format

When using `--json-output`, the command returns structured data:

```json
[
  {
    "id": 49684,
    "message_id": "131da109-9290-4a7d-b480-ce4cc62029ce", 
    "name": "CLI Test Simple 1",
    "queue": "default",
    "selector": "TestMessage",
    "sequence": 1
  },
  {
    "id": 49685,
    "message_id": "87748c3a-0853-4fc7-8214-7d919f13a56b",
    "name": "CLI Test Simple 2", 
    "queue": "default",
    "selector": "TestMessage",
    "sequence": 2
  }
]
```

#### Integration with Testing Workflows

##### CI/CD Pipeline Testing

```bash
#!/bin/bash
# test_imq_pipeline.sh

# Create test messages
RESULT=$(bin/start_odoo imq-test --database $PGDATABASE --simple --count 5 --json-output)
MESSAGE_IDS=$(echo "$RESULT" | jq -r '.[].id')

# Start worker to process them
bin/start_odoo imq-worker --database $PGDATABASE --queue default --max-messages 5 &
WORKER_PID=$!

# Wait for processing and check results
sleep 30
kill $WORKER_PID

# Verify all messages processed successfully
for id in $MESSAGE_IDS; do
  STATUS=$(bin/start_odoo shell --database $PGDATABASE -c "
    msg = env['imq.message'].browse($id)
    print(msg.state)
  ")
  if [ "$STATUS" != "done" ]; then
    echo "Message $id failed: $STATUS"
    exit 1
  fi
done

echo "All test messages processed successfully!"
```

##### Performance Testing

```bash
# Create load test with timing
time bin/start_odoo imq-test --database $PGDATABASE --simple \
  --queue performance_test --count 1000 --verbose

# Monitor queue depth during test  
while true; do
  PENDING=$(bin/start_odoo shell --database $PGDATABASE -c "
    count = env['imq.message'].search_count([('state', '=', 'pending')])
    print(count)
  ")
  echo "Pending messages: $PENDING"
  sleep 5
done
```

#### Troubleshooting

##### Common Issues

1. **Database Connection Errors**
   ```bash
   # Verify database name and access
   echo $PGDATABASE
   bin/start_odoo shell --database $PGDATABASE -c "print('Connected successfully')"
   ```

2. **Queue Not Found**
   ```bash
   # List available queues
   bin/start_odoo shell --database $PGDATABASE -c "
   queues = env['imq.queue'].search([])
   for q in queues:
       print(f'{q.name} ({q.provider}, {q.q_type})')
   "
   ```

3. **Permission Issues**
   ```bash
   # Test with specific user ID
   bin/start_odoo imq-test --database $PGDATABASE --simple \
     --queue default --user-id 1 --verbose
   ```

##### Debug Mode

```bash
# Run in debug mode for immediate execution
bin/start_odoo imq-test --database $PGDATABASE --rpc-method \
  --queue default --debug-mode --enable-logging --verbose
```

### Running All Tests

A comprehensive test suite is available to validate the IMQ Workers v3 implementation:

```bash
# Run all tests
./run_tests.sh
```

The test script performs the following checks:

1. **Module Loading Test**: Verifies the module loads without errors in Odoo
2. **CLI Command Registration Test**: Confirms the `imq-worker` command is properly registered
3. **CLI Help Test**: Validates the command-line help system works
4. **Worker Initialization Test**: Tests worker startup and database connection
5. **Unit Tests**: Runs basic import and utility function tests
6. **CLI Argument Validation**: Ensures invalid arguments are rejected
7. **Database Connection Test**: Verifies database connectivity through the worker

### Manual Testing

#### Test CLI Command
```bash
# Test command registration
bin/start_odoo help | grep imq-worker

# Test help system
bin/start_odoo imq-worker --help

# Test worker with non-existent queue (should exit gracefully)
bin/start_odoo imq-worker --database $PGDATABASE --queue test_queue --max-messages 1
```

#### Test Worker Functionality
```bash
# Test with existing queue
bin/start_odoo imq-worker --database $PGDATABASE --queue default --max-messages 5

# Test with memory limit
bin/start_odoo imq-worker --database $PGDATABASE --queue default --max-rss-memory 512M

# Test with regex pattern
bin/start_odoo imq-worker --database $PGDATABASE --queue "mpy.*" --max-messages 10

# Test with observability (metrics and health checks)
bin/start_odoo imq-worker --database $PGDATABASE --queue default --observability-port 8080
```

### Test Results

The test script provides colored output and a summary:
- 🎉 **All tests passed**: Green success message
- ❌ **Some tests failed**: Red error message with details

### Continuous Integration

The test script is designed to work in CI/CD environments:
- Returns exit code 0 on success
- Returns exit code 1 on failure
- Uses timeout mechanisms to prevent hanging
- Provides detailed error messages

## License

This module is licensed under OPL-1.

## Credits

Created by Cyril MORISSE (@cmorisse)

## Development Documentation

For developers working on IMQ Workers v3, comprehensive development documentation is available in [`./docs/dev/`](./docs/dev/):

- **Analysis**: Codebase analysis and improvement opportunities
- **Implementation**: Progress tracking and milestone documentation  
- **Specifications**: Technical specs and implementation plans

## Contributing

Contributions are welcome! Please submit pull requests or issues on the project repository.

When contributing:
1. Review the development documentation in `./docs/dev/`
2. Run the test suite with `./run_tests.sh`
3. Follow the existing code patterns and conventions

## Support

For questions or support, please contact the author @cmorisse.