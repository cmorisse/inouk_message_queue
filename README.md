# Inouk Message Queue (IMQ)

A powerful asynchronous task processing system for Odoo that enables distributed processing using cloud message queues (AWS SQS) or PostgreSQL-based queuing.

## Overview

Inouk Message Queue (IMQ) allows you to:
- Process heavyweight tasks asynchronously without blocking the user interface
- Distribute work across multiple workers (Odoo processes, AWS Lambda, or any program)
- Handle failures gracefully with automatic retries and comprehensive error tracking
- Monitor and debug task execution with detailed logging

## Features

### 🚀 Multiple Queue Providers
- **AWS SQS**: Both standard and FIFO queues with full feature support
- **PostgreSQL**: Database-based queuing for simpler deployments
- Easy switching between providers without code changes

### 🎯 Simple API
- Decorate any function or method with `@processor` or `@processor_method`
- Call `.run_async()` to enqueue tasks
- Automatic serialization of Odoo models and complex data types

### 🛡️ Robust Error Handling
- **IMQError**: Permanent failures (no retry)
- **IMQRetryableError**: Automatic retries with configurable delays
- **IMQTerminateException**: Graceful termination
- Database transaction safety with automatic rollback

### 📊 Monitoring & Debugging
- Comprehensive logging of all processing attempts
- Console output capture during execution
- Processing time tracking and statistics
- Post-mortem debugging support
- Integration with Slack, Teams, and Odoo chat for notifications

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
    'database_bound_q': True,  # Prefix queue name with database name
})
```

### Worker Configuration

Workers are configured as Odoo cron jobs:
1. Navigate to **Settings > Technical > Automation > Scheduled Actions**
2. Configure the IMQ worker cron job
3. Set execution frequency based on your needs

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

## Best Practices

1. **Use meaningful message names**: The first line of the docstring becomes the message name
   ```python
   @processor('default')
   def process_invoice(invoice):
       """Process invoice {invoice.name}"""
   ```

2. **Always include logging parameter**: 
   ```python
   def my_task(env, data, _imq_logger=None):
       _imq_logger = _imq_logger or _logger
   ```

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

## License

This module is licensed under OPL-1.

## Credits

Created by Cyril MORISSE (@cmorisse)

## Contributing

Contributions are welcome! Please submit pull requests or issues on the project repository.

## Support

For questions or support, please contact the author via Twitter @cmorisse.