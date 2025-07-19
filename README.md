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

## Worker Command (`imqworker`)

The IMQ Workers v3 system provides a standalone worker command that can process messages independently of Odoo's cron system.

### Basic Usage

```bash
# Process messages from default queue
bin/start_odoo imqworker --database $PGDATABASE --queue default

# Process with queue pattern matching
bin/start_odoo imqworker --database $PGDATABASE --queue "mpy.*" --max-messages 100

# Process with memory limit and observability
bin/start_odoo imqworker --database $PGDATABASE --queue default \
  --max-rss-memory 1024M --observability-port 8080
```

### Message Targeting

The `--message` parameter allows you to process a specific message by ID or MessageId. This is particularly useful for debugging, testing, or processing stuck messages:

```bash
# Process specific message by numeric ID
bin/start_odoo imqworker --database $PGDATABASE --queue default --message 49737

# Process specific message by MessageId (UUID)
bin/start_odoo imqworker --database $PGDATABASE --queue default \
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
bin/start_odoo imqworker --database $PGDATABASE --queue default \
  --message 49737 --max-messages 1 --log-level DEBUG

# Process message and exit immediately  
bin/start_odoo imqworker --database $PGDATABASE --queue default \
  --message 49737 --max-messages 1 --worker-name "debug-worker"

# Process message with observability for monitoring
bin/start_odoo imqworker --database $PGDATABASE --queue default \
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
```

## IMQ Dump Command (`imqdump`)

The `imqdump` command provides a kubectl-style inspection tool for IMQ objects, allowing you to examine messages, queues, processors, and processing records in structured YAML or JSON format.

### Basic Usage

```bash
# Dump message information in YAML format (default)
bin/start_odoo imqdump --database $PGDATABASE --message 49737

# Dump message with logs in JSON format
bin/start_odoo imqdump --database $PGDATABASE --message 49737 --include-logs --json

# Dump queue information
bin/start_odoo imqdump --database $PGDATABASE --queue default

# Dump processor by selector
bin/start_odoo imqdump --database $PGDATABASE --processor TestMessage
```

### Object Types

#### Messages (`--message`)

Dump detailed message information including processing history and logs:

```bash
# By numeric ID
bin/start_odoo imqdump --database $PGDATABASE --message 49737

# By MessageId (UUID)  
bin/start_odoo imqdump --database $PGDATABASE --message "8f3ec366-68c8-4945-87cc-aaf2cad5dd0f"

# Include processing logs
bin/start_odoo imqdump --database $PGDATABASE --message 49737 --include-logs
```

#### Queues (`--queue`)

Dump queue configuration and statistics:

```bash
# Queue information with message counts by state
bin/start_odoo imqdump --database $PGDATABASE --queue default
```

#### Processors (`--processor`)

Dump message processor configuration:

```bash
# By numeric ID
bin/start_odoo imqdump --database $PGDATABASE --processor 1

# By selector name
bin/start_odoo imqdump --database $PGDATABASE --processor TestMessage
```

#### Processing Records (`--processing`)

Dump individual processing attempt information:

```bash
# Processing record with logs
bin/start_odoo imqdump --database $PGDATABASE --processing 47495 --include-logs
```

#### Processing Logs (`--logs`)

Dump processing logs in streaming format, similar to `kubectl logs`:

```bash
# Stream format - human-readable log output
bin/start_odoo imqdump --database $PGDATABASE --logs 47497

# JSON format - structured log data
bin/start_odoo imqdump --database $PGDATABASE --logs 47497 --json

# Save logs to file for analysis
bin/start_odoo imqdump --database $PGDATABASE --logs 47497 --output processing_47497.log
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
bin/start_odoo imqdump --database $PGDATABASE --message 49737 --json
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
bin/start_odoo imqdump --database $PGDATABASE --logs 47497
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
bin/start_odoo imqdump --database $PGDATABASE --logs 47497 --json
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
bin/start_odoo imqdump --database $PGDATABASE --message 49737

# Examine detailed logs
bin/start_odoo imqdump --database $PGDATABASE --message 49737 --include-logs

# Stream processing logs for detailed debugging
bin/start_odoo imqdump --database $PGDATABASE --logs 47497

# Check what processor handles the message
bin/start_odoo imqdump --database $PGDATABASE --processor TestMessage
```

#### Monitoring and Operations

```bash
# Export message data for analysis
bin/start_odoo imqdump --database $PGDATABASE --message 49737 --json \
  --output message_49737.json

# Check queue health
bin/start_odoo imqdump --database $PGDATABASE --queue default

# Audit processing attempts
bin/start_odoo imqdump --database $PGDATABASE --processing 47495 --include-logs

# Tail processing logs for monitoring
bin/start_odoo imqdump --database $PGDATABASE --logs 47497 --output /var/log/imq/processing_47497.log
```

#### CI/CD Integration

```bash
#!/bin/bash
# Verify message processing in pipeline

MESSAGE_ID=$(bin/start_odoo imqtest --database test_db --simple --json-output | jq -r '.[0].id')

# Process the message
bin/start_odoo imqworker --database test_db --queue default --message $MESSAGE_ID --max-messages 1

# Verify it completed successfully
FINAL_STATE=$(bin/start_odoo imqdump --database test_db --message $MESSAGE_ID --json | jq -r '.status.state')

if [ "$FINAL_STATE" != "done" ]; then
  echo "Message processing failed: $FINAL_STATE"
  bin/start_odoo imqdump --database test_db --message $MESSAGE_ID --include-logs
  exit 1
fi

echo "Message processed successfully!"
```

## Testing

### IMQ Test CLI Command (`imqtest`)

The `imqtest` command provides a powerful CLI interface for creating and testing IMQ messages. It offers complete feature parity with the GUI test launcher and is perfect for debugging, automation, and load testing.

#### Quick Start

```bash
# List available test processors
bin/start_odoo imqtest --database $PGDATABASE --list-processors

# Create a simple test message
bin/start_odoo imqtest --database $PGDATABASE --simple --queue default --verbose

# Create an RPC method test with custom parameters
bin/start_odoo imqtest --database $PGDATABASE --rpc-method --queue default --param "test_data" --duration 10

# Create multiple messages with JSON output
bin/start_odoo imqtest --database $PGDATABASE --simple --queue default --count 5 --json-output
```

#### Command Syntax

```bash
bin/start_odoo imqtest [OPTIONS] TEST_TYPE
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
bin/start_odoo imqtest --database $PGDATABASE --simple --queue default --verbose

# Output:
# Created simple message 1/1: ID=49682, MessageID=f2f1b01a-8559-4058-9e69-e16b8ec288fa
# Successfully created 1 simple message(s) in queue 'default'
#   - Message ID: 49682, Name: 'CLI Test Simple 1'
```

##### RPC Method Test with Custom Parameters

```bash
# Test RPC method with 30-second duration and custom parameter
bin/start_odoo imqtest --database $PGDATABASE --rpc-method \
  --queue default --param "production_data" --duration 30 --verbose
```

##### Exception Testing

```bash
# Test IMQRetryableError with 60-second delay
bin/start_odoo imqtest --database $PGDATABASE --rpc-method \
  --queue default --raise-exception --exception-type imqretryable \
  --delay-param 60 --pass-imqerror-value
```

##### FIFO Test Sequence

```bash
# Create FIFO test sequence with exception on step 3
bin/start_odoo imqtest --database $PGDATABASE --fifo-test \
  --queue fifo_queue --raise-exception --exception-step fifo_step3 \
  --exception-type usererror --message-group "test-batch-001"
```

##### Batch Message Creation

```bash
# Create 10 simple messages with 2-second delay between each
bin/start_odoo imqtest --database $PGDATABASE --simple \
  --queue default --count 10 --delay 2 --json-output > test_results.json
```

##### Custom Payload Testing

```bash
# Test with custom JSON payload
bin/start_odoo imqtest --database $PGDATABASE --simple \
  --queue default --payload '{"customer_id": 12345, "action": "process_order"}' \
  --selector "OrderProcessor" --name "Order Processing Test"
```

##### Load Testing

```bash
# Create 100 messages quickly for load testing
bin/start_odoo imqtest --database $PGDATABASE --simple \
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
RESULT=$(bin/start_odoo imqtest --database test_db --simple --count 5 --json-output)
MESSAGE_IDS=$(echo "$RESULT" | jq -r '.[].id')

# Start worker to process them
bin/start_odoo imqworker --database test_db --queue default --max-messages 5 &
WORKER_PID=$!

# Wait for processing and check results
sleep 30
kill $WORKER_PID

# Verify all messages processed successfully
for id in $MESSAGE_IDS; do
  STATUS=$(bin/start_odoo shell --database test_db -c "
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
time bin/start_odoo imqtest --database prod_db --simple \
  --queue performance_test --count 1000 --verbose

# Monitor queue depth during test  
while true; do
  PENDING=$(bin/start_odoo shell --database prod_db -c "
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
   bin/start_odoo imqtest --database $PGDATABASE --simple \
     --queue default --user-id 1 --verbose
   ```

##### Debug Mode

```bash
# Run in debug mode for immediate execution
bin/start_odoo imqtest --database $PGDATABASE --rpc-method \
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
2. **CLI Command Registration Test**: Confirms the `imqworker` command is properly registered
3. **CLI Help Test**: Validates the command-line help system works
4. **Worker Initialization Test**: Tests worker startup and database connection
5. **Unit Tests**: Runs basic import and utility function tests
6. **CLI Argument Validation**: Ensures invalid arguments are rejected
7. **Database Connection Test**: Verifies database connectivity through the worker

### Manual Testing

#### Test CLI Command
```bash
# Test command registration
bin/start_odoo help | grep imqworker

# Test help system
bin/start_odoo imqworker --help

# Test worker with non-existent queue (should exit gracefully)
bin/start_odoo imqworker --database mydb --queue test_queue --max-messages 1
```

#### Test Worker Functionality
```bash
# Test with existing queue
bin/start_odoo imqworker --database mydb --queue default --max-messages 5

# Test with memory limit
bin/start_odoo imqworker --database mydb --queue default --max-rss-memory 512M

# Test with regex pattern
bin/start_odoo imqworker --database mydb --queue "mpy.*" --max-messages 10

# Test with observability (metrics and health checks)
bin/start_odoo imqworker --database mydb --queue default --observability-port 8080
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

For questions or support, please contact the author via Twitter @cmorisse.