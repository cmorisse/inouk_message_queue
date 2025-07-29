# CLAUDE.md - IMQ (Inouk Message Queue) Module

This file provides guidance to Claude Code when working specifically with the IMQ (Inouk Message Queue) module.

## Documentation and Tool Usage

### Always Check README.md First
When working with IMQ tools, ALWAYS consult `README.md` in this directory before attempting to use tools:
- **imq-ctl**: Detailed usage examples and syntax in README.md
- **imq-test**: Test command usage and options  
- **imq-worker**: Worker command line options and patterns

### Pattern Recognition
- **Tools starting with `imq`**: Always check `README.md` for documented usage
- **Failed `--help`**: IMQ tools may not have standard help - use README.md instead
- **Custom CLI tools**: This module has extensive CLI documentation in README.md

### Key IMQ Tools Documentation
All IMQ command-line tools are documented with examples in `README.md`:
- `imq-ctl`: kubectl-style inspection tool for messages, queues, processors
- `imq-test`: Testing framework for message processing
- `imq-worker`: Standalone worker with observability features

## IMQ Architecture Overview

### Message States and Queue Depth
- **Queue Depth Definition**: Messages where state is in ('pending', 'retry') AND (planned_time IS NULL OR planned_time <= NOW())
- **Message States**: new, pending, wip, retry, done, terminated, failed, archived, reset
- **'new' messages excluded**: They can stay indefinitely in 'new' state while being entered by users

### Worker Monitoring
- **Observability Server**: Workers expose /status, /healthz, /readyz, /metrics endpoints
- **Queue Depth**: Now included in /status endpoint showing actual backlog of processable messages
- **Message Processing**: Track processed count, queue health, processing rates

### Key Patterns
- **Provider-specific methods**: Methods like `get_message__pgsql`, `store_message__aws_sqs`
- **Worker types**: Standalone workers (v3), ir.cron workers
- **Queue types**: Standard (std) and FIFO queues
- **Message targeting**: Workers can target specific messages by ID

## Development Guidelines

### Testing
- Use `./run_tests.sh` to run the complete test suite
- Tests are in `tests/` directory
- Use `imq-test` for creating test messages

### Debugging
- Use `imq-ctl` to inspect messages, queues, processors, and logs
- Monitor workers via observability endpoints
- Check processing logs with `imq-ctl logs`

### Common Operations
```bash
# Inspect a message
bin/start_odoo imq-ctl --database $PGDATABASE describe message MESSAGE_ID

# Check queue status
bin/start_odoo imq-ctl --database $PGDATABASE describe queue QUEUE_NAME  

# Run worker with observability
bin/start_odoo imq-worker --database $PGDATABASE --queue PATTERN --observability-port 8080

# Check worker status
curl http://localhost:8080/status | python3 -m json.tool
```

Remember: When in doubt about IMQ tool usage, always consult `README.md` first!