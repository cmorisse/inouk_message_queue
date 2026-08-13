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
- **FIFO failure propagation**: A `failed` task blocks every subsequent pending task in the same `group` on a FIFO queue until it is archived. See README.md § "Failure handling in FIFO groups".

### Worker Monitoring
- **Observability Server**: Workers expose /status, /healthz, /readyz, /metrics endpoints
- **Queue Depth**: Now included in /status endpoint showing actual backlog of processable messages
- **Message Processing**: Track processed count, queue health, processing rates

### Key Patterns
- **Provider-specific methods**: Methods like `get_message__pgsql`, `store_message__aws_sqs`
- **Worker types**: Standalone workers (v3), ir.cron workers
- **Queue types**: Standard (std) and FIFO queues
- **Message targeting**: Workers can target specific messages by ID

### Processing notifications — routing is NOT in the workers
The 14 `send_notification()` call sites across `workers/base.py` and `models/worker.py` only
*raise* notifications. Recipients and body are decided in **one** place —
`send_odoo_notification` in `models/queue__odoo.py`, via `_resolve_notification_recipients()`
and `_build_notification_body()`. Change routing or wording there; touching the workers only
changes the **title** (7 hard-coded state labels, duplicated across the two worker
implementations — edit both or they drift).

Recipient cascade: `user_obj` → `message.requesting_user_id` → `message.user_id` → IMQ admin
broadcast, with system/inactive/share candidates rejected down to the broadcast. `danger`
adds the admin broadcast **on top of** the requester. The load-bearing fact is that
`user_id` defaults to `env.user` at enqueue (`api.py:394`), so the requester is known without
any caller change — but an **escalated** task still needs `_imq_requesting_user_id`, else it
notifies the escalation identity itself.

Body = message name + the worker's `message` argument below it (duration on success,
exception class on failure). It used to be silently discarded. Composed via `markupsafe.Markup`
because the browser renders it with OWL `markup()`. Canonical contract: the two method
docstrings. See README § "Processing Notifications".

### Execution Stats axes (`stats_category` / `stats_target`)
Two optional Char columns on `imq.message`, mirrored as **related stored + indexed** fields on `imq.message_processing` (where `processing_time` lives). They are the aggregation/filter axes for execution-time reporting. Passed via kwargs `_imq_stats_category` / `_imq_stats_target`, extracted **once** in `enqueue()` into the processor context, then written onto the message at receive time by **both** `store_message__pgsql` and `store_message__aws_sqs` (same conditional pattern as `_imq_parent_message_id`) — hence identical behavior across providers. Muppy's `mpy_execute` auto-fills `stats_category` with the fabric task name. See README § "Execution Stats" for usage and querying.

### Deduplication window (`_imq_deduplication_interval_s`)
Optional per-enqueue override of `imq.queue.deduplication_interval_s`, extracted in `enqueue()` and threaded `_send_message` → `send_message__pgsql` → `check_message_duplicate`. `None` inherits the queue value (so every pre-existing caller is unchanged); an integer applies to that enqueue only; `0` is a zero-length window (never suppresses) and is deliberately **not** a synonym for "inherit"; negative raises. Honoured by pgsql only — `send_message__aws_sqs` logs a `warning` and ignores it, because AWS fixes the FIFO window at 5 minutes.

**Contrast with the stats axes, and it is the load-bearing difference**: those go into `processor_context` *because a receive site reads them back*. The dedup window has no reader downstream — it is consumed entirely at enqueue — so it is passed as a plain argument and never enters the context or the payload. Locked by `test_dedup_window.py::test_the_window_reaches_neither_the_payload_nor_the_context`.

**⚠ The trap worth knowing before you rely on deduplication at all**: with no explicit `_imq_message_deduplication_id`, `_send_message` falls back to a hash of the message body — and the body carries the calling context. An `ir.cron` gets `lastcall` injected into its context by Odoo (`ir_cron.py`), so the hash differs at every tick and **deduplication can NEVER fire for a cron-dispatched job on a FIFO queue**. This is not specific to any one consumer. Always pass an explicit id from a cron. See README § "Message Deduplication (FIFO Queues)" for usage.

## Development Guidelines

### Testing
- The `tests/` directory holds ordinary Odoo `TransactionCase` tests. Run them through the
  host repo's launcher, e.g. `make test ADDONS=inouk_message_queue TAGS=/inouk_message_queue`
  from the Muppy repo root.
- **`tests/__init__.py` imports each test module explicitly.** A new file that is not added
  there never runs — silently, which looks exactly like a passing test.
- ⚠ **`./run_tests.sh` is NOT the unit-test suite**, despite the name. It is a workers-v3
  smoke script: it calls `bin/start_odoo` (a launcher that does not exist in the Muppy repo —
  there it is `bin/mpy-srv`), boots a server and workers, and says so itself ("Unit tests
  require proper Python path configuration — running basic import tests instead"). It runs
  **none** of the files in `tests/`.
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
bin/start_odoo imq-worker --database $PGDATABASE --queues PATTERN --observability-port 8080

# Check worker status
curl http://localhost:8080/status | python3 -m json.tool
```

Remember: When in doubt about IMQ tool usage, always consult `README.md` first!