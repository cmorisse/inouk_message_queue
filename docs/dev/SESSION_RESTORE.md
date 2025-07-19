# IMQ Workers v3 - Session Restore Information

## Session Overview
This document contains all information needed to restore and continue working on the IMQ Workers v3 implementation project.

## Project Status: Phase 2 Complete ✅

### Current Branch
- **Branch**: `upg_traefik_3630`
- **Main Branch**: `13.0`
- **Status**: Clean working directory

### Environment
- **Working Directory**: `/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue`
- **Database**: `cyril_mpy13c_99_001`
- **Platform**: Linux 6.8.0-59-generic
- **Date**: 2025-07-18

## Implementation Progress

### ✅ Phase 1 Complete
- Basic CLI infrastructure
- Base worker class extraction
- Memory monitoring
- Basic standalone worker implementation

### ✅ Phase 2 Complete
- **Advanced queue pattern matching** with regex support
- **Smart round-robin processing** with health-aware queue selection
- **Enhanced signal handling** (SIGTERM/SIGINT for graceful shutdown, SIGUSR1/SIGUSR2 for operations)
- **Queue health monitoring** with failure tracking and automatic recovery
- **Comprehensive status reporting** and logging
- **Timeout handling** (already implemented in multiple forms)
- **IMQ Logging System** integration:
  - Added IMQLogHandler for database logging
  - Added MpyStringIO for console capture
  - Integrated with message processing flow
  - Supports processor-level log configuration
- **Critical Bug Fixes**:
  - Messages stuck in 'wip' state → Fixed message state update mechanism
  - Empty queues marked unhealthy → Fixed queue health monitoring logic

### 🎯 Next Phase
**Phase 3**: Full observability integration and Kubernetes deployment features

## Key Files and Locations

### Core Implementation Files
```
/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/
├── cli/
│   └── imq_worker.py           # CLI command (working)
├── workers/
│   ├── base.py                 # Base worker functionality
│   ├── standalone.py           # Main standalone worker (Phase 2 complete)
│   └── monitoring.py           # Memory monitoring & observability
├── worker_utils/
│   └── worker_utils.py         # Shared utilities
└── models/
    └── worker.py               # Original worker model (preserved)
```

### Documentation Files
```
├── docs/dev/
│   ├── implementation/
│   │   ├── PHASE1_COMPLETE.md  # Phase 1 completion summary
│   │   ├── PHASE2_COMPLETE.md  # Phase 2 completion summary
│   │   └── BUGFIX_EMPTY_QUEUE_HEALTH.md  # Critical bug fix documentation
│   └── specs/
│       └── workers_v3_implementation_plan.md  # Full implementation plan
```

## Key Features Implemented

### 1. Advanced Queue Pattern Matching
- **Patterns supported**: Exact match, prefix (`queue*`), suffix (`*queue`), wildcard (`queue_*_name`), full regex
- **Implementation**: `_normalize_queue_pattern()` and `_find_matching_queues()` in `standalone.py`
- **Testing**: Successfully tested with wildcard pattern `*` processing multiple queues

### 2. Smart Round-Robin Processing
- **Health-aware selection**: Skips unhealthy queues automatically
- **Load balancing**: Distributes work evenly across healthy queues
- **Implementation**: `_select_next_queue()` and `_is_queue_healthy()` methods

### 3. Enhanced Signal Handling
- **SIGTERM/SIGINT**: Graceful shutdown with current message completion
- **SIGUSR1**: Status logging and health summary on demand
- **SIGUSR2**: Reset queue failure counters
- **Implementation**: `_setup_signal_handlers()` and related methods

### 4. Queue Health Monitoring
- **Failure tracking**: Tracks consecutive failures per queue
- **Automatic recovery**: Retries unhealthy queues after timeout (default: 300s)
- **Configuration**: `max_queue_failures=10`, `queue_failure_timeout=300`

### 5. Comprehensive Timeout Handling
- **Message visibility timeout**: Prevents message duplication during processing
- **Queue failure timeout**: Controls unhealthy queue retry intervals
- **Retry delay timeout**: Handles delayed message retries
- **HTTP timeout**: Prevents hanging on external service calls

## Critical Bug Fixes Applied

### Bug 1: Message State Update Issue
- **Problem**: Messages remained in 'wip' state after processing
- **Root Cause**: Missing `message_obj.write(result)` call in standalone worker
- **Fix**: Added proper message state update in `_process_one_message()` method
- **Location**: `standalone.py` lines 561-582

### Bug 2: Empty Queue Health Monitoring
- **Problem**: Empty queues incorrectly marked as unhealthy after 10 consecutive empty polls
- **Root Cause**: Boolean return values didn't distinguish between empty queue and processing failure
- **Fix**: Changed return values to strings ('processed', 'empty', 'failed') and updated statistics logic
- **Location**: `standalone.py` lines 519, 527, 595, 625 and main processing loop

## Testing Status

### ✅ Successful Tests
1. **Single queue processing**: `bin/start_odoo imqworker --database $PGDATABASE --queue default --max-messages 1`
2. **Multiple queue processing**: `bin/start_odoo imqworker --database $PGDATABASE --queue "*" --max-messages 2`
3. **Empty queue handling**: `timeout 30s bin/start_odoo imqworker --database $PGDATABASE --queue health_check --max-messages 100`
4. **Integration tests**: All 7 tests passing

### Current Test Results
- **Pattern matching**: Successfully matches queues with wildcards
- **Message processing**: Proper state transitions (wip → done/failed)
- **Queue health**: Empty queues remain healthy during continuous polling
- **Round-robin**: Even distribution across multiple queues

## Configuration

### Database Connection
- **Database**: `cyril_mpy13c_99_001`
- **Host**: `37.187.249.236:5432`
- **User**: `cyril_mpy13c_99`
- **Environment**: Production environment variables loaded from `/etc/muppy.env`

### CLI Usage
```bash
# Basic usage
bin/start_odoo imqworker --database $PGDATABASE --queue default --max-messages 100

# Multiple queues with wildcard
bin/start_odoo imqworker --database $PGDATABASE --queue "*" --max-messages 100

# With memory limits
bin/start_odoo imqworker --database $PGDATABASE --queue default --max-rss-memory 512M

# With observability
bin/start_odoo imqworker --database $PGDATABASE --queue default --observability-port 8080
```

## Development Environment

### Required Commands
```bash
# Initialize environment
/usr/local/python/current/bin/ikb init
/usr/local/python/current/bin/ikb install

# Start Odoo
bin/start_odoo

# Run tests
bin/start_odoo --test-enable --stop-after-init -u inouk_message_queue
```

### Environment Variables
```bash
export PGDATABASE=cyril_mpy13c_99_001
export PGHOST=37.187.249.236
export PGPORT=5432
export PGUSER=cyril_mpy13c_99
export PGPASSWORD=25cd441f865a42d3
```

## Phase 3 Roadmap

### Pending Tasks
1. **Enhanced Observability**
   - Prometheus metrics integration
   - Health check endpoints
   - Performance monitoring
   - Grafana dashboard queries

2. **Kubernetes Integration**
   - Deployment manifests
   - ServiceMonitor configuration
   - Liveness/readiness probes
   - Resource limits and requests

3. **Production Features**
   - Connection pooling optimization
   - Error recovery mechanisms
   - Performance tuning
   - Documentation updates

### Architecture Decisions Made
- **Single process/thread per worker**: Implemented in standalone worker
- **REGEX-based queue selection**: Implemented with pattern normalization
- **RSS memory monitoring**: Implemented with configurable limits
- **Graceful shutdown**: Implemented with signal handling
- **Round-robin processing**: Implemented with health awareness

## Known Issues
- **None**: All major issues have been resolved
- **Performance**: No performance issues identified
- **Memory**: Memory monitoring working correctly
- **Stability**: System stable under continuous operation

## Key Technical Patterns

### 1. Queue Pattern Matching
```python
def _normalize_queue_pattern(self, pattern):
    """Convert simple patterns to regex patterns"""
    # Supports *, prefix*, *suffix, and full regex patterns
```

### 2. Health Monitoring
```python
def _is_queue_healthy(self, queue_name):
    """Check if a queue is healthy for processing"""
    # Uses failure count and timeout-based recovery
```

### 3. Signal Handling
```python
def _setup_signal_handlers(self):
    """Setup graceful shutdown on SIGTERM and operational signals"""
    # SIGTERM/SIGINT: graceful shutdown
    # SIGUSR1: status logging
    # SIGUSR2: reset failure counters
```

### 4. Message State Management
```python
# Critical pattern for message state updates
result = self.process_message(env, message_obj, message, {})
message_obj.write(result)  # This was the missing piece!
```

## Next Session Actions
1. Review Phase 3 implementation plan
2. Continue with observability server enhancements
3. Implement Prometheus metrics collection
4. Create Kubernetes deployment manifests
5. Add comprehensive production monitoring

## Context for Claude
- **Project**: IMQ Workers v3 - Standalone CLI workers for Odoo message queue processing
- **Technology**: Python, Odoo 13, PostgreSQL, Docker, Kubernetes
- **Purpose**: Replace ir.cron-based workers with standalone processes for better scalability
- **Status**: Phase 2 complete, ready for Phase 3 observability integration

## Important Notes
- All code follows existing IMQ patterns and conventions
- Backward compatibility maintained with existing worker system
- Security best practices followed (no secrets in code)
- Comprehensive error handling and logging implemented
- Production-ready signal handling and graceful shutdown

This document provides complete context for resuming work on the IMQ Workers v3 implementation project.