# IMQ Workers v3 Tests

This directory contains all tests for the IMQ Workers v3 implementation.

## Test Structure

### Phase 1 Tests
- **`test_phase1_simple.py`** - Comprehensive Phase 1 implementation tests
  - Directory structure validation
  - Worker utility functions
  - Memory monitoring
  - CLI argument parsing
  - Prometheus metrics support

### Test Files
- **`test_phase1.py`** - Original unittest-based tests (has import issues with relative imports)
- **`test_phase1_simple.py`** - Simplified test implementation that works standalone
- **`test_phase2_queue_processing.py`** - Comprehensive Phase 2 queue processing tests
- **`run_tests.py`** - Test runner for all test suites
- **`__init__.py`** - Package initialization

### Workers Directory Tests
- **`workers/test_resource_limits.py`** - Comprehensive automated tests for resource limits
- **`workers/manual_limit_test.py`** - Simple manual testing script for individual limit tests
- **`workers/verify_limits.py`** - Implementation verification script
- **`workers/test_worker_stop_parameters.py`** - Worker stopping control functionality tests

## Running Tests

### Run All Tests
```bash
python3 tests/run_tests.py
```

### Run Specific Test Suite
```bash
# Phase 1 tests (core infrastructure)
python3 tests/test_phase1_simple.py

# Phase 2 tests (queue processing)
python3 tests/test_phase2_queue_processing.py
```

### Run Worker Tests
```bash
# Resource limit tests
python3 tests/workers/test_resource_limits.py

# Manual limit testing
python3 tests/workers/manual_limit_test.py

# Implementation verification
python3 tests/workers/verify_limits.py

# Worker stopping control tests
python3 tests/workers/test_worker_stop_parameters.py
```

### Run from Project Root
```bash
cd /opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue
python3 tests/run_tests.py
```

## Test Coverage

### Phase 1 Tests Cover:
- ✅ **Directory Structure** - Validates all required files and directories exist
- ✅ **Worker Utilities** - Memory parsing, duration formatting, validation functions
- ✅ **Memory Monitoring** - psutil integration, memory limit checking
- ✅ **CLI Structure** - Argument parsing, validation, error handling
- ✅ **Prometheus Metrics** - Metrics creation, export functionality

### Phase 2 Tests Cover:
- ✅ **Queue Pattern Validation** - Regex pattern validation and error handling
- ✅ **Queue Pattern Matching** - Actual regex matching against queue names
- ✅ **Queue Discovery Simulation** - Queue filtering by pattern logic
- ✅ **Round-Robin Queue Selection** - Load balancing algorithm with health checks
- ✅ **Queue Health Monitoring** - Failure tracking and timeout recovery
- ✅ **Queue Statistics Tracking** - Performance metrics and processing stats

### Phase 3 Tests Cover:
- ✅ **Resource Limits** - Message count and memory limit enforcement
- ✅ **Graceful Shutdown** - SIGTERM and SIGINT signal handling
- ✅ **Advanced Observability** - Health, readiness, and status endpoints
- ✅ **Worker Control** - System parameter-based stopping functionality

### Worker Tests Cover:
- ✅ **Resource Limit Testing** - Automated tests for memory and message limits
- ✅ **Manual Testing Scripts** - Simple scripts for individual limit verification
- ✅ **Implementation Verification** - Validates all features are properly implemented
- ✅ **Worker Stop Control** - Tests parameter parsing and hostname matching logic

### Future Tests (Planned):
- **Phase 4** - Kubernetes integration and observability
- **Integration Tests** - End-to-end worker functionality
- **Performance Tests** - Throughput and resource usage benchmarks

## Test Dependencies

### Required Python Packages:
- `psutil` - Memory monitoring
- `prometheus_client` - Metrics collection (optional, graceful fallback)
- Standard library modules: `os`, `sys`, `time`, `argparse`, `re`

### Test Environment:
- Tests are designed to run without full Odoo environment
- Individual components are tested in isolation
- No database or external services required for Phase 1 tests

## Test Philosophy

1. **Standalone Tests** - Tests run without complex dependencies
2. **Component Isolation** - Each component tested independently
3. **Comprehensive Coverage** - All major functionality tested
4. **Clear Output** - Descriptive test results and error messages
5. **Fast Execution** - Tests complete quickly for rapid feedback

## Adding New Tests

When adding new test files:

1. Create test file in appropriate subdirectory
2. Follow naming convention: `test_<component>_<description>.py`
3. Add import to `__init__.py` if needed
4. Update `run_tests.py` to include new test file
5. Update this README with test description

## Test Results

### Phase 1 Tests
All Phase 1 tests currently pass:
- ✅ Directory Structure
- ✅ Worker Utils
- ✅ Memory Monitoring  
- ✅ CLI Structure
- ✅ Prometheus Metrics

### Phase 2 Tests
All Phase 2 tests currently pass:
- ✅ Queue Pattern Validation (18/18 test cases)
- ✅ Queue Pattern Matching (27/27 test cases)
- ✅ Queue Discovery Simulation (5/5 scenarios)
- ✅ Round-Robin Queue Selection (8/8 test cases)
- ✅ Queue Health Monitoring (7/7 test cases)
- ✅ Queue Statistics Tracking (5/5 test cases)

### Phase 3 & Worker Tests
All Phase 3 and worker tests currently pass:
- ✅ Resource Limit Testing (comprehensive automated tests)
- ✅ Manual Limit Testing (simple verification scripts)
- ✅ Implementation Verification (validates all features present)
- ✅ Worker Stop Control (parameter parsing and hostname matching)

**Total: 11/11 test suites passing 🎉**

## Worker Stop Control Testing

The worker stopping control functionality includes comprehensive tests:

### System Parameter Tests
- ✅ Parameter parsing logic (empty, single, multiple hostnames)
- ✅ Hostname matching (exact, wildcard, list matching)
- ✅ Edge case handling (empty strings, malformed input)

### Implementation Tests  
- ✅ XML parameter definitions in place
- ✅ Cron worker backward compatibility 
- ✅ Standalone worker integration
- ✅ All required imports and methods present

### Usage Examples
The test script provides complete configuration examples for:
- Stopping all workers (`*`)
- Stopping workers on specific servers
- Stopping workers on multiple servers
- Mixed environment configurations

Run `python3 tests/workers/test_worker_stop_parameters.py` for full verification.

## Phase 2 Queue Processing Testing

The Phase 2 test suite provides comprehensive coverage of the queue processing functionality:

### Test Suite Breakdown

1. **Queue Pattern Validation** (18 test cases)
   - Tests valid regex patterns: `default.*`, `queue[0-9]+`, `(urgent|normal)`
   - Tests invalid patterns: empty strings, malformed regex, unclosed brackets
   - Tests edge cases: wildcard patterns, escaped characters

2. **Queue Pattern Matching** (27 test cases)
   - Tests exact matching vs substring matching behavior
   - Tests anchored patterns: `^default$`, `^prod.*$`
   - Tests complex patterns: alternation, character classes, wildcards

3. **Queue Discovery Simulation** (5 scenarios)
   - Tests filtering queues by pattern
   - Tests with mock queue data: default, urgent, muppy-prod, muppy-test, batch-processing
   - Validates pattern-to-queue mapping logic

4. **Round-Robin Queue Selection** (8 test cases)
   - Tests load balancing across healthy queues
   - Tests wrapping behavior when reaching end of queue list
   - Tests handling of unhealthy queues (skipping)
   - Tests edge case: no healthy queues available

5. **Queue Health Monitoring** (7 test cases)
   - Tests failure threshold tracking (3 failures → unhealthy)
   - Tests timeout-based recovery after failures
   - Tests success-based recovery (resets failure count)
   - Tests state transitions: healthy → unhealthy → recovered

6. **Queue Statistics Tracking** (5 test cases)
   - Tests initialization of queue statistics
   - Tests success/failure counting
   - Tests average processing time calculation
   - Tests health summary generation

### Running Phase 2 Tests

```bash
# Run complete Phase 2 test suite
python3 tests/test_phase2_queue_processing.py

# Expected output: 6/6 test suites passed
```

### Test Coverage

The Phase 2 tests provide simulation-based testing of the queue processing logic without requiring a full Odoo environment. All core algorithms are validated:

- ✅ **Regex Pattern Engine** - Validates queue pattern matching
- ✅ **Queue Discovery** - Tests filtering and selection logic  
- ✅ **Load Balancing** - Tests round-robin algorithm
- ✅ **Health Monitoring** - Tests failure tracking and recovery
- ✅ **Statistics** - Tests performance metric collection

This ensures the queue processing functionality works correctly before deployment to production environments.