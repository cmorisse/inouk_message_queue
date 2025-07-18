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
- **`run_tests.py`** - Test runner for all test suites
- **`__init__.py`** - Package initialization

## Running Tests

### Run All Tests
```bash
python3 tests/run_tests.py
```

### Run Specific Test Suite
```bash
python3 tests/test_phase1_simple.py
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

### Future Tests (Planned):
- **Phase 2** - Queue processing and regex matching
- **Phase 3** - Resource management and advanced metrics
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

All Phase 1 tests currently pass:
- ✅ Directory Structure
- ✅ Worker Utils
- ✅ Memory Monitoring  
- ✅ CLI Structure
- ✅ Prometheus Metrics

Total: 5/5 test suites passing 🎉