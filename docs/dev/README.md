# IMQ Workers v3 - Development Documentation

This directory contains development documentation for the IMQ Workers v3 implementation.

## Directory Structure

### 📊 Analysis (`./analysis/`)
Research, analysis, and improvement planning documents:

- **[ANALYSIS_NOTES.md](./analysis/ANALYSIS_NOTES.md)** - Comprehensive analysis of the existing IMQ codebase
- **[IMPROVEMENTS.md](./analysis/IMPROVEMENTS.md)** - Identified improvements and optimization opportunities

### 🚀 Implementation (`./implementation/`)
Implementation progress and milestone documentation:

- **[PHASE1_COMPLETE.md](./implementation/PHASE1_COMPLETE.md)** - Phase 1 completion summary and deliverables
- **[TEST_ORGANIZATION.md](./implementation/TEST_ORGANIZATION.md)** - Test structure and organization documentation

### 📋 Specifications (`./specs/`)
Technical specifications and implementation plans:

- **[workers_v3_spec.md](./specs/workers_v3_spec.md)** - Complete specification for Workers v3 standalone implementation
- **[workers_v3_implementation_plan.md](./specs/workers_v3_implementation_plan.md)** - Detailed 4-phase implementation plan

## Development Timeline

1. **Phase 1 ✅ Complete**: Core infrastructure (CLI, base worker, monitoring)
2. **Phase 2**: Advanced features (signal handling, queue management)
3. **Phase 3**: Production features (observability, Kubernetes integration)
4. **Phase 4**: Performance optimization and documentation

## Key Features Implemented

- ✅ CLI command (`imq-worker`) with full argument parsing
- ✅ Standalone worker class with signal handling
- ✅ Memory monitoring and limits
- ✅ Prometheus metrics collection
- ✅ HTTP server for liveness probes and metrics
- ✅ Comprehensive test suite (`run_tests.sh`)
- ✅ Full integration with Muppy/Odoo environment

## Usage Quick Reference

```bash
# Run all tests
./run_tests.sh

# Start standalone worker
bin/start_odoo imq-worker --database $PGDATABASE --queue default --max-messages 100

# Worker with observability
bin/start_odoo imq-worker --database $PGDATABASE --queue "mpy.*" --observability-port 8080
```

## Contributing

When adding new development documentation:
1. Choose appropriate subfolder (`analysis/`, `implementation/`, `specs/`)
2. Use clear, descriptive filenames
3. Update this README.md with new document references
4. Follow existing markdown formatting conventions