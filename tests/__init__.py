# Test modules for IMQ Workers v3
# 
# This package contains all test files for the IMQ Workers v3 implementation.
# Tests are organized by implementation phase and functionality.

from .test_phase1_simple import main as run_phase1_tests

__all__ = ['run_phase1_tests']