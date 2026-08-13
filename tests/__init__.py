# Test modules for IMQ Workers v3
# 
# This package contains all test files for the IMQ Workers v3 implementation.
# Tests are organized by implementation phase and functionality.

from .test_phase1_simple import main as run_phase1_tests

from . import test_stats_axes
from . import test_purge_retention
from . import test_dedup_window
from . import test_console_capture_teardown
from . import test_notification_routing

__all__ = ['run_phase1_tests']