#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for Phase 1 implementation of IMQ Workers v3
"""

import sys
import os
import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestPhase1Implementation(unittest.TestCase):
    """Test Phase 1 components"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_worker_utils(self):
        """Test worker utility functions"""
        from worker_utils.worker_utils import (
            parse_memory_limit, format_duration, format_memory_size,
            validate_queue_pattern, get_worker_name, safe_int
        )
        
        # Test memory limit parsing
        self.assertEqual(parse_memory_limit('1024M'), 1024 * 1024 * 1024)
        self.assertEqual(parse_memory_limit('2G'), 2 * 1024 * 1024 * 1024)
        self.assertEqual(parse_memory_limit('512K'), 512 * 1024)
        self.assertEqual(parse_memory_limit('1024'), 1024)
        self.assertIsNone(parse_memory_limit('invalid'))
        self.assertIsNone(parse_memory_limit(''))
        
        # Test duration formatting
        self.assertEqual(format_duration(0.1), '0.100s')
        self.assertEqual(format_duration(65.0), '1m5s')
        self.assertEqual(format_duration(3665.0), '1h1m')
        
        # Test memory size formatting
        self.assertEqual(format_memory_size(1024), '1.0 KB')
        self.assertEqual(format_memory_size(1024*1024), '1.0 MB')
        self.assertEqual(format_memory_size(1024*1024*1024), '1.0 GB')
        
        # Test queue pattern validation
        self.assertTrue(validate_queue_pattern('default'))
        self.assertTrue(validate_queue_pattern('test.*'))
        self.assertFalse(validate_queue_pattern('[invalid'))
        
        # Test worker name generation
        worker_name = get_worker_name('custom-worker')
        self.assertEqual(worker_name, 'custom-worker')
        
        auto_name = get_worker_name()
        self.assertTrue(auto_name.startswith('imq-worker-'))
        
        # Test safe int
        self.assertEqual(safe_int('42'), 42)
        self.assertEqual(safe_int('invalid', 10), 10)
        self.assertEqual(safe_int(None, 5), 5)
    
    def test_memory_monitor(self):
        """Test memory monitoring"""
        from workers.monitoring import MemoryMonitor
        
        # Test with no limit
        monitor = MemoryMonitor()
        self.assertIsNone(monitor.max_rss_bytes)
        self.assertTrue(monitor.check_memory())
        
        # Test with limit
        monitor = MemoryMonitor('512M')
        self.assertEqual(monitor.max_rss_bytes, 512 * 1024 * 1024)
        
        # Test memory info
        info = monitor.get_memory_info()
        self.assertIn('rss_bytes', info)
        self.assertIn('rss_mb', info)
        self.assertIn('max_rss_bytes', info)
        self.assertIn('usage_percent', info)
        
        # RSS should be positive
        self.assertGreater(info['rss_bytes'], 0)
        self.assertGreater(info['rss_mb'], 0)
    
    def test_metrics_collector(self):
        """Test metrics collection"""
        from workers.monitoring import MetricsCollector
        
        collector = MetricsCollector('test-worker')
        
        # Test message processing recording
        collector.record_message_processed('test-queue', 1.5, success=True)
        collector.record_message_processed('test-queue', 2.0, success=False)
        collector.record_message_processed('other-queue', 1.0, success=True)
        
        # Test average calculations
        global_avg = collector.get_average_duration()
        self.assertAlmostEqual(global_avg, 1.5, places=1)  # (1.5 + 2.0 + 1.0) / 3
        
        queue_avg = collector.get_queue_average_duration('test-queue')
        self.assertAlmostEqual(queue_avg, 1.75, places=1)  # (1.5 + 2.0) / 2
        
        # Test wait timing
        collector.start_waiting()
        import time
        time.sleep(0.01)
        collector.stop_waiting()
        
        # Should have recorded some wait time
        self.assertGreater(collector.total_wait_time, 0)
    
    def test_observability_server(self):
        """Test observability server"""
        from workers.monitoring import ObservabilityServer, MetricsCollector
        
        collector = MetricsCollector('test-worker')
        
        # Test server creation (without starting)
        server = ObservabilityServer(0, collector, '/test-metrics')
        self.assertEqual(server.port, 0)
        self.assertEqual(server.metrics_path, '/test-metrics')
        self.assertEqual(server.metrics_collector, collector)
        
        # Test activity tracking
        initial_time = server.last_activity
        time.sleep(0.01)
        server.update_last_activity()
        self.assertGreater(server.last_activity, initial_time)
    
    def test_cli_argument_parsing(self):
        """Test CLI argument parsing"""
        import argparse
        
        parser = argparse.ArgumentParser()
        parser.add_argument('--database', '-d', required=True)
        parser.add_argument('--queue', '-q', required=True)
        parser.add_argument('--max-messages', type=int, default=0)
        parser.add_argument('--max-rss-memory', type=str, default=None)
        parser.add_argument('--worker-name', '-w', default=None)
        parser.add_argument('--log-level', default='INFO')
        parser.add_argument('--observability-port', type=int, default=0)
        parser.add_argument('--metrics-path', type=str, default='/metrics')
        
        # Test valid arguments
        args = parser.parse_args([
            '--database', 'test_db',
            '--queue', 'default',
            '--max-messages', '100',
            '--max-rss-memory', '512M',
            '--worker-name', 'test-worker',
            '--log-level', 'DEBUG',
            '--observability-port', '8080',
            '--metrics-path', '/custom-metrics'
        ])
        
        self.assertEqual(args.database, 'test_db')
        self.assertEqual(args.queue, 'default')
        self.assertEqual(args.max_messages, 100)
        self.assertEqual(args.max_rss_memory, '512M')
        self.assertEqual(args.worker_name, 'test-worker')
        self.assertEqual(args.log_level, 'DEBUG')
        self.assertEqual(args.observability_port, 8080)
        self.assertEqual(args.metrics_path, '/custom-metrics')
    
    @patch('workers.standalone.Registry')
    @patch('workers.standalone.api.Environment')
    def test_standalone_worker_init(self, mock_env, mock_registry):
        """Test standalone worker initialization"""
        from workers.standalone import StandaloneWorker
        
        # Mock environment
        mock_queue = MagicMock()
        mock_queue.name = 'test-queue'
        mock_queue.provider = 'pgsql'
        mock_env.return_value.__getitem__.return_value.search.return_value = [mock_queue]
        
        # Test worker creation
        worker = StandaloneWorker(
            database='test_db',
            queue_pattern='test.*',
            max_messages=100,
            max_rss_memory='512M',
            worker_name='test-worker',
            log_level='DEBUG',
            observability_port=8080,
            metrics_path='/metrics'
        )
        
        self.assertEqual(worker.database, 'test_db')
        self.assertEqual(worker.queue_pattern, 'test.*')
        self.assertEqual(worker.max_messages, 100)
        self.assertEqual(worker.max_rss_memory, '512M')
        self.assertEqual(worker.worker_name, 'test-worker')
        self.assertEqual(worker.processed_count, 0)
        self.assertFalse(worker.should_stop)
        
        # Test status
        status = worker.get_status()
        self.assertIn('worker_name', status)
        self.assertIn('database', status)
        self.assertIn('queue_pattern', status)
        self.assertIn('processed_count', status)
        
    def test_base_worker_methods(self):
        """Test base worker method structure"""
        from workers.base import BaseWorker
        
        worker = BaseWorker()
        
        # Test method existence
        self.assertTrue(hasattr(worker, 'get_message'))
        self.assertTrue(hasattr(worker, 'store_message'))
        self.assertTrue(hasattr(worker, 'terminate_message'))
        self.assertTrue(hasattr(worker, 'process_message'))
        self.assertTrue(hasattr(worker, 'change_message_visibility'))
        
        # Test logger
        self.assertIsNotNone(worker.logger)


def run_tests():
    """Run all tests"""
    print("Running Phase 1 Implementation Tests...")
    print("=" * 50)
    
    # Change to the correct directory
    os.chdir('/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue')
    
    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPhase1Implementation)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("✓ All Phase 1 tests passed!")
        return 0
    else:
        print("✗ Some tests failed")
        return 1


if __name__ == '__main__':
    sys.exit(run_tests())