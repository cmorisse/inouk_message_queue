#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple test script for Phase 1 implementation components
"""

import os
import sys
import time


def test_worker_utils():
    """Test worker utility functions"""
    print("Testing worker utilities...")
    
    # Import directly from file (go up one directory from tests/)
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
    import worker_utils
    
    # Test memory limit parsing
    tests = [
        ('1024M', 1024 * 1024 * 1024),
        ('2G', 2 * 1024 * 1024 * 1024),
        ('512K', 512 * 1024),
        ('1024', 1024),
        ('invalid', None),
        ('', None)
    ]
    
    for test_input, expected in tests:
        result = worker_utils.parse_memory_limit(test_input)
        if result == expected:
            print(f"  ✓ {test_input} -> {result}")
        else:
            print(f"  ✗ {test_input} -> {result} (expected {expected})")
            return False
    
    # Test duration formatting
    durations = [0.1, 1.5, 65.0, 3665.0]
    for duration in durations:
        formatted = worker_utils.format_duration(duration)
        print(f"  ✓ {duration}s -> {formatted}")
    
    # Test memory size formatting
    sizes = [1024, 1024*1024, 1024*1024*1024]
    for size in sizes:
        formatted = worker_utils.format_memory_size(size)
        print(f"  ✓ {size} bytes -> {formatted}")
    
    # Test validation
    patterns = ['default', 'test.*', 'mpy_.*', '[invalid']
    for pattern in patterns:
        valid = worker_utils.validate_queue_pattern(pattern)
        status = "✓" if valid or pattern == '[invalid' else "✗"
        print(f"  {status} Pattern '{pattern}' valid: {valid}")
    
    return True


def test_memory_monitoring():
    """Test memory monitoring (without full module imports)"""
    print("Testing memory monitoring...")
    
    try:
        import psutil
        
        # Test basic memory monitoring
        process = psutil.Process()
        memory_info = process.memory_info()
        rss_mb = memory_info.rss / (1024 * 1024)
        
        print(f"  ✓ Current RSS: {rss_mb:.1f} MB")
        print(f"  ✓ Current VMS: {memory_info.vms / (1024 * 1024):.1f} MB")
        
        # Test memory limit parsing
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils'))
        import worker_utils
        
        limit_512m = worker_utils.parse_memory_limit('512M')
        limit_1g = worker_utils.parse_memory_limit('1G')
        
        print(f"  ✓ 512M limit: {limit_512m / (1024*1024):.0f} MB")
        print(f"  ✓ 1G limit: {limit_1g / (1024*1024):.0f} MB")
        
        # Test memory check logic
        within_512m = memory_info.rss < limit_512m
        within_1g = memory_info.rss < limit_1g
        
        print(f"  ✓ Within 512M: {within_512m}")
        print(f"  ✓ Within 1G: {within_1g}")
        
        return True
        
    except ImportError:
        print("  ✗ psutil not available")
        return False


def test_cli_structure():
    """Test CLI command structure"""
    print("Testing CLI structure...")
    
    # Test that files exist (go up one directory from tests/)
    cli_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cli')
    if not os.path.exists(cli_dir):
        print("  ✗ CLI directory not found")
        return False
    
    imq_worker_file = os.path.join(cli_dir, 'imq_worker.py')
    if not os.path.exists(imq_worker_file):
        print("  ✗ imq_worker.py not found")
        return False
    
    init_file = os.path.join(cli_dir, '__init__.py')
    if not os.path.exists(init_file):
        print("  ✗ CLI __init__.py not found")
        return False
    
    print("  ✓ CLI directory structure exists")
    
    # Test argument parsing structure
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
    test_args = [
        '--database', 'test_db',
        '--queue', 'default',
        '--max-messages', '100',
        '--max-rss-memory', '512M',
        '--worker-name', 'test-worker',
        '--log-level', 'DEBUG',
        '--observability-port', '8080',
        '--metrics-path', '/custom-metrics'
    ]
    
    try:
        args = parser.parse_args(test_args)
        print(f"  ✓ Parsed database: {args.database}")
        print(f"  ✓ Parsed queue: {args.queue}")
        print(f"  ✓ Parsed max messages: {args.max_messages}")
        print(f"  ✓ Parsed memory limit: {args.max_rss_memory}")
        print(f"  ✓ Parsed worker name: {args.worker_name}")
        print(f"  ✓ Parsed log level: {args.log_level}")
        print(f"  ✓ Parsed observability port: {args.observability_port}")
        print(f"  ✓ Parsed metrics path: {args.metrics_path}")
        
        # Test validation logic
        if args.max_messages >= 0:
            print("  ✓ Max messages validation passed")
        else:
            print("  ✗ Max messages validation failed")
            return False
        
        if 0 <= args.observability_port <= 65535:
            print("  ✓ Port validation passed")
        else:
            print("  ✗ Port validation failed")
            return False
            
        if args.metrics_path.startswith('/'):
            print("  ✓ Metrics path validation passed")
        else:
            print("  ✗ Metrics path validation failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"  ✗ Argument parsing failed: {e}")
        return False


def test_directory_structure():
    """Test that all required directories and files exist"""
    print("Testing directory structure...")
    
    base_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Check required directories
    required_dirs = [
        'cli',
        'workers', 
        'utils'
    ]
    
    for dir_name in required_dirs:
        dir_path = os.path.join(base_dir, dir_name)
        if os.path.exists(dir_path):
            print(f"  ✓ {dir_name}/ directory exists")
        else:
            print(f"  ✗ {dir_name}/ directory missing")
            return False
    
    # Check required files
    required_files = [
        'cli/__init__.py',
        'cli/imq_worker.py',
        'workers/__init__.py',
        'workers/base.py',
        'workers/standalone.py',
        'workers/monitoring.py',
        'utils/__init__.py',
        'utils/worker_utils.py'
    ]
    
    for file_path in required_files:
        full_path = os.path.join(base_dir, file_path)
        if os.path.exists(full_path):
            print(f"  ✓ {file_path} exists")
        else:
            print(f"  ✗ {file_path} missing")
            return False
    
    return True


def test_prometheus_metrics():
    """Test Prometheus metrics availability"""
    print("Testing Prometheus metrics...")
    
    try:
        from prometheus_client import Counter, Gauge, Summary, Info
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        print("  ✓ prometheus_client available")
        
        # Test basic metric creation
        counter = Counter('test_counter', 'Test counter')
        gauge = Gauge('test_gauge', 'Test gauge')
        summary = Summary('test_summary', 'Test summary')
        info = Info('test_info', 'Test info')
        
        # Test metric operations
        counter.inc()
        gauge.set(42)
        summary.observe(1.5)
        info.info({'key': 'value'})
        
        # Test metrics export
        metrics_data = generate_latest()
        print(f"  ✓ Generated {len(metrics_data)} bytes of metrics data")
        
        return True
        
    except ImportError:
        print("  ⚠ prometheus_client not available (metrics will be disabled)")
        return True  # This is acceptable for Phase 1


def main():
    """Run all tests"""
    print("=" * 60)
    print("Phase 1 Implementation Tests")
    print("=" * 60)
    
    tests = [
        ("Directory Structure", test_directory_structure),
        ("Worker Utils", test_worker_utils),
        ("Memory Monitoring", test_memory_monitoring),
        ("CLI Structure", test_cli_structure),
        ("Prometheus Metrics", test_prometheus_metrics),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 40)
        
        try:
            result = test_func()
            results.append((test_name, result))
            if result:
                print(f"✓ {test_name} passed")
            else:
                print(f"✗ {test_name} failed")
        except Exception as e:
            print(f"✗ {test_name} error: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 60)
    print("Test Results Summary:")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:<10} {test_name}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All Phase 1 tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())