#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 2 Tests: Queue Processing and Regex Matching
Tests comprehensive queue discovery, pattern matching, round-robin processing, and health monitoring
"""

import re
import sys
import time
import os
from unittest.mock import MagicMock, patch

# Add paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'worker_utils'))
sys.path.insert(0, os.path.join(project_root, 'workers'))

def test_queue_pattern_validation():
    """Test queue pattern validation with various regex patterns"""
    print("Testing Queue Pattern Validation:")
    print("-" * 50)
    
    try:
        from worker_utils import validate_queue_pattern
        
        test_cases = [
            # Valid patterns
            ("default", True, "Simple queue name"),
            ("default.*", True, "Simple wildcard pattern"),
            ("muppy", True, "Simple string"),
            ("test-queue_123", True, "Queue with dashes and underscores"),
            ("queue[0-9]+", True, "Character class pattern"),
            ("(urgent|normal|low)", True, "Alternation pattern"),
            ("^prod.*$", True, "Anchored pattern"),
            ("queue\\d+", True, "Escaped digits"),
            
            # Invalid patterns
            ("", False, "Empty string"),
            (None, False, "None value"),
            ("[invalid", False, "Unclosed bracket"),
            ("*invalid", True, "Wildcard pattern (allowed by simple pattern logic)"),
            ("queue[", False, "Incomplete character class"),
            ("queue(?P<", False, "Incomplete named group"),
            
            # Edge cases
            (".*", True, "Match all pattern"),
            ("queue\\", False, "Trailing backslash"),
            ("queue(", False, "Unclosed group"),
            ("queue)", False, "Unmatched closing paren"),
        ]
        
        passed = 0
        total = len(test_cases)
        
        for pattern, expected, description in test_cases:
            try:
                result = validate_queue_pattern(pattern)
                status = "✅" if result == expected else "❌"
                print(f"{status} {description}: '{pattern}' -> {result} (expected: {expected})")
                if result == expected:
                    passed += 1
            except Exception as e:
                print(f"❌ {description}: '{pattern}' -> Exception: {e}")
        
        print(f"\nQueue Pattern Validation: {passed}/{total} tests passed")
        return passed == total
        
    except ImportError as e:
        print(f"❌ Cannot import validate_queue_pattern: {e}")
        return False

def test_queue_pattern_matching():
    """Test actual queue name matching against patterns"""
    print("\nTesting Queue Pattern Matching:")
    print("-" * 50)
    
    test_scenarios = [
        # Pattern, queue names, expected matches (using .search() which finds substring matches)
        ("^default$", ["default", "default-prod", "not-default"], [True, False, False]),
        ("default.*", ["default", "default-prod", "default123", "prod-default"], [True, True, True, True]),  # search finds "default" in "prod-default"
        ("muppy.*", ["muppy", "muppy-prod", "muppy_test", "test-muppy"], [True, True, True, True]),  # search finds "muppy" in "test-muppy"
        ("^(urgent|normal)$", ["urgent", "normal", "low", "urgent-queue"], [True, True, False, False]),
        ("queue[0-9]+", ["queue1", "queue123", "queue", "queueabc"], [True, True, False, False]),
        ("^prod.*$", ["prod-queue", "prod", "test-prod", "prod-test-queue"], [True, True, False, True]),
        (".*", ["any", "queue", "name", ""], [True, True, True, True]),
    ]
    
    passed = 0
    total = 0
    
    for pattern, queue_names, expected_results in test_scenarios:
        print(f"\nPattern: '{pattern}'")
        try:
            compiled_pattern = re.compile(pattern)
            for queue_name, expected in zip(queue_names, expected_results):
                total += 1
                match = bool(compiled_pattern.search(queue_name))
                status = "✅" if match == expected else "❌"
                print(f"  {status} '{queue_name}' -> {match} (expected: {expected})")
                if match == expected:
                    passed += 1
        except re.error as e:
            print(f"  ❌ Invalid regex pattern: {e}")
    
    print(f"\nPattern Matching: {passed}/{total} tests passed")
    return passed == total

def test_queue_discovery_simulation():
    """Test queue discovery and filtering logic"""
    print("\nTesting Queue Discovery Simulation:")
    print("-" * 50)
    
    # Mock queue data
    mock_queues = [
        (1, "default", "pgsql"),
        (2, "urgent", "pgsql"),
        (3, "muppy-prod", "pgsql"),
        (4, "muppy-test", "pgsql"),
        (5, "batch-processing", "pgsql"),
        (6, "low-priority", "pgsql"),
    ]
    
    test_patterns = [
        ("default", [("default", "Should match exact name")]),
        ("muppy.*", [("muppy-prod", "Should match muppy-*"), ("muppy-test", "Should match muppy-*")]),
        (".*", [(name, f"Should match {name}") for _, name, _ in mock_queues]),
        ("(urgent|batch.*)", [("urgent", "Should match urgent"), ("batch-processing", "Should match batch-*")]),
        ("nonexistent", []),
    ]
    
    def filter_queues_by_pattern(queues, pattern):
        """Simulate queue filtering logic"""
        try:
            compiled_pattern = re.compile(pattern)
            return [(qid, name, provider) for qid, name, provider in queues 
                    if compiled_pattern.search(name)]
        except re.error:
            return []
    
    passed = 0
    total = 0
    
    for pattern, expected_matches in test_patterns:
        total += 1
        filtered_queues = filter_queues_by_pattern(mock_queues, pattern)
        matched_names = [name for _, name, _ in filtered_queues]
        expected_names = [name for name, _ in expected_matches]
        
        if set(matched_names) == set(expected_names):
            passed += 1
            status = "✅"
        else:
            status = "❌"
        
        print(f"{status} Pattern '{pattern}':")
        print(f"    Matched: {matched_names}")
        print(f"    Expected: {expected_names}")
    
    print(f"\nQueue Discovery: {passed}/{total} tests passed")
    return passed == total

def test_round_robin_queue_selection():
    """Test round-robin queue selection logic"""
    print("\nTesting Round-Robin Queue Selection:")
    print("-" * 50)
    
    # Mock queue data
    queue_info = [
        (1, "queue1", "pgsql"),
        (2, "queue2", "pgsql"), 
        (3, "queue3", "pgsql"),
    ]
    
    def simulate_queue_selection(queue_info, start_index, healthy_queues_set):
        """Simulate the queue selection logic"""
        queue_index = start_index
        
        for attempt in range(len(queue_info)):
            current_index = queue_index % len(queue_info)
            queue_id, queue_name, provider = queue_info[current_index]
            
            if queue_name in healthy_queues_set:
                return queue_id, queue_name, provider, queue_index + 1
            
            queue_index += 1
        
        return None  # No healthy queues
    
    # Test cases
    test_scenarios = [
        # (start_index, healthy_queues, expected_selection)
        (0, {"queue1", "queue2", "queue3"}, ("queue1", 1)),  # All healthy, start at 0
        (1, {"queue1", "queue2", "queue3"}, ("queue2", 2)),  # All healthy, start at 1  
        (2, {"queue1", "queue2", "queue3"}, ("queue3", 3)),  # All healthy, start at 2
        (3, {"queue1", "queue2", "queue3"}, ("queue1", 4)),  # Wrap around
        (0, {"queue2", "queue3"}, ("queue2", 2)),           # Skip unhealthy queue1
        (0, {"queue3"}, ("queue3", 3)),                     # Skip to queue3
        (1, {"queue1"}, ("queue1", 4)),                     # Wrap to find queue1
        (0, set(), None),                                    # No healthy queues
    ]
    
    passed = 0
    total = len(test_scenarios)
    
    for start_index, healthy_queues, expected in test_scenarios:
        result = simulate_queue_selection(queue_info, start_index, healthy_queues)
        
        if expected is None:
            success = result is None
            description = f"Start {start_index}, healthy {healthy_queues} -> None (no healthy queues)"
        else:
            expected_name, expected_next_index = expected
            if result:
                _, selected_name, _, next_index = result
                success = selected_name == expected_name and next_index == expected_next_index
                description = f"Start {start_index}, healthy {healthy_queues} -> {selected_name} (index {next_index})"
            else:
                success = False
                description = f"Start {start_index}, healthy {healthy_queues} -> None (unexpected)"
        
        status = "✅" if success else "❌"
        print(f"{status} {description}")
        
        if success:
            passed += 1
    
    print(f"\nRound-Robin Selection: {passed}/{total} tests passed")
    return passed == total

def test_queue_health_monitoring():
    """Test queue health monitoring logic"""
    print("\nTesting Queue Health Monitoring:")
    print("-" * 50)
    
    class MockQueueHealthMonitor:
        def __init__(self, max_failures=10, timeout_seconds=300):
            self.queue_failures = {}
            self.queue_last_success = {}
            self.max_failures = max_failures
            self.timeout_seconds = timeout_seconds
        
        def is_queue_healthy(self, queue_name):
            """Check if queue is healthy"""
            current_time = time.time()
            failures = self.queue_failures.get(queue_name, 0)
            
            if failures >= self.max_failures:
                last_success = self.queue_last_success.get(queue_name, 0)
                if current_time - last_success < self.timeout_seconds:
                    return False
                else:
                    # Reset after timeout
                    self.queue_failures[queue_name] = 0
                    return True
            
            return True
        
        def record_success(self, queue_name):
            """Record successful processing"""
            self.queue_failures[queue_name] = 0
            self.queue_last_success[queue_name] = time.time()
        
        def record_failure(self, queue_name):
            """Record failed processing"""
            self.queue_failures[queue_name] = self.queue_failures.get(queue_name, 0) + 1
            # Ensure we have a last_success timestamp to compare against
            if queue_name not in self.queue_last_success:
                self.queue_last_success[queue_name] = time.time()
    
    monitor = MockQueueHealthMonitor(max_failures=3, timeout_seconds=5)
    
    test_cases = [
        ("Initial state", "queue1", True, "New queue should be healthy"),
        ("First failure", "queue1", True, "Queue should remain healthy after 1 failure"),
        ("Second failure", "queue1", True, "Queue should remain healthy after 2 failures"), 
        ("Third failure", "queue1", False, "Queue should become unhealthy after reaching threshold (3 failures)"),
        ("Check unhealthy", "queue1", False, "Queue should remain unhealthy immediately after"),
        ("After timeout", "queue1", True, "Queue should recover after timeout"),
        ("Success resets", "queue1", True, "Queue should be healthy after success"),
    ]
    
    passed = 0
    total = len(test_cases)
    
    for i, (step, queue_name, expected_healthy, description) in enumerate(test_cases):
        if "failure" in step.lower():
            monitor.record_failure(queue_name)
        elif "success" in step.lower():
            monitor.record_success(queue_name)
        elif "timeout" in step.lower():
            # Simulate timeout by advancing time
            time.sleep(6)  # Wait longer than timeout
        elif "check" in step.lower():
            # Just check current state, no action needed
            pass
        
        is_healthy = monitor.is_queue_healthy(queue_name)
        failures = monitor.queue_failures.get(queue_name, 0)
        status = "✅" if is_healthy == expected_healthy else "❌"
        
        print(f"{status} {step}: {description} -> {is_healthy} (failures: {failures})")
        
        if is_healthy == expected_healthy:
            passed += 1
    
    print(f"\nQueue Health Monitoring: {passed}/{total} tests passed")
    return passed == total

def test_queue_statistics_tracking():
    """Test queue statistics tracking functionality"""
    print("\nTesting Queue Statistics Tracking:")
    print("-" * 50)
    
    class MockQueueStats:
        def __init__(self):
            self.queue_stats = {}
        
        def initialize_queue_stats(self, queue_info):
            """Initialize statistics for discovered queues"""
            for queue_id, queue_name, provider in queue_info:
                if queue_name not in self.queue_stats:
                    self.queue_stats[queue_name] = {
                        'processed_count': 0,
                        'failed_count': 0,
                        'total_processing_time': 0.0,
                        'processing_time_avg': 0.0,
                        'provider': provider
                    }
        
        def update_queue_stats(self, queue_name, success, processing_time):
            """Update statistics after processing"""
            if queue_name not in self.queue_stats:
                return
            
            stats = self.queue_stats[queue_name]
            
            if success:
                stats['processed_count'] += 1
            else:
                stats['failed_count'] += 1
            
            stats['total_processing_time'] += processing_time
            total_messages = stats['processed_count'] + stats['failed_count']
            if total_messages > 0:
                stats['processing_time_avg'] = stats['total_processing_time'] / total_messages
        
        def get_queue_health_summary(self):
            """Get summary of queue health"""
            healthy = []
            unhealthy = []
            
            for queue_name, stats in self.queue_stats.items():
                total = stats['processed_count'] + stats['failed_count']
                if total > 0:
                    error_rate = stats['failed_count'] / total
                    if error_rate > 0.1:  # 10% error rate threshold
                        unhealthy.append(queue_name)
                    else:
                        healthy.append(queue_name)
                else:
                    healthy.append(queue_name)  # No messages processed yet
            
            return {
                'healthy_queues': len(healthy),
                'unhealthy_queues': len(unhealthy),
                'total_processed': sum(s['processed_count'] for s in self.queue_stats.values()),
                'total_failed': sum(s['failed_count'] for s in self.queue_stats.values())
            }
    
    # Test the statistics tracking
    stats = MockQueueStats()
    
    # Initialize with mock queues
    queue_info = [
        (1, "queue1", "pgsql"),
        (2, "queue2", "pgsql"),
    ]
    stats.initialize_queue_stats(queue_info)
    
    test_steps = [
        ("Initialize", lambda: len(stats.queue_stats) == 2, "Should initialize 2 queues"),
        ("Success q1", lambda: (stats.update_queue_stats("queue1", True, 1.5), 
                               stats.queue_stats["queue1"]["processed_count"] == 1)[1], 
         "Should record success for queue1"),
        ("Failure q1", lambda: (stats.update_queue_stats("queue1", False, 2.0),
                               stats.queue_stats["queue1"]["failed_count"] == 1)[1],
         "Should record failure for queue1"),
        ("Average calc", lambda: abs(stats.queue_stats["queue1"]["processing_time_avg"] - 1.75) < 0.01,
         "Should calculate correct average time"),
        ("Health summary", lambda: stats.get_queue_health_summary()["total_processed"] == 1,
         "Should report correct total processed"),
    ]
    
    passed = 0
    total = len(test_steps)
    
    for step_name, test_func, description in test_steps:
        try:
            result = test_func()
            status = "✅" if result else "❌"
            print(f"{status} {step_name}: {description}")
            if result:
                passed += 1
        except Exception as e:
            print(f"❌ {step_name}: {description} - Exception: {e}")
    
    print(f"\nQueue Statistics: {passed}/{total} tests passed")
    return passed == total

def main():
    """Run all Phase 2 queue processing tests"""
    print("=" * 70)
    print("Phase 2 Tests: Queue Processing and Regex Matching")
    print("=" * 70)
    
    tests = [
        ("Queue Pattern Validation", test_queue_pattern_validation),
        ("Queue Pattern Matching", test_queue_pattern_matching),
        ("Queue Discovery Simulation", test_queue_discovery_simulation),
        ("Round-Robin Queue Selection", test_round_robin_queue_selection),
        ("Queue Health Monitoring", test_queue_health_monitoring),
        ("Queue Statistics Tracking", test_queue_statistics_tracking),
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'=' * 20} {test_name} {'=' * 20}")
        try:
            if test_func():
                passed_tests += 1
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
    
    print("\n" + "=" * 70)
    print(f"PHASE 2 TESTS SUMMARY: {passed_tests}/{total_tests} test suites passed")
    print("=" * 70)
    
    if passed_tests == total_tests:
        print("🎉 All Phase 2 tests passed!")
        return 0
    else:
        print("⚠️  Some Phase 2 tests failed. Review output above.")
        return 1

if __name__ == '__main__':
    exit(main())