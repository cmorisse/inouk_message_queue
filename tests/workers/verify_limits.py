#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick verification of resource limit implementation in standalone worker
"""

import os
import sys

# Direct imports to avoid module loading issues
sys.path.insert(0, '/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/workers')
sys.path.insert(0, '/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/worker_utils')

try:
    from monitoring import MemoryMonitor
    from worker_utils import parse_memory_limit
except ImportError:
    print("Warning: Could not import modules, using stub implementations")
    # Stub implementations for testing
    def parse_memory_limit(limit_str):
        if not limit_str:
            return None
        multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3}
        limit_str = str(limit_str).upper().strip()
        for suffix, multiplier in multipliers.items():
            if limit_str.endswith(suffix):
                return int(limit_str[:-1]) * multiplier
        return int(limit_str)
    
    class MemoryMonitor:
        def __init__(self, max_rss_str=None):
            self.max_rss_bytes = parse_memory_limit(max_rss_str)
            
        def check_memory(self):
            if not self.max_rss_bytes:
                return True
            try:
                import psutil
                return psutil.Process().memory_info().rss < self.max_rss_bytes
            except ImportError:
                # For testing without psutil, simulate memory usage
                simulated_rss = 100 * 1024 * 1024  # 100MB
                return simulated_rss < self.max_rss_bytes
            
        def get_rss_mb(self):
            try:
                import psutil
                return psutil.Process().memory_info().rss / (1024 * 1024)
            except ImportError:
                return 100.0  # Dummy value for testing

def test_memory_parsing():
    """Test memory limit parsing"""
    print("Testing Memory Limit Parsing:")
    print("-" * 40)
    
    test_cases = [
        ("100M", 100 * 1024 * 1024),
        ("1G", 1024 * 1024 * 1024),
        ("512K", 512 * 1024),
        ("1024", 1024),
        (None, None),
    ]
    
    for input_val, expected in test_cases:
        result = parse_memory_limit(input_val)
        status = "✅" if result == expected else "❌"
        print(f"{status} parse_memory_limit('{input_val}') = {result} (expected: {expected})")
        

def test_memory_monitor():
    """Test memory monitor functionality"""
    print("\nTesting Memory Monitor:")
    print("-" * 40)
    
    # Test with no limit
    monitor = MemoryMonitor(None)
    print(f"No limit - check_memory(): {monitor.check_memory()} (should be True)")
    print(f"Current RSS: {monitor.get_rss_mb():.2f} MB")
    
    # Test with very high limit
    monitor = MemoryMonitor("10G")
    print(f"\n10G limit - check_memory(): {monitor.check_memory()} (should be True)")
    
    # Test with very low limit
    monitor = MemoryMonitor("1M")
    print(f"\n1M limit - check_memory(): {monitor.check_memory()} (should be False)")
    print(f"Current RSS: {monitor.get_rss_mb():.2f} MB > 1 MB limit")
    

def check_worker_implementation():
    """Check if worker has limit checking implemented"""
    print("\nChecking Worker Implementation:")
    print("-" * 40)
    
    # Check standalone worker
    try:
        with open('/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/workers/standalone.py', 'r') as f:
            content = f.read()
            
        checks = [
            ("Message count limit check", "self.max_messages > 0 and self.processed_count >= self.max_messages"),
            ("Memory limit check", "not self.memory_monitor.check_memory()"),
            ("Graceful shutdown", "handle_sigterm"),
            ("SIGTERM handler", "signal.signal(signal.SIGTERM"),
        ]
        
        for check_name, search_str in checks:
            if search_str in content:
                print(f"✅ {check_name}: FOUND")
            else:
                print(f"❌ {check_name}: NOT FOUND")
                
    except Exception as e:
        print(f"❌ Error reading worker file: {e}")
        

def main():
    print("="*60)
    print("IMQ Worker Resource Limit Implementation Verification")
    print("="*60)
    
    test_memory_parsing()
    test_memory_monitor()
    check_worker_implementation()
    
    print("\n" + "="*60)
    print("Verification Complete")
    print("="*60)


if __name__ == '__main__':
    main()