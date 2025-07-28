#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for worker stop parameter functionality
"""

import os
import sys
import socket

# Add paths for imports
sys.path.insert(0, '/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/workers')

def test_parameter_parsing():
    """Test the parsing logic for stop parameters"""
    print("Testing Stop Parameter Parsing:")
    print("-" * 40)
    
    test_cases = [
        ("", []),
        ("server1", ["server1"]),
        ("server1,server2", ["server1", "server2"]),
        ("server1, server2, server3", ["server1", "server2", "server3"]),
        ("*", ["*"]),
        ("server1,*,server2", ["server1", "*", "server2"]),
        (",server1,,server2,", ["server1", "server2"]),  # Test empty string handling
    ]
    
    for input_val, expected in test_cases:
        # Simulate the parameter parsing logic
        stopped_workers_nodes = input_val.split(',')
        stopped_workers_nodes = [node.strip() for node in stopped_workers_nodes if node.strip()]
        
        status = "✅" if stopped_workers_nodes == expected else "❌"
        print(f"{status} parse('{input_val}') = {stopped_workers_nodes} (expected: {expected})")

def test_hostname_matching():
    """Test hostname matching logic"""
    print("\nTesting Hostname Matching:")
    print("-" * 40)
    
    current_hostname = socket.gethostname()
    print(f"Current hostname: {current_hostname}")
    
    test_cases = [
        ([], False, "Empty list"),
        ([current_hostname], True, "Exact hostname match"),
        (["other-server"], False, "Different hostname"),
        (["*"], True, "Wildcard match"),
        ([current_hostname, "other-server"], True, "Hostname in list"),
        (["other-server", "*"], True, "Wildcard in list"),
        (["server1", "server2"], False, "No match in list"),
    ]
    
    for nodes, expected, description in test_cases:
        # Simulate the matching logic
        should_stop = current_hostname in nodes or '*' in nodes
        
        status = "✅" if should_stop == expected else "❌"
        print(f"{status} {description}: nodes={nodes} -> should_stop={should_stop}")

def test_implementation_verification():
    """Verify the implementation is in place"""
    print("\nVerifying Implementation:")
    print("-" * 40)
    
    files_to_check = [
        ("/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/data/ir_config_parameter.xml", [
            "imq.STOP_CRON_WORKERS_disabled",
            "imq.STOP_STANDALONE_WORKERS_disabled"
        ]),
        ("/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/models/worker.py", [
            "imq.STOP_CRON_WORKERS",
            "imq.STOP_WORKERS"  # Backward compatibility
        ]),
        ("/opt/muppy/appserver-mpy13c/parts/inouk_addons/inouk_message_queue/workers/standalone.py", [
            "imq.STOP_STANDALONE_WORKERS",
            "_check_stop_parameter",
            "socket.gethostname()"
        ])
    ]
    
    for file_path, search_terms in files_to_check:
        print(f"\nChecking {file_path}:")
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            for term in search_terms:
                if term in content:
                    print(f"  ✅ Found: {term}")
                else:
                    print(f"  ❌ Missing: {term}")
                    
        except Exception as e:
            print(f"  ❌ Error reading file: {e}")

def test_system_parameter_examples():
    """Show example system parameter configurations"""
    print("\nSystem Parameter Configuration Examples:")
    print("-" * 40)
    
    examples = [
        ("Stop all cron workers", "imq.STOP_CRON_WORKERS", "*"),
        ("Stop cron workers on specific server", "imq.STOP_CRON_WORKERS", "prod-server-01.internal"),
        ("Stop cron workers on multiple servers", "imq.STOP_CRON_WORKERS", "server1.local,server2.local"),
        ("Stop all standalone workers", "imq.STOP_STANDALONE_WORKERS", "*"),
        ("Stop standalone workers on current server", "imq.STOP_STANDALONE_WORKERS", socket.gethostname()),
    ]
    
    for description, parameter, value in examples:
        print(f"\n{description}:")
        print(f"  Parameter: {parameter}")
        print(f"  Value: {value}")

def main():
    print("=" * 60)
    print("IMQ Worker Stop Parameter Implementation Test")
    print("=" * 60)
    
    test_parameter_parsing()
    test_hostname_matching()
    test_implementation_verification()
    test_system_parameter_examples()
    
    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60)
    print("\nTo activate stopping:")
    print("1. Go to Odoo Settings > Technical > Parameters > System Parameters")
    print("2. Create or edit parameter:")
    print("   - Key: imq.STOP_CRON_WORKERS or imq.STOP_STANDALONE_WORKERS")
    print("   - Value: hostname or * for all workers")
    print("3. Workers will check parameter and stop gracefully")

if __name__ == '__main__':
    main()