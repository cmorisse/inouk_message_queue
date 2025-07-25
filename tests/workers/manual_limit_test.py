#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Manual Resource Limit Test for IMQ Workers

Simple script to manually test resource limits one at a time.
"""

import os
import sys
import time
import subprocess
import argparse


def test_message_count_limit(database, limit=3):
    """Test message count limit"""
    print(f"\n{'='*60}")
    print(f"Testing Message Count Limit: {limit}")
    print(f"{'='*60}")
    
    # Create test messages
    print(f"\n1. Creating {limit + 2} test messages...")
    for i in range(limit + 2):
        cmd = f'''echo 'env["imq.test_launcher"].create({{"name": "Count Test {i+1}"}}).launch_simple("Message count test {i+1}")' | bin/start_odoo shell --database {database}'''
        subprocess.run(cmd, shell=True, capture_output=True)
    print(f"   ✅ Created {limit + 2} messages")
    
    # Start worker with limit
    print(f"\n2. Starting worker with --max-messages={limit}")
    print(f"   Command: bin/start_odoo imqworker --database {database} --queue default --max-messages {limit}")
    print("\n" + "-"*60)
    
    process = subprocess.Popen([
        'bin/start_odoo', 'imqworker',
        '--database', database,
        '--queue', 'default',
        '--max-messages', str(limit),
        '--worker-name', 'test-message-limit'
    ])
    
    # Wait for completion
    process.wait()
    
    print("-"*60)
    print(f"\n3. Worker exited with code: {process.returncode}")
    
    if process.returncode == 0:
        print("   ✅ SUCCESS: Worker exited cleanly after message limit")
    else:
        print("   ❌ FAILED: Worker exited with error")
        

def test_memory_limit(database, limit="50M"):
    """Test memory limit"""
    print(f"\n{'='*60}")
    print(f"Testing Memory Limit: {limit}")
    print(f"{'='*60}")
    
    # Create test message that allocates memory
    print("\n1. Creating memory-intensive test message...")
    cmd = f'''echo '
# Create a message that will allocate memory
launcher = env["imq.test_launcher"].create({{"name": "Memory Test"}})
result = launcher.launch("TestMessage", {{"allocate_mb": 100, "test": "memory"}})
print(f"Created message ID: {{result.get(\\"id\\")}}")
' | bin/start_odoo shell --database {database}'''
    subprocess.run(cmd, shell=True)
    
    # Start worker with memory limit
    print(f"\n2. Starting worker with --max-rss-memory={limit}")
    print(f"   Command: bin/start_odoo imqworker --database {database} --queue default --max-rss-memory {limit}")
    print("\n" + "-"*60)
    
    process = subprocess.Popen([
        'bin/start_odoo', 'imqworker',
        '--database', database,
        '--queue', 'default',
        '--max-rss-memory', limit,
        '--worker-name', 'test-memory-limit',
        '--log-level', 'INFO'
    ])
    
    # Monitor for a bit
    print("   Monitoring worker (press Ctrl+C to stop)...")
    try:
        process.wait()
    except KeyboardInterrupt:
        process.terminate()
        
    print("-"*60)
    print(f"\n3. Worker exited with code: {process.returncode}")
    

def test_graceful_shutdown(database):
    """Test graceful shutdown"""
    print(f"\n{'='*60}")
    print("Testing Graceful Shutdown (SIGTERM)")
    print(f"{'='*60}")
    
    # Create slow message
    print("\n1. Creating slow-processing test message (10 seconds)...")
    cmd = f'''echo '
launcher = env["imq.test_launcher"].create({{"name": "Slow Test"}})
result = launcher.launch("TestMessage", {{"duration": 10, "test": "slow"}})
print(f"Created message ID: {{result.get(\\"id\\")}}")
' | bin/start_odoo shell --database {database}'''
    subprocess.run(cmd, shell=True)
    
    # Start worker
    print("\n2. Starting worker...")
    process = subprocess.Popen([
        'bin/start_odoo', 'imqworker',
        '--database', database,
        '--queue', 'default',
        '--worker-name', 'test-graceful-shutdown',
        '--log-level', 'INFO'
    ])
    
    # Wait a bit for processing to start
    print("   Waiting 3 seconds for processing to start...")
    time.sleep(3)
    
    # Send SIGTERM
    print("\n3. Sending SIGTERM signal...")
    process.terminate()
    
    # Wait for graceful shutdown
    print("   Waiting for graceful shutdown...")
    start_time = time.time()
    process.wait()
    shutdown_time = time.time() - start_time
    
    print(f"\n4. Worker shutdown completed in {shutdown_time:.2f} seconds")
    print(f"   Exit code: {process.returncode}")
    
    if process.returncode == 0:
        print("   ✅ SUCCESS: Worker performed graceful shutdown")
    else:
        print("   ❌ FAILED: Worker did not shutdown gracefully")


def main():
    parser = argparse.ArgumentParser(description='Manual IMQ Worker Resource Limit Tests')
    parser.add_argument('--database', '-d', required=True, help='Database name')
    parser.add_argument('--test', '-t', required=True, 
                       choices=['message', 'memory', 'shutdown'],
                       help='Test to run: message (count limit), memory (RSS limit), shutdown (SIGTERM)')
    parser.add_argument('--limit', '-l', help='Limit value (e.g., 5 for messages, 100M for memory)')
    
    args = parser.parse_args()
    
    if args.test == 'message':
        limit = int(args.limit) if args.limit else 3
        test_message_count_limit(args.database, limit)
    elif args.test == 'memory':
        limit = args.limit or "50M"
        test_memory_limit(args.database, limit)
    elif args.test == 'shutdown':
        test_graceful_shutdown(args.database)
        

if __name__ == '__main__':
    main()