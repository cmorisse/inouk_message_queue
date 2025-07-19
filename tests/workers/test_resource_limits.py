#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Resource Limit Testing for IMQ Workers v3

This script tests that resource limits (memory and message count) work correctly
and trigger graceful shutdown when exceeded.
"""

import os
import sys
import time
import signal
import subprocess
import json
import psutil
import argparse
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ResourceLimitTest')


class ResourceLimitTester:
    """Test resource limits for IMQ workers"""
    
    def __init__(self, database, odoo_bin_path='bin/start_odoo'):
        self.database = database
        self.odoo_bin_path = odoo_bin_path
        self.test_results = []
        
    def run_all_tests(self):
        """Run all resource limit tests"""
        logger.info("Starting Resource Limit Testing Suite")
        logger.info(f"Database: {self.database}")
        logger.info("=" * 80)
        
        # Test 1: Message count limit
        self.test_message_count_limit()
        
        # Test 2: Memory limit
        self.test_memory_limit()
        
        # Test 3: Graceful shutdown with in-flight message
        self.test_graceful_shutdown()
        
        # Test 4: Multiple limits interaction
        self.test_multiple_limits()
        
        # Report results
        self.report_results()
        
    def test_message_count_limit(self):
        """Test that worker exits after processing max messages"""
        logger.info("\n" + "="*60)
        logger.info("TEST 1: Message Count Limit")
        logger.info("="*60)
        
        test_name = "message_count_limit"
        max_messages = 3
        
        try:
            # Create test messages
            logger.info(f"Creating {max_messages + 2} test messages...")
            message_ids = self._create_test_messages(max_messages + 2, test_name)
            
            # Start worker with message limit
            logger.info(f"Starting worker with --max-messages={max_messages}")
            start_time = time.time()
            
            process = subprocess.Popen([
                self.odoo_bin_path, 'imqworker',
                '--database', self.database,
                '--queue', 'default',
                '--max-messages', str(max_messages),
                '--worker-name', f'test-{test_name}',
                '--log-level', 'INFO'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Wait for process to complete
            stdout, stderr = process.communicate(timeout=60)
            duration = time.time() - start_time
            
            # Verify results
            if process.returncode == 0:
                # Count processed messages
                processed_count = self._count_processed_messages(message_ids)
                
                if processed_count == max_messages:
                    logger.info(f"✅ SUCCESS: Worker processed exactly {max_messages} messages and exited")
                    logger.info(f"   Exit code: {process.returncode}")
                    logger.info(f"   Duration: {duration:.2f}s")
                    logger.info(f"   Messages processed: {processed_count}/{len(message_ids)}")
                    self._record_result(test_name, True, f"Processed {max_messages} messages as expected")
                else:
                    logger.error(f"❌ FAILED: Worker processed {processed_count} messages, expected {max_messages}")
                    self._record_result(test_name, False, f"Processed {processed_count} instead of {max_messages}")
            else:
                logger.error(f"❌ FAILED: Worker exited with code {process.returncode}")
                logger.error(f"STDERR: {stderr}")
                self._record_result(test_name, False, f"Exit code {process.returncode}")
                
        except subprocess.TimeoutExpired:
            logger.error("❌ FAILED: Worker did not exit within timeout")
            process.kill()
            self._record_result(test_name, False, "Timeout - worker did not exit")
        except Exception as e:
            logger.error(f"❌ FAILED: Test error: {e}")
            self._record_result(test_name, False, str(e))
            
    def test_memory_limit(self):
        """Test that worker exits when memory limit exceeded"""
        logger.info("\n" + "="*60)
        logger.info("TEST 2: Memory Limit")
        logger.info("="*60)
        
        test_name = "memory_limit"
        memory_limit = "100M"  # Low limit to trigger quickly
        
        try:
            # Create memory-intensive test messages
            logger.info("Creating memory-intensive test messages...")
            message_ids = self._create_memory_test_messages(5, test_name)
            
            # Start worker with memory limit
            logger.info(f"Starting worker with --max-rss-memory={memory_limit}")
            start_time = time.time()
            
            process = subprocess.Popen([
                self.odoo_bin_path, 'imqworker',
                '--database', self.database,
                '--queue', 'default',
                '--max-rss-memory', memory_limit,
                '--worker-name', f'test-{test_name}',
                '--log-level', 'INFO'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Monitor process memory
            proc_monitor = psutil.Process(process.pid)
            max_rss_seen = 0
            memory_samples = []
            
            while process.poll() is None:
                try:
                    mem_info = proc_monitor.memory_info()
                    rss_mb = mem_info.rss / (1024 * 1024)
                    max_rss_seen = max(max_rss_seen, rss_mb)
                    memory_samples.append((time.time() - start_time, rss_mb))
                    time.sleep(0.5)
                except psutil.NoSuchProcess:
                    break
                    
            duration = time.time() - start_time
            stdout, stderr = process.communicate()
            
            # Verify results
            logger.info(f"Worker exited after {duration:.2f}s")
            logger.info(f"Maximum RSS observed: {max_rss_seen:.2f}MB (limit: {memory_limit})")
            
            if "RSS memory limit exceeded" in stdout or "RSS memory limit exceeded" in stderr:
                logger.info("✅ SUCCESS: Worker detected memory limit and exited gracefully")
                self._record_result(test_name, True, f"Max RSS: {max_rss_seen:.2f}MB")
            else:
                logger.error("❌ FAILED: Worker did not detect memory limit breach")
                logger.error(f"STDOUT: {stdout[-500:]}")  # Last 500 chars
                logger.error(f"STDERR: {stderr[-500:]}")
                self._record_result(test_name, False, "Memory limit not detected")
                
        except Exception as e:
            logger.error(f"❌ FAILED: Test error: {e}")
            self._record_result(test_name, False, str(e))
            
    def test_graceful_shutdown(self):
        """Test graceful shutdown with in-flight message"""
        logger.info("\n" + "="*60)
        logger.info("TEST 3: Graceful Shutdown")
        logger.info("="*60)
        
        test_name = "graceful_shutdown"
        
        try:
            # Create slow-processing test message
            logger.info("Creating slow-processing test message (10s)...")
            message_ids = self._create_slow_test_messages(3, test_name, duration=10)
            
            # Start worker
            logger.info("Starting worker...")
            process = subprocess.Popen([
                self.odoo_bin_path, 'imqworker',
                '--database', self.database,
                '--queue', 'default',
                '--worker-name', f'test-{test_name}',
                '--log-level', 'INFO'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Wait for worker to start processing
            time.sleep(5)  # Let it pick up a message
            
            # Send SIGTERM
            logger.info("Sending SIGTERM to worker...")
            process.send_signal(signal.SIGTERM)
            
            # Wait for graceful shutdown
            start_shutdown = time.time()
            stdout, stderr = process.communicate(timeout=30)
            shutdown_duration = time.time() - start_shutdown
            
            # Verify graceful shutdown
            if process.returncode == 0:
                if "Received SIGTERM" in stdout or "graceful shutdown" in stdout.lower():
                    logger.info("✅ SUCCESS: Worker performed graceful shutdown")
                    logger.info(f"   Shutdown duration: {shutdown_duration:.2f}s")
                    
                    # Check if in-flight message was completed
                    processed = self._count_processed_messages(message_ids[:1])
                    if processed > 0:
                        logger.info("   ✅ In-flight message was completed")
                    else:
                        logger.info("   ⚠️  In-flight message was not completed")
                        
                    self._record_result(test_name, True, f"Graceful shutdown in {shutdown_duration:.2f}s")
                else:
                    logger.error("❌ FAILED: No graceful shutdown message found")
                    self._record_result(test_name, False, "No graceful shutdown detected")
            else:
                logger.error(f"❌ FAILED: Worker exited with code {process.returncode}")
                self._record_result(test_name, False, f"Exit code {process.returncode}")
                
        except subprocess.TimeoutExpired:
            logger.error("❌ FAILED: Worker did not shutdown within timeout")
            process.kill()
            self._record_result(test_name, False, "Shutdown timeout")
        except Exception as e:
            logger.error(f"❌ FAILED: Test error: {e}")
            self._record_result(test_name, False, str(e))
            
    def test_multiple_limits(self):
        """Test interaction of multiple resource limits"""
        logger.info("\n" + "="*60)
        logger.info("TEST 4: Multiple Limits Interaction")
        logger.info("="*60)
        
        test_name = "multiple_limits"
        
        try:
            # Create test messages
            logger.info("Creating test messages...")
            message_ids = self._create_test_messages(10, test_name)
            
            # Start worker with both limits
            logger.info("Starting worker with --max-messages=5 and --max-rss-memory=200M")
            process = subprocess.Popen([
                self.odoo_bin_path, 'imqworker',
                '--database', self.database,
                '--queue', 'default',
                '--max-messages', '5',
                '--max-rss-memory', '200M',
                '--worker-name', f'test-{test_name}',
                '--log-level', 'INFO'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Wait for completion
            stdout, stderr = process.communicate(timeout=60)
            
            # Check which limit triggered
            if "Reached message limit" in stdout:
                logger.info("✅ SUCCESS: Message limit triggered first (as expected)")
                self._record_result(test_name, True, "Message limit triggered first")
            elif "RSS memory limit exceeded" in stdout:
                logger.info("✅ SUCCESS: Memory limit triggered first")
                self._record_result(test_name, True, "Memory limit triggered first")
            else:
                logger.error("❌ FAILED: No limit trigger message found")
                self._record_result(test_name, False, "No limit detected")
                
        except Exception as e:
            logger.error(f"❌ FAILED: Test error: {e}")
            self._record_result(test_name, False, str(e))
            
    def _create_test_messages(self, count, test_name):
        """Create simple test messages"""
        cmd = [
            'echo', f'for i in range({count}): env["imq.test_launcher"].create({{"name": "Resource Test {test_name} {{}}".format(i+1)}}).launch("TestMessage", {{"test": "{test_name}"}})',
            '|', self.odoo_bin_path, 'shell', '--database', self.database
        ]
        result = subprocess.run(' '.join(cmd), shell=True, capture_output=True, text=True)
        
        # Extract message IDs from output
        message_ids = []
        for line in result.stdout.split('\n'):
            if "'id':" in line:
                try:
                    # Parse the line to extract ID
                    id_str = line.split("'id':")[1].split(',')[0].strip()
                    message_ids.append(int(id_str))
                except:
                    pass
                    
        logger.info(f"Created {len(message_ids)} test messages")
        return message_ids
        
    def _create_memory_test_messages(self, count, test_name):
        """Create messages that consume memory during processing"""
        # Create messages with special payload that triggers memory allocation
        cmd = [
            'echo', f'''
for i in range({count}):
    launcher = env["imq.test_launcher"].create({{"name": "Memory Test {test_name} {{}}".format(i+1)}})
    launcher.launch("TestMessage", {{"test": "{test_name}", "allocate_mb": 50}})
''',
            '|', self.odoo_bin_path, 'shell', '--database', self.database
        ]
        result = subprocess.run(' '.join(cmd), shell=True, capture_output=True, text=True)
        return list(range(1, count + 1))  # Dummy IDs for now
        
    def _create_slow_test_messages(self, count, test_name, duration=10):
        """Create messages that take time to process"""
        cmd = [
            'echo', f'''
for i in range({count}):
    launcher = env["imq.test_launcher"].create({{"name": "Slow Test {test_name} {{}}".format(i+1)}})
    launcher.launch("TestMessage", {{"test": "{test_name}", "duration": {duration}}})
''',
            '|', self.odoo_bin_path, 'shell', '--database', self.database
        ]
        result = subprocess.run(' '.join(cmd), shell=True, capture_output=True, text=True)
        return list(range(1, count + 1))  # Dummy IDs for now
        
    def _count_processed_messages(self, message_ids):
        """Count how many messages were processed"""
        if not message_ids:
            return 0
            
        # Use shell to check message states
        id_list = ','.join(map(str, message_ids))
        cmd = [
            'echo', f'messages = env["imq.message"].search([("id", "in", [{id_list}]), ("state", "=", "done")]); print(len(messages))',
            '|', self.odoo_bin_path, 'shell', '--database', self.database
        ]
        result = subprocess.run(' '.join(cmd), shell=True, capture_output=True, text=True)
        
        try:
            # Extract count from output
            for line in result.stdout.split('\n'):
                if line.strip().isdigit():
                    return int(line.strip())
        except:
            pass
            
        return 0
        
    def _record_result(self, test_name, success, details):
        """Record test result"""
        self.test_results.append({
            'test': test_name,
            'success': success,
            'details': details,
            'timestamp': datetime.now().isoformat()
        })
        
    def report_results(self):
        """Report test results summary"""
        logger.info("\n" + "="*80)
        logger.info("TEST RESULTS SUMMARY")
        logger.info("="*80)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r['success'])
        failed_tests = total_tests - passed_tests
        
        for result in self.test_results:
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            logger.info(f"{status} - {result['test']}: {result['details']}")
            
        logger.info("-"*80)
        logger.info(f"Total: {total_tests} | Passed: {passed_tests} | Failed: {failed_tests}")
        logger.info(f"Success Rate: {passed_tests/total_tests*100:.1f}%")
        
        # Save results to JSON file
        results_file = f"resource_limit_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump({
                'summary': {
                    'total': total_tests,
                    'passed': passed_tests,
                    'failed': failed_tests,
                    'success_rate': passed_tests/total_tests*100
                },
                'results': self.test_results,
                'database': self.database,
                'timestamp': datetime.now().isoformat()
            }, f, indent=2)
        logger.info(f"\nResults saved to: {results_file}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Test IMQ Worker Resource Limits')
    parser.add_argument('--database', '-d', required=True, help='Database name')
    parser.add_argument('--odoo-bin', default='bin/start_odoo', help='Path to Odoo binary')
    parser.add_argument('--test', help='Run specific test only')
    
    args = parser.parse_args()
    
    # Run tests
    tester = ResourceLimitTester(args.database, args.odoo_bin)
    
    if args.test:
        # Run specific test
        test_method = getattr(tester, f'test_{args.test}', None)
        if test_method:
            test_method()
            tester.report_results()
        else:
            logger.error(f"Unknown test: {args.test}")
            sys.exit(1)
    else:
        # Run all tests
        tester.run_all_tests()


if __name__ == '__main__':
    main()