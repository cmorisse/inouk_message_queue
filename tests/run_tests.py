#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test runner for IMQ Workers v3 tests
"""

import os
import sys
import subprocess


def run_test_file(test_file, description=""):
    """Run a specific test file"""
    print(f"\n{'='*60}")
    print(f"Running {description or test_file}")
    print(f"{'='*60}")
    
    try:
        test_dir = os.path.dirname(os.path.abspath(__file__))
        test_path = os.path.join(test_dir, test_file)
        result = subprocess.run([sys.executable, test_path], 
                              cwd=test_dir,
                              capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"Error running {test_file}: {e}")
        return False


def main():
    """Run all available tests"""
    print("IMQ Workers v3 Test Runner")
    print("=" * 60)
    
    test_files = [
        ("test_phase1_simple.py", "Phase 1 Implementation Tests"),
        ("test_phase2_queue_processing.py", "Phase 2 Queue Processing Tests"),
        # Add more test files here as they are created
    ]
    
    results = []
    
    for test_file, description in test_files:
        test_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), test_file)
        if os.path.exists(test_path):
            success = run_test_file(test_file, description)
            results.append((description, success))
        else:
            print(f"Warning: {test_file} not found at {test_path}")
            results.append((description, False))
    
    # Summary
    print(f"\n{'='*60}")
    print("Test Results Summary")
    print(f"{'='*60}")
    
    passed = 0
    total = len(results)
    
    for description, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status:<10} {description}")
        if success:
            passed += 1
    
    print(f"\nTotal: {passed}/{total} test suites passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())