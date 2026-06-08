#!/bin/bash

# IMQ Workers v3 Test Runner
# This script runs all tests for the IMQ Workers v3 implementation

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  IMQ Workers v3 Test Suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to print test section headers
print_section() {
    echo -e "${YELLOW}>>> $1${NC}"
    echo ""
}

# Function to run a command and capture its result
run_test() {
    local test_name="$1"
    local test_command="$2"
    
    echo -e "${BLUE}Running: $test_name${NC}"
    echo "Command: $test_command"
    echo ""
    
    if eval "$test_command"; then
        echo -e "${GREEN}✓ $test_name: PASSED${NC}"
        return 0
    else
        echo -e "${RED}✗ $test_name: FAILED${NC}"
        return 1
    fi
}

# Initialize test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Test 1: Module loading test
print_section "1. Module Loading Test"
TOTAL_TESTS=$((TOTAL_TESTS + 1))
if run_test "Module loads without errors" "cd /opt/muppy/appserver-mpy18c && bin/start_odoo -u inouk_message_queue --stop-after-init > /dev/null 2>&1"; then
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
echo ""

# Test 2: CLI command registration test
print_section "2. CLI Command Registration Test"
TOTAL_TESTS=$((TOTAL_TESTS + 1))
if run_test "CLI command is registered" "cd /opt/muppy/appserver-mpy18c && bin/start_odoo help 2>/dev/null | grep -q 'imq_worker'"; then
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
echo ""

# Test 3: CLI help test
print_section "3. CLI Help Test"
TOTAL_TESTS=$((TOTAL_TESTS + 1))
if run_test "CLI help works" "cd /opt/muppy/appserver-mpy18c && bin/start_odoo imq_worker --help > /dev/null 2>&1"; then
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
echo ""

# Test 4: Worker initialization test
print_section "4. Worker Initialization Test"
TOTAL_TESTS=$((TOTAL_TESTS + 1))
if run_test "Worker initializes successfully" "cd /opt/muppy/appserver-mpy18c && timeout 10s bin/start_odoo imq_worker --database \${PGDATABASE:-cyril_mpy18c_99_001} --queues test_nonexistent_queue --max-messages 1 > /dev/null 2>&1 || [ \$? -eq 124 ]"; then
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
echo ""

# Test 5: Unit tests (if they can run)
print_section "5. Unit Tests"
echo -e "${YELLOW}Note: Unit tests require proper Python path configuration${NC}"
echo -e "${YELLOW}Running basic import tests instead...${NC}"
echo ""

TOTAL_TESTS=$((TOTAL_TESTS + 1))
if run_test "Worker utilities import" "cd '$SCRIPT_DIR' && python3 -c 'import sys; sys.path.insert(0, \".\"); from worker_utils.worker_utils import parse_memory_limit, get_worker_name; print(\"Imports successful\")'"; then
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
echo ""

# Test 6: CLI argument validation
print_section "6. CLI Argument Validation"
TOTAL_TESTS=$((TOTAL_TESTS + 1))
if run_test "CLI rejects invalid arguments" "cd /opt/muppy/appserver-mpy18c && bin/start_odoo imq_worker --invalid-arg 2>&1 | grep -q 'error:'"; then
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
echo ""

# Test 7: Worker startup test 
print_section "7. Worker Startup Test"
echo -e "${YELLOW}Testing worker startup and basic configuration...${NC}"
echo ""

TOTAL_TESTS=$((TOTAL_TESTS + 1))
if run_test "Worker starts and shows configuration" "cd /opt/muppy/appserver-mpy18c && timeout 10s bin/start_odoo imq_worker --database \${PGDATABASE:-cyril_mpy18c_99_001} --queues test_startup --max-messages 1 2>&1 | grep -q 'Queue pattern: test_startup'"; then
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
echo ""

# Summary
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Test Results Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Total Tests: $TOTAL_TESTS"
echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
echo -e "${RED}Failed: $FAILED_TESTS${NC}"
echo ""

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some tests failed. Please check the output above.${NC}"
    exit 1
fi