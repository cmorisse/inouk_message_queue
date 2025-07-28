# Bug Fix: Empty Queue Health Monitoring

## Problem Description

The IMQ Workers v3 queue health monitoring system was incorrectly marking empty queues as unhealthy after 10 consecutive empty polls. This caused the worker to skip healthy queues that simply had no messages to process.

## Root Cause

The `_process_one_message` method was returning boolean values (`True`/`False`) which didn't allow the main processing loop to distinguish between:
- No message available (empty queue) - Not a failure
- Processing failure (actual error) - Should count as failure

The main processing loop was treating all `False` returns as failures, causing empty queues to accumulate failure counts.

## Solution

### 1. Enhanced Return Values

Modified `_process_one_message` to return descriptive string values:
- `'processed'` - Message successfully processed
- `'empty'` - No message available (not a failure)
- `'failed'` - Actual processing failure

### 2. Updated Statistics Logic

Modified the main processing loop in `run()` method to handle the new return values:

```python
# Process one message
result = self._process_one_message(registry, queue_id, queue_name)

# Update queue statistics based on result
if result == 'processed':
    self._update_queue_stats(queue_name, True, processing_time)
    # ... handle successful processing
elif result == 'empty':
    # Queue is empty - this is NOT a failure
    consecutive_empty_polls += 1
    # No failure stats update
elif result == 'failed':
    # Actual processing failure - update failure stats
    self._update_queue_stats(queue_name, False, processing_time)
```

### 3. Preserved Existing Behavior

- Successfully processed messages still update success statistics
- Actual processing failures still update failure statistics
- Empty queues no longer affect failure statistics

## Files Modified

- `/workers/standalone.py`:
  - Updated `_process_one_message` return values (lines 504, 519, 542, 587, 617, 514)
  - Modified main processing loop to handle string return values (lines 449-476)

## Testing

### Test Case 1: Normal Processing
```bash
bin/start_odoo imqworker --database $PGDATABASE --queue default --max-messages 1
```
**Result:** ✅ Successfully processed message, queue marked healthy

### Test Case 2: Empty Queue
```bash
timeout 30s bin/start_odoo imqworker --database $PGDATABASE --queue health_check --max-messages 100
```
**Result:** ✅ Queue remained healthy (1/1 healthy) after 30 seconds of empty polling

### Test Case 3: All Integration Tests
```bash
./run_tests.sh
```
**Result:** ✅ All 7 tests passed

## Impact

- **Positive:** Empty queues no longer incorrectly marked as unhealthy
- **Positive:** More accurate queue health monitoring
- **Positive:** Better load balancing across healthy queues
- **Neutral:** No performance impact
- **Neutral:** Backward compatible with existing behavior

## Validation

The fix ensures that:
1. Empty queues remain healthy indefinitely
2. Actual processing failures are still tracked correctly
3. Queue health monitoring accurately reflects processing issues
4. Round-robin load balancing works properly with healthy empty queues

## Related Issues

- **Phase 2 Bug:** Messages stuck in 'wip' state ✅ Fixed
- **Phase 2 Bug:** Empty queues marked unhealthy ✅ Fixed

This fix completes the Phase 2 bug resolution and prepares the system for Phase 3 implementation.