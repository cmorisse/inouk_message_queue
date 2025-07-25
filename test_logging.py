#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test script to verify IMQ logging functionality"""

import logging
from odoo import api, SUPERUSER_ID
from odoo.modules.registry import Registry

_logger = logging.getLogger(__name__)

def test_logging_function(env, payload, _imq_logger=None, _imq_stream=None):
    """Test function that uses IMQ logging"""
    logger = _imq_logger or _logger
    
    logger.info("Test logging function started")
    logger.debug("Received payload: %s", payload)
    
    if _imq_stream:
        print("Console output: This should be captured by IMQ", file=_imq_stream)
        print("Another line of console output", file=_imq_stream)
    
    logger.warning("Test warning message")
    logger.error("Test error message (not a real error)")
    
    result = payload.get('value', 0) * 2
    logger.info("Calculation result: %s", result)
    
    return result

def create_test_message():
    """Create a test message for logging verification"""
    database = 'cyril_mpy13c_99_001'
    
    with Registry(database).cursor() as cr:
        env = api.Environment(cr, SUPERUSER_ID, {})
        
        # Find or create test queue
        queue = env['imq.queue'].search([('name', '=', 'test_logging')], limit=1)
        if not queue:
            queue = env['imq.queue'].create({
                'name': 'test_logging',
                'provider': 'pgsql',
                'active': True,
            })
        
        # Find or create test processor
        processor = env['imq.message_processor'].search([
            ('function', '=', 'test_logging_function'),
            ('module', '=', 'inouk_message_queue.test_logging')
        ], limit=1)
        
        if not processor:
            processor = env['imq.message_processor'].create({
                'name': 'Test Logging Processor',
                'selector': 'test_logging',
                'module': 'inouk_message_queue.test_logging',
                'function': 'test_logging_function',
                'message_type': 'simple',
                'logging_activated': True,
                'capture_log': True,
                'capture_console': True,
                'log_level': '10',  # DEBUG
            })
        
        # Create test message
        from inouk_message_queue import api as imq_api
        imq_api.enqueue_simple(
            env,
            queue.name,
            processor.selector,
            {'value': 42},
            _imq_logging_activated=True,
            _imq_capture_console=True
        )
        
        cr.commit()
        print(f"Created test message in queue '{queue.name}'")

if __name__ == '__main__':
    create_test_message()