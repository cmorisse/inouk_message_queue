#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
import json
import logging
import ast
from odoo.cli import Command
from odoo import api, SUPERUSER_ID
from odoo.modules.registry import Registry

_logger = logging.getLogger(__name__)


class IMQTest(Command):
    """Create test messages for IMQ worker testing"""
    name = 'imqtest'
    
    def run(self, args):
        """Main entry point for the CLI command"""
        parser = argparse.ArgumentParser(
            prog=f'{sys.argv[0]} imqtest',
            description='Create test messages for IMQ worker testing'
        )
        
        # Required arguments
        parser.add_argument('--database', '-d', required=True,
                          help='Database name to connect to')
        
        # Test type selection
        test_group = parser.add_mutually_exclusive_group(required=True)
        test_group.add_argument('--simple', action='store_true',
                               help='Create simple message test using SimpleMessage_processor')
        test_group.add_argument('--rpc-method', action='store_true',
                               help='Create RPC message test using a_task_method')
        test_group.add_argument('--rpc-function', action='store_true',
                               help='Create RPC message test using a_task_procedure')
        test_group.add_argument('--fifo-test', action='store_true',
                               help='Create FIFO test sequence')
        test_group.add_argument('--list-processors', action='store_true',
                               help='List available test processors')
        test_group.add_argument('--reset-queue', action='store_true',
                               help='Reset (delete all messages from) the specified queue')
        
        # Message configuration
        parser.add_argument('--queue', '-q', default='default',
                          help='Queue name to send message to (default: default)')
        parser.add_argument('--count', '-c', type=int, default=1,
                          help='Number of test messages to create (default: 1)')
        parser.add_argument('--name', '-n', type=str, default=None,
                          help='Test message name (auto-generated if not provided)')
        
        # Simple message options
        parser.add_argument('--selector', '-s', default='TestMessage',
                          help='Processor selector for simple messages (default: TestMessage)')
        parser.add_argument('--payload', '-p', type=str, default='{}',
                          help='JSON payload for simple messages (default: {})')
        
        # RPC test options (matching test_launcher)
        parser.add_argument('--param', type=str, default='test_param',
                          help='Parameter for RPC test methods (default: test_param)')
        parser.add_argument('--duration', type=int, default=5,
                          help='Processing duration in seconds (default: 5)')
        
        # Exception testing options (matching test_launcher)
        parser.add_argument('--raise-exception', action='store_true',
                          help='Make test raise an exception')
        parser.add_argument('--exception-type', default='exception',
                          choices=['exception', 'usererror', 'imqerror', 'imqretryable', 'imqterminate'],
                          help='Type of exception to raise (default: exception)')
        parser.add_argument('--exception-step', type=str, default=None,
                          help='Step name that should raise exception (for FIFO tests)')
        parser.add_argument('--delay-param', type=int, default=0,
                          help='Delay in seconds for IMQRetryableError (default: 0)')
        parser.add_argument('--pass-imqerror-value', action='store_true',
                          help='Pass a value to IMQError/IMQRetryableError')
        
        # FIFO specific options
        parser.add_argument('--message-group', type=str, default=None,
                          help='Message group ID for FIFO queues (auto-generated if not provided)')
        
        # Logging and debugging options
        parser.add_argument('--debug-mode', action='store_true',
                          help='Run in debug mode (synchronous execution)')
        parser.add_argument('--enable-logging', action='store_true',
                          help='Enable logging for test messages')
        parser.add_argument('--enable-console', action='store_true',
                          help='Enable console capture for test messages')
        parser.add_argument('--log-level', default='INFO',
                          choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                          help='Log level for test execution')
        
        # Advanced options
        parser.add_argument('--user-id', type=int, default=None,
                          help='User ID to run test as (default: system user)')
        parser.add_argument('--context', type=str, default='{}',
                          help='Additional context for message (JSON format)')
        parser.add_argument('--delay', type=int, default=0,
                          help='Delay between creating messages in seconds')
        
        # Output options
        parser.add_argument('--verbose', '-v', action='store_true',
                          help='Verbose output')
        parser.add_argument('--json-output', action='store_true',
                          help='Output results in JSON format')
        
        # Parse arguments
        try:
            parsed_args = parser.parse_args(args)
        except SystemExit:
            return 1
            
        # Set logging level
        log_level = getattr(logging, parsed_args.log_level)
        logging.basicConfig(level=log_level)
        
        # Connect to database
        try:
            with api.Environment.manage():
                registry = Registry(parsed_args.database)
                with registry.cursor() as cr:
                    env = api.Environment(cr, SUPERUSER_ID, {})
                
                    if parsed_args.list_processors:
                        return self._list_processors(env, parsed_args)
                    elif parsed_args.reset_queue:
                        return self._reset_queue(env, parsed_args)
                    elif parsed_args.simple:
                        return self._create_simple_messages(env, parsed_args)
                    elif parsed_args.rpc_method:
                        return self._create_rpc_messages(env, parsed_args, 'method')
                    elif parsed_args.rpc_function:
                        return self._create_rpc_messages(env, parsed_args, 'function')
                    elif parsed_args.fifo_test:
                        return self._create_fifo_test(env, parsed_args)
                    
        except Exception as e:
            print(f"Error: {e}")
            if parsed_args.verbose:
                import traceback
                traceback.print_exc()
            return 1
            
        return 0
    
    def _list_processors(self, env, args):
        """List available test processors"""
        processors = env['imq.message_processor'].search([
            '|',
            ('name', 'ilike', 'test'),
            ('function', 'ilike', 'test')
        ])
        
        if args.json_output:
            result = []
            for proc in processors:
                result.append({
                    'id': proc.id,
                    'name': proc.name,
                    'selector': proc.selector,
                    'function': proc.function,
                    'module': proc.module,
                    'is_method': proc.is_method,
                    'logging_activated': proc.logging_activated,
                    'capture_log': proc.capture_log
                })
            print(json.dumps(result, indent=2))
        else:
            print("Available Test Processors:")
            print("-" * 80)
            for proc in processors:
                print(f"ID: {proc.id}")
                print(f"Name: {proc.name}")
                print(f"Selector: {proc.selector}")
                print(f"Function: {proc.function}")
                print(f"Module: {proc.module}")
                print(f"Type: {'Method' if proc.is_method else 'Function'}")
                print(f"Logging: {'Enabled' if proc.logging_activated else 'Disabled'}")
                print(f"Log Capture: {'Enabled' if proc.capture_log else 'Disabled'}")
                print("-" * 80)
                
        return 0
    
    def _reset_queue(self, env, args):
        """Reset (delete all messages from) the specified queue"""
        # Find the queue
        queue_model = env['imq.queue']
        queue_record = queue_model.search([('name', '=', args.queue)], limit=1)
        if not queue_record:
            print(f"Error: Queue '{args.queue}' not found")
            return 1
        
        # Find all messages in the queue
        messages = env['imq.message'].search([('queue_id.name', '=', args.queue)])
        count = len(messages)
        
        if count == 0:
            if args.verbose or not args.json_output:
                print(f"Queue '{args.queue}' is already empty (0 messages)")
            if args.json_output:
                print(json.dumps({"queue": args.queue, "deleted_count": 0, "status": "already_empty"}))
            return 0
        
        # Delete all messages
        if args.verbose:
            print(f"Deleting {count} message(s) from queue '{args.queue}'...")
        
        try:
            messages.unlink()
            env.cr.commit()
            
            if args.json_output:
                print(json.dumps({"queue": args.queue, "deleted_count": count, "status": "success"}))
            else:
                print(f"Successfully reset queue '{args.queue}': {count} message(s) deleted")
            return 0
            
        except Exception as e:
            if args.json_output:
                print(json.dumps({"queue": args.queue, "deleted_count": 0, "status": "error", "error": str(e)}))
            else:
                print(f"Error resetting queue '{args.queue}': {e}")
            return 1
    
    def _create_simple_messages(self, env, args):
        """Create simple test messages"""
        from odoo.addons.inouk_message_queue import api as imq_api
        
        # Parse payload
        try:
            payload = json.loads(args.payload)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON payload: {e}")
            return 1
            
        # Parse context
        try:
            context = json.loads(args.context)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON context: {e}")
            return 1
        
        created_messages = []
        
        for i in range(args.count):
            # Add sequence number to payload for multiple messages
            test_payload = payload.copy()
            if args.count > 1:
                test_payload['sequence'] = i + 1
                test_payload['total_count'] = args.count
            
            # Add test metadata for exception testing
            if args.raise_exception:
                test_payload['should_raise_error'] = f"CLI Test Exception ({args.exception_type})"
            
            test_payload.update({
                'test_type': 'simple',
                'created_by': 'imq-test-cli',
                'enable_logging': args.enable_logging,
                'enable_console': args.enable_console
            })
            
            try:
                message = imq_api.send_message(
                    env,
                    args.queue,
                    args.selector,
                    test_payload,
                    message_name=args.name or f"CLI Test Simple {i+1}"
                )
                
                created_messages.append({
                    'id': message['id'],
                    'message_id': message['MessageId'],
                    'name': args.name or f"CLI Test Simple {i+1}",
                    'queue': args.queue,
                    'selector': args.selector,
                    'sequence': i + 1 if args.count > 1 else None
                })
                
                if args.verbose:
                    print(f"Created simple message {i+1}/{args.count}: ID={message['id']}, MessageID={message['MessageId']}")
                    
            except Exception as e:
                print(f"Error creating message {i+1}: {e}")
                return 1
                
            # Delay between messages if specified
            if args.delay > 0 and i < args.count - 1:
                import time
                time.sleep(args.delay)
        
        env.cr.commit()
        
        if args.json_output:
            print(json.dumps(created_messages, indent=2))
        else:
            print(f"Successfully created {len(created_messages)} simple message(s) in queue '{args.queue}'")
            for msg in created_messages:
                print(f"  - Message ID: {msg['id']}, Name: '{msg['name']}'")
                
        return 0
    
    def _create_rpc_messages(self, env, args, rpc_type):
        """Create RPC test messages using test_launcher methods"""
        # Parse context
        try:
            context = json.loads(args.context)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON context: {e}")
            return 1
        
        created_messages = []
        
        # Find a queue record
        queue_model = env['imq.queue']
        queue_record = queue_model.search([('name', '=', args.queue)], limit=1)
        if not queue_record:
            print(f"Error: Queue '{args.queue}' not found")
            return 1
        
        for i in range(args.count):
            # Create test launcher record
            test_name = args.name or f"CLI Test {rpc_type} {i+1}/{args.count}"
            test_launcher = env['imq.test_launcher'].create({
                'name': test_name,
                'type': 'rpc',
                'queue_id': queue_record.id,
                'rpc_test_method': rpc_type == 'method',
                'param': args.param,
                'processing_duration_s': args.duration,
                'should_raise_exception': args.raise_exception,
                'should_raise_exception_type': args.exception_type if args.raise_exception else False,
                'should_raise_exception_stepname': args.exception_step,
                'delay_param': args.delay_param,
                'pass_imqerror_value': args.pass_imqerror_value,
                'message_group': args.message_group
            })
            
            try:
                # Send the RPC message using test_launcher method
                test_launcher.send_rpc_message()
                
                # Get the message ID from launch_result
                launch_result = test_launcher.launch_result
                if launch_result:
                    try:
                        # launch_result is a string representation of a Python dict, parse it
                        result_dict = ast.literal_eval(launch_result)
                        message_id = result_dict.get('id', 'unknown')
                        message_queue_id = result_dict.get('MessageId', 'unknown')
                        message_name = test_name
                    except (ValueError, SyntaxError, AttributeError) as e:
                        # Fallback if parsing fails
                        message_id = "parse_error"
                        message_queue_id = "parse_error"
                        message_name = test_name
                else:
                    message_id = "sync_execution"
                    message_queue_id = "sync_execution" 
                    message_name = test_name
                
                created_messages.append({
                    'id': message_id,
                    'message_id': message_queue_id,
                    'name': message_name,
                    'queue': args.queue,
                    'type': f'rpc_{rpc_type}',
                    'sequence': i + 1 if args.count > 1 else None,
                    'test_launcher_id': test_launcher.id
                })
                
                if args.verbose:
                    print(f"Created RPC {rpc_type} message {i+1}/{args.count}: ID={message_id}, MessageID={message_queue_id}")
                    
            except Exception as e:
                print(f"Error creating RPC {rpc_type} message {i+1}: {e}")
                return 1
                
            # Delay between messages if specified
            if args.delay > 0 and i < args.count - 1:
                import time
                time.sleep(args.delay)
        
        env.cr.commit()
        
        if args.json_output:
            print(json.dumps(created_messages, indent=2))
        else:
            print(f"Successfully created {len(created_messages)} RPC {rpc_type} message(s) in queue '{args.queue}'")
            for msg in created_messages:
                print(f"  - Message ID: {msg['id']}, Name: '{msg['name']}'")
                
        return 0
    
    def _create_fifo_test(self, env, args):
        """Create FIFO test sequence using test_launcher"""
        # Parse context
        try:
            context = json.loads(args.context)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON context: {e}")
            return 1
        
        # Find a queue record
        queue_model = env['imq.queue']
        queue_record = queue_model.search([('name', '=', args.queue)], limit=1)
        if not queue_record:
            print(f"Error: Queue '{args.queue}' not found")
            return 1
            
        if queue_record.q_type != 'fifo':
            print(f"Error: Queue '{args.queue}' is not a FIFO queue (type: {queue_record.q_type})")
            return 1
        
        created_messages = []
        
        for i in range(args.count):
            # Create test launcher record for FIFO test
            test_name = args.name or f"CLI FIFO Test {i+1}/{args.count}"
            message_group = args.message_group or queue_record.get_message_group("CLI-TEST-")
            
            test_launcher = env['imq.test_launcher'].create({
                'name': test_name,
                'queue_id': queue_record.id,
                'processing_duration_s': args.duration,
                'should_raise_exception': args.raise_exception,
                'should_raise_exception_type': args.exception_type if args.raise_exception else False,
                'should_raise_exception_stepname': args.exception_step,
                'delay_param': args.delay_param,
                'pass_imqerror_value': args.pass_imqerror_value,
                'message_group': message_group
            })
            
            try:
                # Launch FIFO test using test_launcher method
                test_launcher.launch_fifo_test()
                
                created_messages.append({
                    'id': f"fifo_sequence_{i+1}",
                    'name': test_name,
                    'queue': args.queue,
                    'type': 'fifo_test',
                    'message_group': message_group,
                    'sequence': i + 1 if args.count > 1 else None,
                    'test_launcher_id': test_launcher.id,
                    'steps': 8  # fifo test creates 8 steps (fifo_step1 through fifo_step8)
                })
                
                if args.verbose:
                    print(f"Created FIFO test sequence {i+1}/{args.count}: Name='{test_name}', Group='{message_group}'")
                    
            except Exception as e:
                print(f"Error creating FIFO test {i+1}: {e}")
                return 1
                
            # Delay between message sequences if specified
            if args.delay > 0 and i < args.count - 1:
                import time
                time.sleep(args.delay)
        
        env.cr.commit()
        
        if args.json_output:
            print(json.dumps(created_messages, indent=2))
        else:
            print(f"Successfully created {len(created_messages)} FIFO test sequence(s) in queue '{args.queue}'")
            for msg in created_messages:
                print(f"  - Test: '{msg['name']}', Group: '{msg['message_group']}', Steps: {msg['steps']}")
                
        return 0


# Register the command
imq_test = IMQTest()