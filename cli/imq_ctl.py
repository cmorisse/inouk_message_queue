#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
import json
import yaml
import logging
from datetime import datetime
from odoo.cli import Command
from odoo import api, SUPERUSER_ID
from odoo.modules.registry import Registry

_logger = logging.getLogger(__name__)


class IMQCtl(Command):
    """IMQ Control - Kubernetes-style resource management for IMQ"""
    name = 'imq-ctl'
    
    def run(self, args):
        """Main entry point for the CLI command"""
        parser = argparse.ArgumentParser(
            prog=f'{sys.argv[0]} imq-ctl',
            description='IMQ Control - Manage IMQ resources (queues, messages, processors)',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Queue management
  %(prog)s get queue                        # List all queues
  %(prog)s get queue default                # Get specific queue details
  %(prog)s create queue test-queue          # Create new queue
  %(prog)s delete queue test-queue          # Delete queue
  %(prog)s describe queue default           # Detailed queue information

  # Message inspection
  %(prog)s get message --queue default      # List messages in queue
  %(prog)s describe message 123             # Message details

  # Processor inspection  
  %(prog)s get processor                    # List all processors
  %(prog)s describe processor TestMessage   # Processor details
            """
        )
        
        # Required arguments
        parser.add_argument('--database', '-d', required=True,
                          help='Database name to connect to')
        
        # Subcommands
        subparsers = parser.add_subparsers(dest='verb', help='Available commands')
        subparsers.required = True
        
        # Get command
        get_parser = subparsers.add_parser('get', help='Display one or many resources')
        get_parser.add_argument('resource_type', choices=['queue', 'message', 'processor', 'processing'],
                              help='Resource type to get')
        get_parser.add_argument('resource_name', nargs='?', help='Specific resource name')
        get_parser.add_argument('--output', '-o', choices=['table', 'yaml', 'json'], default='table',
                              help='Output format')
        get_parser.add_argument('--queue', help='Filter messages by queue name')
        get_parser.add_argument('--state', help='Filter messages by state')
        get_parser.add_argument('--group', help='Filter messages by group (per-run tag)')
        
        # Create command
        create_parser = subparsers.add_parser('create', help='Create a resource')
        create_parser.add_argument('resource_type', choices=['queue'],
                                 help='Resource type to create')
        create_parser.add_argument('resource_name', help='Name of resource to create')
        create_parser.add_argument('--type', choices=['std', 'fifo'], default='std',
                                 help='Queue type (default: std)')
        create_parser.add_argument('--provider', choices=['pgsql', 'aws_sqs'], default='pgsql',
                                 help='Queue provider (default: pgsql)')
        create_parser.add_argument('--visibility-timeout', type=int, default=30,
                                 help='Visibility timeout in seconds (default: 30)')
        create_parser.add_argument('--active', action='store_true', default=True,
                                 help='Create queue as active (default: true)')
        create_parser.add_argument('--dry-run', action='store_true',
                                 help='Show what would be created without creating')
        
        # Delete command
        delete_parser = subparsers.add_parser('delete', help='Delete a resource')
        delete_parser.add_argument('resource_type', choices=['queue'],
                                 help='Resource type to delete')
        delete_parser.add_argument('resource_name', help='Name of resource to delete')
        delete_parser.add_argument('--force', action='store_true',
                                 help='Force deletion without confirmation')
        delete_parser.add_argument('--dry-run', action='store_true',
                                 help='Show what would be deleted without deleting')
        
        # Describe command
        describe_parser = subparsers.add_parser('describe', help='Show detailed information about a resource')
        describe_parser.add_argument('resource_type', choices=['queue', 'message', 'processor', 'processing'],
                                   help='Resource type to describe')
        describe_parser.add_argument('resource_name', help='Name of resource to describe')
        describe_parser.add_argument('--output', '-o', choices=['yaml', 'json'], default='yaml',
                                   help='Output format')
        describe_parser.add_argument('--include-logs', action='store_true',
                                   help='Include processing logs for messages/processing objects')
        describe_parser.add_argument('--output-file', '-f', type=str,
                                   help='Output file path (default: stdout)')
        
        # Logs command
        logs_parser = subparsers.add_parser('logs', help='Show logs for processing records')
        logs_parser.add_argument('resource_type', choices=['processing', 'message'],
                               help='Resource type to show logs for')
        logs_parser.add_argument('resource_name', help='ID of resource to show logs for')
        logs_parser.add_argument('--output', '-o', choices=['stream', 'json'], default='stream',
                               help='Output format')
        logs_parser.add_argument('--output-file', '-f', type=str,
                               help='Output file path (default: stdout)')
        
        # Global options
        parser.add_argument('--verbose', '-v', action='store_true',
                          help='Verbose output')
        
        # Parse arguments
        try:
            parsed_args = parser.parse_args(args)
        except SystemExit:
            return 1
        
        # Set logging level
        if parsed_args.verbose:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.WARNING)
        
        # Connect to database and execute command
        try:
            registry = Registry(parsed_args.database)
            with registry.cursor() as cr:
                env = api.Environment(cr, SUPERUSER_ID, {})
                
                # Route to appropriate handler
                if parsed_args.verb == 'get':
                    return self._handle_get(env, parsed_args)
                elif parsed_args.verb == 'create':
                    return self._handle_create(env, parsed_args)
                elif parsed_args.verb == 'delete':
                    return self._handle_delete(env, parsed_args)
                elif parsed_args.verb == 'describe':
                    return self._handle_describe(env, parsed_args)
                elif parsed_args.verb == 'logs':
                    return self._handle_logs(env, parsed_args)
                else:
                    print(f"Error: Unknown command '{parsed_args.verb}'", file=sys.stderr)
                    return 1
                    
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            if parsed_args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def _handle_get(self, env, args):
        """Handle 'get' command"""
        if args.resource_type == 'queue':
            return self._get_queue(env, args)
        elif args.resource_type == 'message':
            return self._get_message(env, args)
        elif args.resource_type == 'processor':
            return self._get_processor(env, args)
        elif args.resource_type == 'processing':
            return self._get_processing(env, args)
        else:
            print(f"Error: Resource type '{args.resource_type}' not supported for get", file=sys.stderr)
            return 1
    
    def _handle_create(self, env, args):
        """Handle 'create' command"""
        if args.resource_type == 'queue':
            return self._create_queue(env, args)
        else:
            print(f"Error: Resource type '{args.resource_type}' not supported for create", file=sys.stderr)
            return 1
    
    def _handle_delete(self, env, args):
        """Handle 'delete' command"""
        if args.resource_type == 'queue':
            return self._delete_queue(env, args)
        else:
            print(f"Error: Resource type '{args.resource_type}' not supported for delete", file=sys.stderr)
            return 1
    
    def _handle_describe(self, env, args):
        """Handle 'describe' command"""
        if args.resource_type == 'queue':
            return self._describe_queue(env, args)
        elif args.resource_type == 'message':
            return self._describe_message(env, args)
        elif args.resource_type == 'processor':
            return self._describe_processor(env, args)
        elif args.resource_type == 'processing':
            return self._describe_processing(env, args)
        else:
            print(f"Error: Resource type '{args.resource_type}' not supported for describe", file=sys.stderr)
            return 1
    
    def _handle_logs(self, env, args):
        """Handle 'logs' command"""
        if args.resource_type == 'processing':
            return self._logs_processing(env, args)
        elif args.resource_type == 'message':
            return self._logs_message(env, args)
        else:
            print(f"Error: Resource type '{args.resource_type}' not supported for logs", file=sys.stderr)
            return 1
    
    # Queue Management Methods
    def _get_queue(self, env, args):
        """Get queue(s)"""
        if args.resource_name:
            # Get specific queue
            queue = env['imq.queue'].search([('name', '=', args.resource_name)], limit=1)
            if not queue:
                print(f"Error: Queue '{args.resource_name}' not found", file=sys.stderr)
                return 1
            queues = queue
        else:
            # Get all queues
            queues = env['imq.queue'].search([])
        
        if args.output == 'table':
            self._print_queue_table(queues)
        elif args.output == 'yaml':
            self._print_queue_yaml(queues)
        elif args.output == 'json':
            self._print_queue_json(queues)
        
        return 0
    
    def _create_queue(self, env, args):
        """Create queue"""
        if args.dry_run:
            print(f"Would create queue '{args.resource_name}' with:")
            print(f"  Type: {args.type}")
            print(f"  Provider: {args.provider}")
            print(f"  Visibility Timeout: {args.visibility_timeout}s")
            print(f"  Active: {args.active}")
            return 0
        
        # Check if queue already exists
        existing = env['imq.queue'].search([('name', '=', args.resource_name)], limit=1)
        if existing:
            print(f"Error: Queue '{args.resource_name}' already exists", file=sys.stderr)
            return 1
        
        # Create queue
        try:
            queue_data = {
                'name': args.resource_name,
                'q_type': args.type,
                'provider': args.provider,
                'visibility_timeout': args.visibility_timeout,
                'active': args.active,
            }
            
            queue = env['imq.queue'].create(queue_data)
            print(f"Queue '{args.resource_name}' created successfully")
            print(f"  ID: {queue.id}")
            print(f"  Type: {queue.q_type}")
            print(f"  Provider: {queue.provider}")
            return 0
            
        except Exception as e:
            print(f"Error creating queue: {e}", file=sys.stderr)
            return 1
    
    def _delete_queue(self, env, args):
        """Delete queue"""
        # Find queue
        queue = env['imq.queue'].search([('name', '=', args.resource_name)], limit=1)
        if not queue:
            print(f"Error: Queue '{args.resource_name}' not found", file=sys.stderr)
            return 1
        
        if args.dry_run:
            print(f"Would delete queue '{args.resource_name}' (ID: {queue.id})")
            return 0
        
        # Check for messages in queue
        message_count = env['imq.message'].search_count([('queue_id', '=', queue.id)])
        if message_count > 0 and not args.force:
            print(f"Error: Queue '{args.resource_name}' contains {message_count} messages", file=sys.stderr)
            print("Use --force to delete anyway", file=sys.stderr)
            return 1
        
        # Confirmation prompt (unless forced)
        if not args.force:
            response = input(f"Delete queue '{args.resource_name}'? (y/N): ")
            if response.lower() != 'y':
                print("Deletion cancelled")
                return 0
        
        try:
            queue.unlink()
            print(f"Queue '{args.resource_name}' deleted successfully")
            return 0
        except Exception as e:
            print(f"Error deleting queue: {e}", file=sys.stderr)
            return 1
    
    def _describe_queue(self, env, args):
        """Describe queue in detail"""
        queue = env['imq.queue'].search([('name', '=', args.resource_name)], limit=1)
        if not queue:
            print(f"Error: Queue '{args.resource_name}' not found", file=sys.stderr)
            return 1
        
        # Get queue statistics
        message_stats = env['imq.message'].read_group(
            [('queue_id', '=', queue.id)], 
            ['state'], 
            ['state']
        )
        
        stats = {}
        for stat in message_stats:
            stats[stat['state']] = stat['state_count']
        
        # Build detailed queue info
        queue_info = {
            'apiVersion': 'imq/v1',
            'kind': 'Queue',
            'metadata': {
                'id': queue.id,
                'name': queue.name,
                'createdAt': self._format_datetime(queue.create_date),
                'updatedAt': self._format_datetime(queue.write_date)
            },
            'spec': {
                'provider': queue.provider,
                'type': queue.q_type,
                'active': queue.active,
                'visibilityTimeout': queue.visibility_timeout
            },
            'status': {
                'messageCount': stats,
                'totalMessages': sum(stats.values()) if stats else 0
            }
        }
        
        if args.output == 'yaml':
            print(yaml.dump(queue_info, default_flow_style=False, indent=2))
        elif args.output == 'json':
            print(json.dumps(queue_info, indent=2, default=str))
        
        return 0
    
    # Message Management Methods
    def _get_message(self, env, args):
        """Get message(s)"""
        domain = []
        
        if args.queue:
            queue = env['imq.queue'].search([('name', '=', args.queue)], limit=1)
            if not queue:
                print(f"Error: Queue '{args.queue}' not found", file=sys.stderr)
                return 1
            domain.append(('queue_id', '=', queue.id))
        
        if args.state:
            domain.append(('state', '=', args.state))

        if getattr(args, 'group', None):
            domain.append(('group', '=', args.group))

        if args.resource_name:
            if args.resource_name.isdigit():
                domain.append(('id', '=', int(args.resource_name)))
            else:
                domain.append(('queue_message_id', '=', args.resource_name))
        
        messages = env['imq.message'].search(domain, limit=50, order='create_date desc')
        
        if args.output == 'table':
            self._print_message_table(messages)
        elif args.output == 'yaml':
            self._print_message_yaml(messages)
        elif args.output == 'json':
            self._print_message_json(messages)
        
        return 0
    
    def _describe_message(self, env, args):
        """Describe message in detail"""
        if args.resource_name.isdigit():
            message = env['imq.message'].search([('id', '=', int(args.resource_name))], limit=1)
        else:
            message = env['imq.message'].search([('queue_message_id', '=', args.resource_name)], limit=1)
        
        if not message:
            print(f"Error: Message '{args.resource_name}' not found", file=sys.stderr)
            return 1
        
        # Build message data directly (no dependency on imq-dump)
        data = {
            'apiVersion': 'imq/v1',
            'kind': 'Message',
            'metadata': {
                'id': message.id,
                'messageId': message.queue_message_id,
                'name': message.name,
                'createdAt': self._format_datetime(message.create_date),
                'updatedAt': self._format_datetime(message.write_date)
            },
            'spec': {
                'queue': {
                    'id': message.queue_id.id,
                    'name': message.queue_id.name,
                    'provider': message.queue_id.provider,
                    'type': message.queue_id.q_type
                },
                'processor': {
                    'id': message.processor_id.id if message.processor_id else None,
                    'name': message.processor_id.name if message.processor_id else None,
                    'selector': message.processor_id.selector if message.processor_id else None,
                    'function': message.processor_id.function if message.processor_id else None,
                    'module': message.processor_id.module if message.processor_id else None,
                    'isMethod': message.processor_id.is_method if message.processor_id else None
                },
                'user': {
                    'id': message.user_id.id,
                    'name': message.user_id.name,
                    'login': message.user_id.login
                },
                'message': {
                    'type': message.message_type,
                    'group': message.group,
                    'payload': message.payload,
                    'rawMessageBody': message.raw_message_body,
                    'context': message.context
                }
            },
            'status': {
                'state': message.state,
                'attempt': message.attempt,
                'maxAttempts': message.max_number_of_attempts,
                'enqueuedTime': self._format_datetime(message.enqueued_time),
                'startTime': self._format_datetime(message.start_time),
                'endTime': self._format_datetime(message.end_time),
                'plannedTime': self._format_datetime(message.planned_time),
                'visibilityTime': self._format_datetime(message.visibility_time),
                'result': message.result,
                'loggingActivated': message.logging_activated,
                'captureConsole': message.capture_console
            }
        }
        
        # Add processing history - always include list, conditionally include logs
        processings = env['imq.message_processing'].search([
            ('message_id', '=', message.id)
        ], order='create_date desc')
        
        data['status']['processing'] = []
        for proc in processings:
            proc_data = {
                'id': proc.id,
                'workerType': proc.worker_type,
                'state': proc.state,
                'attempt': proc.attempt,
                'startTime': self._format_datetime(proc.start_time),
                'endTime': self._format_datetime(proc.end_time),
                'result': proc.result,
                'createdAt': self._format_datetime(proc.create_date)
            }
            
            # Add logs if requested
            if hasattr(args, 'include_logs') and args.include_logs:
                logs = env['imq.message_processing_log'].search([
                    ('processing_id', '=', proc.id)
                ], order='create_date asc')
                
                proc_data['logs'] = []
                for log in logs:
                    proc_data['logs'].append({
                        'id': log.id,
                        'loggerName': log.logger_name,
                        'logLevel': log.log_level,
                        'message': log.log_message,
                        'createdAt': self._format_datetime(log.create_date)
                    })
            
            data['status']['processing'].append(proc_data)
        
        # Output to file or stdout
        output_content = self._format_output(data, args.output)
        self._write_output(output_content, args)
        return 0
    
    # Processor Management Methods  
    def _get_processor(self, env, args):
        """Get processor(s)"""
        if args.resource_name:
            if args.resource_name.isdigit():
                processors = env['imq.message_processor'].search([('id', '=', int(args.resource_name))], limit=1)
            else:
                processors = env['imq.message_processor'].search([('selector', '=', args.resource_name)], limit=1)
        else:
            processors = env['imq.message_processor'].search([])
        
        if args.output == 'table':
            self._print_processor_table(processors)
        elif args.output == 'yaml':
            self._print_processor_yaml(processors)
        elif args.output == 'json':
            self._print_processor_json(processors)
        
        return 0
    
    def _describe_processor(self, env, args):
        """Describe processor in detail"""
        if args.resource_name.isdigit():
            processor = env['imq.message_processor'].search([('id', '=', int(args.resource_name))], limit=1)
        else:
            processor = env['imq.message_processor'].search([('selector', '=', args.resource_name)], limit=1)
        
        if not processor:
            print(f"Error: Processor '{args.resource_name}' not found", file=sys.stderr)
            return 1
        
        # Use existing imq-dump functionality
        from .imq_dump import IMQDump
        dump_tool = IMQDump()
        
        class FakeArgs:
            def __init__(self):
                self.processor = str(processor.id)
                self.output = args.output
        
        fake_args = FakeArgs()
        result = dump_tool._dump_processor(env, fake_args)
        
        if args.output == 'yaml':
            print(yaml.dump(result, default_flow_style=False, indent=2))
        elif args.output == 'json':
            print(json.dumps(result, indent=2, default=str))
        
        return 0
    
    def _get_processing(self, env, args):
        """Get processing record(s)"""
        domain = []
        
        if args.resource_name:
            if args.resource_name.isdigit():
                domain.append(('id', '=', int(args.resource_name)))
        
        processings = env['imq.message_processing'].search(domain, limit=50, order='create_date desc')
        
        if args.output == 'table':
            self._print_processing_table(processings)
        elif args.output == 'yaml':
            self._print_processing_yaml(processings)
        elif args.output == 'json':
            self._print_processing_json(processings)
        
        return 0
    
    def _describe_processing(self, env, args):
        """Describe processing record in detail"""
        if not args.resource_name.isdigit():
            print("Error: Processing ID must be numeric", file=sys.stderr)
            return 1
        
        processing = env['imq.message_processing'].search([('id', '=', int(args.resource_name))], limit=1)
        if not processing:
            print(f"Error: Processing '{args.resource_name}' not found", file=sys.stderr)
            return 1
        
        # Use existing imq-dump functionality
        from .imq_dump import IMQDump
        dump_tool = IMQDump()
        
        class FakeArgs:
            def __init__(self):
                self.processing = args.resource_name
                self.include_logs = True
                self.output = args.output
        
        fake_args = FakeArgs()
        result = dump_tool._dump_processing(env, fake_args)
        
        if args.output == 'yaml':
            print(yaml.dump(result, default_flow_style=False, indent=2))
        elif args.output == 'json':
            print(json.dumps(result, indent=2, default=str))
        
        return 0
    
    # Table formatting methods
    def _print_queue_table(self, queues):
        """Print queues in table format"""
        if not queues:
            print("No queues found")
            return
        
        print(f"{'NAME':<20} {'TYPE':<8} {'PROVIDER':<10} {'ACTIVE':<8} {'MESSAGES':<10} {'AGE':<15}")
        print("-" * 80)
        
        for queue in queues:
            # Get message count
            message_count = len(queue.imq_message_ids) if hasattr(queue, 'imq_message_ids') else 0
            age = self._calculate_age(queue.create_date)
            active_status = "Yes" if queue.active else "No"
            
            print(f"{queue.name:<20} {queue.q_type:<8} {queue.provider:<10} {active_status:<8} {message_count:<10} {age:<15}")
    
    def _print_message_table(self, messages):
        """Print messages in table format"""
        if not messages:
            print("No messages found")
            return
        
        print(f"{'ID':<8} {'NAME':<25} {'QUEUE':<15} {'STATE':<12} {'TYPE':<8} {'AGE':<15}")
        print("-" * 90)
        
        for message in messages:
            age = self._calculate_age(message.create_date)
            name = (message.name[:22] + '...') if len(message.name) > 25 else message.name
            msg_type = message.message_type or 'simple'
            
            print(f"{message.id:<8} {name:<25} {message.queue_id.name:<15} {message.state:<12} {msg_type:<8} {age:<15}")
    
    def _print_processor_table(self, processors):
        """Print processors in table format"""
        if not processors:
            print("No processors found")
            return
        
        print(f"{'ID':<6} {'NAME':<20} {'SELECTOR':<20} {'FUNCTION':<25} {'MODULE':<30}")
        print("-" * 105)
        
        for processor in processors:
            name = (processor.name[:17] + '...') if len(processor.name) > 20 else processor.name
            selector = (processor.selector[:17] + '...') if processor.selector and len(processor.selector) > 20 else (processor.selector or 'N/A')
            function = (processor.function[:22] + '...') if len(processor.function) > 25 else processor.function
            module = (processor.module[:27] + '...') if len(processor.module) > 30 else processor.module
            
            print(f"{processor.id:<6} {name:<20} {selector:<20} {function:<25} {module}")
    
    def _print_processing_table(self, processings):
        """Print processing records in table format"""
        if not processings:
            print("No processing records found")
            return
        
        print(f"{'ID':<8} {'MESSAGE_ID':<12} {'STATE':<12} {'WORKER_TYPE':<15} {'ATTEMPT':<8} {'AGE':<15}")
        print("-" * 80)
        
        for processing in processings:
            age = self._calculate_age(processing.create_date)
            worker_type = (processing.worker_type[:12] + '...') if len(processing.worker_type) > 15 else processing.worker_type
            
            print(f"{processing.id:<8} {processing.message_id.id:<12} {processing.state:<12} {worker_type:<15} {processing.attempt:<8} {age:<15}")
    
    # YAML/JSON formatting methods
    def _print_queue_yaml(self, queues):
        """Print queues in YAML format"""
        data = []
        for queue in queues:
            data.append({
                'name': queue.name,
                'type': queue.q_type,
                'provider': queue.provider,
                'active': queue.active,
                'visibilityTimeout': queue.visibility_timeout,
                'createdAt': self._format_datetime(queue.create_date)
            })
        print(yaml.dump({'queues': data}, default_flow_style=False, indent=2))
    
    def _print_queue_json(self, queues):
        """Print queues in JSON format"""
        data = []
        for queue in queues:
            data.append({
                'name': queue.name,
                'type': queue.q_type,
                'provider': queue.provider,
                'active': queue.active,
                'visibilityTimeout': queue.visibility_timeout,
                'createdAt': self._format_datetime(queue.create_date)
            })
        print(json.dumps({'queues': data}, indent=2, default=str))
    
    def _print_message_yaml(self, messages):
        """Print messages in YAML format"""
        data = []
        for message in messages:
            data.append({
                'id': message.id,
                'messageId': message.queue_message_id,
                'name': message.name,
                'queue': message.queue_id.name,
                'state': message.state,
                'type': message.message_type or 'simple',
                'createdAt': self._format_datetime(message.create_date)
            })
        print(yaml.dump({'messages': data}, default_flow_style=False, indent=2))
    
    def _print_message_json(self, messages):
        """Print messages in JSON format"""
        data = []
        for message in messages:
            data.append({
                'id': message.id,
                'messageId': message.queue_message_id,
                'name': message.name,
                'queue': message.queue_id.name,
                'state': message.state,
                'type': message.message_type or 'simple',
                'createdAt': self._format_datetime(message.create_date)
            })
        print(json.dumps({'messages': data}, indent=2, default=str))
    
    def _print_processor_yaml(self, processors):
        """Print processors in YAML format"""
        data = []
        for processor in processors:
            data.append({
                'id': processor.id,
                'name': processor.name,
                'selector': processor.selector,
                'function': processor.function,
                'module': processor.module,
                'isMethod': processor.is_method,
                'createdAt': self._format_datetime(processor.create_date)
            })
        print(yaml.dump({'processors': data}, default_flow_style=False, indent=2))
    
    def _print_processor_json(self, processors):
        """Print processors in JSON format"""
        data = []
        for processor in processors:
            data.append({
                'id': processor.id,
                'name': processor.name,
                'selector': processor.selector,
                'function': processor.function,
                'module': processor.module,
                'isMethod': processor.is_method,
                'createdAt': self._format_datetime(processor.create_date)
            })
        print(json.dumps({'processors': data}, indent=2, default=str))
    
    def _print_processing_yaml(self, processings):
        """Print processing records in YAML format"""
        data = []
        for processing in processings:
            data.append({
                'id': processing.id,
                'messageId': processing.message_id.id,
                'state': processing.state,
                'workerType': processing.worker_type,
                'attempt': processing.attempt,
                'createdAt': self._format_datetime(processing.create_date)
            })
        print(yaml.dump({'processings': data}, default_flow_style=False, indent=2))
    
    def _print_processing_json(self, processings):
        """Print processing records in JSON format"""
        data = []
        for processing in processings:
            data.append({
                'id': processing.id,
                'messageId': processing.message_id.id,
                'state': processing.state,
                'workerType': processing.worker_type,
                'attempt': processing.attempt,
                'createdAt': self._format_datetime(processing.create_date)
            })
        print(json.dumps({'processings': data}, indent=2, default=str))
    
    # Utility methods
    def _format_datetime(self, dt):
        """Format datetime for output"""
        if not dt:
            return None
        if hasattr(dt, 'isoformat'):
            return dt.isoformat()
        return str(dt)
    
    def _calculate_age(self, created_date):
        """Calculate age from creation date"""
        if not created_date:
            return "Unknown"
        
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        
        # Handle timezone-naive datetime
        if created_date.tzinfo is None:
            created_date = created_date.replace(tzinfo=timezone.utc)
        
        delta = now - created_date
        
        if delta.days > 0:
            return f"{delta.days}d"
        elif delta.seconds > 3600:
            hours = delta.seconds // 3600
            return f"{hours}h"
        elif delta.seconds > 60:
            minutes = delta.seconds // 60
            return f"{minutes}m"
        else:
            return f"{delta.seconds}s"
    
    # Helper methods for output formatting and file operations
    def _format_output(self, data, output_format):
        """Format data according to output format"""
        if output_format == 'yaml':
            return yaml.dump(data, default_flow_style=False, indent=2)
        elif output_format == 'json':
            return json.dumps(data, indent=2, default=str)
        elif output_format == 'stream':
            # For logs - format as human-readable stream
            return data  # Assume data is already formatted as string for stream
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
    
    def _write_output(self, content, args):
        """Write output to file or stdout"""
        if hasattr(args, 'output_file') and args.output_file:
            with open(args.output_file, 'w') as f:
                f.write(content)
            print(f"Output written to {args.output_file}")
        else:
            print(content, end='')
    
    # Logs command methods
    def _logs_processing(self, env, args):
        """Show logs for a specific processing record"""
        if not args.resource_name.isdigit():
            print("Error: Processing ID must be numeric", file=sys.stderr)
            return 1
        
        processing = env['imq.message_processing'].search([('id', '=', int(args.resource_name))], limit=1)
        if not processing:
            print(f"Error: Processing '{args.resource_name}' not found", file=sys.stderr)
            return 1
        
        # Get logs in chronological order
        logs = env['imq.message_processing_log'].search([
            ('processing_id', '=', processing.id)
        ], order='create_date asc')
        
        if args.output == 'json':
            # JSON format - structured data
            data = {
                'apiVersion': 'imq/v1',
                'kind': 'ProcessingLogs',
                'metadata': {
                    'processingId': processing.id,
                    'messageId': processing.message_id.id,
                    'messageName': processing.message_id.name,
                    'workerType': processing.worker_type,
                    'createdAt': self._format_datetime(processing.create_date)
                },
                'spec': {
                    'processing': {
                        'state': processing.state,
                        'attempt': processing.attempt,
                        'startTime': self._format_datetime(processing.start_time),
                        'endTime': self._format_datetime(processing.end_time),
                        'result': processing.result
                    }
                },
                'logs': []
            }
            
            for log in logs:
                data['logs'].append({
                    'id': log.id,
                    'timestamp': self._format_datetime(log.create_date),
                    'loggerName': log.logger_name,
                    'level': log.log_level,
                    'message': log.log_message
                })
            
            output_content = self._format_output(data, 'json')
        else:
            # Stream format - human-readable log output
            header = f"""# Processing Logs for ID: {processing.id}
# Message: {processing.message_id.name} (ID: {processing.message_id.id})
# Worker Type: {processing.worker_type}
# State: {processing.state}
# Attempt: {processing.attempt}
# Start Time: {self._format_datetime(processing.start_time)}
# End Time: {self._format_datetime(processing.end_time)}
# Result: {processing.result}
# Total Log Entries: {len(logs)}
#
# Log Stream:
# -----------

"""
            
            log_lines = []
            for log in logs:
                timestamp = self._format_datetime(log.create_date)
                level = log.log_level if log.log_level else 'INFO'
                logger = log.logger_name or 'Unknown'
                message = log.log_message or ''
                
                # Format timestamp for readability
                if timestamp:
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Remove last 3 digits from microseconds
                    except:
                        formatted_time = timestamp
                else:
                    formatted_time = 'Unknown'
                
                # Format log level
                level_name = {
                    '10': 'DEBUG',
                    '20': 'INFO', 
                    '30': 'WARNING',
                    '40': 'ERROR',
                    '50': 'CRITICAL'
                }.get(str(level), str(level))
                
                # Create log line similar to standard Python logging format
                log_line = f"{formatted_time} {logger} {level_name} {message}"
                log_lines.append(log_line)
            
            # Return as a single string for stream output
            output_content = header + '\n'.join(log_lines) + '\n'
        
        self._write_output(output_content, args)
        return 0
    
    def _logs_message(self, env, args):
        """Show all processing logs for a message"""
        if not args.resource_name.isdigit():
            print("Error: Message ID must be numeric", file=sys.stderr)
            return 1
        
        message = env['imq.message'].search([('id', '=', int(args.resource_name))], limit=1)
        if not message:
            print(f"Error: Message '{args.resource_name}' not found", file=sys.stderr)
            return 1
        
        # Get all processing records for this message
        processings = env['imq.message_processing'].search([
            ('message_id', '=', message.id)
        ], order='create_date asc')
        
        if not processings:
            print(f"No processing records found for message {args.resource_name}")
            return 0
        
        if args.output == 'json':
            # JSON format - structured data
            data = {
                'apiVersion': 'imq/v1',
                'kind': 'MessageLogs',
                'metadata': {
                    'messageId': message.id,
                    'messageUuid': message.queue_message_id,
                    'messageName': message.name,
                    'queueName': message.queue_id.name,
                    'createdAt': self._format_datetime(message.create_date)
                },
                'processings': []
            }
            
            for processing in processings:
                # Get logs for this processing
                logs = env['imq.message_processing_log'].search([
                    ('processing_id', '=', processing.id)
                ], order='create_date asc')
                
                processing_data = {
                    'id': processing.id,
                    'workerType': processing.worker_type,
                    'state': processing.state,
                    'attempt': processing.attempt,
                    'startTime': self._format_datetime(processing.start_time),
                    'endTime': self._format_datetime(processing.end_time),
                    'result': processing.result,
                    'logs': []
                }
                
                for log in logs:
                    processing_data['logs'].append({
                        'id': log.id,
                        'timestamp': self._format_datetime(log.create_date),
                        'loggerName': log.logger_name,
                        'level': log.log_level,
                        'message': log.log_message
                    })
                
                data['processings'].append(processing_data)
            
            output_content = self._format_output(data, 'json')
        else:
            # Stream format - aggregate all logs chronologically
            header = f"""# Message Logs for ID: {message.id}
# Message: {message.name} (UUID: {message.queue_message_id})
# Queue: {message.queue_id.name}
# Processing Records: {len(processings)}
#
# Aggregated Log Stream:
# ---------------------

"""
            
            # Collect all logs from all processings
            all_logs = []
            for processing in processings:
                logs = env['imq.message_processing_log'].search([
                    ('processing_id', '=', processing.id)
                ], order='create_date asc')
                
                for log in logs:
                    all_logs.append({
                        'processing_id': processing.id,
                        'processing_attempt': processing.attempt,
                        'log': log
                    })
            
            # Sort all logs by timestamp
            all_logs.sort(key=lambda x: x['log'].create_date if x['log'].create_date else datetime.min)
            
            log_lines = []
            for log_entry in all_logs:
                log = log_entry['log']
                proc_id = log_entry['processing_id']
                attempt = log_entry['processing_attempt']
                
                timestamp = self._format_datetime(log.create_date)
                level = log.log_level if log.log_level else 'INFO'
                logger = log.logger_name or 'Unknown'
                message_text = log.log_message or ''
                
                # Format timestamp for readability
                if timestamp:
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    except:
                        formatted_time = timestamp
                else:
                    formatted_time = 'Unknown'
                
                # Format log level
                level_name = {
                    '10': 'DEBUG',
                    '20': 'INFO', 
                    '30': 'WARNING',
                    '40': 'ERROR',
                    '50': 'CRITICAL'
                }.get(str(level), str(level))
                
                # Create log line with processing context
                log_line = f"{formatted_time} [P{proc_id}-A{attempt}] {logger} {level_name} {message_text}"
                log_lines.append(log_line)
            
            output_content = header + '\n'.join(log_lines) + '\n'
        
        self._write_output(output_content, args)
        return 0


# Register the command
imq_ctl = IMQCtl()