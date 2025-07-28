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


class IMQDump(Command):
    """Dump IMQ objects in YAML or JSON format"""
    name = 'imqdump'
    
    def run(self, args):
        """Main entry point for the CLI command"""
        parser = argparse.ArgumentParser(
            prog=f'{sys.argv[0]} imqdump',
            description='Dump IMQ objects (messages, queues, processors, processing) in YAML or JSON format'
        )
        
        # Required arguments
        parser.add_argument('--database', '-d', required=True,
                          help='Database name to connect to')
        
        # Object type selection
        object_group = parser.add_mutually_exclusive_group(required=True)
        object_group.add_argument('--message', '-m', type=str, metavar='ID',
                                help='Dump message by ID or MessageId')
        object_group.add_argument('--queue', '-q', type=str, metavar='NAME',
                                help='Dump queue by name')
        object_group.add_argument('--processor', '-p', type=str, metavar='ID_OR_SELECTOR',
                                help='Dump processor by ID or selector')
        object_group.add_argument('--processing', type=str, metavar='ID',
                                help='Dump message processing by ID')
        object_group.add_argument('--logs', type=str, metavar='PROCESSING_ID',
                                help='Dump processing logs stream for processing ID')
        
        # Output format
        parser.add_argument('--json', action='store_true',
                          help='Output in JSON format (default: YAML)')
        parser.add_argument('--output', '-o', type=str, default=None,
                          help='Output file path (default: stdout)')
        
        # Filtering and options
        parser.add_argument('--include-logs', action='store_true',
                          help='Include processing logs for messages/processing objects')
        parser.add_argument('--verbose', '-v', action='store_true',
                          help='Verbose output with debug information')
        
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
        
        # Connect to database and execute dump
        try:
            with api.Environment.manage():
                registry = Registry(parsed_args.database)
                with registry.cursor() as cr:
                    env = api.Environment(cr, SUPERUSER_ID, {})
                    
                    # Route to appropriate dump method
                    if parsed_args.message:
                        result = self._dump_message(env, parsed_args)
                    elif parsed_args.queue:
                        result = self._dump_queue(env, parsed_args)
                    elif parsed_args.processor:
                        result = self._dump_processor(env, parsed_args)
                    elif parsed_args.processing:
                        result = self._dump_processing(env, parsed_args)
                    elif parsed_args.logs:
                        result = self._dump_logs(env, parsed_args)
                    else:
                        print("Error: No object type specified", file=sys.stderr)
                        return 1
                    
                    if result is None:
                        return 1
                    
                    # Format and output result
                    self._output_result(result, parsed_args)
                    return 0
                    
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            if parsed_args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def _dump_message(self, env, args):
        """Dump message object"""
        message_id = args.message
        
        # Try to find message by ID (numeric) or MessageId (UUID)
        if message_id.isdigit():
            message = env['imq.message'].search([('id', '=', int(message_id))], limit=1)
        else:
            message = env['imq.message'].search([('queue_message_id', '=', message_id)], limit=1)
        
        if not message:
            print(f"Error: Message '{message_id}' not found", file=sys.stderr)
            return None
        
        # Build message data
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
        
        # Add processing history
        processings = env['imq.message_processing'].search([
            ('message_id', '=', message.id)
        ], order='create_date desc')
        
        if processings:
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
                if args.include_logs:
                    logs = env['imq.message_processing_log'].search([
                        ('processing_id', '=', proc.id)
                    ], order='create_date asc')
                    
                    if logs:
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
        
        return data
    
    def _dump_queue(self, env, args):
        """Dump queue object"""
        queue_name = args.queue
        
        queue = env['imq.queue'].search([('name', '=', queue_name)], limit=1)
        if not queue:
            print(f"Error: Queue '{queue_name}' not found", file=sys.stderr)
            return None
        
        # Get queue statistics
        message_stats = env['imq.message'].read_group([
            ('queue_id', '=', queue.id)
        ], ['state'], ['state'])
        
        stats = {}
        for stat in message_stats:
            stats[stat['state']] = stat['state_count']
        
        data = {
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
        
        # Add optional fields if they exist
        if hasattr(queue, 'message_retention_period'):
            data['spec']['messageRetentionPeriod'] = queue.message_retention_period
        
        return data
    
    def _dump_processor(self, env, args):
        """Dump processor object"""
        processor_ref = args.processor
        
        # Try to find processor by ID (numeric) or selector
        if processor_ref.isdigit():
            processor = env['imq.message_processor'].search([('id', '=', int(processor_ref))], limit=1)
        else:
            processor = env['imq.message_processor'].search([('selector', '=', processor_ref)], limit=1)
        
        if not processor:
            print(f"Error: Processor '{processor_ref}' not found", file=sys.stderr)
            return None
        
        data = {
            'apiVersion': 'imq/v1',
            'kind': 'MessageProcessor',
            'metadata': {
                'id': processor.id,
                'name': processor.name,
                'selector': processor.selector,
                'createdAt': self._format_datetime(processor.create_date),
                'updatedAt': self._format_datetime(processor.write_date)
            },
            'spec': {
                'function': processor.function,
                'module': processor.module,
                'isMethod': processor.is_method,
                'maxAttempt': processor.max_attempt,
                'retryDelayS': processor.retry_delay_s,
                'loggingActivated': processor.logging_activated,
                'captureLog': processor.capture_log,
                'captureConsole': processor.capture_console,
                'logLevel': processor.log_level,
                'logFormat': processor.log_format,
                'forceVisibilityTimeout': processor.force_visibility_timeout,
                'visibilityTimeout': processor.visibility_timeout,
                'notifications': {
                    'messageProcessingStart': processor.notify_message_processing_start,
                    'messageProcessingEnd': processor.notify_message_processing_end,
                    'messageProcessingFail': processor.notify_message_processing_fail,
                    'messageProcessingRetry': processor.notify_message_processing_retry,
                    'messageProcessingTerminate': processor.notify_message_processing_terminate
                }
            }
        }
        
        return data
    
    def _dump_processing(self, env, args):
        """Dump message processing object"""
        processing_id = args.processing
        
        if not processing_id.isdigit():
            print(f"Error: Processing ID must be numeric", file=sys.stderr)
            return None
        
        processing = env['imq.message_processing'].search([('id', '=', int(processing_id))], limit=1)
        if not processing:
            print(f"Error: Processing '{processing_id}' not found", file=sys.stderr)
            return None
        
        data = {
            'apiVersion': 'imq/v1',
            'kind': 'MessageProcessing',
            'metadata': {
                'id': processing.id,
                'createdAt': self._format_datetime(processing.create_date),
                'updatedAt': self._format_datetime(processing.write_date)
            },
            'spec': {
                'message': {
                    'id': processing.message_id.id,
                    'messageId': processing.message_id.queue_message_id,
                    'name': processing.message_id.name
                },
                'workerType': processing.worker_type
            },
            'status': {
                'state': processing.state,
                'attempt': processing.attempt,
                'startTime': self._format_datetime(processing.start_time),
                'endTime': self._format_datetime(processing.end_time),
                'result': processing.result
            }
        }
        
        # Add logs if requested
        if args.include_logs:
            logs = env['imq.message_processing_log'].search([
                ('processing_id', '=', processing.id)
            ], order='create_date asc')
            
            if logs:
                data['status']['logs'] = []
                for log in logs:
                    data['status']['logs'].append({
                        'id': log.id,
                        'loggerName': log.logger_name,
                        'logLevel': log.log_level,
                        'message': log.log_message,
                        'createdAt': self._format_datetime(log.create_date)
                    })
        
        return data
    
    def _dump_logs(self, env, args):
        """Dump processing logs in streaming format"""
        processing_id = args.logs
        
        if not processing_id.isdigit():
            print(f"Error: Processing ID must be numeric", file=sys.stderr)
            return None
        
        processing = env['imq.message_processing'].search([('id', '=', int(processing_id))], limit=1)
        if not processing:
            print(f"Error: Processing '{processing_id}' not found", file=sys.stderr)
            return None
        
        # Get logs in chronological order
        logs = env['imq.message_processing_log'].search([
            ('processing_id', '=', processing.id)
        ], order='create_date asc')
        
        if args.json:
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
            
            return data
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
            return header + '\n'.join(log_lines) + '\n'
    
    def _format_datetime(self, dt):
        """Format datetime for output"""
        if not dt:
            return None
        if hasattr(dt, 'isoformat'):
            return dt.isoformat()
        return str(dt)
    
    def _output_result(self, data, args):
        """Output result in requested format"""
        # Handle string format (for log streams)
        if isinstance(data, str):
            output = data
        elif args.json:
            output = json.dumps(data, indent=2, default=str)
        else:
            output = yaml.dump(data, default_flow_style=False, indent=2)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"Output written to {args.output}")
        else:
            print(output, end='')


# Register the command
imq_dump = IMQDump()