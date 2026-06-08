#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import logging
from odoo.cli import Command

_logger = logging.getLogger(__name__)


class ImqWorker(Command):
    """Run IMQ worker to process messages from queues"""
    name = 'imq_worker'
    
    def run(self, args):
        """Main entry point for the CLI command"""
        parser = argparse.ArgumentParser(
            prog=f'{sys.argv[0]} imq_worker',
            description='Run standalone IMQ worker for processing message queues',
            formatter_class=argparse.RawTextHelpFormatter,
        )
        
        # Required arguments
        parser.add_argument('--database', '-d', required=False,
                          help='Database name to connect to. Default to $PGDATABASE')
        parser.add_argument('--queues', '-q', default=None,
                          help=(
                              'Queue selector: a single pattern that can match one '
                              'or several active queues. Three accepted forms:\n'
                              '  - exact name:   --queues=default\n'
                              '  - glob:         --queues="mpy.*"   or   --queues="*"\n'
                              '  - regex:        --queues="(default|pack8s|healthcheck)"\n'
                              'To target a specific list of queues by name, use a '
                              'regex alternation as shown above.'
                          ))
        parser.add_argument('--queue', default=None,
                          help=(
                              '[DEPRECATED] Alias for --queues with identical '
                              'semantics. Use --queues instead. Emits a deprecation '
                              'warning on stderr when used.'
                          ))
        
        # Optional limits
        parser.add_argument('--max-messages', type=int, default=0,
                          help='Exit after processing N messages (0=unlimited)')
        parser.add_argument('--max-rss-memory', type=str, default=None,
                          help='Exit when RSS memory exceeds limit (e.g., 1024M)')
        parser.add_argument('--max-thread-delta', type=int, default=0,
                          help='Exit worker when OS-thread count grows by more '
                               'than N above the startup baseline. Requires an '
                               'external supervisor (K8s/systemd) to restart '
                               'the worker. 0=disabled.')
        parser.add_argument('--thread-warn-percent', type=int, default=50,
                          help='Log post-task thread sample at INFO when '
                               'thread_delta >= this %% of --max-thread-delta '
                               '(default 50). Ignored if --max-thread-delta=0.')
        parser.add_argument('--status-summary-interval-s', type=int, default=300,
                          help='Wall-clock interval in seconds between periodic '
                               'status summary logs (default 300 = 5 min). '
                               '0 disables the periodic summary (shutdown and '
                               'leak-detection summaries still fire).')
        
        # Worker configuration
        parser.add_argument('--worker-name', '-w', default=None,
                          help='Worker identifier for logging')
        parser.add_argument('--log-level', default='INFO',
                          choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                          help='Logging level')
        
        # Observability
        parser.add_argument('--observability-port', type=int, default=0,
                          help='Port for liveness probe and metrics HTTP server (Default is 0=disabled)')
        parser.add_argument('--metrics-path', type=str, default='/metrics',
                          help='HTTP path for Prometheus metrics endpoint')
        parser.add_argument('--queue-depth-caching-period-s', type=int, default=30,
                          help='Queue depth metrics caching period in seconds (Default: 30)')
        parser.add_argument('--metrics-export-mode', type=str, default='network',
                          choices=['network', 'textfile'],
                          help='Metrics export mode: "network" (HTTP, default) or "textfile" (node_exporter textfile collector)')
        parser.add_argument('--textfile-dir', type=str, default='/var/lib/node_exporter/textfile',
                          help='Directory for textfile collector output (used with --metrics-export-mode=textfile)')
        
        # Message targeting
        parser.add_argument('--message', '-m', type=str, default=None,
                          help='Optional: Process specific message by ID or MessageId within the specified queue')
        
        # Parse arguments
        try:
            parsed_args = parser.parse_args(args)
        except SystemExit as e:
            # argparse calls sys.exit on error, we want to return the code instead
            return e.code

        # Resolve --queues / --queue (deprecated alias)
        if parsed_args.queue and parsed_args.queues:
            print("Error: --queue and --queues are mutually exclusive. Use --queues.",
                  file=sys.stderr)
            return 1
        queue_value = parsed_args.queues or parsed_args.queue
        if not queue_value:
            print("Error: --queues is required.", file=sys.stderr)
            return 1
        if parsed_args.queue:
            print("Warning: --queue is deprecated, use --queues instead.",
                  file=sys.stderr)

        # Validate arguments
        if parsed_args.database:
            database_name = parsed_args.database
        else:
            database_name = os.environ.get('PGDATABASE')

        if not database_name:
            print("Error: --database not set and $PGDATABASE is not defined.", file=sys.stderr)
            return 1

        if parsed_args.max_messages < 0:
            print("Error: --max-messages must be >= 0", file=sys.stderr)
            return 1

        if parsed_args.max_thread_delta < 0:
            print("Error: --max-thread-delta must be >= 0", file=sys.stderr)
            return 1

        if not (0 < parsed_args.thread_warn_percent <= 100):
            print("Error: --thread-warn-percent must be in (0, 100]", file=sys.stderr)
            return 1

        if parsed_args.status_summary_interval_s < 0:
            print("Error: --status-summary-interval-s must be >= 0", file=sys.stderr)
            return 1
            
        if parsed_args.observability_port < 0 or parsed_args.observability_port > 65535:
            print("Error: --observability-port must be between 0 and 65535", file=sys.stderr)
            return 1
            
        if not parsed_args.metrics_path.startswith('/'):
            print("Error: --metrics-path must start with '/'", file=sys.stderr)
            return 1
            
        if parsed_args.queue_depth_caching_period_s < 1:
            print("Error: --queue-depth-caching-period-s must be >= 1 second", file=sys.stderr)
            return 1

        if parsed_args.metrics_export_mode == 'textfile' and parsed_args.observability_port:
            print("Warning: --observability-port is ignored in textfile mode", file=sys.stderr)
            
        # Validate message ID format if provided
        if parsed_args.message:
            import re
            uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
            if not (parsed_args.message.isdigit() or re.match(uuid_pattern, parsed_args.message)):
                print("Error: --message must be a numeric ID or valid MessageId UUID", file=sys.stderr)
                return 1
        
        # Set up logging
        log_level = getattr(logging, parsed_args.log_level.upper())
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s %(name)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Import and initialize worker (import here to avoid circular imports)
        try:
            from odoo.addons.inouk_message_queue.workers.standalone import StandaloneWorker
            
            # Create worker instance
            worker = StandaloneWorker(
                database=database_name,
                queue_pattern=queue_value,
                max_messages=parsed_args.max_messages,
                max_rss_memory=parsed_args.max_rss_memory,
                max_thread_delta=parsed_args.max_thread_delta,
                thread_warn_percent=parsed_args.thread_warn_percent,
                status_summary_interval_s=parsed_args.status_summary_interval_s,
                worker_name=parsed_args.worker_name,
                log_level=parsed_args.log_level,
                observability_port=parsed_args.observability_port,
                metrics_path=parsed_args.metrics_path,
                target_message_id=parsed_args.message,
                queue_depth_caching_period_s=parsed_args.queue_depth_caching_period_s,
                metrics_export_mode=parsed_args.metrics_export_mode,
                textfile_dir=parsed_args.textfile_dir
            )
            
            # Run worker
            return worker.run()
            
        except ImportError as e:
            _logger.error(f"Failed to import worker components: {e}")
            return 1
        except Exception as e:
            _logger.error(f"Failed to initialize worker: {e}")
            return 1