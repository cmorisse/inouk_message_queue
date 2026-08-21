# -*- coding: utf-8 -*-
{
    'name': 'Inouk Message Queue',
    'summary': 'Inouk Message Queue a.k.a. IMQ is a task queue for Odoo: turn a model\nmethod into an async task with one decorator, and get exactly-once delivery, ordered\nFIFO processing, automatic retries and full log capture.',
    'description': 'A task queue built for Odoo business applications, where transactional\nsafety and auditability matter more than raw throughput. Runs on PostgreSQL with no extra\ninfrastructure, or on AWS SQS, switchable per queue without touching application code.\nTasks are consumed by standalone workers exposing Prometheus metrics and Kubernetes\nhealth probes. See README.md.',
    'author': 'Cyril MORISSE (twitter @cmorisse)',
    # FSL-1.1-MIT (Functional Source License) — see the LICENSE file.
    # Odoo's 'license' vocabulary has no FSL value; 'Other proprietary' is the
    # closest honest mapping — FSL is source-available, not OSI-approved
    # (each version converts to MIT two years after its release).
    'license': 'Other proprietary',
    'category': 'Extra Tools',
    'version': '0.5.0',
    'depends': ['inouk_core', 'mail'],
    'data': [
        'security/res_users.xml',
        'security/res_groups.xml',
        'security/ir.model.access.csv',
        'security/ir_rule.xml',
        'data/mail_channel.xml',
        'data/ir_cron_worker.xml',
        'data/ir_config_parameter.xml',
        'data/imq_queue.xml',
        'data/imq_message_processor.xml',
        'views/queue_views.xml',
        'views/message_processing_log.xml',
        'views/message_processing.xml',
        'views/message_processor.xml',
        'views/message_views.xml',
        'views/test_launcher.xml',
        'views/ir_cron_views.xml',
        'menu.xml'
    ],
    'demo': [],
    'application': True,
    'auto_install': False,
    'installable': True,
    'external_dependencies': {
        'python': ['psutil', 'prometheus_client'],
    }
}
