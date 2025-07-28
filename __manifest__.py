# -*- coding: utf-8 -*-
{
    'name': 'Inouk Message Queue',
    'summary': 'Inouk Message Queue a.k.a. IMQ allows to process tasks \nand interconnect asynchronously Odoo subprocesses with external programs using\ncloud message queues (AWS SQS for now but more to come).',
    'description': 'A toolkit to manage heavyweight tasks and distribute them \nas asynchronous processing on workers that can be Odoo, AWS Lambda or any program\nable to consume Messages.',
    'author': 'Cyril MORISSE (twitter @cmorisse)',
    'license': 'OPL-1',
    'category': 'Extra Tools',
    'version': '0.1',
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
    'installable': True
}
