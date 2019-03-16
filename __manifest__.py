##############################################################################
#
#    Inouk Message Queue
#    Copyright (c) 2018-2019  Cyril MORISSE (twitter: @cmorisse)
#
##############################################################################
{
    'name': "Inouk Message Queue",
    'summary': """Inouk Message Queue a.k.a. IMQ allows to process tasks 
and interconnect asynchronously Odoo subprocesses with external programs using
cloud message queues (AWS SQS for now but more to come).""",

    'description': """A toolkit to manage heavyweight tasks and distribute them 
as asynchronous processing on workers that can be Odoo, AWS Lambda or any program
able to consume Messages.""",

    'author': "Cyril MORISSE (twitter @cmorisse)",
    'license': 'OPL-1',
    #'website': "",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/master/openerp/addons/base/module/module_data.xml
    # for the full list
    'category': 'Extra Tools',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': [
        'base',
        'inouk_core'
    ],

    # always loaded
    'data': [
        #'views/web_assets_loader.xml',

        # Configuration data
        'data/ir_cron_worker.xml',
        'data/imq_queue.xml',

        # Security objects first as other objects references them
        'security/res_users.xml',
        'security/groups.xml',
        'security/ir.model.access.csv',
        'security/ir_rule.xml',

        # views
        'views/queue.xml',
        'views/message_processing_log.xml',
        'views/message_processing.xml',
        'views/message_processor.xml',
        'views/message.xml',
        'views/test_launcher.xml',
        
        # menus: after views and wizards
        'menu.xml',

    ],
    # only loaded in demonstration mode
    'demo': [
        #'demo_data/demo.xml',
    ],
    'application': True,
    'auto_install': False,
    'installable': True
}
