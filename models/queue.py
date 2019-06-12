import pickle
import datetime
import json

from odoo import models, fields, api
import odoo
from odoo.exceptions import except_orm
from odoo.tools.translate import _

from ..api import send_message


"""Identifies all managed queues."""

QUEUE_PROVIDERS = [
    ('aws_sqs', "AWS SQS"),
]

class IMQQueue(models.Model):
    _name = 'imq.queue'
    _description = "IMQ - Queue"
    _order= 'name'
    _sql_constraints = [
        (
            'name_queue_uniq', 
            'UNIQUE(name)', 
            _("Queue name must be unique among all queue providers.")
        )
    ]
    
    # fields
    name = fields.Char(size=20, index=True, uniq=True, required=True)
    sqs_name = fields.Char(compute='_compute_sqs_name')
    provider = fields.Selection(QUEUE_PROVIDERS, required=True)
    region = fields.Char()
    key = fields.Char()
    secret = fields.Char()
    description = fields.Text()
    test_result = fields.Text()

    @api.multi
    @api.depends('name')
    def _compute_sqs_name(self):
        for record in self:
            record.sqs_name = "{}_{}".format(
                record.name,
                record.env.cr.dbname
            )



    @api.multi
    def copy(self, default=None):
        self.ensure_one()
        chosen_name = default.get('name') if default else ''
        new_name = chosen_name or _('%s (copy)') % self.name
        default = dict(default or {}, name=new_name)
        return super(IMQQueue, self).copy(default)    


    @api.multi
    def btn_send_simple_message(self):
        """ Sends a simple message"""
        self.ensure_one()
        result = send_message(
            self.env,
            self.name,
            "TestMessage",
            None,  # payload
            None,  # MessageGroup
            "This is a test Message name",
        )
        result_str = json.dumps(result, sort_keys=True, indent=4)
        self.test_result = result_str

