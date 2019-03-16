# -*- coding: utf-8 -*-
import pickle
import datetime
from openerp import models, fields, api
import openerp
from openerp.exceptions import except_orm
from openerp.tools.translate import _

"""Identified all managed queues."""

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
    provider = fields.Selection(QUEUE_PROVIDERS, required=True)
    region = fields.Char()
    key = fields.Char()
    secret = fields.Char()
    description = fields.Text()

    @api.multi
    def copy(self, default=None):
        self.ensure_one()
        chosen_name = default.get('name') if default else ''
        new_name = chosen_name or _('%s (copy)') % self.name
        default = dict(default or {}, name=new_name)
        return super(IMQQueue, self).copy(default)    
