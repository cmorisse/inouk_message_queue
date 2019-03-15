# -*- coding: utf-8 -*-
import pickle
import datetime
from openerp import models, fields, api
import openerp
from openerp.exceptions import except_orm
from openerp.tools.translate import _

"""Identified all managed queues."""

QUEUE_TYPES = [
    ('aws_sqs', "AWS SQS"),
]

class IMQQueue(models.Model):
    _name = 'imq.queue'
    _description = "IMQ - Queue"
    _order= 'name'
    _sql_constraints = [
        ('name_queue_uniq', 'UNIQUE(type,name)', _("Queue name must be unique."))
    ]

    # fields
    name = fields.Char(size=20, index=True, uniq=True, required=True)
    type = fields.Selection(QUEUE_TYPES)
    region = fields.Char()
    key = fields.Char()
    secret = fields.Char()
    description = fields.Text()



