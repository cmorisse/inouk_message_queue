import pickle
import datetime
import json
import logging
import sys
import traceback
import base64
import uuid

import requests
import slackdown

import odoo
from odoo import models, fields, api
from odoo.exceptions import except_orm
from odoo.tools import DEFAULT_SERVER_DATETIME_FORMAT
from odoo.tools.translate import _
from odoo.tools.safe_eval import safe_eval

import boto3
import pymsteams

from ..api import send_message

_logger = logging.getLogger('IMQ.queue')


class IMQOdooQueue(models.Model):
    _inherit = 'imq.queue'

    use_odoo_notifications = fields.Boolean(default=True)

    def render_icon__odoo(self, icon:str):
        """ return icon as Font Awesome class """
        icon_map = {
            ":bear:": "fa-paw",
        }
        _icon = icon_map.get(icon, None)
        return _icon

    def send_odoo_notification(
        self, message_type, message_title, message, icon=None, message_obj=None, 
        sticky=False, user_obj=None
    ):
        """ Send Odoo notifications to all queues in record set. 
        :param message: when formatted, must use slack markdown
        """
        for record in self:
            if record.use_odoo_notifications:
                body_html = slackdown.render(message or "")
                if message_obj:
                    _now = datetime.datetime.now().strftime(DEFAULT_SERVER_DATETIME_FORMAT)
                    obj_url = f'<b>Message: </b><a href="{message_obj.get_form_url()}">{message_obj.name}</a> <br> <b>At: </b>{_now}'
                    body_html += obj_url
                if user_obj is None:
                    user_obj = self.env.user
                try:
                    user_obj.ik_notify(
                        message_type,
                        message_title or "",
                        body_html, 
                        force_icon=self.render_icon__odoo(icon),
                        sticky=True if (message_type=='danger' or sticky) else False,
                    )
                except AttributeError:
                    _logger.error("Failed to call res.users::ik_notify(). Is Addon inouk_notifications installed ?")
                except:
                    raise

    def btn_test_odoo_notifications(self):
        """ Sends an Odoo test notifications."""
        self.ensure_one()
        message = "Queue: *%s* is ready to send notifications." % self.name
        self.send_odoo_notification(
            'info',
            "IMQ Notification Test",
            message, 
            icon=":bear:",  # 'fa-paw',
            sticky=True
        )
        return True

