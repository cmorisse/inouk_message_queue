import pickle
import datetime
import json
import logging
import sys
import traceback
import base64
import uuid

import requests
# slackdown removed - using plain text for Odoo 18 notifications

import odoo
from odoo import models, fields, api
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
        """Send Odoo notifications using Odoo 18 standard simple_notification.

        :param message_type: "danger", "warning", "success" or "info".
        :param message_title: The title of the notification.
        :param message: The message text (slack markdown format).
        :param icon: DEPRECATED - Ignored in Odoo 18.
        :param message_obj: Optional IMQ message object to include button in notification.
        :param sticky: When set the notification must be explicitly closed.
        :param user_obj: Target user(s). If None, broadcasts to all IMQ administrators.
                         If set, notifies only that specific user.
        """
        for record in self:
            if record.use_odoo_notifications:

                # Build action_button if message_obj is provided
                action_button = None
                if message_obj:
                    action_button = {
                        'model': message_obj._name,
                        'res_id': message_obj.id,
                        'name': 'Open IMQ Message',
                    }
                    body_html = message_obj.name
                else:
                    body_html = message or ""
                # Determine target users
                if user_obj:
                    # Notify specific user(s) only
                    target_users = user_obj
                else:
                    # Broadcast to all IMQ administrators
                    imq_admin_group = self.env.ref(
                        'inouk_message_queue.group_admin',
                        raise_if_not_found=False
                    )
                    if imq_admin_group:
                        target_users = imq_admin_group.users.filtered(
                            lambda u: u.active and not u.share
                        )
                    else:
                        _logger.warning(
                            "IMQ admin group not found, falling back to current user"
                        )
                        target_users = self.env.user

                for user in target_users:
                    try:
                        user.ik_notify(
                            message_type,
                            message_title or "",
                            body_html,
                            sticky=True if (message_type == 'danger' or sticky) else False,
                            action_button=action_button,
                        )
                    except AttributeError:
                        _logger.error(
                            "Failed to call res.users::ik_notify(). "
                            "Is Addon inouk_notifications installed?"
                        )
                    except Exception as e:
                        _logger.error("Failed to notify user %s: %s", user.login, e)

    def btn_test_odoo_notifications(self):
        """Send an Odoo test notification to all IMQ administrators."""
        self.ensure_one()
        message = "Queue: *%s* is ready to send notifications." % self.name
        self.send_odoo_notification(
            'info',
            "IMQ Notification Test",
            message,
            sticky=True,
            # user_obj=None by default → broadcasts to all users
        )
        return True

