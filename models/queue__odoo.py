import pickle
import datetime
import json
import logging
import sys
import traceback
import base64
import uuid

import requests
from markupsafe import Markup
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

    def _imq_system_users(self):
        """Users that must never be the *target* of a processing notification.

        These identities execute work on nobody's behalf in particular (daemon,
        cron, superuser), so notifying them notifies no one. A message whose
        candidate recipient resolves to one of them falls back to the admin
        broadcast in :meth:`_resolve_notification_recipients`.

        :return: a (possibly empty) 'res.users' recordset.
        """
        users = self.env['res.users']
        for xmlid in ('base.user_root', 'inouk_message_queue.user_imq'):
            users |= self.env.ref(xmlid, raise_if_not_found=False) or users
        return users

    def _get_imq_admin_users(self):
        """Notifiable members of the IMQ admin group — the historical broadcast target.

        Falls back to the current user when the group is missing (fresh/partial
        install), so a notification is never silently dropped.

        :return: a 'res.users' recordset, never empty.
        """
        imq_admin_group = self.env.ref(
            'inouk_message_queue.group_admin',
            raise_if_not_found=False
        )
        if not imq_admin_group:
            _logger.warning(
                "IMQ admin group not found, falling back to current user"
            )
            return self.env.user
        return imq_admin_group.users.filtered(lambda u: u.active and not u.share)

    def _resolve_notification_recipients(self, message_obj, message_type):
        """Decide who receives a processing notification.

        Contract (this method is the single authority on toast routing — the
        workers only *raise* notifications, they never choose recipients):

        1. ``message_obj.requesting_user_id`` — set on escalated paths, where the
           executing identity is a system user acting on someone's behalf
           (``_imq_requesting_user_id`` at enqueue time). Always preferred.
        2. else ``message_obj.user_id`` — defaults to ``env.user`` at enqueue, so
           it names whoever launched the task on ordinary (non-escalated) paths.
           This is what makes requester-routing work without touching a single
           caller.
        3. A candidate that is inactive, a portal/share user, or one of
           :meth:`_imq_system_users` is rejected and we fall back to (4).
        4. No usable candidate (including ``message_obj=None``, e.g. the queue
           test button) → broadcast to :meth:`_get_imq_admin_users`.
        5. ``message_type == 'danger'`` → the admin broadcast is added **on top
           of** the requester. Failures stay operationally visible; only routine
           success/info traffic is narrowed.

        Caveat worth knowing: an escalated task that omits
        ``_imq_requesting_user_id`` falls through to (2) and notifies the
        escalation identity itself (e.g. ``mgx-system``). The fix belongs at the
        caller — pass the requesting user — not here.

        :param message_obj: the 'imq.message' being processed, or None.
        :param message_type: "danger", "warning", "success" or "info".
        :return: a 'res.users' recordset.
        """
        recipients = self.env['res.users']
        if message_obj:
            candidate = message_obj.requesting_user_id or message_obj.user_id
            if (
                candidate
                and candidate.active
                and not candidate.share
                and candidate not in self._imq_system_users()
            ):
                recipients = candidate

        if message_type == 'danger' or not recipients:
            recipients |= self._get_imq_admin_users()

        return recipients

    def _build_notification_body(self, message, message_obj):
        """Compose the toast body: message name, then the caller's text below it.

        The name alone answers "what ran"; the caller-supplied ``message`` adds
        "how it went" (duration on success, exception class on failure). Both are
        escaped through ``Markup.__mod__`` because the browser renders this body
        with OWL's ``markup()`` — an unescaped message name would be an HTML
        injection vector.

        :param message: caller text (may be empty — then only the name is shown).
        :param message_obj: the 'imq.message', or None to use ``message`` alone.
        :return: str or Markup, ready for ``ik_notify``.
        """
        if not message_obj:
            return message or ""
        if not message:
            return message_obj.name or ""
        return Markup('%s<div class="text-muted small">%s</div>') % (
            message_obj.name or "",
            message,
        )

    def send_odoo_notification(
        self, message_type, message_title, message, icon=None, message_obj=None,
        sticky=False, user_obj=None
    ):
        """Send Odoo notifications using Odoo 18 standard simple_notification.

        :param message_type: "danger", "warning", "success" or "info".
        :param message_title: The title of the notification.
        :param message: The message text (slack markdown format). Rendered below
            the message name — this is where the processing duration and the
            exception class end up.
        :param icon: DEPRECATED - Ignored in Odoo 18.
        :param message_obj: Optional IMQ message object. Provides the button
            target, the body's first line, and the routing candidates.
        :param sticky: When set the notification must be explicitly closed.
        :param user_obj: Target user(s). When set, notifies exactly those users.
            When None, recipients are resolved from the message — see
            :meth:`_resolve_notification_recipients` for the full contract.
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
                body_html = record._build_notification_body(message, message_obj)

                # Determine target users: an explicit user_obj always wins,
                # otherwise route to whoever launched the task.
                target_users = user_obj or record._resolve_notification_recipients(
                    message_obj, message_type
                )

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

