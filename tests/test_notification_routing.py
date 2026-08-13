# -*- coding: utf-8 -*-
"""Tests for processing-notification routing and body composition.

Both concerns live in imq.queue (models/queue__odoo.py) and are exercised
directly — no bus, no worker, no OWL rendering:

  - _resolve_notification_recipients(): who gets the toast (requester first,
    admin broadcast as fallback and on failures);
  - _build_notification_body(): the message name, with the caller's text
    (duration on success, exception class on failure) rendered below it.
"""

from markupsafe import Markup

from odoo.tests.common import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestNotificationRouting(TransactionCase):

    def setUp(self):
        super().setUp()
        self.queue = self.env.ref('inouk_message_queue.imq_queue__debug')
        self.admin_group = self.env.ref('inouk_message_queue.group_admin')

        self.requester = self.env['res.users'].create({
            'name': "Notification Requester",
            'login': 'imq_notif_requester',
        })
        self.launcher = self.env['res.users'].create({
            'name': "Notification Launcher",
            'login': 'imq_notif_launcher',
        })

    def _message(self, user_id=None, requesting_user_id=None, name="A task"):
        return self.env['imq.message'].create({
            'name': name,
            'queue_id': self.queue.id,
            'user_id': (user_id or self.launcher).id,
            'requesting_user_id': requesting_user_id and requesting_user_id.id,
        })

    def _admins(self):
        return self.admin_group.users.filtered(lambda u: u.active and not u.share)

    #
    # Recipient resolution
    #
    def test_requesting_user_wins_over_user_id(self):
        """An escalated task notifies the requester, not the executing identity."""
        msg = self._message(
            user_id=self.launcher, requesting_user_id=self.requester
        )
        recipients = self.queue._resolve_notification_recipients(msg, 'success')
        self.assertEqual(recipients, self.requester)

    def test_falls_back_to_user_id(self):
        """Ordinary paths carry no requesting user — user_id names the launcher."""
        msg = self._message(user_id=self.launcher)
        recipients = self.queue._resolve_notification_recipients(msg, 'success')
        self.assertEqual(recipients, self.launcher)

    def test_inactive_candidate_falls_back_to_admins(self):
        msg = self._message(user_id=self.launcher)
        self.launcher.active = False
        recipients = self.queue._resolve_notification_recipients(msg, 'success')
        self.assertEqual(recipients, self._admins())

    def test_share_candidate_falls_back_to_admins(self):
        # `share` is computed from groups_id — it cannot be set directly.
        portal = self.env['res.users'].create({
            'name': "Portal Watcher",
            'login': 'imq_notif_portal',
            'groups_id': [(6, 0, [self.env.ref('base.group_portal').id])],
        })
        self.assertTrue(portal.share)
        msg = self._message(user_id=portal)
        recipients = self.queue._resolve_notification_recipients(msg, 'success')
        self.assertEqual(recipients, self._admins())

    def test_system_candidate_falls_back_to_admins(self):
        """user_imq acts on nobody's behalf — notifying it notifies no one."""
        msg = self._message(user_id=self.env.ref('inouk_message_queue.user_imq'))
        recipients = self.queue._resolve_notification_recipients(msg, 'success')
        self.assertEqual(recipients, self._admins())

    def test_failure_also_broadcasts_to_admins(self):
        """danger keeps operational visibility: requester UNION admins."""
        msg = self._message(user_id=self.launcher)
        recipients = self.queue._resolve_notification_recipients(msg, 'danger')
        self.assertIn(self.launcher, recipients)
        for admin in self._admins():
            self.assertIn(admin, recipients)

    def test_no_message_broadcasts_to_admins(self):
        """Non-regression for btn_test_odoo_notifications (no message_obj)."""
        recipients = self.queue._resolve_notification_recipients(None, 'info')
        self.assertEqual(recipients, self._admins())

    #
    # Body composition
    #
    def test_body_carries_name_and_duration(self):
        msg = self._message(name="Create DB 'msa2' on cluster 'pgha-msa2'")
        body = self.queue._build_notification_body(
            "Duration=0hours0min4s", msg
        )
        self.assertIn("Create DB &#39;msa2&#39; on cluster &#39;pgha-msa2&#39;", body)
        self.assertIn("Duration=0hours0min4s", body)

    def test_body_without_caller_text_is_the_name_alone(self):
        """Start-of-processing passes message="" — no empty <div> is appended."""
        msg = self._message(name="A task")
        body = self.queue._build_notification_body("", msg)
        self.assertEqual(body, "A task")

    def test_body_escapes_the_message_name(self):
        """The browser renders this body with markup() — never inject raw HTML."""
        msg = self._message(name="<b>boom</b>")
        body = self.queue._build_notification_body("Duration=0hours0min1s", msg)
        self.assertIsInstance(body, Markup)
        self.assertNotIn("<b>boom</b>", body)
        self.assertIn("&lt;b&gt;boom&lt;/b&gt;", body)

    def test_body_without_message_obj_is_the_caller_text(self):
        body = self.queue._build_notification_body("Plain text", None)
        self.assertEqual(body, "Plain text")
