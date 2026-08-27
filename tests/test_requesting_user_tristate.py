# -*- coding: utf-8 -*-
"""The three cases of `_imq_requesting_user_id` at enqueue (docs/multi_tenancy.md).

An id attributes the message to that user; omitting the kwarg attributes it to the
enqueuing user; `False`, explicitly, makes a SYSTEM message — `requesting_user_id`
stays NULL so `company_id` derives to NULL and no tenant-scoped rule ever matches it.

The third case is the one this suite exists for: a plain `or` fallback swallows an
explicit False into "attribute to the reader", which is how a vault-internal finding
ended up filed under a tenant's tenancy before the distinction was made.
"""

from odoo.tests.common import TransactionCase, tagged

from odoo.addons.inouk_message_queue.api import enqueue


def _tristate_probe_task(env, **kwargs):
    """Module-level callable used as an IMQ runnable in tests."""
    return True


@tagged('post_install', '-at_install')
class TestImqRequestingUserTristate(TransactionCase):

    def setUp(self):
        super().setUp()
        self.queue = self.env.ref('inouk_message_queue.imq_queue__debug')

    def _enqueue(self, **extra):
        resp = enqueue(_tristate_probe_task, self.env,
                       _imq_queue_name=self.queue.name, **extra)
        return self.env['imq.message'].browse(resp['id'])

    def test_omitted_attributes_to_the_enqueuing_user(self):
        msg = self._enqueue()
        self.assertEqual(msg.requesting_user_id, self.env.user)
        self.assertEqual(msg.company_id, self.env.user.company_id)

    def test_an_id_attributes_to_that_user(self):
        other = self.env.ref('base.user_admin')
        msg = self._enqueue(_imq_requesting_user_id=other.id)
        self.assertEqual(msg.requesting_user_id, other)
        self.assertEqual(msg.company_id, other.company_id)

    def test_explicit_false_makes_a_system_message(self):
        """False -> NULL requesting user, NULL company: invisible to tenant rules."""
        msg = self._enqueue(_imq_requesting_user_id=False)
        self.assertFalse(msg.requesting_user_id)
        self.assertFalse(msg.company_id)
        # user_id (who enqueued) is still recorded — system-ness lives in the
        # requesting/company pair, not in losing the operational trace.
        self.assertEqual(msg.user_id, self.env.user)
