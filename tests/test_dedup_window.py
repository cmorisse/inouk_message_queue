# -*- coding: utf-8 -*-
"""Per-enqueue deduplication window (`_imq_deduplication_interval_s`).

The window in which a FIFO queue looks for a duplicate used to come from ONE place:
`imq.queue.deduplication_interval_s`. That value is shared by every processor on the queue
and editable from the GUI, so a caller whose correctness leans on a window was borrowing it
rather than owning it — and the failure was undetectable, since a suppressed enqueue is the
*absence* of a message.

These tests drive `enqueue()` directly against the debug queue: no worker, no AWS. The queue
ships as `std` and deduplication is only evaluated on FIFO, so each test flips `q_type`
inside its own transaction (the rollback puts it back).
"""

import json
import datetime

from odoo.exceptions import UserError
from odoo.tests.common import TransactionCase, tagged

from odoo.addons.inouk_message_queue.api import enqueue


def _dedup_probe_task(env, **kwargs):
    """Module-level callable used as an IMQ runnable in tests."""
    return True


@tagged('post_install', '-at_install')
class TestImqDedupWindow(TransactionCase):

    DEDUP_ID = 'dedup-window-probe'

    def setUp(self):
        super().setUp()
        self.Message = self.env['imq.message']
        self.queue = self.env.ref('inouk_message_queue.imq_queue__debug')
        # Deduplication is only evaluated on FIFO queues (send_message__pgsql). Rolled back.
        self.queue.write({'q_type': 'fifo', 'deduplication_interval_s': 300})

    def _enqueue(self, interval_s=None, dedup_id=None):
        """Enqueue one probe. Returns the raw response so a suppression is visible."""
        kwargs = {'_imq_queue_name': self.queue.name, 'foo': 'bar',
                  '_imq_message_deduplication_id': dedup_id or self.DEDUP_ID}
        if interval_s is not None:
            kwargs['_imq_deduplication_interval_s'] = interval_s
        return enqueue(_dedup_probe_task, self.env, **kwargs)

    def _sent(self):
        return self.Message.search_count(
            [('message_deduplication_id', '=', self.DEDUP_ID)])

    def _age_the_only_message(self, seconds):
        """Push the queued message back in time. Raw SQL: `enqueued_time` is what the
        duplicate check reads, and it reads it with raw SQL too."""
        self.env.flush_all()
        self.env.cr.execute("""
            UPDATE imq_message SET enqueued_time = enqueued_time - make_interval(secs => %s)
             WHERE message_deduplication_id = %s
        """, (seconds, self.DEDUP_ID))
        self.env.invalidate_all()

    # ----- the inheritance rule: every pre-existing caller lands here ---------
    def test_no_kwarg_inherits_the_queues_window(self):
        self._enqueue()
        response = self._enqueue()
        self.assertEqual(response.get('error_code'), 'MESSAGE_IS_DUPLICATED')
        self.assertEqual(self._sent(), 1,
                         "the queue's 300 s window must still apply when nothing is passed")

    def test_no_kwarg_lets_a_message_older_than_the_queues_window_through(self):
        self._enqueue()
        self._age_the_only_message(301)
        self._enqueue()
        self.assertEqual(self._sent(), 2, "past the queue's window, it is not a duplicate")

    # ----- owning the window --------------------------------------------------
    def test_a_shorter_window_lets_through_what_the_queue_would_suppress(self):
        """The point of the feature: the caller's window wins over the queue's."""
        self._enqueue()
        self._age_the_only_message(10)          # well inside the queue's 300 s
        self._enqueue(interval_s=5)
        self.assertEqual(self._sent(), 2)

    def test_a_longer_window_suppresses_what_the_queue_would_let_through(self):
        self._enqueue()
        self._age_the_only_message(600)         # well outside the queue's 300 s
        response = self._enqueue(interval_s=3600)
        self.assertEqual(response.get('error_code'), 'MESSAGE_IS_DUPLICATED')
        self.assertEqual(self._sent(), 1)

    def test_zero_is_a_zero_length_window_not_an_inheritance(self):
        """`0` and `None` must not mean the same thing — that distinction is what saves a
        special case in check_message_duplicate."""
        self._enqueue()
        self._enqueue(interval_s=0)
        self.assertEqual(self._sent(), 2, "nothing can fall inside a zero-length window")

    def test_a_negative_window_is_refused(self):
        """Not clamped to 0: silently turning a bug into 'never deduplicate' would hide it
        behind behaviour that looks deliberate."""
        with self.assertRaises(UserError):
            self._enqueue(interval_s=-1)

    def test_a_non_numeric_window_is_refused(self):
        with self.assertRaises(UserError):
            self._enqueue(interval_s='ten minutes')

    # ----- what tells this mechanism apart from the stats axes -----------------
    def test_the_window_reaches_neither_the_payload_nor_the_context(self):
        """THE test of this file. The stats axes ride in `processor_context` because a
        receive site reads them back; this value is consumed entirely at enqueue and has no
        reader downstream. Carrying it further would suggest it means something at
        execution — and leaking it into the payload would hand it to the task as a kwarg."""
        response = self._enqueue(interval_s=42)
        message = self.Message.browse(response['id'])
        body = json.loads(message.raw_message_body)

        self.assertNotIn('_imq_deduplication_interval_s', json.dumps(body['payload']))
        self.assertNotIn('_imq_deduplication_interval_s', json.dumps(body['context']))
        # The ordinary task kwarg still gets through, so the strip is not over-broad.
        self.assertIn('bar', json.dumps(body['payload']))

    # ----- the trap this feature exists to expose ------------------------------
    def test_a_body_hash_cannot_deduplicate_a_cron_dispatch(self):
        """Why an explicit id is required, not merely convenient.

        With no `_imq_message_deduplication_id`, `_send_message` falls back to hashing the
        message body — and an `ir.cron` puts `lastcall` in the context that the body
        carries, so the hash differs at every tick. Deduplication therefore can NEVER fire
        for a cron-dispatched job on a FIFO queue. Asserted here so the day someone removes
        an explicit id 'because the queue deduplicates anyway', a test says otherwise."""
        env_a = self.env(context=dict(self.env.context, lastcall='2026-01-01 00:00:00'))
        env_b = self.env(context=dict(self.env.context, lastcall='2026-01-01 00:01:00'))
        common = {'_imq_queue_name': self.queue.name}
        before = self.Message.search_count([('queue_id', '=', self.queue.id)])
        enqueue(_dedup_probe_task, env_a, **common)
        enqueue(_dedup_probe_task, env_b, **common)
        after = self.Message.search_count([('queue_id', '=', self.queue.id)])
        self.assertEqual(after - before, 2,
                         "two ticks differing only by `lastcall` hash differently, so the "
                         "implicit body-hash dedup cannot suppress either")
