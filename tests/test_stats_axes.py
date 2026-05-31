# -*- coding: utf-8 -*-
"""Tests for the execution-stats reporting axes (stats_category / stats_target).

Covers the whole plumbing without needing a running worker or AWS:
  - enqueue() carries the axes in processor_context and strips them from the
    executed task's payload (single source of truth);
  - the receive sites store_message__pgsql AND store_message__aws_sqs populate
    the columns identically (provider parity);
  - the related-stored fields propagate from imq.message to imq.message_processing.
"""

import json
import datetime
from unittest.mock import MagicMock

import jsonpickle

from odoo.tests.common import TransactionCase, tagged

from odoo.addons.inouk_message_queue.api import enqueue


def _stats_probe_task(env, **kwargs):
    """Module-level callable used as an IMQ runnable in tests."""
    return True


@tagged('post_install', '-at_install')
class TestImqStatsAxes(TransactionCase):

    def setUp(self):
        super().setUp()
        # Dedicated pgsql testing queue (data/imq_queue.xml).
        self.queue = self.env.ref('inouk_message_queue.imq_queue__debug')
        self.worker = self.env['imq.worker']

    def _enqueue(self, category=None, target=None):
        kwargs = {'_imq_queue_name': self.queue.name, 'foo': 'bar'}
        if category is not None:
            kwargs['_imq_stats_category'] = category
        if target is not None:
            kwargs['_imq_stats_target'] = target
        resp = enqueue(_stats_probe_task, self.env, **kwargs)
        return self.env['imq.message'].browse(resp['id'])

    def test_enqueue_carries_axes_in_context_not_payload(self):
        """Axes ride in context; never leak into the executed task's kwargs."""
        msg = self._enqueue(category='probe_cat', target='probe_tgt')
        body = json.loads(msg.raw_message_body)

        self.assertEqual(body['context']['_imq_stats_category'], 'probe_cat')
        self.assertEqual(body['context']['_imq_stats_target'], 'probe_tgt')

        # The stats keys must NOT appear anywhere in the pickled payload.
        payload_dump = json.dumps(body['payload'])
        self.assertNotIn('_imq_stats_category', payload_dump)
        self.assertNotIn('_imq_stats_target', payload_dump)
        # But the regular task kwarg survives.
        self.assertIn('bar', payload_dump)

        # pgsql sets the columns at ENQUEUE (Finding B), so the still-'pending'
        # message already carries its bucket — get_task_status can hint before pickup.
        self.assertEqual(msg.stats_category, 'probe_cat')
        self.assertEqual(msg.stats_target, 'probe_tgt')

    def test_store_message_pgsql_populates_columns(self):
        """Real pgsql receive path writes the columns from context."""
        msg = self._enqueue(category='cat_pg', target='tgt_pg')
        self.worker.store_message__pgsql(
            self.queue, msg, datetime.datetime.now())
        self.assertEqual(msg.stats_category, 'cat_pg')
        self.assertEqual(msg.stats_target, 'tgt_pg')

    def test_related_stored_propagates_to_processing(self):
        """imq.message_processing mirrors the axes (related stored)."""
        msg = self._enqueue(category='cat_proc', target='tgt_proc')
        self.worker.store_message__pgsql(
            self.queue, msg, datetime.datetime.now())
        processing = msg.create_processing_object()
        self.assertEqual(processing.stats_category, 'cat_proc')
        self.assertEqual(processing.stats_target, 'tgt_proc')

    def test_store_message_sqs_populates_columns(self):
        """SQS receive path writes the SAME columns from the same context keys.

        Provider parity: the snippet in store_message__aws_sqs mirrors the one in
        store_message__pgsql. A mocked boto sqs_message is enough — no AWS.
        """
        body_values = {
            'type': 'rpc',
            'context': {
                '_imq_stats_category': 'cat_sqs',
                '_imq_stats_target': 'tgt_sqs',
            },
            'payload': {'self': None, 'args': [], 'kwargs': {}},
            'user_id': self.env.ref('inouk_message_queue.user_imq').id,
            'module_name': __name__,
            'function_name': '_stats_probe_task',
        }
        sqs_message = MagicMock()
        sqs_message.message_id = 'test-sqs-stats-001'
        sqs_message.body = jsonpickle.encode(body_values)
        sqs_message.attributes = {
            'ApproximateFirstReceiveTimestamp': '1700000000000',
            'MessageGroupId': 'g-stats',
        }
        sqs_message.message_attributes = {
            'name': {'StringValue': 'probe'},
            'code': {'StringValue': 'n/a'},
        }

        # NB: this hand-crafted body references no registered processor, so
        # store_message__aws_sqs logs a benign CRITICAL and takes its
        # processor_id=None branch. The stats columns are populated regardless —
        # that independence is exactly what we assert.
        msg = self.worker.store_message__aws_sqs(
            self.queue, sqs_message, datetime.datetime.now())
        self.assertEqual(msg.stats_category, 'cat_sqs')
        self.assertEqual(msg.stats_target, 'tgt_sqs')

    # --- Consumer: duration estimate (get_task_status hint) ---

    def _make_message(self, category, target):
        return self.env['imq.message'].create({
            'name': 'est-probe',
            'queue_id': self.queue.id,
            'queue_message_id': self.queue.generate_message_id(),
            'stats_category': category,
            'stats_target': target,
        })

    def _add_done_processing(self, msg, seconds):
        start = datetime.datetime(2026, 1, 1, 0, 0, 0)
        return self.env['imq.message_processing'].create({
            'message_id': msg.id,
            'attempt': 1,
            'start_time': start,
            'end_time': start + datetime.timedelta(seconds=seconds),
            'state': 'done',
        })

    def test_estimate_duration_precise_bucket(self):
        """Enough samples in category+target → precise estimate."""
        cat = 'test_cat_est_precise'
        msg = self._make_message(cat, 'tgt_B')
        for s in (10, 20, 30, 40):
            self._add_done_processing(msg, s)
        est = self.env['imq.message'].estimate_task_duration(cat, 'tgt_B')
        self.assertEqual(est['based_on'], 'category+target')
        self.assertEqual(est['sample_count'], 4)
        self.assertEqual(est['min_seconds'], 10)
        self.assertEqual(est['max_seconds'], 40)
        self.assertEqual(est['p50_seconds'], 25)  # median of 10,20,30,40

    def test_estimate_duration_falls_back_to_category(self):
        """Thin precise bucket (< MIN_SAMPLES) degrades to category-wide."""
        cat = 'test_cat_est_fallback'
        msg_a = self._make_message(cat, 'tgt_A')      # only 2 samples
        for s in (100, 200):
            self._add_done_processing(msg_a, s)
        msg_b = self._make_message(cat, 'tgt_B')      # 4 samples
        for s in (10, 20, 30, 40):
            self._add_done_processing(msg_b, s)
        # tgt_A has 2 < MIN_SAMPLES → fall back to whole category (6 samples)
        est = self.env['imq.message'].estimate_task_duration(cat, 'tgt_A')
        self.assertEqual(est['based_on'], 'category')
        self.assertEqual(est['sample_count'], 6)

    def test_estimate_duration_unknown_returns_none(self):
        self.assertIsNone(
            self.env['imq.message'].estimate_task_duration('no_such_category_xyz'))
        self.assertIsNone(
            self.env['imq.message'].estimate_task_duration(False))

    def test_get_task_status_includes_expected_block(self):
        """A tagged message with history exposes 'expected' + 'eta_seconds'."""
        cat = 'test_cat_est_status'
        msg = self._make_message(cat, 'tgt_S')
        for s in (60, 60, 60, 60):
            self._add_done_processing(msg, s)
        msg.write({'state': 'wip', 'start_time': datetime.datetime.now()})
        [status] = self.env['imq.message'].get_task_status([msg.id])
        self.assertIn('expected', status)
        self.assertEqual(status['expected']['based_on'], 'category+target')
        self.assertEqual(status['expected']['p50_seconds'], 60)
        # Fresh wip (elapsed ~0) → wait anchored on p95*margin (60*1.10 ≈ 66).
        self.assertIn('next_poll_seconds', status)
        self.assertGreaterEqual(status['next_poll_seconds'], 60)
        self.assertFalse(status.get('overdue'))

    def test_get_task_status_default_poll_without_stats(self):
        """No history → next_poll_seconds still present with the sensible default."""
        msg = self._make_message('no_history_cat_xyz', 'tgt')
        msg.write({'state': 'wip', 'start_time': datetime.datetime.now()})
        [status] = self.env['imq.message'].get_task_status([msg.id])
        self.assertNotIn('expected', status)
        self.assertEqual(status['next_poll_seconds'], 30)

    # --- Launch-time hint (build_launch_hint / enqueue opt-in) ---

    def test_build_launch_hint_with_history(self):
        cat = 'test_cat_launch'
        msg = self._make_message(cat, 'tgt_L')
        for s in (30, 30, 30, 30):
            self._add_done_processing(msg, s)
        hint = self.env['imq.message'].build_launch_hint(cat, 'tgt_L')
        self.assertEqual(hint['expected']['based_on'], 'category+target')
        self.assertGreaterEqual(hint['next_poll_seconds'], 30)  # p95(30)*1.1≈33
        self.assertIn('poll once', hint['hint'])

    def test_build_launch_hint_sensible_default(self):
        hint = self.env['imq.message'].build_launch_hint('no_such_cat_launch')
        self.assertIsNone(hint['expected'])
        self.assertEqual(hint['next_poll_seconds'], 30)
        self.assertEqual(hint['hint'], "No stats available for now; poll in 30s")

    def test_enqueue_opt_in_returns_hint_default(self):
        """enqueue with the opt-in flag enriches the response; flag never leaks."""
        resp = enqueue(
            _stats_probe_task, self.env,
            _imq_queue_name=self.queue.name,
            _imq_stats_category='test_cat_enqueue_hint',  # no history → default
            _imq_return_duration_hint=True,
        )
        self.assertEqual(resp['next_poll_seconds'], 30)
        self.assertIsNone(resp['expected'])
        self.assertIn('hint', resp)
        body = json.loads(self.env['imq.message'].browse(resp['id']).raw_message_body)
        self.assertNotIn('_imq_return_duration_hint', json.dumps(body['payload']))
