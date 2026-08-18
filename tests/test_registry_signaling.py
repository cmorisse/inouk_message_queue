# -*- coding: utf-8 -*-
"""The standalone worker must notice what OTHER processes change.

`Registry.check_signaling()` is how one Odoo process learns that another has invalidated the
registry or the ORM caches. Odoo calls it per HTTP request, per cron tick and per RPC
dispatch — and a CLI worker goes through none of those. Without it, every `@ormcache` value
a worker reads is frozen at boot, `ir.config_parameter.get_param` included.

**What made it urgent**: this worker reads its own kill switch that way.
`imq.STOP_STANDALONE_WORKERS` ships *disabled* and is renamed into existence when an operator
wants workers to stop, so what a running worker has cached is the MISS — and the emergency
stop does nothing, silently.

**What these tests can and cannot prove.** `check_signaling` starts with
`if self.in_test_mode(): return self`, and `TransactionCase` is test mode — so the real
cross-process invalidation is not observable from here, and pretending otherwise would be
worse than admitting it. What is testable, and is what these cover:

  - the normal path is undisturbed (the call is a no-op under test, which is exactly the
    condition under which "it still works" is worth asserting);
  - the throttle holds, so an idle worker polling twice a second does not query 170k times
    a day;
  - a returned registry is USED — the failure that would leave a worker on stale model
    definitions after a module upgrade;
  - it runs BEFORE the message is fetched, so a multi-second registry reload cannot happen
    with a message in flight, where it could be lost to its own visibility timeout;
  - a failing check never stops the queue draining.

The remaining claim — that a parameter changed elsewhere is actually seen — needs two
processes and belongs in a manual check: run a worker, set `imq.STOP_STANDALONE_WORKERS=*`,
confirm it stops within `signaling_check_period_s`.
"""

import logging

from unittest.mock import MagicMock, patch

from odoo.tests.common import TransactionCase, tagged

from odoo.addons.inouk_message_queue.workers.standalone import StandaloneWorker


def _worker(period=5):
    """A StandaloneWorker WITHOUT running __init__.

    Constructing one registers Prometheus collectors in a process-global registry, so a
    second instance raises `Duplicated timeseries in CollectorRegistry` — which says nothing
    about the code under test. `__new__` plus the handful of attributes these methods
    actually touch keeps the real methods under test and leaves the worker's construction
    side effects out of it."""
    w = StandaloneWorker.__new__(StandaloneWorker)
    w.last_signaling_check = 0.0
    w.signaling_check_period_s = period
    w.logger = logging.getLogger('test_registry_signaling')
    w.target_message_id = None
    w.current_message = None
    return w


@tagged('post_install', '-at_install')
class TestRegistrySignaling(TransactionCase):

    def test_the_first_check_always_runs(self):
        """A worker must not start life inside its own throttle window."""
        w = _worker()
        registry, cr = MagicMock(), MagicMock()
        w._refresh_signaling(registry, cr)
        registry.check_signaling.assert_called_once_with(cr)

    def test_the_throttle_holds(self):
        """An idle worker polls twice a second. Per-poll would be ~170k checks a day, all
        of them finding nothing; the period bounds the cost AND the staleness."""
        w = _worker(period=5)
        registry, cr = MagicMock(), MagicMock()
        with patch('odoo.addons.inouk_message_queue.workers.standalone.time.time',
                   side_effect=[100.0, 101.0, 102.0, 106.0]):
            w._refresh_signaling(registry, cr)   # t=100 -> runs
            w._refresh_signaling(registry, cr)   # t=101 -> throttled
            w._refresh_signaling(registry, cr)   # t=102 -> throttled
            w._refresh_signaling(registry, cr)   # t=106 -> period elapsed, runs
        self.assertEqual(registry.check_signaling.call_count, 2)

    def test_a_swapped_registry_is_returned_not_discarded(self):
        """THE regression this guards. On a registry-sequence change `check_signaling`
        returns a DIFFERENT registry and swaps the process-wide singleton. Discarding it
        leaves the worker running old model definitions against a new schema — which looks
        like it works until it does not."""
        w = _worker()
        old_registry, new_registry, cr = MagicMock(), MagicMock(), MagicMock()
        old_registry.check_signaling.return_value = new_registry
        self.assertIs(w._refresh_signaling(old_registry, cr), new_registry)

    def test_the_cursor_is_passed_so_no_extra_connection_is_opened(self):
        """`check_signaling(cr=None)` opens its own cursor. The worker already holds one."""
        w = _worker()
        registry, cr = MagicMock(), MagicMock()
        w._refresh_signaling(registry, cr)
        self.assertIs(registry.check_signaling.call_args.args[0], cr)
        registry.cursor.assert_not_called()

    def test_a_failing_check_never_stops_the_queue_draining(self):
        """Worst case is the behaviour that existed before this: keep the caches we had."""
        w = _worker()
        registry, cr = MagicMock(), MagicMock()
        registry.check_signaling.side_effect = RuntimeError("signaling table is on fire")
        self.assertIs(w._refresh_signaling(registry, cr), registry)

    def test_it_runs_before_the_message_is_fetched(self):
        """Ordering is not incidental: a registry reload takes seconds, so doing it with a
        message in flight risks losing that message to its own visibility timeout — and
        `Registry.new()` opens its own cursors, so holding locks first invites a deadlock."""
        w = _worker()
        queue = self.env['imq.queue'].create({'name': 'probe-signaling', 'provider': 'pgsql'})
        calls = []

        # The REAL test cursor, not a mock: `api.Environment` asserts `isinstance(cr,
        # BaseCursor)`, and the point here is the order of two calls, not the plumbing
        # between them. `get_message` returning None ends the method right after the pair.
        registry = MagicMock()
        registry.cursor.return_value.__enter__.return_value = self.env.cr
        registry.check_signaling.side_effect = lambda cr: calls.append('signaling') or registry
        with patch.object(StandaloneWorker, 'get_message',
                          side_effect=lambda *a, **k: calls.append('get_message')):
            result = w._process_one_message(registry, queue_id=queue.id, queue_name='probe')

        self.assertEqual(result, 'empty')
        self.assertEqual(calls, ['signaling', 'get_message'])
