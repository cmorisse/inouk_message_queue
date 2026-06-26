# -*- coding: utf-8 -*-
"""Tiered retention purge (purge_messages_history).

Verifies the three age tiers:
  - young (< logs threshold): message + processing + logs all kept;
  - mid (logs < age < messages): logs purged, message + processing kept
    (so processing_time / duration-hint history survives);
  - old (> messages threshold): message deleted (cascades to processing + logs).

Runs inside a TransactionCase, so the raw-SQL DELETEs are rolled back — real
rows in the test DB are never actually lost.
"""

import datetime
from unittest.mock import patch

from odoo.tests.common import TransactionCase, tagged


@tagged('post_install', '-at_install')
class TestImqPurgeRetention(TransactionCase):

    def setUp(self):
        super().setUp()
        self.queue = self.env.ref('inouk_message_queue.imq_queue__debug')
        icp = self.env['ir.config_parameter'].sudo()
        # logs older than 10h purged; whole message older than 100h deleted.
        icp.set_param('imq.logs_retention_period_in_hours', '10')
        icp.set_param('imq.messages_retention_period_in_hours', '100')

    def _msg(self, hours_ago):
        end = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
        msg = self.env['imq.message'].create({
            'name': 'purge-probe',
            'queue_id': self.queue.id,
            'queue_message_id': self.queue.generate_message_id(),
            'state': 'done',
            'end_time': end,
        })
        proc = self.env['imq.message_processing'].create({
            'message_id': msg.id, 'attempt': 1, 'state': 'done',
            'start_time': end, 'end_time': end,
        })
        log = self.env['imq.message_processing_log'].create({
            'message_id': msg.id, 'processing_id': proc.id,
            'log_level': '20', 'log_message': 'probe',
        })
        return msg, proc, log

    def test_tiered_purge(self):
        young_m, _young_p, young_l = self._msg(0)      # < 10h  → keep all
        mid_m, mid_p, mid_l = self._msg(50)            # 10<50<100 → logs only
        old_m, _old_p, _old_l = self._msg(200)         # > 100h → delete message
        self.env.flush_all()  # raw SQL below must see ORM-created rows
        ids = {
            'young_m': young_m.id, 'young_l': young_l.id,
            'mid_m': mid_m.id, 'mid_p': mid_p.id, 'mid_l': mid_l.id,
            'old_m': old_m.id,
        }

        # purge_messages_history commits between phases (fine for the cron, but
        # forbidden inside a test) — neutralise the commit for the test run.
        with patch.object(self.env.cr, 'commit', lambda: None):
            self.env['imq.message'].purge_messages_history()
        self.env.invalidate_all()  # deletes were raw SQL

        Msg = self.env['imq.message']
        Proc = self.env['imq.message_processing']
        Log = self.env['imq.message_processing_log']

        # young: everything kept
        self.assertTrue(Msg.browse(ids['young_m']).exists())
        self.assertTrue(Log.browse(ids['young_l']).exists())
        # mid: message + processing (processing_time) kept, log purged
        self.assertTrue(Msg.browse(ids['mid_m']).exists())
        self.assertTrue(Proc.browse(ids['mid_p']).exists())
        self.assertFalse(Log.browse(ids['mid_l']).exists())
        # old: message gone (cascade removes processing + logs)
        self.assertFalse(Msg.browse(ids['old_m']).exists())
