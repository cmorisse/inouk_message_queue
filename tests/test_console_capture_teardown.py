# -*- coding: utf-8 -*-
"""Tearing down the console capture — twice, safely.

The standalone worker closes the capture TWICE per message: once on the nominal path and
once in the `finally` that covers every failure path. That is deliberate, and fine — as
long as the teardown is idempotent. It was not, and the symptom was a line that looked
like a database problem and was in fact pure bookkeeping:

    ERROR Failed to log final console output: Cursor already closed

Two defects, either of which alone produced it:

  * `MpyStringIO.close()` wrote the leftover buffer but never cleared it, so the second
    close replayed the same text against the cursor the first one had just closed;
  * `stop_stream_capture()` left the stream on TLS — which is per-THREAD, not per-message —
    so every LATER message re-closed that same dead stream in its own `finally`. Observed
    in production: one console-capturing message, then one error per message until the
    worker restarted, including messages with `capture_console=False` that never opened a
    stream of their own.

No database here on purpose. The bug lives entirely in object lifetime, and the assertion
that matters is "how many times is a row written", which a mocked Environment states
directly. A real cursor would also mean committing rows on a second connection from inside
a test — which is the very thing the appender guidance says not to do casually.
"""
from unittest import mock

from odoo.tests.common import TransactionCase, tagged

from odoo.addons.inouk_message_queue.workers import standalone
from odoo.addons.inouk_message_queue.workers.base import TLS


@tagged('post_install', '-at_install')
class TestConsoleCaptureTeardown(TransactionCase):

    def setUp(self):
        super().setUp()
        # TLS is thread-local and this test runs in a worker-less thread that other tests
        # share. Leave it as we found it.
        previous = (getattr(TLS, '_imq_stream', None), getattr(TLS, 'log_cursor', None))
        self.addCleanup(lambda: (setattr(TLS, '_imq_stream', previous[0]),
                                 setattr(TLS, 'log_cursor', previous[1])))

    def _stream(self):
        """A capture stream on a mock cursor — nothing reaches Postgres."""
        return standalone.MpyStringIO(42, 7, 1, mock.MagicMock(name='log_cursor'))

    def _patched_create(self):
        """Patch the module's `api` and hand back the create() mock the stream will call."""
        patcher = mock.patch.object(standalone, 'api')
        fake_api = patcher.start()
        self.addCleanup(patcher.stop)
        env = fake_api.Environment.return_value
        return env.__getitem__.return_value.sudo.return_value.create

    def _worker(self):
        """A StandaloneWorker without its __init__ — stop_stream_capture touches only TLS."""
        return standalone.StandaloneWorker.__new__(standalone.StandaloneWorker)

    # ----- the precondition: a line with no newline stays in the buffer -------
    def test_a_trailing_partial_line_waits_in_the_buffer(self):
        """No leftover, no bug — which is why the error only ever appeared after a task
        whose console output did not end on a newline."""
        create = self._patched_create()
        stream = self._stream()
        stream.write("complete line\npartial")
        self.assertEqual(create.call_count, 1, "the complete line is written immediately")
        self.assertEqual(stream._mpy_buffer, "partial")

    # ----- close() is idempotent ---------------------------------------------
    def test_closing_twice_writes_the_leftover_once(self):
        create = self._patched_create()
        stream = self._stream()
        stream.write("partial")
        stream.close()
        self.assertEqual(create.call_count, 1)
        self.assertEqual(create.call_args[0][0]['log_message'], "partial")

        stream.close()
        self.assertEqual(create.call_count, 1, "the second close has nothing left to say")

    def test_a_failed_write_is_swallowed_and_never_retried(self):
        """The buffer is cleared BEFORE the write, so a write that fails (the cursor IS
        gone, in the real failure) costs one line and not an endless retry."""
        create = self._patched_create()
        create.side_effect = Exception("Cursor already closed")
        stream = self._stream()
        stream.write("partial")

        stream.close()          # must not raise
        self.assertEqual(create.call_count, 1)
        stream.close()
        self.assertEqual(create.call_count, 1)

    # ----- stop_stream_capture() drops what it closed ------------------------
    def test_stop_stream_capture_clears_the_thread_local(self):
        create = self._patched_create()
        worker = self._worker()
        cursor = mock.MagicMock(name='log_cursor')
        env = mock.MagicMock()
        env.registry.cursor.return_value = cursor
        message_obj, processing_obj = mock.MagicMock(), mock.MagicMock()
        message_obj.id, processing_obj.id, message_obj.user_id.id = 42, 7, 1

        worker.start_stream_capture(env, message_obj, processing_obj)
        TLS._imq_stream.write("partial")
        worker.stop_stream_capture()

        self.assertIsNone(TLS._imq_stream, "the stream must not outlive the message")
        self.assertIsNone(TLS.log_cursor)
        self.assertEqual(create.call_count, 1)
        self.assertEqual(cursor.close.call_count, 1)

    def test_the_double_teardown_of_one_message_is_harmless(self):
        """The nominal path, then the `finally` — the worker's actual sequence."""
        create = self._patched_create()
        worker = self._worker()
        TLS.log_cursor = mock.MagicMock(name='log_cursor')
        TLS._imq_stream = self._stream()
        TLS._imq_stream.write("partial")

        worker.stop_stream_capture()
        worker.stop_stream_capture()

        self.assertEqual(create.call_count, 1)

    def test_a_later_message_does_not_reclose_the_previous_stream(self):
        """The regression itself. Message A captures console; messages B and C do not, so
        they never call start_stream_capture — only the `finally`. They used to find A's
        stream on TLS and replay its buffer against A's closed cursor, once each, forever.
        """
        create = self._patched_create()
        worker = self._worker()
        TLS.log_cursor = mock.MagicMock(name='log_cursor')
        TLS._imq_stream = self._stream()
        TLS._imq_stream.write("partial")

        worker.stop_stream_capture()    # message A ends
        for _ in range(2):              # messages B and C: cleanup only
            worker.stop_stream_capture()
            worker.stop_stream_capture()

        self.assertEqual(create.call_count, 1,
                         "A's last line is written once, and never by anyone else")
