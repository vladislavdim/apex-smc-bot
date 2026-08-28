import sqlite3
import unittest

from core.signal_lifecycle import (
    ACTIVE,
    CANCELLED,
    WAITING_ENTRY,
    activated_at_for,
    barrier_hits,
    entry_touched,
    mark_active,
    mark_finished,
    register_waiting,
    state_for,
)


class SignalLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def test_waiting_active_cancelled_lifecycle(self):
        register_waiting(self.conn, 7)
        self.assertEqual(state_for(self.conn, 7), WAITING_ENTRY)
        mark_active(self.conn, 7)
        self.assertEqual(state_for(self.conn, 7), ACTIVE)
        self.assertIsNotNone(activated_at_for(self.conn, 7))
        mark_finished(self.conn, 7, CANCELLED, "entry_expired")
        row = self.conn.execute(
            "SELECT status, cancel_reason FROM signal_execution_state WHERE signal_id=7"
        ).fetchone()
        self.assertEqual(row, (CANCELLED, "entry_expired"))

    def test_legacy_signal_defaults_to_active(self):
        self.assertEqual(state_for(self.conn, 99), ACTIVE)

    def test_entry_touch_uses_interval_then_directional_snapshot(self):
        self.assertTrue(entry_touched("BULLISH", 100, current=105, low=99, high=106))
        self.assertTrue(entry_touched("BULLISH", 100, current=99))
        self.assertTrue(entry_touched("BEARISH", 100, current=101))
        self.assertFalse(entry_touched("BEARISH", 100, current=99))

    def test_barriers_are_direction_aware(self):
        bullish = barrier_hits("BULLISH", 95, 110, 120, low=94, high=111)
        self.assertEqual(bullish, {"sl": True, "tp1": True, "tp2": False})
        bearish = barrier_hits("BEARISH", 105, 90, 80, low=79, high=106)
        self.assertEqual(bearish, {"sl": True, "tp1": True, "tp2": True})


if __name__ == "__main__":
    unittest.main()
