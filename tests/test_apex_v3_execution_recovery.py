from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from apex.db.execution_recovery import append_recovery, recovery_path, replay_recovery
from apex.db.repositories.executions import ExecutionRepository
from apex.db.state_db import migrate_state


class ExecutionRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.compatibility_path = os.path.join(self.tmp.name, "brain.db")
        self.state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            return sqlite3.connect(self.state_path)

        self.factory = factory
        conn = factory(); migrate_state(conn); conn.close()

    def test_snapshot_then_update_replay_is_ordered_and_idempotent(self):
        values = {
            "signal_id": 81, "mode": "live", "exchange": "binance_futures",
            "symbol": "BTCUSDT", "direction": "LONG", "status": "ENTRY_PENDING",
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 115, "tp3": None,
            "quantity": 0.25, "risk_usdt": 5, "balance_usdt": 1000,
            "leverage": 5, "entry_order_id": "entry-81", "last_error": "",
        }
        append_recovery(self.compatibility_path, {
            "kind": "execution_snapshot", "values": values,
        })
        append_recovery(self.compatibility_path, {
            "kind": "execution_update", "signal_id": 81,
            "status": "PROTECTED", "changes": {"stop_order_id": "stop-81"},
        })

        self.assertEqual(replay_recovery(self.compatibility_path, self.factory), 2)
        execution = ExecutionRepository(self.factory).get(81)
        self.assertEqual((execution["status"], execution["stop_order_id"]), ("PROTECTED", "stop-81"))
        self.assertFalse(recovery_path(self.compatibility_path).exists())
        self.assertEqual(replay_recovery(self.compatibility_path, self.factory), 0)
        self.assertFalse(os.path.exists(self.compatibility_path))

    def test_failed_replay_keeps_complete_journal(self):
        append_recovery(self.compatibility_path, {
            "kind": "execution_update", "signal_id": 999,
            "status": "PROTECTED", "changes": {"stop_order_id": "stop-missing"},
        })

        with self.assertRaises(Exception):
            replay_recovery(self.compatibility_path, self.factory)

        self.assertTrue(recovery_path(self.compatibility_path).exists())


if __name__ == "__main__":
    unittest.main()
