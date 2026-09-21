from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from core import control_loop


class LearningAuthorityTests(unittest.TestCase):
    def test_legacy_learning_has_no_runtime_module(self):
        self.assertFalse(Path("core/learning.py").exists())

    def test_loss_streak_is_advisory_and_cannot_change_live_risk(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "brain.db")
            conn = sqlite3.connect(path)
            conn.execute("""CREATE TABLE signals (
                id INTEGER PRIMARY KEY, grade TEXT, signal_type TEXT,
                result TEXT, created_at TEXT, closed_at TEXT
            )""")
            conn.execute("""CREATE TABLE signal_execution_state (
                signal_id INTEGER PRIMARY KEY, status TEXT
            )""")
            now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
            for signal_id in range(1, 7):
                conn.execute(
                    "INSERT INTO signals VALUES(?,?,?,?,?,?)",
                    (signal_id, "FAST", "FAST", "sl", now, now),
                )
                conn.execute(
                    "INSERT INTO signal_execution_state VALUES(?,?)", (signal_id, "closed")
                )
            conn.commit(); conn.close()
            state = control_loop.rebuild_strategy_risk_states(path)["FAST"]
            self.assertEqual(state["consecutive_losses"], 6)
            self.assertEqual(state["mode"], "NORMAL")
            self.assertEqual(state["live_risk_multiplier"], 1.0)
            self.assertIsNone(state["live_paused_until"])
            self.assertTrue(state["reason"].startswith("ADVISORY_ONLY:"))


if __name__ == "__main__":
    unittest.main()
