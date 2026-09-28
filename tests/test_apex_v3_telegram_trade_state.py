"""Telegram trade list must show only State-owned live positions."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from apex.db.state_db import migrate_state
from apex.domain.ids import derived_id
from apex.ui.telegram.trades import fetch_live_trades, format_trade_view


class TelegramTradeStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "state.db")
        with sqlite3.connect(self.path) as conn:
            migrate_state(conn)
            for signal_id, status, result, mode, position in (
                (1, "ACTIVE", "pending", "live", True),
                (2, "CLOSED", "tp2", "live", True),
                (3, "CLOSED", "sl", "live", True),
                (4, "ACTIVE", "pending", "paper", True),
                (5, "ACTIVE", "pending", "live", False),
            ):
                entity = derived_id("signal", "test", signal_id)
                conn.execute(
                    """INSERT INTO executions(signal_entity_id,signal_id,mode,symbol,direction,
                        status,position_id,plan_json,plan_hash) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (entity, signal_id, mode, f"PAIR{signal_id}USDT", "BULLISH",
                     "PROTECTED", derived_id("position", "test", signal_id) if position else None,
                     "{}", "hash"),
                )
                conn.execute(
                    """INSERT INTO signal_lifecycle(signal_entity_id,signal_id,status,result)
                       VALUES(?,?,?,?)""",
                    (entity, signal_id, "active" if status == "ACTIVE" else "closed", result),
                )
                conn.execute(
                    """INSERT INTO manager_positions(signal_entity_id,signal_id,symbol,strategy,
                        direction,management_tf,initial_entry,initial_sl,initial_tp1,
                        manager_version,snapshot_json,snapshot_hash,status,close_result)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (entity, signal_id, f"PAIR{signal_id}USDT", "MTF", "BULLISH", "1h",
                     100, 95, 110, 2, "{}", "hash", status,
                     result if result != "pending" else None),
                )
        self.factory = lambda: sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)

    def tearDown(self):
        self.tmp.cleanup()

    def test_only_confirmed_live_positions_appear(self):
        self.assertEqual([r["id"] for r in fetch_live_trades("active", 12, self.factory)], [1])
        self.assertEqual([r["id"] for r in fetch_live_trades("take", 12, self.factory)], [2])
        self.assertEqual([r["id"] for r in fetch_live_trades("stop", 12, self.factory)], [3])
        self.assertIn("PAIR1USDT", format_trade_view("active", fetch_live_trades("active", 12, self.factory)))

    def test_state_failure_does_not_fall_back_to_legacy(self):
        with self.assertRaises(sqlite3.OperationalError):
            fetch_live_trades("active", 12, lambda: sqlite3.connect(":memory:"))
