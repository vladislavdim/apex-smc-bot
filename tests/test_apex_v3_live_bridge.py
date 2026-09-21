from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

from apex.config.settings import ApexConfig
from apex.db.connection import _CONNECT
from apex.db.memory_db import migrate_memory
from apex.db.repositories.executions import ExecutionRepository
from apex.db.state_db import migrate_state
from apex.learning.live_bridge import LiveBridgeError, LiveLearningBridge
from apex.domain.ids import derived_id, is_id


class LiveLearningBridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.compat = os.path.join(self.temp.name, "brain.db")
        self.state = os.path.join(self.temp.name, "state.db")
        self.memory = os.path.join(self.temp.name, "memory.db")
        config_env = {
            "RENDER_GIT_COMMIT": "a" * 40,
            "APEX_COMPAT_DB_PATH": self.compat,
            "APEX_STATE_DB_PATH": self.state,
            "APEX_MEMORY_DB_PATH": self.memory,
        }
        self.config = ApexConfig.from_env(config_env)
        state = sqlite3.connect(self.state)
        memory = sqlite3.connect(self.memory)
        migrate_state(state)
        migrate_memory(memory)
        state.close()
        memory.close()
        conn = sqlite3.connect(self.compat)
        conn.executescript("""
            CREATE TABLE signals (
                id INTEGER PRIMARY KEY, symbol TEXT, direction TEXT, signal_type TEXT,
                entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL, grade TEXT,
                created_at TEXT
            );
            CREATE TABLE trade_executions (
                id INTEGER PRIMARY KEY, signal_id INTEGER, mode TEXT, symbol TEXT,
                status TEXT, quantity REAL, entry_order_id TEXT, stop_order_id TEXT
            );
            INSERT INTO signals VALUES(
                7,'BTCUSDT','BULLISH','FAST',100,105,110,115,95,'FAST',
                '2026-09-11 01:02:03'
            );
        """)
        conn.commit()
        conn.close()
        self.bridge = LiveLearningBridge(
            self.config,
            compatibility_db_path=self.compat,
            state_factory=lambda: _CONNECT(self.state),
            memory_factory=lambda: _CONNECT(self.memory),
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_register_is_idempotent_and_preserves_review_metadata(self):
        candidate_id = self.bridge.register_signal(7, groq={"decision": "APPROVE"})
        self.assertEqual(candidate_id, self.bridge.register_signal(7))
        row = sqlite3.connect(self.memory).execute(
            "SELECT groq_json,executed FROM live_candidates WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        self.assertIn("APPROVE", row[0])
        self.assertEqual(row[1], 0)

    def test_noncanonical_scan_run_is_normalized_to_snapshot_id(self):
        candidate_id = self.bridge.register_signal(7, snapshot_id="scan-round-17")
        row = sqlite3.connect(self.memory).execute(
            "SELECT candidate_json FROM live_candidates WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        self.assertIn('"snapshot_id":"snap_', row[0])

    def test_pending_entry_is_not_a_confirmed_position(self):
        candidate_id = self.bridge.register_signal(7)
        conn = sqlite3.connect(self.compat)
        conn.execute(
            "INSERT INTO trade_executions VALUES(3,7,'live','BTCUSDT','ENTRY_PENDING',1,'entry-1',NULL)"
        )
        conn.commit(); conn.close()
        result = self.bridge.sync_execution(7)
        self.assertFalse(result["position_confirmed"])
        state = sqlite3.connect(self.state).execute(
            "SELECT execution_id,position_id FROM trade_correlation WHERE candidate_id=?", (candidate_id,),
        ).fetchone()
        self.assertTrue(is_id(state[0], "execution"))
        self.assertIsNone(state[1])

    def test_only_protected_exchange_position_marks_candidate_executed(self):
        candidate_id = self.bridge.register_signal(7)
        conn = sqlite3.connect(self.compat)
        conn.execute(
            "INSERT INTO trade_executions VALUES(3,7,'live','BTCUSDT','PROTECTED',1,'entry-1','stop-1')"
        )
        conn.commit(); conn.close()
        result = self.bridge.sync_execution(7)
        self.assertTrue(result["position_confirmed"])
        self.assertTrue(is_id(result["execution_id"], "execution"))
        self.assertTrue(is_id(result["position_id"], "position"))
        executed = sqlite3.connect(self.memory).execute(
            "SELECT executed FROM live_candidates WHERE candidate_id=?", (candidate_id,),
        ).fetchone()[0]
        self.assertEqual(executed, 1)

    def test_canonical_state_position_does_not_require_legacy_execution_row(self):
        candidate_id = self.bridge.register_signal(7)
        execution_id = derived_id("execution", candidate_id)
        ExecutionRepository(lambda: _CONNECT(self.state)).register({
            "signal_id": 7, "candidate_id": candidate_id,
            "execution_id": execution_id, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "PROTECTED", "quantity": 1,
            "entry": 100, "sl": 95, "tp1": 105, "tp2": 110,
            "entry_order_id": "entry-state", "stop_order_id": "stop-state",
        })
        result = self.bridge.sync_execution(7)
        self.assertTrue(result["position_confirmed"])
        self.assertEqual(result["execution_id"], execution_id)
        state = sqlite3.connect(self.state).execute(
            """SELECT e.signal_entity_id,c.signal_id
                 FROM executions e
                 JOIN trade_correlation c ON c.candidate_id=e.candidate_id
                WHERE e.signal_id=7"""
        ).fetchone()
        self.assertTrue(is_id(state[0], "signal"))
        self.assertEqual(state[0], state[1])

    def test_canonical_state_status_wins_over_stale_legacy_projection(self):
        candidate_id = self.bridge.register_signal(7)
        execution_id = derived_id("execution", candidate_id)
        ExecutionRepository(lambda: _CONNECT(self.state)).register({
            "signal_id": 7, "candidate_id": candidate_id,
            "execution_id": execution_id, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "ENTRY_PENDING", "quantity": 1,
            "entry": 100, "sl": 95, "tp1": 105, "tp2": 110,
            "entry_order_id": "entry-state",
        })
        conn = sqlite3.connect(self.compat)
        conn.execute(
            "INSERT INTO trade_executions VALUES(3,7,'live','BTCUSDT','PROTECTED',1,'entry-old','stop-old')"
        )
        conn.commit(); conn.close()
        result = self.bridge.sync_execution(7)
        self.assertEqual(result["status"], "ENTRY_PENDING")
        self.assertFalse(result["position_confirmed"])

    def test_accounting_snapshot_uses_state_geometry_and_memory_strategy(self):
        candidate_id = self.bridge.register_signal(7)
        ExecutionRepository(lambda: _CONNECT(self.state)).register({
            "signal_id": 7, "candidate_id": candidate_id,
            "execution_id": derived_id("execution", candidate_id),
            "mode": "live", "symbol": "BTCUSDT", "direction": "BULLISH",
            "status": "PROTECTED", "quantity": 1.25,
            "entry": 100, "sl": 95, "tp1": 105, "tp2": 110, "tp3": 115,
            "entry_order_id": "entry-state", "stop_order_id": "stop-state",
        })
        conn = sqlite3.connect(self.compat)
        conn.execute("DELETE FROM signals WHERE id=7")
        conn.commit(); conn.close()
        snapshot = self.bridge.execution_accounting_snapshot(7)
        self.assertEqual(snapshot["strategy"], "FAST")
        self.assertEqual(snapshot["quantity"], 1.25)
        self.assertEqual(snapshot["initial_sl"], 95)
        self.assertEqual(snapshot["terminal_tp"], 115)

    def test_candle_close_or_unresolved_funding_cannot_enter_live_memory(self):
        self.bridge.register_signal(7)
        conn = sqlite3.connect(self.compat)
        conn.execute(
            "INSERT INTO trade_executions VALUES(3,7,'live','BTCUSDT','PROTECTED',1,'entry-1','stop-1')"
        )
        conn.commit(); conn.close()
        self.bridge.sync_execution(7)
        with self.assertRaisesRegex(LiveBridgeError, "confirmed_closed_fills_required"):
            self.bridge.record_confirmed_outcome(7, {"status": "CANDLE_CLOSED", "net_r": 2})
        accounting = {
            "status": "CLOSED", "entry": 100, "exit_price": 110,
            "gross_r": 2, "net_r": 1.9, "fees_quote": 0.5,
            "funding_quote": None, "exit_time": 1_789_000_000_000,
        }
        with self.assertRaisesRegex(LiveBridgeError, "funding_unresolved"):
            self.bridge.record_confirmed_outcome(7, accounting)
        count = sqlite3.connect(self.memory).execute(
            "SELECT COUNT(*) FROM live_trade_outcomes"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_complete_confirmed_accounting_is_written_once(self):
        self.bridge.register_signal(7)
        conn = sqlite3.connect(self.compat)
        conn.execute(
            "INSERT INTO trade_executions VALUES(3,7,'live','BTCUSDT','PROTECTED',1,'entry-1','stop-1')"
        )
        conn.commit(); conn.close()
        self.bridge.sync_execution(7)
        accounting = {
            "status": "CLOSED", "entry": 100, "exit_price": 110,
            "gross_r": 2, "net_r": 1.88, "fees_quote": 0.5,
            "funding_quote": 0.1, "exit_time": 1_789_000_000_000,
            "accounting_basis": "confirmed_fills_fees_and_funding",
        }
        outcome = self.bridge.record_confirmed_outcome(7, accounting)
        self.assertEqual(outcome, self.bridge.record_confirmed_outcome(7, accounting))
        count = sqlite3.connect(self.memory).execute(
            "SELECT COUNT(*) FROM live_trade_outcomes"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_pending_outcome_sync_records_only_complete_accounting(self):
        self.bridge.register_signal(7)
        conn = sqlite3.connect(self.compat)
        conn.execute(
            "INSERT INTO trade_executions VALUES(3,7,'live','BTCUSDT','PROTECTED',1,'entry-1','stop-1')"
        )
        conn.commit(); conn.close()
        self.bridge.sync_execution(7)
        unresolved = lambda _signal_id: {
            "status": "CLOSED", "entry": 100, "exit_price": 110,
            "gross_r": 2, "net_r": 1.9, "fees_quote": 0.5,
            "funding_quote": None, "exit_time": 1_789_000_000_000,
        }
        self.assertEqual(self.bridge.sync_confirmed_outcomes(unresolved), [])
        resolved = lambda _signal_id: {
            **unresolved(_signal_id), "net_r": 1.88, "funding_quote": -0.1,
        }
        written = self.bridge.sync_confirmed_outcomes(resolved)
        self.assertEqual(len(written), 1)
        self.assertEqual(self.bridge.sync_confirmed_outcomes(resolved), [])

    def test_confirmed_outcome_sync_uses_state_candidate_mapping(self):
        candidate_id = self.bridge.register_signal(7)
        execution_id = derived_id("execution", candidate_id)
        ExecutionRepository(lambda: _CONNECT(self.state)).register({
            "signal_id": 7, "candidate_id": candidate_id,
            "execution_id": execution_id, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "PROTECTED", "quantity": 1,
            "entry": 100, "sl": 95, "tp1": 105, "tp2": 110,
            "entry_order_id": "entry-state", "stop_order_id": "stop-state",
        })
        self.bridge.sync_execution(7)
        conn = sqlite3.connect(self.compat)
        conn.execute("DELETE FROM signals WHERE id=7")
        conn.commit(); conn.close()
        accounting = lambda signal_id: {
            "status": "CLOSED", "entry": 100, "exit_price": 110,
            "gross_r": 2, "net_r": 1.88, "fees_quote": 0.5,
            "funding_quote": 0.1, "exit_time": 1_789_000_000_000,
            "accounting_basis": "confirmed_fills_after_commissions_and_funding",
            "source_signal_id": signal_id,
        }
        written = self.bridge.sync_confirmed_outcomes(accounting)
        self.assertEqual(len(written), 1)
        row = sqlite3.connect(self.memory).execute(
            "SELECT strategy,symbol,direction FROM live_trade_outcomes"
        ).fetchone()
        self.assertEqual(row, ("FAST", "BTCUSDT", "LONG"))


if __name__ == "__main__":
    unittest.main()
