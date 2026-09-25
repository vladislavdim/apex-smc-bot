import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from apex.db.repositories.strategy_decisions import configure_strategy_decision_state, record_strategy_decision
from core.telegram_dashboard import configure_dashboard_state, fetch_watchlist
from apex.db.state_db import migrate_state
from core.strategy_validation import validation_report, walk_forward_report


class StrategyObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "brain.db")
        configure_strategy_decision_state(None)
        configure_dashboard_state(None)
        self.audit_patch = patch("apex.db.repositories.strategy_decisions._emit_setup_audit_decision")
        self.audit_patch.start()

    def tearDown(self):
        configure_strategy_decision_state(None)
        configure_dashboard_state(None)
        self.audit_patch.stop()
        self.tmp.cleanup()

    def test_decision_journal_keeps_structure_and_groq_evidence(self):
        candidate = {
            "symbol": "BTCUSDT", "grade": "MTF", "timeframe": "1h", "direction": "BEARISH",
            "structure": {"direction": "BEARISH", "event": "CHOCH"},
            "_external_quality_review": {"decision": "REJECT", "confidence": 0.8},
        }
        record_strategy_decision(candidate, "REJECT", "groq_quality_gate", "CONFLICT", db_path=self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT direction,structure_direction,structure_event,outcome,groq_decision FROM strategy_decisions"
            ).fetchone()
        self.assertEqual(row, ("BEARISH", "BEARISH", "CHOCH", "REJECT", "REJECT"))

    def test_production_decision_and_watchlist_share_state_db(self):
        state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            conn = sqlite3.connect(state_path)
            conn.row_factory = sqlite3.Row
            return conn

        conn = factory()
        migrate_state(conn)
        conn.close()
        with sqlite3.connect(self.db_path) as legacy:
            legacy.execute("""CREATE TABLE timing_queue (
                symbol TEXT,direction TEXT,timeframe TEXT,entry REAL,sl REAL,tp1 REAL,tp2 REAL,
                grade TEXT,timing_score INTEGER,created_at TEXT,expires_at TEXT,status TEXT)""")
            legacy.execute("""CREATE TABLE signals (
                id INTEGER PRIMARY KEY,symbol TEXT,direction TEXT,timeframe TEXT,entry REAL,sl REAL,
                tp1 REAL,tp2 REAL,grade TEXT,result TEXT,created_at TEXT)""")
            legacy.execute("CREATE TABLE signal_execution_state(signal_id INTEGER,status TEXT)")
        configure_strategy_decision_state(factory)
        configure_dashboard_state(factory)
        candidate = {
            "symbol": "ETHUSDT", "grade": "SWING", "timeframe": "4h",
            "direction": "BEARISH", "entry": 200, "sl": 210, "tp1": 180,
        }
        record_strategy_decision(
            candidate, "WAIT", "groq_quality_gate", "GROQ_WAIT",
            evidence={"candidate": candidate}, db_path=self.db_path,
        )
        rows = fetch_watchlist(self.db_path)
        self.assertEqual([row["symbol"] for row in rows], ["ETHUSDT"])
        with sqlite3.connect(self.db_path) as legacy:
            self.assertIsNone(legacy.execute(
                "SELECT 1 FROM sqlite_master WHERE name='strategy_decisions'"
            ).fetchone())

    def test_report_separates_long_short_and_marks_small_samples(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE signals (
                id INTEGER PRIMARY KEY,symbol TEXT,direction TEXT,grade TEXT,signal_type TEXT,
                entry REAL,sl REAL,tp1 REAL,tp2 REAL,tp3 REAL,result TEXT,created_at TEXT,closed_at TEXT)""")
            conn.executemany(
                "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (1,"BTCUSDT","BULLISH","MTF",None,100,95,110,115,120,"tp1","2026-01-01","2026-01-02"),
                    (2,"ETHUSDT","BEARISH","MTF",None,100,105,90,85,80,"sl","2026-01-02","2026-01-03"),
                ],
            )
        report = validation_report(self.db_path, min_samples=30)
        self.assertEqual(report["closed_samples"], 2)
        self.assertEqual({row["direction"] for row in report["groups"]}, {"BULLISH", "BEARISH"})
        self.assertTrue(all(row["status"] == "INSUFFICIENT_SAMPLE" for row in report["groups"]))

    def test_walk_forward_uses_only_future_chunks(self):
        rows = [{"r": 1.0 if index % 2 else -1.0} for index in range(20)]
        report = walk_forward_report(rows, min_train=10, test_size=5)
        self.assertEqual(report["status"], "MEASURED")
        self.assertEqual(report["oos_samples"], 10)
        self.assertEqual([fold["train_samples"] for fold in report["folds"]], [10, 15])


if __name__ == "__main__":
    unittest.main()
