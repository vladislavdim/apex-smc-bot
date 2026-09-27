from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from apex.db.repositories.executions import ExecutionRepository
from apex.db.state_db import migrate_state
from apex.execution.orders import (
    ExecutionConfig,
    _v3_risk_admission,
    configure_execution_state,
)
from apex.ui.telegram.system import format_system_status
from core.apex_v2 import dashboard_snapshot, emit_dashboard_snapshot


class ProductionAuditFixTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.compat_path = os.path.join(self.tmp.name, "brain.db")
        self.state_path = os.path.join(self.tmp.name, "apex_state.db")

        def factory():
            conn = sqlite3.connect(self.state_path)
            conn.row_factory = sqlite3.Row
            return conn

        self.factory = factory
        conn = factory()
        migrate_state(conn)
        conn.close()
        configure_execution_state(factory)

    def tearDown(self):
        configure_execution_state(None)
        self.tmp.cleanup()

    def test_state_repository_reports_committed_risk(self):
        repo = ExecutionRepository(self.factory)
        repo.register({
            "signal_id": 1, "mode": "live", "symbol": "BTCUSDT",
            "direction": "BULLISH", "status": "PROTECTED",
            "risk_usdt": 5.0, "balance_usdt": 1000.0,
        })
        repo.register({
            "signal_id": 2, "mode": "live", "symbol": "ETHUSDT",
            "direction": "BEARISH", "status": "ENTRY_PENDING",
            "risk_usdt": 7.0, "balance_usdt": 1000.0,
        })
        exposure = repo.risk_exposure(
            "BULLISH", ("PROTECTED", "ENTRY_PENDING"),
        )
        self.assertEqual(exposure["portfolio_risk_usdt"], 12.0)
        self.assertEqual(exposure["same_side_risk_usdt"], 5.0)

    def test_v3_risk_admission_preserves_candidate_geometry(self):
        config = ExecutionConfig(
            enabled=True, mode="live", leverage=5, risk_pct=0.5,
            max_risk_pct=1.0, max_total_risk_pct=3.0,
            max_same_side_risk_pct=2.0,
        )
        candidate = {
            "symbol": "BTCUSDT", "grade": "MTF", "direction": "BULLISH",
            "entry": 100.0, "sl": 95.0, "tp1": 110.0, "tp2": 115.0,
            "rr": 2.0,
        }
        decision, domain = _v3_risk_admission(
            candidate, 10, config, equity_quote=1000.0,
            daily_loss_locked=False,
        )
        self.assertEqual(decision.decision, "KEEP")
        self.assertEqual(decision.final_risk_pct, 0.5)
        self.assertEqual((domain.entry, domain.initial_sl, domain.tp1, domain.tp2),
                         (100.0, 95.0, 110.0, 115.0))

    def test_telegram_system_shows_cached_binance_balance(self):
        text = format_system_status({
            "status": "READY", "health": "HEALTHY", "ready": True,
            "new_entries": "ON", "release_sha": "abcdef1234567890",
            "components": {},
            "execution": {
                "mode": "live", "live_armed": True, "live_active_count": 1,
                "account": {
                    "available": True, "wallet_balance": 123.45,
                    "available_balance": 100.25,
                    "cross_unrealized_pnl": 2.5,
                    "cache_age_seconds": 10, "stale": False,
                },
            },
        })
        self.assertIn("123.45 USDT", text)
        self.assertIn("100.25 USDT", text)
        self.assertIn("+2.50 USDT", text)
        self.assertIn("FRESH", text)

    def test_dashboard_prefers_state_execution_and_balance(self):
        repo = ExecutionRepository(self.factory)
        repo.register({
            "signal_id": 21, "mode": "live", "symbol": "SOLUSDT",
            "direction": "BULLISH", "status": "ENTRY_PENDING",
            "entry": 100.0, "sl": 95.0, "tp1": 110.0, "tp2": 115.0,
            "quantity": 1.0, "risk_usdt": 5.0, "balance_usdt": 1000.0,
            "leverage": 5,
        })
        with self.factory() as conn:
            conn.execute(
                """INSERT INTO execution_account_cache(
                       exchange,wallet_balance,available_balance,
                       cross_unrealized_pnl,fetched_at_epoch,attempted_at_epoch,last_error
                   ) VALUES('binance_futures',1000,900,12.5,2000000000,2000000000,'')"""
            )
            conn.commit()
        env = {
            "APEX_COMPAT_DB_PATH": self.compat_path,
            "APEX_STATE_DB_PATH": self.state_path,
        }
        with patch.dict(os.environ, env, clear=False):
            snap = dashboard_snapshot(self.compat_path, require_state=True)
        self.assertEqual(snap["execution_health"]["source"], "APEX_STATE_DB")
        self.assertEqual(snap["execution_health"]["statuses"]["ENTRY_PENDING"], 1)
        self.assertEqual(snap["execution_health"]["account"]["wallet_balance"], 1000.0)

    def test_production_dashboard_rejects_missing_state_instead_of_legacy_fallback(self):
        with patch.dict(os.environ, {
            "APEX_COMPAT_DB_PATH": self.compat_path,
            "APEX_STATE_DB_PATH": self.state_path + ".missing",
        }, clear=False):
            with self.assertRaisesRegex(RuntimeError, "dashboard_state_projection_unavailable"):
                dashboard_snapshot(self.compat_path, require_state=True)
            with patch("core.setup_audit.emit_event") as emit:
                self.assertFalse(emit_dashboard_snapshot(self.compat_path, require_state=True))
                emit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
