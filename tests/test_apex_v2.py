import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from core.apex_v2 import (
    assess_entry_execution,
    build_trade_thesis,
    dashboard_snapshot,
    ensure_apex_v2_schema,
    freeze_trade_thesis,
    material_events,
    portfolio_risk_snapshot,
    record_decision,
    store_market_state,
    store_portfolio_snapshot,
    upsert_incident,
)


class ApexV2Tests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        handle.close()
        self.db_path = handle.name

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db_path + suffix)
            except FileNotFoundError:
                pass

    def candidate(self):
        return {
            "symbol": "BTCUSDT", "strategy": "MTF", "direction": "BULLISH",
            "timeframe": "15m", "entry": 100.0, "sl": 95.0,
            "tp1": 110.0, "tp2": 115.0, "rr": 2.0,
        }

    def test_schema_is_additive_and_idempotent(self):
        ensure_apex_v2_schema(self.db_path)
        ensure_apex_v2_schema(self.db_path)
        conn = sqlite3.connect(self.db_path)
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertIn("apex_v2_theses", tables)
        self.assertIn("apex_v2_opportunities", tables)
        self.assertIn("apex_v2_incidents", tables)

    def test_thesis_freezes_original_levels_once(self):
        first = freeze_trade_thesis(7, self.candidate(), {"state": "VALID", "thesis": "original"}, self.db_path)
        changed = dict(self.candidate(), entry=999.0, sl=998.0, tp1=1001.0)
        second = freeze_trade_thesis(7, changed, {"state": "INVALID"}, self.db_path)
        self.assertEqual(first["entry"], 100.0)
        self.assertEqual(second["entry"], 100.0)
        self.assertEqual(second["thesis"], "original")

    def test_thesis_contains_versions_and_expected_path(self):
        thesis = build_trade_thesis(self.candidate(), {"state": "VALID"})
        self.assertEqual(thesis["version"], "2.0")
        self.assertEqual(thesis["invalidation"]["price"], 95.0)
        self.assertEqual(thesis["expected_path"][1]["r"], 2.0)
        self.assertEqual(thesis["versions"]["manager_version"], 2)

    def test_execution_classifies_target_already_passed(self):
        bnb = {"direction": "BULLISH", "entry": 752.8, "sl": 744.214, "tp1": 754.15}
        result = assess_entry_execution(
            bnb, decision_price=754.85, post_signal_low=753.65, post_signal_high=757.35,
        )
        self.assertEqual(result["state"], "TARGET_ALREADY_PASSED")
        self.assertFalse(result["entry_available"])
        self.assertTrue(result["target_already_passed"])

    def test_portfolio_risk_is_cached_input_only_and_blocks_concentration(self):
        snapshot = portfolio_risk_snapshot([
            {"symbol": "AAVEUSDT", "direction": "BULLISH", "risk_pct": 0.75},
            {"symbol": "BNBUSDT", "direction": "LONG", "risk_pct": 0.75},
            {"symbol": "ETHUSDT", "direction": "BULLISH", "risk_pct": 0.75},
        ], max_positions=5, max_total_risk_pct=4.0, max_same_side_risk_pct=2.0)
        self.assertEqual(snapshot["long_risk_pct"], 2.25)
        self.assertFalse(snapshot["allow_new_open"])
        self.assertIn("MAX_SAME_SIDE_RISK", snapshot["reasons"])

    def test_material_events_are_deduplicated_and_noise_is_ignored(self):
        self.assertEqual(material_events(["BOS", "bos", "NO_CHANGE", "TP1_HIT"]), ["BOS", "TP1_HIT"])

    def test_decisions_and_snapshots_are_idempotent(self):
        self.assertTrue(record_decision(
            action_id="a-1", decision_source="GROQ_MANAGER", action="HOLD",
            signal_id=1, context={"gate": "fresh"}, db_path=self.db_path,
        ))
        self.assertFalse(record_decision(
            action_id="a-1", decision_source="GROQ_MANAGER", action="CLOSE",
            signal_id=1, context={"gate": "fresh"}, db_path=self.db_path,
        ))
        market = store_market_state({"regime": "trend", "btc_direction": "bullish"}, self.db_path)
        portfolio = portfolio_risk_snapshot([])
        store_portfolio_snapshot(portfolio, self.db_path)
        upsert_incident("gate:test", "gate", "warning", "test", db_path=self.db_path)
        snap = dashboard_snapshot(self.db_path)
        self.assertEqual(market["regime"], "TREND")
        self.assertEqual(snap["portfolio"]["risk_state"], "OK")
        self.assertEqual(len(snap["open_incidents"]), 1)
        upsert_incident("gate:test", "gate", "warning", "resolved", resolved=True, db_path=self.db_path)
        self.assertEqual(dashboard_snapshot(self.db_path)["open_incidents"], [])


class DashboardV2SourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("stats_server.py", encoding="utf-8") as handle:
            cls.stats = handle.read()
        with open("core/runtime_observability.py", encoding="utf-8") as handle:
            cls.patch = handle.read()

    def test_v2_sections_and_read_only_contract_exist(self):
        for section in ("APEX V2 · System overview", "Portfolio & Risk", "Trade Manager 2.0", "Opportunity Review"):
            self.assertIn(section, self.stats)
        self.assertIn("Dashboard V2 только читает статистику", self.stats)

    def test_expired_pending_rows_are_excluded(self):
        self.assertIn("_not_expired", self.stats)
        self.assertIn("parsed > now_utc", self.stats)

    def test_runtime_release_patch_keeps_v2_renderer(self):
        self.assertIn("renderV2();renderFunnels()", self.patch)

    def test_rr_catalog_has_no_old_ceiling(self):
        with open("core/strategy_catalog.py", encoding="utf-8") as handle:
            catalog = handle.read()
        self.assertIn('"TP1 RR is at least 2.0"', catalog)
        self.assertNotIn('"TP1 RR is between 2.0 and 4.0"', catalog)

    def test_dashboard_joins_v2_snapshot_and_drops_expired_pending(self):
        import stats_server
        release = "abc123"
        events = [
            {"event_key": "a", "kind": "attempt", "strategy": "FAST", "symbol": "BNBUSDT",
             "occurred_at": "2026-09-08T07:48:11+00:00", "payload": {
                 "release_sha": release, "attempt_key": "a", "strategy": "FAST", "symbol": "BNBUSDT",
                 "finished_at": "2026-09-08T07:48:11+00:00", "outcome": "FILTERED",
                 "checks": [], "candidate": {}, "stop": {"label": "rr < 2.0", "snapshot": {
                     "direction": "BULLISH", "entry": 752.8, "sl": 744.214, "tp1": 754.15,
                     "current_price": 754.85, "rr": 0.16,
                 }},
             }},
            {"event_key": "l", "kind": "ltf_watch", "strategy": "ZONE", "symbol": "XLMUSDT",
             "occurred_at": "2026-09-08T08:00:00+00:00", "payload": {
                 "release_sha": release, "state": "WAITING", "expires_at": "2020-01-01T00:00:00+00:00",
             }},
            {"event_key": "v", "kind": "apex_v2_snapshot", "strategy": "SYSTEM", "symbol": "",
             "occurred_at": "2026-09-08T08:00:00+00:00", "payload": {
                 "release_sha": release, "versions": {"apex_version": "2.0"},
                 "portfolio": {"risk_state": "OK", "open_positions": 0},
                 "execution_mode": {"mode": "paper", "live_armed": False}, "open_incidents": [],
             }},
        ]
        with patch.object(stats_server, "_fetch", return_value=events):
            result = stats_server.build_dashboard(release=release)
        self.assertEqual(result["ltf_watch"]["waiting"], 0)
        self.assertEqual(result["versions"]["apex_version"], "2.0")
        self.assertEqual(result["opportunity_review"]["counts"]["TARGET_ALREADY_PASSED"], 1)


if __name__ == "__main__":
    unittest.main()
