import os
import json
import sqlite3
import tempfile
import unittest

from core.backup_restore import backup_sqlite, verify_sqlite_backup
from core.apex_v2 import dashboard_snapshot
from core.execution_simulator import ExecutionModel, simulate_fill, validate_protection_replace
from core.groq_calibration import calibration_summary, record_outcome, record_prediction
from core.portfolio_dependency import build_dependency_graph, latest_dependency_snapshot, persist_dependency_snapshot
from core.release_guard import evaluate_release_gate, restart_invariants
from core.replay_lab import FrozenEntry, persist_replay_bundle, replay_dashboard_summary, replay_three_tracks
from core.shadow_evidence import evaluate_shadow_rule, load_shadow_evaluations, persist_shadow_evaluation
from core.source_registry import SourcePolicyError, registry_snapshot, source_contract, validate_source_usage
from core.strategy_diagnostics import compare_release_funnels, funnel_snapshot
from external_sources.gate_microstructure import OrderBookReducer, TradeFlow, summarize_order_book


class SourceAndMicrostructureTests(unittest.TestCase):
    def test_source_contract_keeps_gate_and_binance_boundaries(self):
        self.assertTrue(validate_source_usage("gate", "candles"))
        self.assertTrue(validate_source_usage("binance", "validated_execution"))
        with self.assertRaises(SourcePolicyError):
            validate_source_usage("binance", "candles")
        with self.assertRaises(SourcePolicyError):
            validate_source_usage("coinalyze", "scanner")
        self.assertFalse(source_contract("coinalyze")["can_influence_entry"])
        self.assertEqual(source_contract("coinmetrics_community")["mode"], "CONTEXT")
        self.assertGreaterEqual(len(registry_snapshot()), 5)

    def test_order_book_sequence_gap_requires_resync(self):
        reducer = OrderBookReducer("BTCUSDT")
        self.assertTrue(reducer.apply_snapshot([[100, 2]], [[101, 3]], 10))
        self.assertTrue(reducer.apply_delta([[100, 3]], [], 11, 11))
        self.assertFalse(reducer.apply_delta([], [], 13, 13))
        self.assertEqual(reducer.status, "RESYNC_REQUIRED")
        self.assertFalse(reducer.apply_delta([], [], 14, 14))
        features = reducer.features()
        self.assertEqual(features["scope"], "SHADOW_CONTEXT")
        self.assertFalse(features["institutional_intent"])

    def test_flow_and_empty_book_are_bounded(self):
        flow = TradeFlow()
        flow.add("buy", 2); flow.add("sell", 1); flow.add("bad", 99)
        self.assertEqual(flow.snapshot()["trades"], 2)
        self.assertEqual(summarize_order_book([], [101, 1])["levels"], 0)


class ReplayTests(unittest.TestCase):
    def snapshot(self):
        return FrozenEntry(7, "AAVEUSDT", "MTF", "BULLISH", 100, 95, 105, 110, 115, 10)

    def test_three_tracks_are_isolated_and_partial_is_sequential(self):
        candles = [
            {"id": "c1", "open": 100, "high": 106, "low": 99, "close": 105},
            {"id": "c2", "open": 105, "high": 112, "low": 104, "close": 111},
            {"id": "c3", "open": 111, "high": 116, "low": 110, "close": 115},
        ]
        actions = {
            "c1": {"action": "PARTIAL_EXIT", "fraction": 0.5, "price": 105},
            "c2": {"action": "MOVE_STOP_TO_BREAKEVEN"},
        }
        result = replay_three_tracks(self.snapshot(), candles, actual_actions=actions)
        self.assertEqual(result["ACTUAL"]["status"], "CLOSED")
        self.assertIn("TP", result["ACTUAL"]["targets_reached"])
        self.assertEqual(result["NO_MANAGER"]["exit_reason"], "TP")
        self.assertEqual(result["NO_MANAGER"]["quantity"], 10)
        self.assertNotEqual(result["ACTUAL"]["events"], result["NO_MANAGER"]["events"])
        self.assertTrue(any(x["action"] == "PARTIAL_EXIT" for x in result["ACTUAL"]["events"] if x["type"] == "ACTION"))

    def test_invalid_breakeven_before_tp1_is_rejected(self):
        result = replay_three_tracks(self.snapshot(), [{"id": "c1", "open": 100, "high": 103, "low": 99, "close": 102}], actual_actions={"c1": "MOVE_STOP_TO_BREAKEVEN"})
        self.assertEqual(result["ACTUAL"]["status"], "OPEN")
        self.assertTrue(any(x["type"] == "ACTION_REJECTED" for x in result["ACTUAL"]["events"]))

    def test_playbook_only_uses_gate_relative_volume_in_replay(self):
        candles = [{"id": f"c{i}", "open": 100, "high": 106 if i == 0 else 102, "low": 99, "close": 101, "volume": 10} for i in range(19)]
        candles.append({"id": "c19", "open": 101, "high": 102, "low": 100, "close": 100.5, "volume": 20})
        result = replay_three_tracks(self.snapshot(), candles)
        self.assertTrue(any(event.get("action") == "PARTIAL_EXIT" for event in result["PLAYBOOK_ONLY"]["events"] if event.get("type") == "ACTION"))
        self.assertEqual(result["NO_MANAGER"]["status"], "OPEN")

    def test_replay_persistence_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "brain.db")
            candles = [{"id": "c1", "open": 100, "high": 106, "low": 99, "close": 105}]
            result = replay_three_tracks(self.snapshot(), candles)
            a = persist_replay_bundle(self.snapshot(), candles, result, db_path=db)
            b = persist_replay_bundle(self.snapshot(), candles, result, db_path=db)
            self.assertEqual(a, b)
            with sqlite3.connect(db) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM apex_v2_replay_runs").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM apex_v2_replay_track_results").fetchone()[0], 3)
            self.assertEqual(len(replay_dashboard_summary(db)), 1)


class EvidenceAndDiagnosticsTests(unittest.TestCase):
    def test_shadow_gate_reports_full_ab_and_never_activates(self):
        rows = [{"signal_id": i, "strategy": "FAST", "rule_id": "RULE", "status": "CLOSED", "closed_at": f"2026-09-{i+1:02d}", "old_pass": i % 2 == 0, "new_pass": True, "old_r": 0.1, "new_r": 0.3, "delta_r": 0.2} for i in range(30)]
        summary = evaluate_shadow_rule(rows)
        self.assertTrue(summary["promotion_proposed"])
        self.assertFalse(summary["auto_activated"])
        self.assertEqual(summary["new_only"], 15)
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "brain.db")
            persist_shadow_evaluation("FAST", "RULE", summary, db)
            self.assertEqual(load_shadow_evaluations(db)[0]["rule_id"], "RULE")

    def test_calibration_and_dependency_are_persistent_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "brain.db")
            record_prediction("a1", "HOLD", 0.8, strategy="FAST", db_path=db)
            record_outcome("a1", outcome_label=True, reward_r=0.2, db_path=db)
            self.assertEqual(calibration_summary(db)["resolved"], 1)
            graph = build_dependency_graph({"BTCUSDT": list(range(25)), "ETHUSDT": list(range(25)), "AAVEUSDT": list(reversed(range(25)))}, min_samples=2)
            self.assertEqual(graph["source"], "Gate_closed_returns")
            persist_dependency_snapshot(graph, db)
            self.assertEqual(latest_dependency_snapshot(db)["source"], "Gate_closed_returns")

    def test_release_guard_and_backup(self):
        self.assertEqual(evaluate_release_gate({"tests_green": True, "manager_version": 2, "gate_coverage": 1.0})["decision"], "PROCEED")
        self.assertFalse(evaluate_release_gate({"tests_green": False, "gate_coverage": 1.0})["decision"] == "PROCEED")
        self.assertFalse(restart_invariants({"reconciliation_required": True, "protective_orders_touched": True})["ok"])
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = os.path.join(tmp, "src.db"), os.path.join(tmp, "backup.db")
            with sqlite3.connect(src) as conn:
                conn.execute("CREATE TABLE test(id INTEGER)"); conn.execute("INSERT INTO test VALUES(1)")
            report = backup_sqlite(src, dst)
            self.assertTrue(report["ok"])
            self.assertTrue(verify_sqlite_backup(dst, ["test"])["ok"])

    def test_dashboard_exposes_new_diagnostics_without_secrets(self):
        with tempfile.TemporaryDirectory() as tmp:
            snap = dashboard_snapshot(os.path.join(tmp, "brain.db"))
            self.assertTrue(snap["source_registry"])
            self.assertIn("replay_v2", snap)
            self.assertIn("groq_calibration", snap)
            self.assertNotIn("api_key", json.dumps(snap).lower())

    def test_funnel_uses_reached_denominators_and_deduplicates(self):
        data = funnel_snapshot([
            {"event_key": "a", "stage": "CORE", "outcome": "PASS"},
            {"event_key": "a", "stage": "CORE", "outcome": "PASS"},
            {"event_key": "b", "stage": "TRIGGER", "outcome": "FAIL"},
        ])
        core = next(row for row in data["steps"] if row["stage"] == "CORE")
        trigger = next(row for row in data["steps"] if row["stage"] == "TRIGGER")
        self.assertEqual(core["reached"], 1)
        self.assertEqual(core["pass_rate"], 1.0)
        self.assertEqual(trigger["pass_rate"], 0.0)
        self.assertEqual(compare_release_funnels({"v1": []})["v1"]["attempts"], 0)


class ExecutionSimulatorTests(unittest.TestCase):
    def test_fill_is_paper_only_and_protection_replace_is_safe(self):
        fill = simulate_fill("BUY", 2, 100, bid=99.9, ask=100.1, model=ExecutionModel(fee_bps=4, slippage_bps=1))
        self.assertEqual(fill["status"], "FILLED")
        self.assertEqual(fill["execution_scope"], "REPLAY_ONLY")
        self.assertEqual(validate_protection_replace({"order_id": "old"}, {"order_id": "new"})["status"], "PLACE_THEN_CONFIRM_THEN_CANCEL")
        self.assertEqual(validate_protection_replace({"order_id": "same"}, {"order_id": "same"})["status"], "NOOP")


if __name__ == "__main__":
    unittest.main()
