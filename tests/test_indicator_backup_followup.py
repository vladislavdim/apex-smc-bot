import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

from apex.db.audit_db import migrate_audit, stage_legacy_events, retire_legacy_events
from apex.db.backup import BrainPersistence
from apex.market.adaptive_indicators import LegacyAdaptiveIndicators
from apex.market.indicators import average_true_range, average_directional_index, ema_value
from apex.telemetry import event_log
from apex.telemetry.signal_trace import signal_trace


class WilderReferenceTests(unittest.TestCase):
    def test_matches_independent_talib_reference_at_multiple_history_lengths(self):
        fixture = json.loads((Path(__file__).parent / "fixtures/wilder_reference.json").read_text())
        for case in fixture["cases"]:
            for reference in case["expected"]:
                with self.subTest(case=case["name"], length=reference["length"]):
                    candles = case["candles"][:reference["length"]]
                    self.assertAlmostEqual(average_true_range(candles), reference["atr"], places=10)
                    self.assertAlmostEqual(average_directional_index(candles), reference["adx"], places=10)

    def test_atr_includes_gap_and_smooths_after_seed(self):
        bars = [{"high": 10, "low": 8, "close": 9},
                {"high": 15, "low": 12, "close": 14},
                {"high": 13, "low": 10, "close": 11},
                {"high": 12, "low": 10, "close": 11}]
        self.assertEqual(average_true_range(bars, 2), 3.5)

    def test_adx_requires_full_warmup_and_flat_is_zero(self):
        bars = [{"high": 100, "low": 100, "close": 100}] * 28
        self.assertIsNone(average_directional_index(bars[:-1]))
        self.assertEqual(average_directional_index(bars), 0)

    def test_indicators_are_symmetric_for_long_and_short(self):
        bars = [{"high": 100+i+2, "low": 100+i-1, "close": 100+i} for i in range(80)]
        reflected = [{"high": 400-b["low"], "low": 400-b["high"], "close": 400-b["close"]} for b in bars]
        self.assertEqual(average_directional_index(bars), average_directional_index(reflected))
        self.assertEqual(average_true_range(bars), average_true_range(reflected))

    def test_median_resists_outlier_and_volume_uses_twenty_previous_bars(self):
        bars = [{"open": 100, "high": 101, "low": 99, "close": 100,
                 "volume": i, "is_closed": True} for i in range(200)]
        bars[-1]["high"] = 200
        result = LegacyAdaptiveIndicators(Mock(return_value=bars), ema_value).get_precomputed_indicators("X")
        self.assertEqual(result["atr_med"], 2)
        self.assertGreater(result["atr"], 2)
        self.assertEqual(result["avg_vol"], 188.5)
        self.assertEqual(result["indicator_version"], "wilder-v1")

    def test_invalid_ohlc_does_not_emit_partial_trading_indicators(self):
        bars = [{"high": float("nan"), "low": 99, "close": 100, "volume": 10, "is_closed": True}] * 200
        result = LegacyAdaptiveIndicators(Mock(return_value=bars), ema_value).get_precomputed_indicators("X")
        self.assertEqual(result, {})


class AuditArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = str(Path(self.tmp.name) / "state.db")
        self.audit = str(Path(self.tmp.name) / "audit.db")
        with closing(sqlite3.connect(self.state)) as conn:
            migrate_audit(conn)
            conn.execute("CREATE TABLE executions(signal_id INTEGER)")
            conn.execute("INSERT INTO executions VALUES (465)")
            conn.executemany("INSERT INTO setup_audit_events(event_key,kind,occurred_at,payload_json,synced) VALUES (?,'attempt','2026-09-30',?,?)",
                             [("one", '{"signal_id":465}', 1), ("two", '{"direction":"BEARISH"}', 0)])
            conn.commit()

    def tearDown(self):
        self.tmp.cleanup()

    def count(self, path, table="setup_audit_events"):
        with closing(sqlite3.connect(path)) as conn:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_no_source_deletion_before_durable_backup(self):
        receipts = stage_legacy_events(self.state, self.audit)
        for status in ("backup_failed", "stale_remote", "busy", "not_configured"):
            self.assertEqual(retire_legacy_events(self.state, receipts, {"status": status}), 0)
        self.assertEqual(self.count(self.state), 2)
        self.assertEqual(self.count(self.audit), 2)

    def test_durable_archive_is_bounded_idempotent_and_keeps_trading_state(self):
        receipts = stage_legacy_events(self.state, self.audit, limit=1)
        self.assertEqual(len(receipts), 1)
        self.assertEqual(stage_legacy_events(self.state, self.audit, limit=1), receipts)
        self.assertEqual(retire_legacy_events(self.state, receipts, {"status": "saved"}), 1)
        self.assertEqual(retire_legacy_events(self.state, receipts, {"status": "saved"}), 0)
        self.assertEqual(self.count(self.audit), 1)
        self.assertEqual(self.count(self.state), 1)
        self.assertEqual(self.count(self.state, "executions"), 1)

    def test_changed_source_is_not_removed_with_old_receipt(self):
        receipts = stage_legacy_events(self.state, self.audit)
        with closing(sqlite3.connect(self.state)) as conn:
            conn.execute("UPDATE setup_audit_events SET payload_json='{}' WHERE event_key='one'")
            conn.commit()
        self.assertEqual(retire_legacy_events(self.state, receipts, {"status": "saved"}), 1)
        self.assertEqual(self.count(self.state), 1)
        with self.assertRaisesRegex(RuntimeError, "collision"):
            stage_legacy_events(self.state, self.audit)

    def test_same_database_is_rejected(self):
        with self.assertRaises(ValueError):
            stage_legacy_events(self.state, self.state)


class BackupAndTelemetryRegressionTests(unittest.TestCase):
    def test_scheduled_state_checkpoint_releases_only_its_own_fence(self):
        import ast
        import asyncio
        from types import SimpleNamespace
        from apex.app.runtime import RuntimeSupervisor
        source = Path(__file__).resolve().parents[1] / "apex/app/worker.py"
        node = next(node for node in ast.parse(source.read_text()).body
                    if isinstance(node, ast.AsyncFunctionDef) and node.name == "backup_state_db_to_github")
        for status in ("saved", "unchanged", "busy", "stale_remote", "backup_failed", "not_configured"):
            with self.subTest(status=status):
                runtime = RuntimeSupervisor()
                runtime.activate()
                runtime.inhibit_entries("STATE_BACKUP_DEFERRED")
                runtime.inhibit_entries("UNRELATED_SAFETY_FENCE")
                recover = Mock()
                namespace = {"asyncio": asyncio, "_state_backup_async_lock": None,
                             "_STATE_PERSISTENCE": SimpleNamespace(backup=Mock(return_value={"status": status})),
                             "_V3_RUNTIME": runtime, "_v3_recover_incident": recover}
                exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)
                asyncio.run(namespace["backup_state_db_to_github"]("scheduled_after_retries_exhausted"))
                reasons = runtime.snapshot()["reason_codes"]
                self.assertIn("UNRELATED_SAFETY_FENCE", reasons)
                self.assertEqual("STATE_BACKUP_DEFERRED" not in reasons, status in {"saved", "unchanged"})
                self.assertEqual(recover.called, status in {"saved", "unchanged"})
                self.assertFalse(runtime.allows_new_entries)

    def test_optional_audit_initialization_failure_keeps_state_outbox(self):
        import ast
        import asyncio
        import logging
        from types import SimpleNamespace
        from apex.domain.enums import ComponentState
        source = Path(__file__).resolve().parents[1] / "apex/app/worker.py"
        node = next(node for node in ast.parse(source.read_text()).body
                    if isinstance(node, ast.AsyncFunctionDef) and node.name == "_v3_prepare_audit_store")
        events = SimpleNamespace(DB_PATH="state.db")
        persistence = SimpleNamespace(configured=True, db_path="audit.db",
            restore=Mock(return_value={"reason": "REMOTE_MISSING", "ready": False}))
        namespace = {"asyncio": asyncio, "logging": logging, "_audit_store_ready": False,
                     "_AUDIT_PERSISTENCE": persistence, "_audit_event_log": events,
                     "_closing": closing, "_audit_connect": Mock(side_effect=sqlite3.OperationalError("locked")),
                     "_V3_RUNTIME": Mock(), "_V3_COMPONENT_STATE": ComponentState}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)
        self.assertFalse(asyncio.run(namespace["_v3_prepare_audit_store"]()))
        self.assertEqual(events.DB_PATH, "state.db")
        self.assertFalse(namespace["_audit_store_ready"])

    def test_signal_trace_distinguishes_orders_and_fills_and_resolves_typed_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, memory = str(Path(tmp) / "state.db"), str(Path(tmp) / "memory.db")
            with closing(sqlite3.connect(state)) as conn:
                conn.executescript("""
                    CREATE TABLE executions(signal_id INTEGER,signal_entity_id TEXT,status TEXT,entry_order_id TEXT);
                    INSERT INTO executions VALUES (465,'sig_example','ENTRY_PENDING','order-1');
                    CREATE TABLE trade_correlation(signal_id TEXT,candidate_id TEXT);
                    INSERT INTO trade_correlation VALUES ('sig_example','cand_example');
                    CREATE TABLE execution_fills(signal_id INTEGER,qty TEXT);
                """)
            with closing(sqlite3.connect(memory)) as conn:
                conn.executescript("""
                    CREATE TABLE live_candidates(signal_id TEXT,risk_json TEXT);
                    INSERT INTO live_candidates VALUES ('sig_example','{"approved":true}');
                """)
            before = signal_trace(state, 465, memory_path=memory)
            self.assertFalse(before["exchange_execution_confirmed"])
            self.assertEqual(before["evidence"]["trade_correlation"]["rows"][0]["candidate_id"], "cand_example")
            self.assertTrue(before["evidence"]["live_candidates"]["rows"][0]["risk_json"]["approved"])
            self.assertEqual(before["sources"]["brain"], "UNAVAILABLE")
            with closing(sqlite3.connect(state)) as conn:
                conn.execute("INSERT INTO execution_fills VALUES (465,'0.1')")
                conn.commit()
            self.assertTrue(signal_trace(state, 465)["exchange_execution_confirmed"])

    def test_unicode_telemetry_batches_stay_below_server_byte_limit(self):
        from types import SimpleNamespace
        config = SimpleNamespace(integrations=SimpleNamespace(stats_ingest_url="https://example.invalid", stats_ingest_token="test"))
        post = Mock(return_value=SimpleNamespace(status_code=200))
        events = [{"event_key": str(i), "payload": {"text": "я" * 59000}} for i in range(20)]
        with patch.object(event_log.ApexConfig, "from_env", return_value=config), patch("requests.post", post):
            self.assertTrue(event_log._post_events(events))
        self.assertGreater(post.call_count, 1)
        for call in post.call_args_list:
            self.assertLess(len(json.dumps(call.kwargs["json"]).encode()), 1_800_000)

    def test_slow_snapshot_cannot_overlap_another_store(self):
        first = BrainPersistence("/tmp/unused-one.db", "owner/repo", "test", session=Mock())
        second = BrainPersistence("/tmp/unused-two.db", "owner/repo", "test", session=Mock())
        started, finish = threading.Event(), threading.Event()
        def slow(_reason):
            started.set()
            finish.wait(5)
            return {"status": "saved"}
        with patch.object(first, "_backup_locked", side_effect=slow), patch.object(second, "_backup_locked") as other:
            thread = threading.Thread(target=first.backup)
            thread.start()
            try:
                self.assertTrue(started.wait(2))
                self.assertEqual(second.backup()["status"], "busy")
                other.assert_not_called()
            finally:
                finish.set()
                thread.join(2)

    def test_heartbeat_word_in_real_content_is_not_ignored_by_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "hash.db")
            with closing(sqlite3.connect(path)) as conn:
                conn.execute("CREATE TABLE knowledge(body TEXT)")
                conn.execute("INSERT INTO knowledge VALUES ('heartbeat diagnostic')")
                conn.commit()
                before = BrainPersistence._logical_hash(path)
                conn.execute("UPDATE knowledge SET body='heartbeat changed'")
                conn.commit()
                self.assertNotEqual(before, BrainPersistence._logical_hash(path))

    def test_large_payload_remains_valid_json_and_keeps_correlation(self):
        payload = {"signal_id": 465, "direction": "BEARISH", "checks": ["я" * 100000]}
        text = event_log._payload_text(payload)
        self.assertLessEqual(len(text), 60000)
        self.assertEqual(json.loads(text)["signal_id"], 465)
        self.assertTrue(json.loads(text)["payload_truncated"])


if __name__ == "__main__":
    unittest.main()
