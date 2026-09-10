import json
import os
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core import setup_audit


class SetupAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.previous_db_path = setup_audit.DB_PATH
        self.previous_ingest_env = {
            key: os.environ.get(key)
            for key in ("APEX_STATS_INGEST_URL", "APEX_STATS_INGEST_TOKEN")
        }
        setup_audit.DB_PATH = os.path.join(self.tmp.name, "audit.db")
        os.environ.pop("APEX_STATS_INGEST_URL", None)
        os.environ.pop("APEX_STATS_INGEST_TOKEN", None)

    def tearDown(self):
        try:
            setup_audit._EVENT_QUEUE.join()
        except Exception:
            pass
        setup_audit.DB_PATH = self.previous_db_path
        for key, value in self.previous_ingest_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _rows(self):
        setup_audit._EVENT_QUEUE.join()
        conn = sqlite3.connect(setup_audit.DB_PATH)
        try:
            return conn.execute("SELECT kind,payload_json FROM setup_audit_events ORDER BY created_at,event_key").fetchall()
        finally:
            conn.close()

    def test_filtered_attempt_records_exact_failed_gate(self):
        @setup_audit.audit_strategy("FAST")
        def sample(symbol, fail):
            if setup_audit.audit_test("FAST_X", fail, "test condition", "fail", 10):
                return setup_audit.audit_fail("FAST_R", "test condition", locals(), "fail", 11)
            return {"symbol": symbol, "direction": "BULLISH", "entry": 10, "sl": 9, "tp1": 12, "rr": 2.0}

        self.assertIsNone(sample("BTCUSDT", True))
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        payload = json.loads(rows[0][1])
        self.assertEqual(payload["outcome"], "FILTERED")
        self.assertEqual(payload["stop"]["code"], "FAST_R")
        self.assertEqual(payload["checks"][0]["state"], "FAIL")
        self.assertTrue(payload["checks"][0]["blocking_stop"])
        self.assertEqual(payload["stop"]["blocking_check_code"], "FAST_X")
        self.assertEqual(payload["stop"]["snapshot"]["symbol"], "BTCUSDT")

    def test_passed_or_adjacent_check_never_owns_stop(self):
        @setup_audit.audit_strategy("SWING")
        def sample(symbol):
            setup_audit.audit_test("LTF_DATA", False, "LTF data available", "not candles", 10)
            setup_audit.audit_test("FRESH_BOS", True, "Fresh BOS", "not fresh_bos", 20)
            setup_audit.audit_test("ZONE", False, "Zone available", "not zone", 21)
            return setup_audit.audit_fail("SWING_STOP", "Fresh BOS", locals(), "not fresh_bos", 22)

        self.assertIsNone(sample("ETHUSDT"))
        payload = json.loads(self._rows()[0][1])
        owners = [check for check in payload["checks"] if check["blocking_stop"]]
        self.assertEqual([item["code"] for item in owners], ["FRESH_BOS"])
        self.assertFalse(payload["checks"][0]["blocking_stop"])
        self.assertFalse(payload["checks"][2]["blocking_stop"])

    def test_candidate_gets_private_correlation_key_and_pass_gate(self):
        @setup_audit.audit_strategy("ZONE")
        def sample(symbol):
            if setup_audit.audit_test("ZONE_X", False, "test condition", "False", 20):
                return setup_audit.audit_fail("ZONE_R", "test condition", locals(), "False", 21)
            return {"symbol": symbol, "direction": "BEARISH", "entry": 10, "sl": 11, "tp1": 8, "rr": 2.0}

        candidate = sample("ETHUSDT")
        self.assertTrue(candidate.get("_audit_attempt_key"))
        rows = self._rows()
        payload = json.loads(rows[-1][1])
        self.assertEqual(payload["outcome"], "CANDIDATE")
        self.assertEqual(payload["checks"][0]["state"], "PASS")
        self.assertEqual(payload["candidate"]["rr"], 2.0)

    def test_decision_event_preserves_attempt_key_and_levels(self):
        candidate = {"symbol": "SOLUSDT", "scan_type": "FAST", "direction": "BULLISH",
                     "entry": 100, "sl": 98, "tp1": 104, "rr": 2.0, "_audit_attempt_key": "abc"}
        setup_audit.emit_decision_event(candidate, "WAIT", "groq_quality_gate", "reason", {"x": 1})
        rows = self._rows()
        payload = json.loads(rows[-1][1])
        self.assertEqual(payload["attempt_key"], "abc")
        self.assertEqual(payload["stage"], "groq_quality_gate")
        self.assertEqual(payload["entry"], 100)

    def test_retry_flush_posts_one_batch_and_marks_every_event(self):
        for index in range(3):
            setup_audit._persist_event({
                "event_key": f"event-{index}", "kind": "attempt", "strategy": "FAST",
                "symbol": "BTCUSDT", "occurred_at": f"2026-09-10T12:00:0{index}+00:00",
                "payload": {"index": index},
            })
        os.environ["APEX_STATS_INGEST_URL"] = "https://stats.invalid/ingest"
        os.environ["APEX_STATS_INGEST_TOKEN"] = "test-token"
        with patch("requests.post", return_value=SimpleNamespace(status_code=200)) as post:
            setup_audit._flush_unsynced(100)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(len(post.call_args.kwargs["json"]), 3)
        conn = sqlite3.connect(setup_audit.DB_PATH)
        try:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM setup_audit_events WHERE synced=1").fetchone()[0], 3)
        finally:
            conn.close()

    def test_retry_flush_chunks_large_backlog_below_server_limit(self):
        for index in range(45):
            setup_audit._persist_event({
                "event_key": f"bulk-{index}", "kind": "attempt", "strategy": "FAST",
                "symbol": "BTCUSDT", "occurred_at": f"2026-09-10T12:01:{index:02d}+00:00",
                "payload": {"index": index},
            })
        os.environ["APEX_STATS_INGEST_URL"] = "https://stats.invalid/ingest"
        os.environ["APEX_STATS_INGEST_TOKEN"] = "test-token"
        with patch("requests.post", return_value=SimpleNamespace(status_code=200)) as post:
            setup_audit._flush_unsynced(100)
        self.assertEqual(post.call_count, 3)
        self.assertEqual([len(call.kwargs["json"]) for call in post.call_args_list], [20, 20, 5])


if __name__ == "__main__":
    unittest.main()
