import json
import os
import sqlite3
import tempfile
import unittest

from core import setup_audit


class SetupAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        setup_audit.DB_PATH = os.path.join(self.tmp.name, "audit.db")
        os.environ.pop("APEX_STATS_INGEST_URL", None)
        os.environ.pop("APEX_STATS_INGEST_TOKEN", None)

    def tearDown(self):
        try:
            setup_audit._EVENT_QUEUE.join()
        except Exception:
            pass
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
        self.assertEqual(payload["stop"]["snapshot"]["symbol"], "BTCUSDT")

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


if __name__ == "__main__":
    unittest.main()
