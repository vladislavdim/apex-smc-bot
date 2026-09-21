import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from core.signal_quality_gate import (
    _candidate_view,
    _extract_json,
    _normalize_review,
    _persist_review,
    review_signal_candidate,
)
from external_sources.models import empty_context


def empty_news(symbol="BTCUSDT"):
    return {
        "symbol": symbol, "risk_level": "LOW", "phase": "NORMAL",
        "nearest_critical_event": None, "critical_events": [], "headlines": [],
        "prediction": "no_directional_prediction", "news_data_unavailable": True,
        "data_quality": {"available_sources": [], "failed_sources": [], "age_seconds": None},
    }


EMPTY_ZONES = {
    "available": False, "symbol": "BTCUSDT", "zones": [],
    "rule": "historical zones are context only",
}
class SignalQualityGateTests(unittest.TestCase):
    def test_extracts_fenced_json(self):
        parsed = _extract_json('```json\n{"decision":"REJECT","confidence":0.8}\n```')
        self.assertEqual(parsed["decision"], "REJECT")

    def test_invalid_response_waits_for_final_confirmation(self):
        review = _normalize_review(None, "not json")
        self.assertEqual(review["decision"], "WAIT")
        self.assertTrue(review["degraded"])

    def test_unknown_decision_waits(self):
        review = _normalize_review({"decision": "BLOCK", "confidence": 1}, "{}")
        self.assertEqual(review["decision"], "WAIT")
        self.assertTrue(review["degraded"])

    def test_candidate_view_does_not_mutate_trade_levels(self):
        source = {
            "symbol": "ETHUSDT", "entry": 100, "sl": 95, "tp1": 110,
            "text": "secret", "_v3_strategy_trace": {"first_check": {"check_id": "FAST.CORE"}},
        }
        view = _candidate_view(source)
        self.assertEqual((view["entry"], view["sl"], view["tp1"]), (100, 95, 110))
        self.assertNotIn("text", view)
        self.assertEqual(view["strategy_check_journal"]["first_check"]["check_id"], "FAST.CORE")
        self.assertEqual(source["entry"], 100)

    def test_persistence_keeps_machine_readable_strict_fields(self):
        review = _normalize_review({
            "decision": "APPROVE", "confidence": 0.8,
            "reason_codes": ["CONTEXT_OK"], "short_summary": "ready",
        }, "{}")
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "reviews.db")
            with patch("core.signal_quality_gate.DB_PATH", path):
                _persist_review(
                    {"symbol": "BTCUSDT", "grade": "FAST", "direction": "LONG"},
                    {}, {}, {}, review,
                )
            with sqlite3.connect(path) as conn:
                row = conn.execute(
                    "SELECT reason_codes_json, short_summary FROM ai_signal_reviews"
                ).fetchone()
        self.assertEqual(json.loads(row[0]), ["CONTEXT_OK"])
        self.assertEqual(row[1], "ready")


class SignalQualityGateAsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._folder = tempfile.TemporaryDirectory()
        self._memory_env = patch.dict(
            os.environ, {"APEX_MEMORY_DB_PATH": os.path.join(self._folder.name, "memory.db")}
        )
        self._memory_env.start()

    def tearDown(self):
        self._memory_env.stop()
        self._folder.cleanup()

    async def test_no_external_data_and_groq_failure_preserve_apex_candidate(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "MTF", "entry": 100, "sl": 95, "tp1": 110, "rr": 2}
        context = empty_context("BTCUSDT")
        context["external_data_unavailable"] = True
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=context)), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, lambda *_: None)
        self.assertEqual(review["decision"], "WAIT")
        self.assertTrue(review["degraded"])
        self.assertEqual((candidate["entry"], candidate["sl"], candidate["tp1"]), (100, 95, 110))

    async def test_external_block_reaches_groq_and_strict_rejects(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "MTF", "entry": 100, "sl": 95, "tp1": 110, "rr": 2}
        captured = []
        def ask(prompt, tokens):
            captured.append(prompt)
            return '{"decision":"REJECT","confidence":0.8,"reason_codes":["CONTEXT_CONFLICT"],"short_summary":"conflict"}'
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("BTCUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, ask)
        self.assertEqual(review["decision"], "REJECT")
        self.assertIn("RELEVANT LIVE CONTEXT", captured[0])
        self.assertIn("NEWS RISK CONTEXT", captured[0])
        self.assertIn("HISTORICAL ZONE MAP", captured[0])
        self.assertNotIn("MARKET MEMORY", captured[0])
        self.assertNotIn("CLOSED-LOOP", captured[0])
        self.assertEqual((candidate["entry"], candidate["sl"], candidate["tp1"], candidate["rr"]), (100, 95, 110, 2))

    async def test_fast_prompt_excludes_irrelevant_options_and_onchain(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "FAST", "entry": 100, "sl": 95, "tp1": 110, "rr": 2}
        context = empty_context("BTCUSDT")
        context["open_interest"] = {"value": 100, "status": "fresh"}
        context["options_context"] = {"dvol": 99, "status": "fresh"}
        context["onchain_activity"] = {"btc_large_transfers_usd": 123456, "status": "fresh"}
        captured = []
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=context)), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            await review_signal_candidate(
                candidate,
                lambda prompt, _tokens: captured.append(prompt) or '{"decision":"APPROVE","confidence":0.8,"reason_codes":["CONTEXT_OK"],"short_summary":"ready"}',
            )
        relevant_block = captured[0].split("RELEVANT LIVE CONTEXT", 1)[1].split("NEWS RISK CONTEXT", 1)[0]
        self.assertIn('"oi_velocity"', relevant_block)
        self.assertNotIn("dvol", relevant_block)
        self.assertNotIn("btc_large_transfers_usd", relevant_block)

    async def test_groq_cannot_replace_candidate_levels(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "WYCKOFF", "entry": 100, "sl": 95, "tp1": 110, "tp2": 120, "tp3": 130, "rr": 2}
        def ask(prompt, tokens):
            return '{"decision":"APPROVE","confidence":0.9,"reason_codes":["OK"],"short_summary":"ready","target":999999}'
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("BTCUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            await review_signal_candidate(candidate, ask)
        self.assertEqual((candidate["entry"], candidate["sl"], candidate["tp1"], candidate["tp2"], candidate["tp3"], candidate["rr"]), (100, 95, 110, 120, 130, 2))

    async def test_malformed_first_response_recovers_with_compact_json_retry(self):
        candidate = {"symbol": "KAITOUSDT", "direction": "BEARISH", "grade": "MTF", "entry": 0.303, "sl": 0.30656, "tp1": 0.2945, "rr": 2.39}
        calls = []
        def ask(prompt, tokens):
            calls.append(prompt)
            if len(calls) == 1:
                return "The setup looks acceptable but this is not JSON."
            return '{"decision":"APPROVE","confidence":0.82,"reason_codes":["STRUCTURE_VALID"],"short_summary":"structure and RR valid"}'
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("KAITOUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news("KAITOUSDT"))), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, ask)
        self.assertEqual(len(calls), 2)
        self.assertLess(len(calls[1]), 6000)
        self.assertEqual(review["decision"], "APPROVE")
        self.assertFalse(review["degraded"])
        self.assertEqual((candidate["entry"], candidate["sl"], candidate["tp1"], candidate["rr"]), (0.303, 0.30656, 0.2945, 2.39))

    async def test_two_malformed_responses_stay_degraded(self):
        candidate = {"symbol": "KAITOUSDT", "direction": "BEARISH", "grade": "MTF", "entry": 0.303, "sl": 0.30656, "tp1": 0.2945, "rr": 2.39}
        calls = []
        def ask(prompt, tokens):
            calls.append(prompt)
            return "not json"
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("KAITOUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news("KAITOUSDT"))), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, ask)
        self.assertEqual(len(calls), 2)
        self.assertTrue(review["degraded"])
        self.assertEqual(review["confidence"], 0.0)

    async def test_low_confidence_approval_waits(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "MTF", "entry": 100, "sl": 95, "tp1": 110, "rr": 2}
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("BTCUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, lambda *_: '{"decision":"APPROVE","confidence":0.4,"reason_codes":["CONTEXT_OK"],"short_summary":"ready"}')
        self.assertEqual(review["decision"], "WAIT")
        self.assertIn("GROQ_CONFIDENCE_BELOW_MIN", review["reason_codes"])

    async def test_legacy_or_geometry_injecting_schema_fails_closed(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "FAST", "entry": 100, "sl": 95, "tp1": 110, "rr": 2}
        calls = []
        def ask(prompt, tokens):
            calls.append(prompt)
            return '{"valid":true,"decision":"APPROVE","confidence":0.99,"reasons":["ok"],"sl":99}'
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("BTCUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, ask)
        self.assertEqual(len(calls), 2)
        self.assertEqual(review["decision"], "WAIT")
        self.assertTrue(review["degraded"])
        self.assertEqual(review["reason_codes"], ["GROQ_BAD_SCHEMA"])
        self.assertEqual((candidate["entry"], candidate["sl"], candidate["tp1"]), (100, 95, 110))


if __name__ == "__main__":
    unittest.main()
