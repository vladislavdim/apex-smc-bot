import unittest
from unittest.mock import AsyncMock, patch

from core.signal_quality_gate import _candidate_view, _extract_json, _normalize_review, review_signal_candidate
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
EMPTY_LEARNING = {
    "available": False, "closed_results_total": 0,
    "new_strategy_research_ready": False,
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

    def test_candidate_view_does_not_mutate_trade_levels(self):
        source = {"symbol": "ETHUSDT", "entry": 100, "sl": 95, "tp1": 110, "text": "secret"}
        view = _candidate_view(source)
        self.assertEqual((view["entry"], view["sl"], view["tp1"]), (100, 95, 110))
        self.assertNotIn("text", view)
        self.assertEqual(source["entry"], 100)


class SignalQualityGateAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_external_data_and_groq_failure_preserve_apex_candidate(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "MTF", "entry": 100, "sl": 95, "tp1": 110, "rr": 2}
        context = empty_context("BTCUSDT")
        context["external_data_unavailable"] = True
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=context)), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.build_learning_context", return_value=EMPTY_LEARNING), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, lambda *_: None)
        self.assertEqual(review["decision"], "WAIT")
        self.assertTrue(review["degraded"])
        self.assertEqual((candidate["entry"], candidate["sl"], candidate["tp1"]), (100, 95, 110))

    async def test_external_block_reaches_groq_and_valid_false_rejects(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "MTF", "entry": 100, "sl": 95, "tp1": 110, "rr": 2}
        captured = []
        def ask(prompt, tokens):
            captured.append(prompt)
            return '{"valid": false, "decision": "APPROVE", "confidence": 0.8, "reasons": ["conflict"]}'
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("BTCUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.build_learning_context", return_value=EMPTY_LEARNING), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, ask)
        self.assertEqual(review["decision"], "REJECT")
        self.assertIn("EXTERNAL MARKET CONTEXT", captured[0])
        self.assertIn("NEWS RISK CONTEXT", captured[0])
        self.assertIn("MARKET MEMORY", captured[0])
        self.assertIn("HISTORICAL ZONE MAP", captured[0])
        self.assertIn("CLOSED-LOOP OUTCOME EVIDENCE", captured[0])
        self.assertEqual((candidate["entry"], candidate["sl"], candidate["tp1"], candidate["rr"]), (100, 95, 110, 2))

    async def test_groq_cannot_replace_candidate_levels(self):
        candidate = {"symbol": "BTCUSDT", "direction": "BULLISH", "grade": "WYCKOFF", "entry": 100, "sl": 95, "tp1": 110, "tp2": 120, "tp3": 130, "rr": 2}
        def ask(prompt, tokens):
            return '{"valid": true, "decision": "APPROVE", "target": 999999, "reasons": ["ok"]}'
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("BTCUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news())), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.build_learning_context", return_value=EMPTY_LEARNING), \
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
            return '{"valid":true,"decision":"APPROVE","confidence":0.82,"reasons":["structure and RR valid"],"risks":[]}'
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("KAITOUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=empty_news("KAITOUSDT"))), \
             patch("core.signal_quality_gate.build_zone_context", return_value=EMPTY_ZONES), \
             patch("core.signal_quality_gate.build_learning_context", return_value=EMPTY_LEARNING), \
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
             patch("core.signal_quality_gate.build_learning_context", return_value=EMPTY_LEARNING), \
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
             patch("core.signal_quality_gate.build_learning_context", return_value=EMPTY_LEARNING), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(candidate, lambda *_: '{"valid":true,"decision":"APPROVE","confidence":0.4}')
        self.assertEqual(review["decision"], "WAIT")


if __name__ == "__main__":
    unittest.main()
