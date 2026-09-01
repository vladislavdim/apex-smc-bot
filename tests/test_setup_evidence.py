import json
import os
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from unittest.mock import AsyncMock, patch

from core.setup_evidence import (
    assess_candidate,
    ensure_setup_evidence_schema,
    persist_assessment,
    setup_evidence_dashboard,
)
from core.signal_quality_gate import review_signal_candidate
from core.telegram_dashboard import format_setup_evidence_dashboard
from external_sources.models import empty_context


def mtf(direction="BULLISH"):
    bullish = direction == "BULLISH"
    return {
        "symbol": "BTCUSDT", "grade": "MTF", "scan_type": "mtf", "direction": direction,
        "timeframe": "1h", "entry": 100, "sl": 95 if bullish else 105,
        "tp1": 110 if bullish else 90, "tp2": 115 if bullish else 85,
        "tp3": 120 if bullish else 80, "rr": 2.0,
        "technical_evidence": {
            "causal_matrix_ready": True,
            "timeframe_alignment": {"1h": direction, "4h": direction, "1d": direction},
            "ob": {"found": True}, "fvg": {"found": True},
            "structure_event": {"type": "BOS", "direction": direction},
            "bos_choch": True, "volume_confirmed": True, "btc_confirmed": True,
        },
    }


class SetupEvidenceTests(unittest.TestCase):
    def test_long_and_short_use_mirrored_geometry(self):
        self.assertEqual(assess_candidate(mtf("BULLISH"))["state"], "STRONG")
        self.assertEqual(assess_candidate(mtf("BEARISH"))["state"], "STRONG")

    def test_invalid_geometry_is_fatal_and_levels_are_immutable(self):
        candidate = mtf()
        candidate["sl"] = 105
        before = deepcopy(candidate)
        result = assess_candidate(candidate)
        self.assertEqual(result["state"], "INVALID")
        self.assertEqual(candidate, before)
        self.assertEqual(result["dimensions"]["conflict_risk"], "FATAL")

    def test_missing_trigger_is_developing_not_rescued_by_weak_facts(self):
        candidate = mtf()
        candidate["technical_evidence"].pop("structure_event")
        candidate["technical_evidence"]["bos_choch"] = False
        context = empty_context("BTCUSDT")
        context["funding"]["bias"] = "bullish"
        result = assess_candidate(candidate, context)
        self.assertEqual(result["state"], "DEVELOPING")
        self.assertIn("fresh 15m BOS/CHoCH trigger", result["missing"])

    def test_correlated_participation_is_one_domain(self):
        context = empty_context("BTCUSDT")
        for key in ("large_orders", "whale_activity", "smart_money", "live_tape"):
            context[key]["bias"] = "bullish"
        result = assess_candidate(mtf(), context)
        domain = result["domains"]["participation_derivatives"]
        self.assertEqual(domain["independent_domains"], ["participation"])
        self.assertEqual(result["state"], "STRONG")

    def test_independent_derivatives_and_participation_can_be_exceptional(self):
        context = empty_context("BTCUSDT")
        context["large_orders"]["bias"] = "bullish"
        context["open_interest"]["change_1h_pct"] = 2.5
        result = assess_candidate(mtf(), context)
        self.assertEqual(result["state"], "EXCEPTIONAL")

    def test_falling_open_interest_alone_does_not_confirm_direction(self):
        context = empty_context("BTCUSDT")
        context["open_interest"]["change_1h_pct"] = -4.0
        result = assess_candidate(mtf(), context)
        self.assertEqual(result["state"], "STRONG")
        self.assertNotIn("derivatives", result["domains"]["participation_derivatives"]["independent_domains"])

    def test_two_independent_external_conflict_domains_cap_at_valid(self):
        context = empty_context("BTCUSDT")
        context["large_orders"]["bias"] = "bearish"
        context["large_orders"]["source"] = "provider-a"
        context["exchange_flow"]["bias"] = "bearish"
        context["exchange_flow"]["source"] = "provider-b"
        result = assess_candidate(mtf(), context)
        self.assertEqual(result["state"], "VALID")
        self.assertEqual(result["dimensions"]["conflict_risk"], "MATERIAL")

    def test_all_strategy_matrices_classify_complete_backbones(self):
        base = {"symbol": "XUSDT", "direction": "BULLISH", "entry": 100, "sl": 95,
                "tp1": 110, "rr": 2, "technical_evidence": {"causal_matrix_ready": True}}
        cases = [
            ("SWING", {"htf_dir": "BULLISH", "ob": True, "structure_event": True, "structure_event_1h": True}),
            ("ZONE", {"zone": "Discount", "zone_type": "OB", "structure_event": True}),
            ("FAST", {"zone": "Discount OB", "ob": True, "structure_event": True}),
            ("WYCKOFF", {"phases": "SC → AR → ST → Spring → SOS", "spring": True, "sos": True}),
        ]
        for strategy, evidence in cases:
            with self.subTest(strategy=strategy):
                candidate = deepcopy(base)
                candidate.update({"grade": strategy, "scan_type": strategy.lower()})
                candidate["technical_evidence"].update(evidence)
                self.assertIn(assess_candidate(candidate)["state"], {"VALID", "STRONG"})

    def test_storage_is_idempotent_and_dashboard_has_no_controls(self):
        candidate = mtf()
        assessment = assess_candidate(candidate)
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "test.db")
            ensure_setup_evidence_schema(path)
            persist_assessment(candidate, assessment, "FINAL", path)
            persist_assessment(candidate, assessment, "FINAL", path)
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM setup_assessments").fetchone()[0], 1)
            text = format_setup_evidence_dashboard(setup_evidence_dashboard(path))
        self.assertIn("STRONG", text)
        self.assertNotIn("Активировать", text)
        self.assertNotIn("Отклонить", text)


class SetupEvidenceGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_groq_cannot_promote_developing_candidate(self):
        candidate = mtf()
        candidate["technical_evidence"].pop("structure_event")
        candidate["technical_evidence"]["bos_choch"] = False
        news = {"risk_level": "LOW", "data_quality": {"available_sources": [], "failed_sources": []}}
        with patch("core.signal_quality_gate.collect_external_context", new=AsyncMock(return_value=empty_context("BTCUSDT"))), \
             patch("core.signal_quality_gate.collect_news_context", new=AsyncMock(return_value=news)), \
             patch("core.signal_quality_gate.build_zone_context", return_value={"available": False, "zones": []}), \
             patch("core.signal_quality_gate.build_learning_context", return_value={"available": False}), \
             patch("core.signal_quality_gate.persist_context"), patch("core.signal_quality_gate.persist_news_context"), \
             patch("core.signal_quality_gate.persist_assessment"), patch("core.signal_quality_gate._persist_review"):
            review = await review_signal_candidate(
                candidate,
                lambda *_: json.dumps({"valid": True, "decision": "APPROVE", "confidence": 0.99}),
            )
        self.assertEqual(review["decision"], "WAIT")
        self.assertEqual(review["setup_assessment"]["state"], "DEVELOPING")


if __name__ == "__main__":
    unittest.main()
