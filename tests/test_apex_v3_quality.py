from __future__ import annotations

import unittest

from apex.domain.enums import Decision, Direction, Strategy
from apex.domain.models import Candidate
from apex.quality.context_relevance import relevant_context, relevant_external_context
from apex.quality.groq_schema import parse_review
from apex.quality.integrity import validate_candidate
from apex.quality.setup_evidence import assess_evidence, participation_group


def candidate(**overrides) -> Candidate:
    values = {
        "candidate_id": "cand_" + "1" * 32,
        "symbol": "BTCUSDT",
        "strategy": Strategy.MTF,
        "direction": Direction.LONG,
        "entry": 100,
        "initial_sl": 90,
        "tp1": 120,
        "tp2": 130,
        "tp3": None,
        "rr": 2.0,
        "snapshot_id": "snap_" + "1" * 32,
    }
    values.update(overrides)
    return Candidate(**values)


class ApexV3QualityTests(unittest.TestCase):
    def test_integrity_accepts_valid_long_without_changing_geometry(self):
        source = candidate()
        result = validate_candidate(source)
        self.assertTrue(result.valid)
        self.assertEqual(result.calculated_rr, 2.0)
        self.assertEqual((source.entry, source.initial_sl, source.tp2), (100, 90, 130))

    def test_integrity_is_symmetric_and_rejects_bad_target_order(self):
        valid_short = candidate(
            direction=Direction.SHORT, initial_sl=110, tp1=80, tp2=70,
        )
        self.assertTrue(validate_candidate(valid_short).valid)
        invalid = candidate(tp1=119, tp2=118, rr=1.9)
        result = validate_candidate(invalid)
        self.assertFalse(result.valid)
        self.assertIn("LONG_TARGET_ORDER_INVALID", result.reason_codes)
        self.assertIn("RR_BELOW_MIN", result.reason_codes)

    def test_tp2_and_tp3_do_not_inflate_or_replace_tp1_rr(self):
        source = candidate(tp1=120, tp2=150, tp3=200, rr=2.0)
        result = validate_candidate(source)
        self.assertTrue(result.valid)
        self.assertEqual(result.calculated_rr, 2.0)
        mismatch = validate_candidate(candidate(tp1=120, tp2=150, rr=5.0))
        self.assertTrue(mismatch.valid)
        self.assertIn("REPORTED_RR_DIFFERS_FROM_TP1", mismatch.warning_codes)

    def test_stale_critical_data_is_fail_closed(self):
        result = validate_candidate(candidate(), critical_data_fresh=False)
        self.assertFalse(result.valid)
        self.assertIn("CRITICAL_DATA_STALE", result.reason_codes)

    def test_participation_inputs_are_one_domain_not_multiple_votes(self):
        self.assertEqual(participation_group(volume=False, cvd=True, taker=False), "PASS")
        evidence = assess_evidence({
            "CORE": "PASS", "LOCATION": "PASS", "TRIGGER": "PASS",
            "PARTICIPATION": "PASS", "GEOMETRY": "PASS", "CONTEXT": "UNKNOWN",
        })
        self.assertTrue(evidence.complete)
        self.assertEqual(len(evidence.domains), 6)

    def test_missing_required_evidence_is_explicit(self):
        result = assess_evidence({"CORE": "PASS", "LOCATION": "FAIL", "TRIGGER": "PASS", "GEOMETRY": "PASS"})
        self.assertFalse(result.complete)
        self.assertEqual(result.reason_codes, ("EVIDENCE_LOCATION_FAIL",))

    def test_context_filter_uses_contract_and_drops_noise(self):
        result = relevant_context(Strategy.FAST, {
            "cvd_real": {"value": 1}, "options": {"iv": 1}, "random": 2,
        })
        self.assertEqual(result, {"cvd_real": {"value": 1}})

    def test_external_context_is_projected_to_contract_names(self):
        result = relevant_external_context(Strategy.FAST, {
            "open_interest": {"value": 10},
            "live_tape": {"buy_usd_60s": 5},
            "options_context": {"dvol": 60},
            "data_quality": {"available_sources": ["gate"]},
        })
        self.assertEqual(result["fields"]["oi_velocity"], {"value": 10})
        self.assertEqual(result["fields"]["cvd_real"], {"buy_usd_60s": 5})
        self.assertNotIn("options", result["fields"])
        self.assertEqual(result["rule"], "LIVE_CONTEXT_ONLY_NOT_A_STRATEGY_GATE")

    def test_universe_context_is_exposed_only_to_relevant_strategies(self):
        market_context = {
            "as_of": "2026-09-11T00:00:00+00:00",
            "timeframes": {
                "1h": {
                    "relative_strength": {"BTCUSDT": {"excess_return": 0.02}},
                    "breadth": {"bullish_pct": 0.6},
                },
            },
        }
        mtf = relevant_external_context(Strategy.MTF, {}, market_context)
        fast = relevant_external_context(Strategy.FAST, {}, market_context)
        self.assertIn("relative_strength", mtf["fields"])
        self.assertIn("breadth", mtf["fields"])
        self.assertNotIn("relative_strength", fast["fields"])
        self.assertNotIn("breadth", fast["fields"])
        self.assertEqual(mtf["market_context_as_of"], market_context["as_of"])

    def test_options_context_is_relevant_to_swing_but_not_mtf(self):
        context = {"options_context": {"source": "deribit", "dvol": 61}}
        swing = relevant_external_context(Strategy.SWING, context)
        mtf = relevant_external_context(Strategy.MTF, context)
        self.assertEqual(swing["fields"]["options"], context["options_context"])
        self.assertNotIn("options", mtf["fields"])

    def test_groq_schema_fails_safe_on_timeout_or_invalid_action(self):
        self.assertEqual(parse_review(None).decision, Decision.WAIT)
        self.assertEqual(parse_review({
            "decision": "CLOSE", "confidence": 1, "reason_codes": ["X"], "short_summary": "x",
        }).decision, Decision.WAIT)

    def test_groq_cannot_inject_geometry(self):
        result = parse_review({
            "decision": "APPROVE", "confidence": 0.8,
            "reason_codes": ["OK"], "short_summary": "ready", "sl": 99,
        })
        self.assertEqual(result.decision, Decision.WAIT)
        self.assertEqual(result.reason_codes, ("GROQ_BAD_SCHEMA",))

    def test_valid_groq_review_is_bounded(self):
        result = parse_review({
            "decision": "APPROVE", "confidence": 0.8,
            "reason_codes": ["CONTEXT_OK"], "short_summary": "ready",
        })
        self.assertEqual(result.decision, Decision.APPROVE)
        self.assertEqual(result.confidence, 0.8)


if __name__ == "__main__":
    unittest.main()
