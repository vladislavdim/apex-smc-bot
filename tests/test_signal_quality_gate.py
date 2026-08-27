import unittest

from core.signal_quality_gate import _candidate_view, _extract_json, _normalize_review


class SignalQualityGateTests(unittest.TestCase):
    def test_extracts_fenced_json(self):
        parsed = _extract_json('```json\n{"decision":"REJECT","confidence":0.8}\n```')
        self.assertEqual(parsed["decision"], "REJECT")

    def test_invalid_response_preserves_existing_decision(self):
        review = _normalize_review(None, "not json")
        self.assertEqual(review["decision"], "APPROVE")
        self.assertTrue(review["degraded"])

    def test_unknown_decision_preserves_existing_decision(self):
        review = _normalize_review({"decision": "BLOCK", "confidence": 1}, "{}")
        self.assertEqual(review["decision"], "APPROVE")

    def test_candidate_view_does_not_mutate_trade_levels(self):
        source = {"symbol": "ETHUSDT", "entry": 100, "sl": 95, "tp1": 110, "text": "secret"}
        view = _candidate_view(source)
        self.assertEqual((view["entry"], view["sl"], view["tp1"]), (100, 95, 110))
        self.assertNotIn("text", view)
        self.assertEqual(source["entry"], 100)


if __name__ == "__main__":
    unittest.main()
