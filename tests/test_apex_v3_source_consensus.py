from __future__ import annotations

import unittest

from apex.market.source_consensus import SourceValue, from_context, numeric_consensus


class SourceConsensusTests(unittest.TestCase):
    def test_close_sources_have_high_agreement(self):
        result = numeric_consensus((
            SourceValue("gate", 100, 10, 11, "FRESH"),
            SourceValue("coinalyze", 103, 10, 12, "FRESH"),
        ))
        self.assertEqual(result.status, "HIGH")
        self.assertEqual(result.value, 101.5)
        self.assertFalse(result.can_block_strategy)

    def test_large_difference_is_context_conflict_not_a_strategy_reject(self):
        result = numeric_consensus((
            SourceValue("gate", 100, 10, 11, "FRESH"),
            SourceValue("other", 150, 10, 12, "FRESH"),
        ))
        self.assertEqual(result.status, "CONTEXT_CONFLICT")
        self.assertGreaterEqual(result.dispersion_pct, 0.2)
        self.assertFalse(result.can_block_strategy)

    def test_stale_and_unknown_values_are_not_treated_as_zero(self):
        result = numeric_consensus((
            SourceValue("gate", None, None, None, "UNKNOWN"),
            SourceValue("other", 0, 10, 12, "STALE"),
        ))
        self.assertEqual(result.status, "UNKNOWN")
        self.assertIsNone(result.value)

    def test_context_adapter_preserves_per_source_provenance(self):
        result = from_context({
            "gate": {"value": {"rate": "0.01"}, "event_time": 10, "received_at": 11, "freshness": "FRESH"},
            "coinalyze": {"value": {"rate": "0.011"}, "event_time": 9, "received_at": 12, "freshness": "FRESH"},
        }, "rate")
        self.assertEqual([row.source for row in result.sources], ["gate", "coinalyze"])
        self.assertEqual(result.status, "MEDIUM")


if __name__ == "__main__":
    unittest.main()
