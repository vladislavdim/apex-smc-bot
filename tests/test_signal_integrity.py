import unittest

from core.signal_integrity import validate_candidate


class SignalIntegrityTests(unittest.TestCase):
    def test_valid_bullish_candidate_is_accepted_without_mutation(self):
        candidate = {
            "symbol": "BTCUSDT", "direction": "BULLISH", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 120, "tp3": 130, "rr": 2,
        }
        before = candidate.copy()
        report = validate_candidate(candidate, current_price=99)
        self.assertTrue(report["valid"])
        self.assertEqual(report["calculated_rr"], 2.0)
        self.assertEqual(candidate, before)

    def test_valid_bearish_candidate_is_accepted(self):
        candidate = {
            "symbol": "ETHUSDT", "direction": "BEARISH", "entry": 100,
            "sl": 105, "tp1": 90, "tp2": 80, "tp3": 70, "rr": 2,
        }
        self.assertTrue(validate_candidate(candidate, current_price=101)["valid"])

    def test_invalid_level_order_is_rejected(self):
        candidate = {
            "symbol": "BTCUSDT", "direction": "BULLISH", "entry": 100,
            "sl": 105, "tp1": 110, "tp2": 120, "tp3": 130,
        }
        report = validate_candidate(candidate)
        self.assertFalse(report["valid"])
        self.assertTrue(any("SL < entry" in error for error in report["errors"]))

    def test_non_monotonic_targets_are_rejected(self):
        candidate = {
            "symbol": "BTCUSDT", "direction": "BEARISH", "entry": 100,
            "sl": 105, "tp1": 90, "tp2": 95, "tp3": 70,
        }
        self.assertFalse(validate_candidate(candidate)["valid"])

    def test_tp1_below_two_r_is_rejected(self):
        candidate = {
            "symbol": "BTCUSDT", "direction": "BULLISH", "entry": 100,
            "sl": 90, "tp1": 115, "tp2": 120, "tp3": 125,
        }
        report = validate_candidate(candidate)
        self.assertFalse(report["valid"])
        self.assertTrue(any("below 2.0" in error for error in report["errors"]))

    def test_already_stopped_or_reached_target_is_rejected(self):
        candidate = {
            "symbol": "BTCUSDT", "direction": "BULLISH", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 120, "tp3": 130,
        }
        self.assertFalse(validate_candidate(candidate, current_price=94)["valid"])
        self.assertFalse(validate_candidate(candidate, current_price=111)["valid"])


if __name__ == "__main__":
    unittest.main()
