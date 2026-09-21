import unittest

from apex.ui.risk_calculator import calc_risk


class RiskCalculatorTests(unittest.TestCase):
    def test_preserves_legacy_position_size_contract(self):
        self.assertEqual(
            calc_risk(1000, 1, 100, 98),
            {
                "risk_amount": 10.0,
                "position_size": 500.0,
                "sl_distance": 2.0,
                "leverage": 0.5,
                "contracts": 5.0,
            },
        )

    def test_caps_displayed_leverage_at_twenty(self):
        result = calc_risk(1000, 1, 100, 99.99)
        self.assertEqual(result["leverage"], 20)

    def test_zero_stop_distance_has_no_position_size(self):
        self.assertIsNone(calc_risk(1000, 1, 100, 100))


if __name__ == "__main__":
    unittest.main()
