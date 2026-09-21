import unittest
import sys
from types import ModuleType
from unittest.mock import patch

from apex.market.indicators import average_true_range, ema_value
from apex.market import engine_bridge


class IndicatorBridgeTests(unittest.TestCase):
    def test_atr_uses_true_range_with_previous_close(self):
        candles = [
            {"high": 10, "low": 8, "close": 9},
            {"high": 15, "low": 12, "close": 14},
            {"high": 13, "low": 10, "close": 11},
        ]
        self.assertEqual(average_true_range(candles, period=2), 5.0)

    def test_atr_rejects_invalid_or_insufficient_history(self):
        candles = [{"high": 2, "low": 1, "close": 1.5}]
        self.assertIsNone(average_true_range([], period=1))
        self.assertIsNone(average_true_range(candles, period=0))
        self.assertIsNone(average_true_range(candles, period=1))

    def test_ema_uses_seed_average_and_standard_alpha(self):
        self.assertEqual(ema_value([1, 2, 3], 3), 2.0)
        self.assertEqual(ema_value([1, 2, 3, 4], 3), 3.0)

    def test_ema_rejects_incomplete_or_invalid_windows(self):
        self.assertIsNone(ema_value([], 3))
        self.assertIsNone(ema_value([1, 2], 3))
        self.assertIsNone(ema_value([1, 2], 0))

    def test_vwap_and_liquidity_calls_are_deferred_to_canonical_engine(self):
        fake = ModuleType("core.smc_engine")
        fake.calculate_vwap = lambda candles: {"source": "vwap", "count": len(candles)}
        fake.get_liquidity_heatmap = lambda candles: {"source": "heatmap", "count": len(candles)}
        with patch.dict(sys.modules, {"core.smc_engine": fake}):
            self.assertEqual(engine_bridge.calculate_vwap([1]), {"source": "vwap", "count": 1})
            self.assertEqual(
                engine_bridge.get_liquidity_heatmap([1, 2]),
                {"source": "heatmap", "count": 2},
            )


if __name__ == "__main__":
    unittest.main()
