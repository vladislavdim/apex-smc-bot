import unittest
from unittest.mock import Mock

from apex.market.derived_context import LegacyDerivedContext


class DerivedContextTests(unittest.TestCase):
    def test_higher_timeframe_threshold_and_cache_are_preserved(self):
        candles = [{
            "open": 100 + index, "high": 101 + index,
            "low": 99 + index, "close": 100 + index,
        } for index in range(14)]
        reader = Mock(return_value=candles)
        context = LegacyDerivedContext(reader)
        first = context.get_higher_tf_context("BTCUSDT")
        second = context.get_higher_tf_context("BTCUSDT")
        self.assertEqual(first["trend"], "BULLISH")
        self.assertEqual(first["weekly_change"], 5.6)
        self.assertIs(first, second)
        reader.assert_called_once_with("BTCUSDT", "1d", 14)

    def test_sideways_regime_thresholds_are_preserved(self):
        candles = [{
            "open": 100.0, "high": 100.1, "low": 99.9, "close": 100.0,
        } for _ in range(50)]
        context = LegacyDerivedContext(Mock(return_value=candles))
        result = context.get_market_regime("BTCUSDT")
        self.assertEqual(result["mode"], "SIDEWAYS")
        self.assertEqual(result["confidence"], 80)
        self.assertEqual(result["direction"], "BEARISH")

    def test_missing_candles_remain_unknown(self):
        context = LegacyDerivedContext(Mock(return_value=[]))
        self.assertEqual(context.get_market_regime("BTCUSDT"), {
            "mode": "UNKNOWN", "direction": "NONE", "confidence": 0,
        })
        self.assertEqual(context.get_higher_tf_context("BTCUSDT"), {
            "trend": "UNKNOWN", "near_resistance": False, "note": "",
        })


if __name__ == "__main__":
    unittest.main()
