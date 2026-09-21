import unittest
from unittest.mock import Mock

from apex.market.regime_v2 import LegacyRegimeV2


def _candles(closes, ranges=None):
    ranges = ranges or [2.0] * len(closes)
    return [
        {
            "close": float(close),
            "high": float(close) + ranges[index] / 2,
            "low": float(close) - ranges[index] / 2,
        }
        for index, close in enumerate(closes)
    ]


class LegacyRegimeV2Tests(unittest.TestCase):
    def test_short_history_preserves_standard_unknown_allowlist(self):
        get_candles = Mock(return_value=_candles(range(10)))
        result = LegacyRegimeV2(get_candles).detect("BTCUSDT")
        self.assertEqual(result, {"type": "unknown", "enabled": ["MTF", "ZONE"]})
        get_candles.assert_called_once_with("BTCUSDT", "4h", 50)

    def test_volatile_trend_preserves_strategy_allowlist(self):
        closes = [100.0 + index for index in range(50)]
        ranges = [1.0] * 43 + [4.0] * 7
        result = LegacyRegimeV2(
            Mock(return_value=_candles(closes, ranges)),
        ).detect("ETHUSDT")
        self.assertEqual(result, {
            "type": "trend",
            "enabled": ["MTF", "FAST", "SWING"],
        })

    def test_slow_trend_preserves_strategy_allowlist(self):
        closes = [100.0 + index for index in range(50)]
        result = LegacyRegimeV2(Mock(return_value=_candles(closes))).detect("SOLUSDT")
        self.assertEqual(result, {"type": "trend_slow", "enabled": ["MTF", "ZONE"]})

    def test_provider_error_preserves_expanded_unknown_allowlist(self):
        result = LegacyRegimeV2(Mock(side_effect=RuntimeError("offline"))).detect("AAVEUSDT")
        self.assertEqual(result, {
            "type": "unknown",
            "enabled": ["MTF", "ZONE", "SWING"],
        })


if __name__ == "__main__":
    unittest.main()
