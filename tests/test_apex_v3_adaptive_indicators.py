import unittest
from unittest.mock import Mock, patch

from apex.market.adaptive_indicators import LegacyAdaptiveIndicators


def _candles(count=25):
    return [
        {"open": 100.0 + i, "high": 102.0 + i, "low": 99.0 + i,
         "close": 101.0 + i, "volume": 10.0 + i}
        for i in range(count)
    ]


class AdaptiveIndicatorsTests(unittest.TestCase):
    def test_indicators_preserve_legacy_contract(self):
        get_candles = Mock(return_value=_candles())
        ema_value = Mock(side_effect=lambda _closes, period: float(period))
        provider = LegacyAdaptiveIndicators(get_candles, ema_value)
        result = provider.get_precomputed_indicators("BTCUSDT", "4h")
        self.assertEqual(result["atr"], 3.0)
        self.assertEqual(result["atr_med"], 3.0)
        self.assertEqual(result["volatility_factor"], 1.0)
        self.assertEqual((result["ema20"], result["ema50"], result["ema200"]), (20.0, 50.0, 200.0))
        self.assertEqual(result["avg_vol"], 24.0)
        self.assertTrue(result["hh_hl"])
        self.assertFalse(result["ll_lh"])
        get_candles.assert_called_once_with("BTCUSDT", "4h", 100)

    def test_indicator_result_is_cached_per_symbol_and_timeframe(self):
        get_candles = Mock(return_value=_candles())
        provider = LegacyAdaptiveIndicators(get_candles, Mock(return_value=1.0))
        with patch("apex.market.adaptive_indicators.time.time", side_effect=[10, 20]):
            first = provider.get_precomputed_indicators("ETHUSDT", "1h")
            second = provider.get_precomputed_indicators("ETHUSDT", "1h")
        self.assertIs(first, second)
        get_candles.assert_called_once()

    def test_adaptive_defaults_are_cached(self):
        provider = LegacyAdaptiveIndicators(Mock(return_value=[]), Mock())
        provider.get_precomputed_indicators = Mock(return_value={})
        with patch("apex.market.adaptive_indicators.time.time", side_effect=[10, 20]):
            first = provider.get_adaptive_params("AAVEUSDT", [{}], "4h")
            second = provider.get_adaptive_params("AAVEUSDT", [{}], "4h")
        self.assertIs(first, second)
        self.assertEqual(first, {"volatility_factor": 1.0, "adx": 25.0, "adx_strong": False, "adx_weak": False})
        provider.get_precomputed_indicators.assert_called_once_with("AAVEUSDT", "4h")


if __name__ == "__main__":
    unittest.main()
