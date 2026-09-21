import unittest
from unittest.mock import Mock, patch

from apex.market.btc_correlation import BtcCorrelationProvider


def _candles(closes):
    return [{"close": float(value)} for value in closes]


class BtcCorrelationProviderTests(unittest.TestCase):
    def test_btc_identity_is_cached_without_market_reads(self):
        get_candles = Mock()
        get_shared = Mock()
        provider = BtcCorrelationProvider(get_candles, get_shared)
        with patch("apex.market.btc_correlation.time.time", side_effect=[10, 20]):
            first = provider.get("BTCUSDT")
            second = provider.get("BTCUSDT")
        self.assertIs(first, second)
        self.assertEqual(first, {
            "corr": 1.0,
            "level": "high",
            "btc_dir": "BULLISH",
            "desc": "BTC itself",
        })
        get_candles.assert_not_called()
        get_shared.assert_not_called()

    def test_positive_correlation_preserves_threshold_and_direction(self):
        closes = [100, 102, 101, 105, 104, 108, 107, 111]
        btc = _candles(closes)
        alt = _candles([value * 2 for value in closes])
        provider = BtcCorrelationProvider(
            Mock(return_value=alt), Mock(return_value=btc),
        )
        result = provider.get("ETHUSDT", period=5)
        self.assertEqual(result["corr"], 1.0)
        self.assertEqual(result["level"], "high")
        self.assertEqual(result["btc_dir"], "BULLISH")

    def test_missing_history_preserves_uncached_fallback(self):
        get_candles = Mock(return_value=[])
        provider = BtcCorrelationProvider(get_candles, Mock(return_value=[]))
        first = provider.get("AAVEUSDT")
        second = provider.get("AAVEUSDT")
        self.assertEqual(first, {
            "corr": 0.7,
            "level": "moderate",
            "btc_dir": "UNKNOWN",
            "desc": "нет данных",
        })
        self.assertIsNot(first, second)
        self.assertEqual(get_candles.call_count, 4)


if __name__ == "__main__":
    unittest.main()
