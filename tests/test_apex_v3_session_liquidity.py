import unittest
from unittest.mock import Mock, patch

from apex.market.session_liquidity import SessionLiquidityProvider


class SessionLiquidityProviderTests(unittest.TestCase):
    def test_uses_latest_closed_bar_and_prior_twenty_bar_average(self):
        candles = [{"volume": 10.0} for _ in range(23)]
        candles[-2]["volume"] = 6.9
        candles[-1]["volume"] = 1000.0
        get_candles = Mock(return_value=candles)
        provider = SessionLiquidityProvider(get_candles)

        self.assertEqual(provider.check("BTCUSDT", "1h"), {
            "ratio": 0.69,
            "ok": False,
            "desc": "Vol ratio: 0.69x (LOW)",
        })
        get_candles.assert_called_once_with("BTCUSDT", "1h", 25)

    def test_ratio_at_threshold_is_accepted(self):
        candles = [{"volume": 10.0} for _ in range(22)]
        candles[-2]["volume"] = 7.0
        result = SessionLiquidityProvider(Mock(return_value=candles)).check("ETHUSDT")
        self.assertEqual(result["ratio"], 0.7)
        self.assertTrue(result["ok"])
        self.assertEqual(result["desc"], "Vol ratio: 0.70x")

    def test_short_history_returns_fail_open_default_without_caching(self):
        get_candles = Mock(return_value=[{"volume": 1.0}] * 21)
        provider = SessionLiquidityProvider(get_candles)
        provider.check("AAVEUSDT")
        provider.check("AAVEUSDT")
        self.assertEqual(get_candles.call_count, 2)

    def test_successful_result_is_cached_per_symbol_and_timeframe(self):
        get_candles = Mock(return_value=[{"volume": 10.0}] * 22)
        provider = SessionLiquidityProvider(get_candles)
        with patch("apex.market.session_liquidity.time.time", side_effect=[10, 20]):
            first = provider.check("BNBUSDT", "4h")
            second = provider.check("BNBUSDT", "4h")
        self.assertIs(first, second)
        get_candles.assert_called_once()


if __name__ == "__main__":
    unittest.main()
