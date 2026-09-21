from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from apex.market import gate_tickers


class GateTickerProviderTests(unittest.TestCase):
    def setUp(self):
        gate_tickers._pairs_cache = []
        gate_tickers._pairs_cache_time = 0
        gate_tickers._price_cache = {}
        gate_tickers._last_price_update = 0

    def test_prices_are_normalized_and_cached(self):
        response = Mock()
        response.json.return_value = [{
            "contract": "BTC_USDT", "last": "65000",
            "change_percentage": "1.234", "volume_24h_quote": "123456",
        }]
        with patch("apex.market.gate_tickers._request_get", return_value=response) as get:
            first = gate_tickers.get_live_prices()
            second = gate_tickers.get_live_prices()
        self.assertEqual(first["BTCUSDT"], {
            "price": 65000.0, "change": 1.23, "volume": 123456.0,
        })
        self.assertIs(second, first)
        get.assert_called_once()

    def test_price_failure_returns_last_cache(self):
        gate_tickers._price_cache = {"BTCUSDT": {"price": 1}}
        with patch("apex.market.gate_tickers._request_get", side_effect=RuntimeError):
            self.assertEqual(gate_tickers.get_live_prices()["BTCUSDT"]["price"], 1)

    def test_liquid_universe_is_bounded_and_cached(self):
        response = Mock()
        response.json.return_value = [{"contract": "BTC_USDT"}]
        with patch("apex.market.gate_tickers._request_get", return_value=response) as get, \
             patch("apex.market.gate_tickers.select_gate_pairs", return_value=[
                 "BTCUSDT", "ETHUSDT",
             ]):
            self.assertEqual(gate_tickers.get_top_pairs(1), ["BTCUSDT"])
            self.assertEqual(gate_tickers.get_top_pairs(2), ["BTCUSDT", "ETHUSDT"])
        get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
