import unittest
from unittest.mock import Mock, patch

from apex.market import optional_prices


class OptionalPricesProviderTests(unittest.TestCase):
    def setUp(self):
        optional_prices._yahoo_cache = {}
        optional_prices._yahoo_cache_time = 0
        optional_prices._cryptocompare_cache = {}
        optional_prices._cryptocompare_cache_time = 0
        optional_prices._messari_cache = {}
        optional_prices._messari_cache_time = 0

    @staticmethod
    def _response(payload):
        response = Mock()
        response.json.return_value = payload
        return response

    def test_yahoo_prices_are_normalized_and_cached(self):
        payload = {"quoteResponse": {"result": [{
            "symbol": "BTC-USD", "regularMarketPrice": 123.5,
            "regularMarketChangePercent": 1.234,
        }]}}
        with patch.object(optional_prices, "_request_get", return_value=self._response(payload)) as get:
            first = optional_prices.get_yahoo_finance_prices()
            second = optional_prices.get_yahoo_finance_prices()
        self.assertEqual(first["BTCUSDT"], {"price": 123.5, "change": 1.23, "source": "Yahoo"})
        self.assertIs(first, second)
        get.assert_called_once()

    def test_cryptocompare_candles_are_bounded_and_normalized(self):
        rows = [{
            "open": index, "high": index + 2, "low": index - 1,
            "close": index + 1, "volumeto": index * 10,
        } for index in range(1, 8)]
        with patch.object(
            optional_prices, "_request_get",
            return_value=self._response({"Data": {"Data": rows}}),
        ):
            candles = optional_prices.get_cryptocompare_candles("BTCUSDT", "4h", 3)
        self.assertEqual(len(candles), 3)
        self.assertEqual(candles[-1]["close"], 8.0)
        self.assertEqual(candles[-1]["volume"], 70.0)

    def test_optional_provider_failure_is_non_authoritative(self):
        with patch.object(optional_prices, "_request_get", side_effect=RuntimeError("offline")):
            self.assertEqual(optional_prices.get_cryptocompare_prices(), {})
            self.assertIsNone(optional_prices.get_messari_data("BTCUSDT"))


if __name__ == "__main__":
    unittest.main()
