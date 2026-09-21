import unittest
from unittest.mock import Mock, patch

from apex.market import quote_provider


class QuoteProviderTests(unittest.TestCase):
    def test_gate_quote_has_priority(self):
        response = Mock()
        response.json.return_value = [{
            "last": "123.45", "change_percentage": "1.236",
        }]
        with patch(
            "external_sources.pair_registry.get_pair",
            return_value={"gate_symbol": "BTC_USDT"},
        ), patch.object(quote_provider, "_request_get", return_value=response) as get:
            result = quote_provider.get_price_realtime("BTCUSDT")
        self.assertEqual(result, {
            "price": 123.45, "change": 1.24, "source": "Gate.io Futures",
        })
        get.assert_called_once()

    def test_coingecko_is_display_fallback_only(self):
        gate = Mock()
        gate.raise_for_status.side_effect = RuntimeError("offline")
        coingecko = Mock()
        coingecko.json.return_value = {
            "bitcoin": {"usd": 100.0, "usd_24h_change": -2.345},
        }
        with patch(
            "external_sources.pair_registry.get_pair",
            return_value={"gate_symbol": "BTC_USDT"},
        ), patch.object(quote_provider, "_request_get", side_effect=[gate, coingecko]):
            result = quote_provider.get_price_realtime("BTCUSDT")
        self.assertEqual(result, {
            "price": 100.0, "change": -2.35, "source": "CoinGecko",
        })

    def test_unknown_symbol_and_gate_failure_return_none(self):
        with patch(
            "external_sources.pair_registry.get_pair",
            side_effect=RuntimeError("offline"),
        ), patch.object(quote_provider, "_request_get") as get:
            self.assertIsNone(quote_provider.get_price_realtime("UNKNOWNUSDT"))
        get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
