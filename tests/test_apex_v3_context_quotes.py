import unittest
from unittest.mock import Mock, patch

from apex.market import context_quotes


class ContextQuoteProviderTests(unittest.TestCase):
    def setUp(self):
        context_quotes._fear_greed_cache = {}
        context_quotes._fear_greed_cache_time = 0

    def test_fear_greed_is_typed_and_cached(self):
        response = Mock()
        response.json.return_value = {"data": [{
            "value": "27", "value_classification": "Fear", "timestamp": "123",
        }]}
        with patch.object(context_quotes, "_request_get", return_value=response) as get:
            first = context_quotes.get_fear_greed()
            second = context_quotes.get_fear_greed()
        self.assertEqual(first, {"value": 27, "label": "Fear", "updated": "123"})
        self.assertIs(first, second)
        get.assert_called_once()

    def test_gate_funding_preserves_percent_conversion(self):
        response = Mock()
        response.json.return_value = {"funding_rate": "0.0001"}
        with patch.object(context_quotes, "_gate_contract", return_value="BTC_USDT"), patch.object(
            context_quotes, "_request_get", return_value=response,
        ):
            self.assertEqual(context_quotes.get_funding_rate("BTCUSDT"), 0.01)

    def test_open_interest_preserves_trend_thresholds(self):
        response = Mock()
        response.json.return_value = [
            {"open_interest": "100"}, {"open_interest": "103"},
        ]
        with patch.object(context_quotes, "_gate_contract", return_value="BTC_USDT"), patch.object(
            context_quotes, "_request_get", return_value=response,
        ):
            result = context_quotes.get_open_interest("BTCUSDT")
        self.assertEqual(result, {"current": 103.0, "change_pct": 3.0, "trend": "GROWING"})
        response.raise_for_status.assert_called_once_with()

    def test_provider_failures_remain_unknown(self):
        with patch.object(context_quotes, "_request_get", side_effect=RuntimeError("offline")):
            self.assertIsNone(context_quotes.get_fear_greed())
            self.assertIsNone(context_quotes.get_funding_rate("BTCUSDT"))
            self.assertIsNone(context_quotes.get_open_interest("BTCUSDT"))


if __name__ == "__main__":
    unittest.main()
