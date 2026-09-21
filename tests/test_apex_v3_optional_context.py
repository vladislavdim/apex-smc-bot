import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from apex.market import optional_context


def _settings(**overrides):
    values = {
        "twelvedata_api_key": "", "mobula_api_key": "",
        "coinalyze_api_key": "", "lunarcrush_api_key": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class OptionalContextProviderTests(unittest.TestCase):
    def test_missing_keys_do_not_issue_requests(self):
        providers = optional_context.OptionalContextProviders(_settings())
        with patch.object(optional_context, "_request_get") as get:
            self.assertEqual(providers.get_twelvedata_candles("BTCUSDT"), [])
            self.assertEqual(providers.get_mobula_price("BTCUSDT"), {})
            self.assertEqual(providers.get_coinalyze_data("BTCUSDT"), {})
            self.assertEqual(providers.get_lunarcrush_data("BTCUSDT"), {})
        get.assert_not_called()

    def test_twelvedata_candles_keep_chronological_order(self):
        response = Mock()
        response.json.return_value = {"values": [
            {"open": "2", "high": "3", "low": "1", "close": "2.5", "volume": "20"},
            {"open": "1", "high": "2", "low": "0.5", "close": "1.5", "volume": "10"},
        ]}
        providers = optional_context.OptionalContextProviders(
            _settings(twelvedata_api_key="key")
        )
        with patch.object(optional_context, "_request_get", return_value=response) as get:
            candles = providers.get_twelvedata_candles("BTCUSDT", "4h", 2)
        self.assertEqual([row["close"] for row in candles], [1.5, 2.5])
        self.assertEqual(get.call_args.kwargs["params"]["apikey"], "key")

    def test_lunarcrush_classification_is_context_only(self):
        response = Mock(status_code=200)
        response.json.return_value = {"data": {
            "galaxy_score": 70, "sentiment": 65, "alt_rank": 4,
        }}
        providers = optional_context.OptionalContextProviders(
            _settings(lunarcrush_api_key="key")
        )
        with patch.object(optional_context, "_request_get", return_value=response):
            result = providers.get_lunarcrush_data("BTCUSDT")
        self.assertEqual(result["signal"], "BULLISH")
        self.assertEqual(result["source"], "lunarcrush")


if __name__ == "__main__":
    unittest.main()
