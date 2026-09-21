import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from apex.market import optional_signals


def _settings(coinglass_api_key="", santiment_api_key=""):
    return SimpleNamespace(
        coinglass_api_key=coinglass_api_key,
        santiment_api_key=santiment_api_key,
    )


class OptionalSignalProviderTests(unittest.TestCase):
    def test_missing_keys_keep_optional_values_unknown(self):
        providers = optional_signals.OptionalSignalProviders(_settings())
        with patch.object(optional_signals, "_request_get") as get, patch.object(
            optional_signals, "_request_post",
        ) as post:
            self.assertIsNone(providers.get_liquidations("BTCUSDT"))
            self.assertIsNone(providers.get_santiment_data("BTCUSDT"))
        get.assert_not_called()
        post.assert_not_called()

    def test_liquidation_bias_and_cache_are_preserved(self):
        response = Mock()
        response.json.return_value = {"code": "0", "data": [{
            "longLiquidationUsd": "300", "shortLiquidationUsd": "100",
        }]}
        providers = optional_signals.OptionalSignalProviders(_settings(coinglass_api_key="key"))
        with patch.object(optional_signals, "_request_get", return_value=response) as get:
            first = providers.get_liquidations("BTCUSDT")
            second = providers.get_liquidations("BTCUSDT")
        self.assertEqual(first, {
            "long_liq_usd": 300.0, "short_liq_usd": 100.0,
            "total_usd": 400.0, "bias": "LONGS_WIPED",
        })
        self.assertIs(first, second)
        get.assert_called_once()

    def test_santiment_threshold_is_preserved(self):
        response = Mock()
        response.json.return_value = {"data": {"getMetric": {"timeseriesData": [
            {"value": 0.2}, {"value": 0.4},
        ]}}}
        providers = optional_signals.OptionalSignalProviders(_settings(santiment_api_key="key"))
        with patch.object(optional_signals, "_request_post", return_value=response):
            self.assertEqual(providers.get_santiment_data("BTCUSDT"), {
                "sentiment": 0.3, "signal": "BULLISH",
            })

    def test_whale_alerts_are_filtered_and_bounded(self):
        response = Mock(text=(
            "<title><![CDATA[Feed]]></title>"
            "<title><![CDATA[Bitcoin moved to exchange]]></title>"
            "<title><![CDATA[Unrelated update]]></title>"
        ))
        providers = optional_signals.OptionalSignalProviders(_settings())
        with patch.object(optional_signals, "_request_get", return_value=response):
            self.assertEqual(providers.get_whale_alerts(), ["Bitcoin moved to exchange"])


if __name__ == "__main__":
    unittest.main()
