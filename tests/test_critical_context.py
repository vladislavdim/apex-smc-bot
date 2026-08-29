import unittest
from unittest.mock import AsyncMock, patch

from external_sources.aggregator import (
    _apply_nondirectional_context,
    _finish,
    format_external_context,
)
from external_sources.coinmetrics import collect as collect_coinmetrics
from external_sources.coinmetrics import normalize as normalize_coinmetrics
from external_sources.deribit_options import collect as collect_deribit
from external_sources.deribit_options import normalize as normalize_deribit
from external_sources.dex_liquidity import collect as collect_dex_liquidity
from external_sources.dex_liquidity import normalize as normalize_dex_liquidity
from external_sources.models import empty_context
from news_context.aggregator import format_news_context, normalize_news_context
from news_context.official_macro import collect as collect_bls_actuals
from news_context.official_macro import normalize as normalize_bls_actuals


class CriticalContextNormalizationTests(unittest.TestCase):
    def test_deribit_normalizes_options_positioning_and_dvol(self):
        payload = {
            "summaries": {"result": [
                {"instrument_name": "BTC-30AUG26-100000-C", "open_interest": 10,
                 "volume": 4, "underlying_price": 100_000},
                {"instrument_name": "BTC-30AUG26-90000-P", "open_interest": 20,
                 "volume": 6, "underlying_price": 100_100},
            ]},
            "volatility": {"result": {"data": [
                [2, 50, 53, 49, 52], [1, 49, 51, 48, 50],
            ]}},
        }
        result = normalize_deribit(payload, "BTCUSDT")
        self.assertEqual(result["put_call_oi_ratio"], 2.0)
        self.assertEqual(result["put_call_volume_ratio"], 1.5)
        self.assertEqual(result["positioning"], "put_heavy")
        self.assertEqual(result["dvol_change_1h"], 2.0)

    def test_coinmetrics_normalizes_daily_network_change(self):
        payload = {"data": [
            {"time": "2026-08-28", "AdrActCnt": "120", "TxCnt": "220",
             "FeeTotNtv": "1.1", "TxTfrValAdjUSD": "1500"},
            {"time": "2026-08-27", "AdrActCnt": "100", "TxCnt": "200",
             "FeeTotNtv": "1", "TxTfrValAdjUSD": "1000"},
        ]}
        result = normalize_coinmetrics(payload, "btc")
        self.assertIsNotNone(result)
        self.assertEqual(result["active_addresses_change_1d_pct"], 20.0)
        self.assertEqual(result["adjusted_transfer_usd_change_1d_pct"], 50.0)

    def test_dexscreener_uses_only_verified_contract_stable_pool(self):
        address = "0xVerified"
        payload = [
            {
                "chainId": "ethereum", "dexId": "good", "pairAddress": "pair-good",
                "baseToken": {"address": address, "symbol": "ABC"},
                "quoteToken": {"address": "0xUSDT", "symbol": "USDT"},
                "liquidity": {"usd": 750_000}, "volume": {"h24": 200_000},
                "txns": {"h24": {"buys": 100, "sells": 90}},
                "priceChange": {"h1": 1.5, "h24": 4.0},
            },
            {
                "chainId": "ethereum", "dexId": "wrong-address",
                "baseToken": {"address": "0xOther", "symbol": "ABC"},
                "quoteToken": {"address": "0xUSDT", "symbol": "USDT"},
                "liquidity": {"usd": 9_000_000},
            },
        ]
        result = normalize_dex_liquidity(payload, address)
        self.assertIsNotNone(result)
        self.assertEqual(result["dex"], "good")
        self.assertEqual(result["liquidity_risk"], "adequate")

    def test_bls_normalizes_published_actuals_without_forecast(self):
        payload = {"Results": {"series": [{
            "seriesID": "CUUR0000SA0",
            "data": [
                {"year": "2026", "period": "M07", "value": "315", "footnotes": [{}]},
                {"year": "2026", "period": "M06", "value": "314", "footnotes": [{}]},
                {"year": "2025", "period": "M07", "value": "300", "footnotes": [{}]},
            ],
        }]}}
        result = normalize_bls_actuals(payload)["cpi_all_urban"]
        self.assertEqual(result["observation"], "2026-07")
        self.assertEqual(result["change_12m_pct"], 5.0)
        self.assertNotIn("forecast", result)

    def test_nondirectional_sources_never_create_external_vote(self):
        context = empty_context("BTCUSDT")
        _apply_nondirectional_context(context, {
            "source": "deribit_options", "status": "fresh", "age_seconds": 0,
            "normalized": {"positioning": "put_heavy", "dvol": 70},
        }, "options_context")
        result = _finish(context, "BULLISH")
        self.assertEqual(result["external_bias"], "unknown")
        self.assertEqual(result["external_confidence"], 0.0)
        self.assertFalse(result["conflicts"])

    def test_groq_blocks_explain_critical_context_limits(self):
        external = format_external_context(empty_context("BTCUSDT"), "MTF")
        news = format_news_context(normalize_news_context("BTCUSDT", [], []))
        for label in ("Options (Deribit)", "Network Activity (Coin Metrics)",
                      "Verified DEX Liquidity"):
            self.assertIn(label, external)
        self.assertIn("cannot create a signal", external)
        self.assertIn("Official published macro actuals (BLS)", news)
        self.assertIn("not the next release", news)


class CriticalContextCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_deribit_is_explicitly_limited_to_btc_and_eth(self):
        result = await collect_deribit("SOLUSDT")
        self.assertEqual(result["status"], "unsupported_pair")

    async def test_dex_liquidity_requires_verified_contract(self):
        with patch("external_sources.dex_liquidity.get_pair", return_value={}):
            result = await collect_dex_liquidity("ABCUSDT")
        self.assertEqual(result["status"], "not_configured")

    async def test_coinmetrics_community_request_needs_no_secret(self):
        async def uncached(_key, _ttl, _stale, fetcher):
            return await fetcher(), "fresh", 0

        request = AsyncMock(return_value={"data": [
            {"time": "2026-08-28", "AdrActCnt": "10", "TxCnt": "20"},
        ]})
        with patch("external_sources.coinmetrics.cache.get_or_fetch", new=uncached), \
             patch("external_sources.coinmetrics.http_client.get_json", new=request):
            result = await collect_coinmetrics("BTCUSDT")
        self.assertEqual(result["status"], "fresh")
        self.assertEqual(request.await_args.args[1]["assets"], "btc")
        self.assertEqual(len(request.await_args.args), 2)

    async def test_bls_request_is_keyless_and_bounded_to_two_years(self):
        async def uncached(_key, _ttl, _stale, fetcher):
            return await fetcher(), "fresh", 0

        request = AsyncMock(return_value={
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": [{
                "seriesID": "LNS14000000",
                "data": [{"year": "2026", "period": "M07", "value": "4.1",
                          "footnotes": [{}]}],
            }]},
        })
        with patch("news_context.official_macro.cache.get_or_fetch", new=uncached), \
             patch("news_context.official_macro.http_client.post_json", new=request):
            result = await collect_bls_actuals()
        self.assertEqual(result["status"], "fresh")
        payload = request.await_args.args[1]
        self.assertEqual(int(payload["endyear"]) - int(payload["startyear"]), 1)
        self.assertNotIn("registrationkey", payload)


if __name__ == "__main__":
    unittest.main()
