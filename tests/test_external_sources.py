import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from external_sources.aggregator import (
    _apply_futures,
    _finish,
    _status,
    collect_external_context,
    format_external_context,
)
from external_sources.cache import TTLCache
from external_sources.crypto_monitor import normalize as normalize_crypto_monitor
from external_sources.crypto_monitor import collect as collect_crypto_monitor
from external_sources.exchange_fallback import normalize as normalize_futures
from external_sources.exchange_fallback import _provider_symbol
from external_sources.models import empty_context
from external_sources.smart_money import collect as collect_smart_money
from external_sources.smart_money import normalize as normalize_smart_money
from external_sources.whale_tracker import collect as collect_whale_tracker


class ExternalSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_deduplicates_requests(self):
        cache = TTLCache()
        fetcher = AsyncMock(return_value={"ok": True})
        await cache.get_or_fetch("x", 60, 120, fetcher)
        await cache.get_or_fetch("x", 60, 120, fetcher)
        self.assertEqual(fetcher.await_count, 1)

    async def test_stale_fallback_on_api_failure(self):
        cache = TTLCache()
        await cache.get_or_fetch("x", 0, 1000, AsyncMock(return_value={"ok": True}))
        value, status, _ = await cache.get_or_fetch(
            "x", 0, 1000, AsyncMock(side_effect=TimeoutError())
        )
        self.assertEqual(value, {"ok": True})
        self.assertEqual(status, "stale_fallback")

    async def test_missing_whale_url_is_not_configured(self):
        with patch.dict(os.environ, {}, clear=True):
            result = await collect_whale_tracker("ETHUSDT")
        self.assertEqual(result["status"], "not_configured")

    async def test_missing_crypto_monitor_url_and_key_is_not_configured(self):
        with patch.dict(os.environ, {}, clear=True):
            result = await collect_crypto_monitor("BTCUSDT")
        self.assertEqual(result["status"], "not_configured")

    async def test_non_ethereum_whale_pair_is_not_faked(self):
        result = await collect_whale_tracker("TONUSDT")
        self.assertEqual(result["status"], "unsupported_pair")

    async def test_whale_tracker_uses_documented_token_filter(self):
        async def fetch_uncached(_key, _ttl, _stale, fetcher):
            return await fetcher(), "fresh", 0

        request = AsyncMock(return_value={"transactions": []})
        with patch.dict(os.environ, {"WHALE_TRACKER_API_URL": "https://tracker.test"}, clear=True), \
             patch("external_sources.whale_tracker.cache.get_or_fetch", new=fetch_uncached), \
             patch("external_sources.whale_tracker.http_client.get_json", new=request):
            result = await collect_whale_tracker("ETHUSDT")
        self.assertEqual(result["status"], "fresh")
        self.assertEqual(request.await_args.kwargs["params"]["token"], "ETH")
        self.assertNotIn("symbol", request.await_args.kwargs["params"])

    async def test_deepblue_public_token_row_is_pair_specific(self):
        payload = {
            "1h": [{"token_symbol": "SOL", "buy_vol": 120, "sell_vol": 80, "txn_count": 9}],
            "24h": [{"token_symbol": "SOL", "buy_vol": 900, "sell_vol": 700, "txn_count": 40}],
        }
        with patch(
            "external_sources.smart_money.cache.get_or_fetch",
            new=AsyncMock(return_value=(payload, "fresh", 0)),
        ):
            result = await collect_smart_money("SOLUSDT")
        self.assertEqual(result["status"], "fresh")
        self.assertEqual(result["scope"], "token_level_top20")
        self.assertEqual(result["payload"]["1h"]["token_symbol"], "SOL")

    async def test_deepblue_absent_top20_pair_is_not_faked_neutral(self):
        payload = {
            "1h": [{"token_symbol": "ETH", "buy_vol": 1, "sell_vol": 2}],
            "24h": [],
        }
        with patch(
            "external_sources.smart_money.cache.get_or_fetch",
            new=AsyncMock(return_value=(payload, "cached", 10)),
        ):
            result = await collect_smart_money("SOLUSDT")
        self.assertEqual(result["status"], "no_pair_data")

    async def test_empty_provider_response_is_unavailable(self):
        with patch(
            "external_sources.smart_money.cache.get_or_fetch",
            new=AsyncMock(side_effect=ValueError("empty response")),
        ):
            result = await collect_smart_money("SOLUSDT")
        self.assertEqual(result["status"], "unavailable")

    async def test_slow_provider_is_bounded_and_fail_open(self):
        async def slow(_symbol):
            await asyncio.sleep(0.1)
            return {"source": "public_futures", "status": "fresh", "age_seconds": 0}

        unavailable = AsyncMock(return_value={"source": "x", "status": "not_configured"})
        with patch("external_sources.aggregator._COLLECT_TIMEOUT_SECONDS", 0.01), \
             patch("external_sources.aggregator.exchange_fallback.collect", new=slow), \
             patch("external_sources.aggregator.crypto_monitor.collect", new=unavailable), \
             patch("external_sources.aggregator.whale_tracker.collect", new=unavailable), \
             patch("external_sources.aggregator.smart_money.collect", new=unavailable):
            context = await collect_external_context("BTCUSDT", "BULLISH")
        self.assertTrue(context["external_data_unavailable"])
        self.assertTrue(any("public_futures" in item for item in context["data_quality"]["failed_sources"]))

    def test_public_futures_normalization_prefers_gate_and_converts_contracts(self):
        raw = {
            "source": "public_futures",
            "status": "fresh",
            "age_seconds": 0,
            "payload": {
                "premium": {"lastFundingRate": "0.0005"},
                "oi_1h": [
                    {"sumOpenInterestValue": "100"},
                    {"sumOpenInterestValue": "110"},
                ],
                "oi_4h": [
                    {"sumOpenInterestValue": "100"},
                    {"sumOpenInterestValue": "120"},
                ],
                "depth": {"bids": [["10", "2"]], "asks": [["11", "1"]]},
                "bybit": {},
                "gate_1h": [
                    {"open_interest": "200"},
                    {"open_interest": "220", "long_liq_size": "7", "short_liq_size": "9"},
                ],
                "gate_4h": [
                    {"open_interest": "200"},
                    {"open_interest": "240"},
                ],
                "gate_contract": {
                    "quanto_multiplier": "0.001",
                    "mark_price": "10",
                    "funding_rate": "0.0004",
                },
                "gate_depth": {
                    "bids": [{"p": "10", "s": "3"}],
                    "asks": [{"p": "11", "s": "1"}],
                },
            },
        }
        result = normalize_futures(raw)["normalized"]
        self.assertEqual(result["oi"], 220.0)
        self.assertEqual(result["oi_1h"], 10.0)
        self.assertEqual(result["oi_4h"], 20.0)
        self.assertEqual(result["buy"], 30.0)
        self.assertAlmostEqual(result["short_liq"], 0.09)

    def test_scaled_derivative_symbols_are_explicit(self):
        self.assertEqual(_provider_symbol("PEPEUSDT"), "1000PEPEUSDT")
        self.assertEqual(_provider_symbol("SATSUSDT"), "1000SATSUSDT")
        self.assertEqual(_provider_symbol("BTCUSDT"), "BTCUSDT")

    def test_crypto_monitor_documented_payload_normalization(self):
        raw = {
            "source": "crypto_monitor",
            "status": "fresh",
            "age_seconds": 0,
            "payload": {
                "detail": {
                    "symbol": "BTC",
                    "open_interest": "5000",
                    "oi_change_1h": "3.5",
                    "oi_change_4h": "8.0",
                    "funding_rate": "0.0002",
                },
                "liquidation": {
                    "orders": [
                        {"symbol": "BTC", "side": "long", "value": "100"},
                        {"symbol": "BTC", "side": "short", "value": "50"},
                    ]
                },
                "orders": {
                    "data": [
                        {"symbol": "BTC", "side": "buy", "value_usd": "300"},
                        {"symbol": "BTC", "side": "sell", "value_usd": "125"},
                    ]
                },
            },
        }
        result = normalize_crypto_monitor(raw, "BTCUSDT")["normalized"]
        self.assertEqual(result["oi"], 5000.0)
        self.assertEqual(result["oi_1h"], 3.5)
        self.assertEqual(result["long_liq"], 100.0)
        self.assertEqual(result["buy"], 300.0)

    def test_smart_money_normalization(self):
        raw = {
            "source": "deepbluealpha",
            "status": "fresh",
            "age_seconds": 0,
            "payload": {
                "1h": {"buy_vol": "12", "sell_vol": "8", "txn_count": "7"},
                "24h": {"buy_vol": "100", "sell_vol": "70"},
            },
        }
        result = normalize_smart_money(raw)["normalized"]
        self.assertEqual(result["buy_usd"], 12.0)
        self.assertEqual(result["transaction_count"], 7)
        self.assertEqual(result["buy_24h_usd"], 100.0)

    def test_stale_external_value_is_excluded(self):
        context = empty_context("BTCUSDT")
        stale = {
            "source": "public_futures",
            "status": "stale_fallback",
            "age_seconds": 121,
            "normalized": {"oi": 100, "funding": 0.001},
        }
        _status(context, stale)
        _apply_futures(context, stale)
        self.assertIsNone(context["open_interest"]["value"])
        self.assertTrue(context["data_quality"]["failed_sources"])

    def test_independent_sources_create_explicit_conflict(self):
        context = empty_context("BTCUSDT")
        context["large_orders"].update({"bias": "bullish", "source": "public_futures"})
        context["smart_money"].update({"bias": "bearish", "source": "deepbluealpha"})
        result = _finish(context, "BULLISH")
        self.assertTrue(any("CONFLICT" in item for item in result["conflicts"]))
        self.assertEqual(result["external_bias"], "neutral")

    def test_single_source_cannot_create_technical_conflict(self):
        context = empty_context("BTCUSDT")
        context["large_orders"].update({"bias": "bearish", "source": "public_futures"})
        result = _finish(context, "BULLISH")
        self.assertFalse(any("technical" in item for item in result["conflicts"]))
        self.assertEqual(result["external_confidence"], 0.25)

    def test_compact_groq_block_has_required_fields(self):
        text = format_external_context(empty_context("BTCUSDT"), "FAST")
        for field in (
            "Open Interest", "Funding", "Liquidations", "Large Orders",
            "Exchange Flow", "Whale Activity", "Smart Money", "Data age",
            "Source quality", "Conflicts",
        ):
            self.assertIn(field, text)
        self.assertIn("FAST", text)

    def test_levels_are_not_changed_by_external_normalization(self):
        candidate = {
            "entry": 100, "sl": 95, "tp1": 110, "tp2": 120,
            "tp3": 130, "rr": 2,
        }
        before = candidate.copy()
        context = empty_context("BTCUSDT")
        _apply_futures(context, {
            "source": "public_futures",
            "status": "fresh",
            "age_seconds": 0,
            "normalized": {
                "oi": 1, "oi_1h": 2, "oi_4h": 3, "funding": 0,
                "buy": 1, "sell": 2, "long_liq": 0, "short_liq": 0,
            },
        })
        self.assertEqual(candidate, before)


if __name__ == "__main__":
    unittest.main()
