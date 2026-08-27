import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from external_sources.aggregator import _apply_futures, _finish, format_external_context
from external_sources.cache import TTLCache
from external_sources.exchange_fallback import normalize as normalize_futures
from external_sources.models import empty_context
from external_sources.smart_money import collect as collect_smart_money
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
        value, status, _ = await cache.get_or_fetch("x", 0, 1000, AsyncMock(side_effect=TimeoutError()))
        self.assertEqual(value, {"ok": True})
        self.assertEqual(status, "stale_fallback")

    async def test_missing_whale_url_is_not_configured(self):
        with patch.dict(os.environ, {}, clear=True):
            result = await collect_whale_tracker("ETHUSDT")
        self.assertEqual(result["status"], "not_configured")

    async def test_non_ethereum_whale_pair_is_not_faked(self):
        result = await collect_whale_tracker("TONUSDT")
        self.assertEqual(result["status"], "unsupported_pair")

    async def test_non_eth_deepblue_is_not_pair_specific(self):
        result = await collect_smart_money("SOLUSDT")
        self.assertEqual(result["status"], "unsupported_pair")

    def test_public_futures_normalization(self):
        raw = {"source": "public_futures", "status": "fresh", "payload": {
            "premium": {"lastFundingRate": "0.0005"},
            "oi_1h": [{"sumOpenInterestValue": "100"}, {"sumOpenInterestValue": "110"}],
            "oi_4h": [{"sumOpenInterestValue": "100"}, {"sumOpenInterestValue": "120"}],
            "depth": {"bids": [["10", "2"]], "asks": [["11", "1"]]},
            "bybit": {}, "gate": [{"long_liq_size": "7", "short_liq_size": "9"}],
        }}
        result = normalize_futures(raw)["normalized"]
        self.assertEqual(result["oi_1h"], 10.0)
        self.assertEqual(result["oi_4h"], 20.0)
        self.assertEqual(result["buy"], 20.0)
        self.assertEqual(result["short_liq"], 9.0)

    def test_empty_and_conflicting_context(self):
        context = empty_context("BTCUSDT")
        context["large_orders"]["bias"] = "bullish"
        context["whale_activity"]["bias"] = "bearish"
        result = _finish(context, "BULLISH")
        self.assertTrue(any("CONFLICT" in item for item in result["conflicts"]))

    def test_compact_groq_block_has_required_fields(self):
        text = format_external_context(empty_context("BTCUSDT"), "FAST")
        for field in ("Open Interest", "Funding", "Liquidations", "Large Orders", "Exchange Flow", "Whale Activity", "Smart Money", "Source quality", "Conflicts"):
            self.assertIn(field, text)
        self.assertIn("FAST", text)

    def test_levels_are_not_changed_by_external_normalization(self):
        candidate = {"entry": 100, "sl": 95, "tp1": 110, "tp2": 120, "tp3": 130, "rr": 2}
        before = candidate.copy()
        context = empty_context("BTCUSDT")
        _apply_futures(context, {"source": "public_futures", "normalized": {"oi": 1, "oi_1h": 2, "oi_4h": 3, "funding": 0, "buy": 1, "sell": 2, "long_liq": 0, "short_liq": 0}})
        self.assertEqual(candidate, before)
