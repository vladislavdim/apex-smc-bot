import json
import math
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from core import smc_engine
from core.historical_zones import build_zone_context, refresh_zones
from core.outcome_learning import (
    build_learning_context,
    capture_signal_evidence,
    close_learning_loop,
)
from external_sources import defillama, live_tape, oli, pair_registry


class MarketIntelligenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "brain.db")

    async def asyncTearDown(self):
        await live_tape.stop()
        self.tmp.cleanup()

    async def test_pair_registry_verifies_provider_symbols_and_scaled_gate_contract(self):
        async def get_json(url, params=None, headers=None):
            if "gateio" in url:
                return [
                    {"name": "BTC_USDT", "quanto_multiplier": "0.0001"},
                    {"name": "1000PEPE_USDT", "quanto_multiplier": "1"},
                ]
            if "binance" in url:
                return {"symbols": [
                    {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING"},
                    {"symbol": "1000PEPEUSDT", "contractType": "PERPETUAL", "status": "TRADING"},
                ]}
            return {"result": {"list": [
                {"symbol": "BTCUSDT", "contractType": "LinearPerpetual", "status": "Trading"},
                {"symbol": "PEPEUSDT", "contractType": "LinearPerpetual", "status": "Trading"},
            ]}}

        hyper = [{"universe": [{"name": "BTC"}, {"name": "PEPE"}]}, []]
        pair_registry._snapshot.clear()
        pair_registry._checked_at = 0
        with patch.object(pair_registry, "_DB_PATH", self.db_path), \
             patch.object(pair_registry.http_client, "get_json", new=get_json), \
             patch.object(pair_registry.http_client, "post_json", new=AsyncMock(return_value=hyper)):
            rows = await pair_registry.refresh_pair_registry(
                ["BTCUSDT", "PEPEUSDT"], force=True,
            )
        self.assertEqual(rows["PEPEUSDT"]["gate_symbol"], "1000PEPE_USDT")
        self.assertEqual(rows["PEPEUSDT"]["binance_symbol"], "1000PEPEUSDT")
        self.assertEqual(rows["PEPEUSDT"]["bybit_symbol"], "PEPEUSDT")
        self.assertEqual(rows["PEPEUSDT"]["gate_status"], "supported")
        self.assertTrue(rows["BTCUSDT"]["binance_supported"])
        self.assertTrue(rows["BTCUSDT"]["bybit_supported"])
        self.assertTrue(rows["BTCUSDT"]["hyperliquid_supported"])

        failure = AsyncMock(side_effect=TimeoutError())
        with patch.object(pair_registry, "_DB_PATH", self.db_path), \
             patch.object(pair_registry.http_client, "get_json", new=failure), \
             patch.object(pair_registry.http_client, "post_json", new=failure):
            fallback = await pair_registry.refresh_pair_registry(["BTCUSDT"], force=True)
        self.assertTrue(fallback["BTCUSDT"]["gate_supported"])
        self.assertTrue(fallback["BTCUSDT"]["binance_supported"])
        self.assertEqual(fallback["BTCUSDT"]["gate_status"], "unavailable")

    async def test_gate_candles_use_registry_symbol_and_record_real_coverage(self):
        class Response:
            status_code = 200

            @staticmethod
            def json():
                return [{"t": 123, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"}]

        pair_registry._snapshot["PEPEUSDT"] = {
            **pair_registry.get_pair("PEPEUSDT"),
            "gate_symbol": "1000PEPE_USDT",
        }
        with patch.object(pair_registry, "_DB_PATH", self.db_path), \
             patch("core.smc_engine.requests.get", return_value=Response()) as request:
            candles = smc_engine._fetch_gate("PEPEUSDT", "1h", 10)
            row = pair_registry.get_pair("PEPEUSDT")
        self.assertEqual(request.call_args.kwargs["params"]["contract"], "1000PEPE_USDT")
        self.assertEqual(candles[0]["timestamp"], 123)
        self.assertEqual(row["gate_candles_status"], "available")
        self.assertEqual(row["gate_candles_count"], 1)

    async def test_unmapped_asset_is_never_silently_replaced_with_bitcoin(self):
        with patch("core.smc_engine.requests.get") as request:
            with self.assertRaisesRegex(ValueError, "No CG ID"):
                smc_engine._fetch_synthetic("NOTAREALPAIRUSDT", "1h", 10)
        request.assert_not_called()

    async def test_live_tape_combines_three_exchanges_and_liquidation_sides(self):
        pair = {
            "gate_symbol": "BTC_USDT", "binance_symbol": "BTCUSDT",
            "bybit_symbol": "BTCUSDT", "gate_multiplier": 0.001,
        }
        live_tape._configured_symbols = ["BTCUSDT"]
        live_tape._provider_to_apex = {
            ("gate", "BTC_USDT"): "BTCUSDT",
            ("binance", "BTCUSDT"): "BTCUSDT",
            ("bybit", "BTCUSDT"): "BTCUSDT",
        }
        live_tape._trades.clear(); live_tape._liquidations.clear(); live_tape._latest.clear()
        with patch("external_sources.live_tape.get_pair", return_value=pair):
            live_tape.ingest_gate({
                "event": "update", "channel": "futures.trades",
                "result": [{"contract": "BTC_USDT", "size": "2", "price": "100", "create_time_ms": 1_000_000}],
            })
            live_tape.ingest_binance({"e": "aggTrade", "s": "BTCUSDT", "E": 1_000_000, "p": "100", "q": "2", "m": False})
            live_tape.ingest_binance({"e": "forceOrder", "E": 1_000_000, "o": {"s": "BTCUSDT", "S": "SELL", "ap": "100", "z": "1"}})
            live_tape.ingest_bybit({"topic": "publicTrade.BTCUSDT", "ts": 1_000_000, "data": [{"T": 1_000_000, "p": "100", "v": "3", "S": "Sell"}]})
            live_tape.ingest_bybit({"topic": "allLiquidation.BTCUSDT", "ts": 1_000_000, "data": [{"T": 1_000_000, "p": "100", "v": "2", "S": "Buy"}]})
            data = live_tape.snapshot("BTCUSDT", now=1000.5)
        self.assertEqual(set(data["sources"]), {"gate", "binance", "bybit"})
        self.assertGreater(data["buy_usd_60s"], 0)
        self.assertGreater(data["sell_usd_60s"], 0)
        self.assertEqual(data["long_liq_usd_300s"], 300.0)
        self.assertEqual(data["short_liq_usd_300s"], 0.0)

    async def test_historical_zone_refresh_is_idempotent_for_same_candle(self):
        candles = []
        for index in range(80):
            center = 100 + math.sin(index / 3) * 4
            candles.append({
                "open": center - 0.2, "high": center + 1,
                "low": center - 1, "close": center + 0.2,
            })
        first = refresh_zones("BTCUSDT", "1h", candles, self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            first_events = conn.execute("SELECT COUNT(*) FROM historical_zone_events").fetchone()[0]
        second = refresh_zones("BTCUSDT", "1h", candles, self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            second_events = conn.execute("SELECT COUNT(*) FROM historical_zone_events").fetchone()[0]
        self.assertGreater(first["zones"], 0)
        self.assertEqual(first["zones"], second["zones"])
        self.assertEqual(first_events, second_events)
        self.assertTrue(build_zone_context("BTCUSDT", 100, "1h", db_path=self.db_path)["available"])

    async def test_closed_loop_confirms_only_after_objective_sample(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE market_memory_snapshots (signal_id INTEGER PRIMARY KEY,max_favorable_pct REAL,max_adverse_pct REAL)")
            conn.executemany(
                "INSERT INTO market_memory_snapshots VALUES (?,?,?)",
                [(index, 2.0, -0.5) for index in range(1, 13)],
            )
        candidate = {
            "symbol": "BTCUSDT", "grade": "MTF", "direction": "BULLISH",
            "timeframe": "1h", "entry": 100, "sl": 95, "tp1": 110,
            "_external_quality_review": {
                "context": {"external_bias": "bullish", "external_confidence": 0.7},
                "news_context": {"risk_level": "LOW"},
                "historical_zones": {"zones": [{"zone_type": "support"}]},
            },
        }
        with patch.dict(os.environ, {"CLOSED_LOOP_MIN_SAMPLES": "12", "NEW_STRATEGY_MIN_CLOSED_TRADES": "30"}):
            for signal_id in range(1, 13):
                capture_signal_evidence(signal_id, candidate, self.db_path)
                close_learning_loop(signal_id, "tp1" if signal_id <= 9 else "sl", self.db_path)
            context = build_learning_context(candidate, self.db_path)
        self.assertEqual(context["comparable_condition"]["samples"], 12)
        self.assertEqual(context["comparable_condition"]["state"], "confirmed")
        self.assertFalse(context["new_strategy_research_ready"])

    async def test_oli_never_guesses_addresses(self):
        with patch.dict(os.environ, {}, clear=True):
            result = await oli.collect("ETHUSDT")
        self.assertEqual(result["status"], "not_configured")

    async def test_defillama_is_normalized_as_non_directional_regime(self):
        payload = {
            "stable": [
                {"totalCirculatingUSD": {"peggedUSD": 100}},
                {"totalCirculatingUSD": {"peggedUSD": 101}},
            ],
            "dexs": {"total24h": 12_000}, "oi": {"total24h": 8_000},
        }
        with patch.object(defillama.cache, "get_or_fetch", new=AsyncMock(return_value=(payload, "fresh", 0))):
            result = await defillama.collect("SOLUSDT")
        self.assertEqual(result["normalized"]["method"], "global_slow_regime_not_directional")
        self.assertNotIn("bias", result["normalized"])
        self.assertEqual(result["normalized"]["dex_volume_24h_usd"], 12_000)


if __name__ == "__main__":
    unittest.main()
