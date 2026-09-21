from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
import sqlite3
import tempfile
import unittest

from apex.market.candles import confirmed_candles
from apex.market.derivatives import PointInTimeContext, normalize_contract_stats, normalize_funding
from apex.market.freshness import assess_candles
from apex.market.health import MarketHealthRegistry
from apex.market.levels import LevelSide, LevelState, PriceLevel, activate, advance_level
from apex.market.level_memory import persist_level_events
from apex.market.microstructure import GateOrderBook
from apex.market.orderflow import proxy_cvd, real_cvd
from apex.market.breadth import market_breadth
from apex.market.regime import classify_regime
from apex.market.relative_strength import relative_strength
from apex.market.snapshots import build_snapshot
from apex.market.volume import volume_features, vwap
from apex.market.volume_profile import volume_profile
from apex.market.universe import UniverseContextStore, build_universe_context
from core.historical_zones import refresh_zones


def candle(opened: int, *, closed: bool | None = None) -> dict:
    row = {
        "timestamp": opened,
        "open": 100,
        "high": 110,
        "low": 90,
        "close": 105,
        "volume": 10,
    }
    if closed is not None:
        row["is_closed"] = closed
    return row


class ApexV3MarketTests(unittest.TestCase):
    def test_forming_candle_is_never_returned(self):
        rows = confirmed_candles(
            [candle(0), candle(900), candle(1800, closed=False)],
            "15m",
            as_of=1800,
        )
        self.assertEqual([row["open_time"] for row in rows], [0, 900])
        self.assertTrue(all(row["is_closed"] for row in rows))

    def test_millisecond_timestamps_are_normalized_and_deduplicated(self):
        rows = confirmed_candles(
            [candle(1_780_000_000_000), candle(1_780_000_000_000)],
            "5m",
            as_of=1_780_000_301,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["open_time"], 1_780_000_000)

    def test_stale_market_data_is_wait_not_strategy_failure(self):
        rows = confirmed_candles([candle(0)], "1h", as_of=20_000)
        result = assess_candles("1h", rows, as_of=20_000)
        self.assertEqual(result.status, "STALE")
        self.assertEqual(result.reason_code, "GATE_STALE_1H")

    def test_weekly_gate_rows_are_closed_and_fresh_in_canonical_time(self):
        week = 7 * 86400
        rows = confirmed_candles([candle(0), candle(week)], "1w", as_of=2 * week)
        self.assertEqual([row["close_time"] for row in rows], [week, 2 * week])
        self.assertEqual(assess_candles("1w", rows, as_of=2 * week).status, "FRESH")

    def test_snapshot_uses_one_as_of_and_reports_missing_required_tf(self):
        result = build_snapshot(
            symbol="BTCUSDT",
            as_of=datetime.fromtimestamp(1800, timezone.utc),
            raw_candles={"15m": [candle(0), candle(900)]},
            required_timeframes=("15m", "4h"),
        )
        self.assertEqual(result.snapshot.symbol, "BTCUSDT")
        self.assertIn("GATE_DATA_UNAVAILABLE", result.wait_reason_codes)
        self.assertEqual(len(result.snapshot.candles["15m"]), 2)

    def test_real_and_proxy_cvd_cannot_be_confused(self):
        real = real_cvd([
            {"side": "buy", "size": 2, "price": 100},
            {"side": "sell", "size": 1, "price": 100},
        ])
        proxy = proxy_cvd([candle(0)])
        self.assertEqual(real["kind"], "CVD_REAL")
        self.assertEqual(real["mode"], "LIVE_CONTEXT")
        self.assertEqual(proxy["kind"], "CVD_PROXY")
        self.assertEqual(proxy["mode"], "PROXY")
        self.assertEqual(real["delta_notional"], 100)

    def test_orderbook_gap_requires_resync_and_hides_stale_depth(self):
        book = GateOrderBook("BTCUSDT")
        self.assertTrue(book.snapshot([[100, 2]], [[101, 3]], 10))
        self.assertTrue(book.features(now=book.observed_at)["available"])
        self.assertFalse(book.delta([[100, 4]], [], 12, 12))
        view = book.features(now=book.observed_at)
        self.assertEqual(view["status"], "RESYNC_REQUIRED")
        self.assertFalse(view["available"])
        self.assertNotIn("bid_depth_usd", view)

    def test_orderbook_labels_visible_liquidity_not_hidden_stops(self):
        book = GateOrderBook("BTCUSDT")
        book.snapshot([[100, 2]], [[101, 1]], 10, observed_at=1000)
        view = book.features(now=1001)
        self.assertEqual(view["liquidity_kind"], "VISIBLE_ORDERBOOK_LIQUIDITY")
        self.assertFalse(view["hidden_stops_claimed"])
        self.assertEqual(view["age_seconds"], 1)

    def test_derivatives_unknown_is_not_zero_and_point_in_time_has_no_future_leak(self):
        observations = list(normalize_contract_stats({
            "time": 100,
            "open_interest": "42",
            "lsr_account": None,
            "long_liq_usd": None,
            "short_liq_usd": None,
        }, received_at=110))
        observations.append(normalize_funding({"t": 200, "r": "0.0001"}, received_at=205))
        before = PointInTimeContext(observations).as_of(150)
        self.assertTrue(before["open_interest"]["available"])
        self.assertFalse(before["long_short_ratio"]["available"])
        self.assertIsNone(before["long_short_ratio"]["value"])
        self.assertFalse(before["funding"]["available"])
        self.assertIsNone(before["funding"]["value"])

    def test_current_gate_contract_stat_aliases_preserve_usd_liquidations_and_ratios(self):
        observations = normalize_contract_stats({
            "time": 1000,
            "open_interest": "10",
            "long_liq_usd_new": "125.5", "short_liq_usd_new": "25.5",
            "long_users": 60, "short_users": 40,
            "long_taker_size": "30", "short_taker_size": "20",
            "top_long_account": 12, "top_short_account": 8,
            "top_long_size": "9", "top_short_size": "3",
        }, received_at=1001)
        values = PointInTimeContext(observations).as_of(1001)
        self.assertEqual(values["liquidations"]["value"]["long_usd"], 125.5)
        self.assertEqual(values["long_short_ratio"]["value"], {
            "accounts": 1.5, "takers": 1.5,
            "top_accounts": 1.5, "top_positions": 3.0,
        })

    def test_point_in_time_rejects_late_arriving_past_observation(self):
        observation = normalize_funding({"t": 100, "r": "0.0001"}, received_at=200)
        before_receipt = PointInTimeContext([observation]).as_of(150)
        after_receipt = PointInTimeContext([observation]).as_of(250)
        self.assertFalse(before_receipt["funding"]["available"])
        self.assertTrue(after_receipt["funding"]["available"])

    def test_expired_derivatives_are_unavailable_not_zero(self):
        observations = normalize_contract_stats({"time": 100, "open_interest": 42}, received_at=101)
        result = PointInTimeContext(observations).as_of(100 + 3 * 3600)
        self.assertFalse(result["open_interest"]["available"])
        self.assertIsNone(result["open_interest"]["value"])

    def test_health_registry_retains_last_success_during_failure(self):
        registry = MarketHealthRegistry()
        registry.record("gate", "BTCUSDT", "15m", ok=True, now=100)
        registry.record("gate", "BTCUSDT", "15m", ok=False, now=110)
        row = registry.snapshot(now=120)[0]
        self.assertEqual(row["status"], "DEGRADED")
        self.assertEqual(row["last_success"], 100)
        self.assertEqual(row["age_seconds"], 20)

    def test_internal_context_engines_only_consume_closed_gate_rows(self):
        rows = []
        for index in range(25):
            row = candle(index * 900, closed=True)
            row.update({
                "close_time": (index + 1) * 900,
                "open": 99.5 + index,
                "high": 101 + index,
                "low": 99 + index,
                "close": 100 + index,
                "volume": 10 + index,
            })
            rows.append(row)
        future = candle(25 * 900, closed=False)
        future.update({"close_time": 26 * 900, "close": 1_000_000, "volume": 1_000_000})
        mixed = rows + [future]
        self.assertTrue(volume_features(mixed)["available"])
        self.assertEqual(volume_features(mixed)["raw_volume"], 34)
        self.assertTrue(vwap(mixed)["available"])
        self.assertEqual(classify_regime(mixed).direction, "UP")
        profile = volume_profile(mixed)
        self.assertTrue(profile["val"] <= profile["poc"] <= profile["vah"])

    def test_relative_strength_joins_only_matching_close_times(self):
        symbol = [
            {**candle(0, closed=True), "close_time": 900, "close": 100},
            {**candle(900, closed=True), "close_time": 1800, "close": 110},
        ]
        btc = [
            {**candle(0, closed=True), "close_time": 900, "close": 100},
            {**candle(900, closed=True), "close_time": 1800, "close": 105},
            {**candle(1800, closed=True), "close_time": 2700, "close": 200},
        ]
        result = relative_strength(symbol, btc, benchmark="BTCUSDT")
        self.assertEqual(result["samples"], 1)
        self.assertAlmostEqual(result["excess_return"], 0.05)

    def test_breadth_counts_only_symbols_with_two_closed_bars(self):
        universe = {
            "A": ({**candle(0, closed=True), "close_time": 900, "close": 100}, {**candle(900, closed=True), "close_time": 1800, "close": 110}),
            "B": ({**candle(0, closed=True), "close_time": 900, "close": 100}, {**candle(900, closed=True), "close_time": 1800, "close": 90}),
            "C": ({**candle(0, closed=False), "close_time": 900, "close": 100},),
        }
        result = market_breadth(universe)
        self.assertEqual(result["symbols"], 2)
        self.assertEqual(result["bullish_pct"], 0.5)
        self.assertEqual(result["bearish_pct"], 0.5)

    def test_universe_context_is_point_in_time_and_excludes_stale_symbols(self):
        def rows(closes, *, forming=None):
            result = []
            for index, close in enumerate(closes):
                result.append({
                    "timestamp": index * 900, "open": close, "high": close + 1,
                    "low": close - 1, "close": close, "volume": 10,
                })
            if forming is not None:
                result.append({
                    "timestamp": len(closes) * 900, "open": forming,
                    "high": forming + 1, "low": forming - 1,
                    "close": forming, "volume": 10, "is_closed": False,
                })
            return result

        context = build_universe_context({"15m": {
            "SOLUSDT": rows([100, 100, 110], forming=1_000_000),
            "BTCUSDT": rows([100, 100, 105]),
            "ETHUSDT": rows([100, 100, 102]),
            "STALEUSDT": rows([100, 100]),
        }}, as_of=datetime.fromtimestamp(2700, timezone.utc))
        view = context.for_symbol("SOLUSDT", ("15m",))
        timeframe = view["timeframes"]["15m"]
        self.assertEqual(timeframe["breadth"]["symbols"], 3)
        self.assertEqual(timeframe["breadth"]["excluded_stale"], 1)
        self.assertAlmostEqual(
            timeframe["relative_strength"]["BTCUSDT"]["excess_return"], 0.05,
        )
        self.assertEqual(view["market_cap"]["status"], "NOT_COLLECTED")
        self.assertEqual(view["authority"], "LIVE_CONTEXT")
        self.assertFalse(view["can_change_strategy_gate"])

    def test_universe_store_uses_existing_candles_and_rechecks_as_of(self):
        store = UniverseContextStore()
        rows = [
            {"timestamp": 0, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"timestamp": 900, "open": 100, "high": 111, "low": 99, "close": 110, "volume": 10},
            {"timestamp": 1800, "open": 110, "high": 1_001, "low": 109, "close": 1_000, "volume": 10},
        ]
        self.assertEqual(
            store.update("SOLUSDT", "15m", rows, as_of=datetime.fromtimestamp(1800, timezone.utc)),
            2,
        )
        view = store.view(
            "SOLUSDT", ("15m",), as_of=datetime.fromtimestamp(1800, timezone.utc),
        )
        self.assertEqual(view["timeframes"]["15m"]["breadth"]["symbols"], 1)
        self.assertEqual(view["timeframes"]["15m"]["breadth"]["as_of_close_time"], 1800)

    def test_universe_store_is_bounded(self):
        store = UniverseContextStore(max_bars=20)
        rows = [
            {
                "timestamp": index * 900, "open": 100, "high": 101,
                "low": 99, "close": 100 + index, "volume": 10,
            }
            for index in range(60)
        ]
        boundary = datetime.fromtimestamp(60 * 900, timezone.utc)
        self.assertEqual(store.update("BTCUSDT", "15m", rows, as_of=boundary), 20)
        self.assertEqual(store.stored_bars("BTCUSDT", "15m"), 20)
        store.clear()
        empty = store.view("BTCUSDT", ("15m",), as_of=boundary)
        self.assertEqual(empty["timeframes"]["15m"]["breadth"]["symbols"], 0)

    def test_demand_level_lifecycle_uses_closed_candles_only(self):
        level = activate(PriceLevel("lvl_1", "BTCUSDT", "OB", LevelSide.DEMAND, 95, 100, 10))
        forming = {"low": 94, "high": 101, "close": 96, "close_time": 20, "is_closed": False}
        self.assertEqual(advance_level(level, forming), level)
        touched = advance_level(level, {"low": 97, "high": 102, "close": 99, "close_time": 20, "is_closed": True})
        self.assertEqual(touched.state, LevelState.TOUCHED)
        reacted = advance_level(touched, {"low": 98, "high": 103, "close": 102, "close_time": 30, "is_closed": True})
        self.assertEqual(reacted.state, LevelState.REACTED)

    def test_supply_and_demand_breaks_are_symmetric(self):
        demand = activate(PriceLevel("d", "BTCUSDT", "FVG", LevelSide.DEMAND, 95, 100, 10))
        supply = activate(PriceLevel("s", "BTCUSDT", "FVG", LevelSide.SUPPLY, 100, 105, 10))
        broken_demand = advance_level(demand, {"low": 90, "high": 99, "close": 94, "close_time": 20, "is_closed": True})
        broken_supply = advance_level(supply, {"low": 101, "high": 110, "close": 106, "close_time": 20, "is_closed": True})
        self.assertEqual(broken_demand.state, LevelState.BROKEN)
        self.assertEqual(broken_supply.state, LevelState.BROKEN)
        flipped_demand = advance_level(broken_demand, {"low": 94, "high": 98, "close": 94, "close_time": 30, "is_closed": True})
        flipped_supply = advance_level(broken_supply, {"low": 102, "high": 106, "close": 106, "close_time": 30, "is_closed": True})
        self.assertEqual((flipped_demand.state, flipped_demand.side), (LevelState.FLIPPED, LevelSide.SUPPLY))
        self.assertEqual((flipped_supply.state, flipped_supply.side), (LevelState.FLIPPED, LevelSide.DEMAND))

    def test_level_expiry_is_deterministic(self):
        level = PriceLevel("x", "BTCUSDT", "OB", LevelSide.DEMAND, 95, 100, 10, expires_at=20)
        expired = advance_level(level, {"low": 100, "high": 101, "close": 100, "close_time": 20, "is_closed": True})
        self.assertEqual(expired.state, LevelState.EXPIRED)

    def test_legacy_zone_map_uses_latest_explicitly_closed_candle(self):
        candles = []
        for index in range(80):
            center = 100 + math.sin(index / 3) * 4
            candles.append({
                "timestamp": 1_700_000_000 + index * 3600,
                "open": center - 0.2, "high": center + 1,
                "low": center - 1, "close": center + 0.2,
                "is_closed": True,
            })
        with tempfile.TemporaryDirectory() as directory:
            db_path = os.path.join(directory, "zones.db")
            result = refresh_zones("BTCUSDT", "1h", candles, db_path)
            with sqlite3.connect(db_path) as conn:
                payloads = [json.loads(row[0]) for row in conn.execute(
                    "SELECT candle_json FROM historical_zone_events"
                )]
                states = {row[0] for row in conn.execute(
                    "SELECT lifecycle_state FROM historical_zones"
                )}
        self.assertEqual(result["status"], "updated")
        self.assertTrue(payloads)
        self.assertTrue(any(row["timestamp"] == candles[-1]["timestamp"] for row in payloads))
        self.assertTrue(states <= {state.value for state in LevelState})

    def test_legacy_zone_map_cannot_advance_from_forming_candle(self):
        candles = []
        for index in range(80):
            center = 100 + math.sin(index / 3) * 4
            candles.append({
                "timestamp": 1_700_000_000 + index * 3600,
                "open": center - 0.2, "high": center + 1,
                "low": center - 1, "close": center + 0.2,
                "is_closed": True,
            })
        forming = {
            "timestamp": candles[-1]["timestamp"] + 3600,
            "open": 100, "high": 1_000, "low": 1, "close": 1,
            "is_closed": False,
        }
        with tempfile.TemporaryDirectory() as directory:
            db_path = os.path.join(directory, "zones.db")
            refresh_zones("BTCUSDT", "1h", candles, db_path)
            with sqlite3.connect(db_path) as conn:
                before = conn.execute("SELECT COUNT(*) FROM historical_zone_events").fetchone()[0]
            refresh_zones("BTCUSDT", "1h", candles + [forming], db_path)
            with sqlite3.connect(db_path) as conn:
                after = conn.execute("SELECT COUNT(*) FROM historical_zone_events").fetchone()[0]
        self.assertEqual(before, after)

    def test_level_transitions_are_idempotent_live_context_not_trade_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            memory_path = os.path.join(directory, "memory.db")
            event = {
                "level_id": "historical-zone:7", "symbol": "BTCUSDT",
                "timeframe": "1h", "kind": "SUPPORT", "side": "DEMAND",
                "event_type": "TOUCHED", "event_time": 1_700_000_000,
            }
            second_level = {**event, "level_id": "historical-zone:8"}
            from unittest.mock import patch
            with patch.dict(os.environ, {"APEX_MEMORY_DB_PATH": memory_path}):
                self.assertEqual(persist_level_events([event, second_level]), 2)
                self.assertEqual(persist_level_events([event, second_level]), 0)
            with sqlite3.connect(memory_path) as conn:
                rows = conn.execute(
                    "SELECT event_type,payload_json,source,provenance FROM live_market_events"
                ).fetchall()
        self.assertEqual(len(rows), 2)
        row = rows[0]
        payload = json.loads(row[1])
        self.assertEqual(row[0], "LEVEL_TOUCHED")
        self.assertEqual((row[2], row[3]), ("GATE_DERIVED", "PRIMARY_MARKET"))
        self.assertEqual(payload["authority"], "LIVE_CONTEXT")
        self.assertFalse(payload["can_change_strategy_gate"])


if __name__ == "__main__":
    unittest.main()
