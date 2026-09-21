from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from datetime import datetime, timezone

from apex.market.gate_client import GateMarketClient, gate_contract, normalize_gate_candles
from apex.market.live_context import fetch_live_context
from apex.market.provider import GateSnapshotProvider
from apex.market.universe import build_universe_context


class FakeGate:
    def __init__(self, failures=()):
        self.failures = set(failures)
        self.calls = []

    def __call__(self, url, params, timeout):
        endpoint = url.rsplit("/", 1)[-1]
        self.calls.append((endpoint, dict(params), timeout))
        if endpoint in self.failures:
            raise TimeoutError(endpoint)
        if endpoint == "contract_stats":
            return [{"time": 1000, "open_interest": "5", "lsr_account": "1.2", "long_liq_usd": "20", "short_liq_usd": "10"}]
        if endpoint == "funding_rate":
            return [{"t": 1000, "r": "0.0001"}]
        if endpoint == "trades":
            return [
                {"create_time": 1000, "price": "100", "size": "2", "side": "buy"},
                {"create_time": 1000, "price": "100", "size": "1", "side": "sell"},
                {"create_time": 2000, "price": "100", "size": "99", "side": "buy"},
            ]
        if endpoint == "order_book":
            return {"id": 7, "bids": [{"p": "99", "s": "2"}], "asks": [{"p": "101", "s": "1"}]}
        if endpoint == "candlesticks":
            return [
                [0, "10", "105", "110", "90", "100"],
                [900, "12", "106", "111", "91", "105"],
            ]
        return []


class GateClientTests(unittest.TestCase):
    def test_contract_normalization_is_strict(self):
        self.assertEqual(gate_contract("btcusdt"), "BTC_USDT")
        self.assertEqual(gate_contract("BTC-USDT"), "BTC_USDT")
        with self.assertRaises(ValueError):
            gate_contract("BTCUSD")

    def test_live_context_is_point_in_time_and_sources_remain_non_authoritative(self):
        fake = FakeGate()
        client = GateMarketClient(get_json=fake, clock=lambda: 1100)
        result = fetch_live_context(client, "BTCUSDT", as_of=1100)
        self.assertEqual(result.errors, ())
        self.assertEqual(result.derivatives["open_interest"]["value"]["contracts"], 5.0)
        self.assertEqual(result.derivatives["funding"]["value"]["rate"], 0.0001)
        self.assertEqual(result.microstructure["cvd_real"]["trades"], 2)
        self.assertEqual(result.microstructure["cvd_real"]["delta_notional"], 100.0)
        self.assertEqual(result.microstructure["cvd_real"]["source"], "gate_rest_trades")
        book = result.microstructure["visible_orderbook"]
        self.assertEqual(book["liquidity_kind"], "VISIBLE_ORDERBOOK_LIQUIDITY")
        self.assertFalse(book["hidden_stops_claimed"])
        self.assertEqual(book["availability"], "FORWARD_ONLY")

    def test_optional_failure_is_unknown_not_zero_and_other_sources_survive(self):
        client = GateMarketClient(get_json=FakeGate({"contract_stats"}), clock=lambda: 1100)
        result = fetch_live_context(client, "BTCUSDT", as_of=1100)
        self.assertIn("contract_stats:TimeoutError", result.errors)
        self.assertIsNone(result.derivatives["open_interest"]["value"])
        self.assertEqual(result.derivatives["open_interest"]["availability"], "UNKNOWN")
        self.assertTrue(result.derivatives["funding"]["available"])
        self.assertTrue(result.microstructure["cvd_real"]["available"])

    def test_late_rest_microstructure_is_not_visible_to_past_snapshot(self):
        client = GateMarketClient(get_json=FakeGate(), clock=lambda: 1200)
        result = fetch_live_context(client, "BTCUSDT", as_of=1100)
        self.assertFalse(result.microstructure["cvd_real"]["available"])
        self.assertIsNone(result.microstructure["cvd_real"]["value"])
        self.assertFalse(result.microstructure["visible_orderbook"]["available"])
        self.assertIsNone(result.microstructure["visible_orderbook"]["value"])
        self.assertIn("trades:ValueError", result.errors)
        self.assertIn("order_book:ValueError", result.errors)

    def test_client_uses_bounded_public_endpoints(self):
        fake = FakeGate()
        client = GateMarketClient(get_json=fake, clock=lambda: 1100)
        client.candles("BTCUSDT", "15m", limit=99999)
        client.contract_stats("BTCUSDT", limit=99999)
        client.funding_history("BTCUSDT", limit=99999)
        client.public_trades("BTCUSDT", limit=99999)
        client.order_book("BTCUSDT", limit=99999)
        limits = {name: params["limit"] for name, params, _ in fake.calls}
        self.assertEqual(limits, {"candlesticks": 2000, "contract_stats": 100, "funding_rate": 1000, "trades": 1000, "order_book": 100})

    def test_client_translates_canonical_long_intervals_at_gate_boundary(self):
        fake = FakeGate()
        client = GateMarketClient(get_json=fake, clock=lambda: 1100)
        client.candles("BTCUSDT", "1w", limit=100)
        client.candles("BTCUSDT", "1M", limit=100)
        intervals = [
            params["interval"]
            for endpoint, params, _ in fake.calls
            if endpoint == "candlesticks"
        ]
        self.assertEqual(intervals, ["7d", "30d"])

    def test_client_singleflights_same_request_and_reuses_bounded_cache(self):
        calls = 0
        guard = Lock()

        def transport(url, params, timeout):
            nonlocal calls
            with guard:
                calls += 1
            return []

        client = GateMarketClient(get_json=transport, clock=lambda: 1000)
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: client.candles("BTCUSDT", "15m", limit=100), range(20)))
        self.assertEqual(calls, 1)
        client.clear_cache()
        client.candles("BTCUSDT", "15m", limit=100)
        self.assertEqual(calls, 2)

    def test_gate_candle_arrays_are_normalized_without_guessing_columns(self):
        rows = normalize_gate_candles([[1000, "5", "103", "105", "99", "100", "ignored"]])
        self.assertEqual(rows, [{
            "open_time": 1000.0, "volume": 5.0, "close": 103.0,
            "high": 105.0, "low": 99.0, "open": 100.0,
        }])

    def test_snapshot_provider_aligns_required_timeframes_and_drops_forming_bars(self):
        fake = FakeGate()
        client = GateMarketClient(get_json=fake, clock=lambda: 1800)
        result = GateSnapshotProvider(client, candle_limit=100).build(
            "FAST", "BTCUSDT", as_of=datetime.fromtimestamp(1800, timezone.utc),
        )
        self.assertEqual(set(result.freshness), {"15m", "1h", "4h"})
        self.assertEqual(len(result.snapshot.candles["15m"]), 2)
        self.assertEqual(len(result.snapshot.candles["1h"]), 0)
        self.assertIn("GATE_DATA_UNAVAILABLE", result.wait_reason_codes)
        self.assertEqual(result.snapshot.microstructure_context["visible_orderbook"]["availability"], "FORWARD_ONLY")
        self.assertIn("vwap", result.snapshot.volume)
        self.assertIn("volume_profile", result.snapshot.volume)
        self.assertEqual(result.snapshot.volume["cvd_proxy"]["mode"], "PROXY")
        self.assertIn("15m", result.snapshot.structure)
        self.assertEqual(result.snapshot.market_context["status"], "UNAVAILABLE")

    def test_forming_bar_cannot_leak_into_derived_snapshot_engines(self):
        def transport(url, params, timeout):
            endpoint = url.rsplit("/", 1)[-1]
            if endpoint == "candlesticks":
                return [
                    [0, "10", "100", "101", "99", "100"],
                    [900, "12", "101", "102", "99", "100"],
                    # Opened exactly at as_of: still forming and deliberately
                    # extreme enough to corrupt every derived feature.
                    [1800, "999999", "999999", "999999", "1", "101"],
                ]
            return []

        client = GateMarketClient(get_json=transport, clock=lambda: 1800)
        result = GateSnapshotProvider(client, candle_limit=100).build(
            "FAST", "BTCUSDT", as_of=datetime.fromtimestamp(1800, timezone.utc),
        )
        self.assertEqual(len(result.snapshot.candles["15m"]), 2)
        self.assertEqual(result.snapshot.volume["raw_volume"], 12.0)
        self.assertLess(result.snapshot.volume["vwap"]["value"], 200)
        self.assertLess(result.snapshot.volume["volume_profile"]["poc"], 200)

    def test_snapshot_provider_projects_preloaded_universe_without_new_api_calls(self):
        fake = FakeGate()
        client = GateMarketClient(get_json=fake, clock=lambda: 1800)
        raw = {
            "15m": {
                symbol: [
                    {"timestamp": 0, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
                    {"timestamp": 900, "open": 100, "high": 111, "low": 99, "close": close, "volume": 12},
                ]
                for symbol, close in (("SOLUSDT", 110), ("BTCUSDT", 105), ("ETHUSDT", 102))
            },
        }
        universe = build_universe_context(
            raw, as_of=datetime.fromtimestamp(1800, timezone.utc),
        )
        result = GateSnapshotProvider(
            client, candle_limit=100, universe_context=universe,
        ).build("FAST", "SOLUSDT", as_of=datetime.fromtimestamp(1800, timezone.utc))
        view = result.snapshot.market_context
        self.assertIn("relative_strength", view["timeframes"]["15m"])
        self.assertFalse(view["can_change_strategy_gate"])
        # The shared context is pure calculation; only the ordinary snapshot
        # provider endpoints appear in the transport log.
        self.assertFalse(any(endpoint == "contracts" for endpoint, _, _ in fake.calls))

    def test_snapshot_provider_cannot_read_future_universe_bar(self):
        raw = {"15m": {
            symbol: [
                {"timestamp": 0, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
                {"timestamp": 900, "open": 100, "high": 111, "low": 99, "close": 110, "volume": 10},
                {"timestamp": 1800, "open": 110, "high": 1_001, "low": 109, "close": 1_000, "volume": 10},
            ]
            for symbol in ("SOLUSDT", "BTCUSDT")
        }}
        universe = build_universe_context(
            raw, as_of=datetime.fromtimestamp(2700, timezone.utc),
        )
        fake = FakeGate()
        result = GateSnapshotProvider(
            GateMarketClient(get_json=fake, clock=lambda: 1800),
            candle_limit=100, universe_context=universe,
        ).build("FAST", "SOLUSDT", as_of=datetime.fromtimestamp(1800, timezone.utc))
        strength = result.snapshot.market_context["timeframes"]["15m"]["relative_strength"]["BTCUSDT"]
        self.assertEqual(strength["samples"], 1)
        self.assertEqual(result.snapshot.market_context["as_of"], "1970-01-01T00:30:00+00:00")


if __name__ == "__main__":
    unittest.main()
