import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock

from apex.domain.ids import new_id
from apex.domain.models import MarketRegime, MarketSnapshot
from apex.market.candle_router import GateCandleRouter
from apex.market.snapshot_scope import use_market_snapshot


def _candles(count):
    return [{"close": float(index)} for index in range(count)]


class GateCandleRouterTests(unittest.TestCase):
    def _router(self, **overrides):
        values = {
            "cache": {}, "get_shared": Mock(return_value=[]),
            "update_shared": Mock(),
            "fetch_gate": Mock(return_value={"candles": _candles(5)}),
            "gate_available": Mock(return_value=True),
            "record_health": Mock(), "last_closed_at": Mock(return_value="closed"),
        }
        values.update(overrides)
        return GateCandleRouter(**values), values

    def test_short_cache_cannot_satisfy_larger_request(self):
        cache = {"BTCUSDT_1h": (_candles(2), 10**20)}
        router, dependencies = self._router(cache=cache)
        result = router.get_candles("BTCUSDT", "1h", 4)
        self.assertEqual(len(result), 4)
        dependencies["fetch_gate"].assert_called_once_with("BTCUSDT", "1h", 4)

    def test_gate_failure_records_exact_reason(self):
        router, dependencies = self._router(
            fetch_gate=Mock(return_value={"candles": [], "error": "Gate HTTP 503"}),
        )
        self.assertEqual(router.get_candles("AAVEUSDT", "15m", 120), [])
        self.assertEqual(
            dependencies["record_health"].call_args.kwargs["reason"],
            "SMC adapter: Gate HTTP 503",
        )

    def test_batch_omits_empty_results(self):
        router, _ = self._router()
        router.get_candles = Mock(side_effect=lambda symbol, *_: _candles(3) if symbol == "BTCUSDT" else [])
        result = asyncio.run(router.fetch_candles_batch(["BTCUSDT", "ETHUSDT"]))
        self.assertEqual(list(result), ["BTCUSDT"])

    def test_snapshot_scope_bypasses_cache_shared_and_gate(self):
        router, dependencies = self._router(
            cache={"BTCUSDT_1h": (_candles(9), 10**20)},
        )
        snapshot = MarketSnapshot(
            snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
            as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
            candles={"1h": tuple(_candles(4))}, structure={}, levels=(),
            regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
            volume={}, derivatives_context={}, microstructure_context={},
        )

        with use_market_snapshot(snapshot):
            self.assertEqual(router.get_candles("BTCUSDT", "1h", 2), _candles(4)[-2:])
            self.assertEqual(router.get_candles("BTCUSDT", "4h", 2), [])
            self.assertEqual(router.get_candles("ETHUSDT", "1h", 2), [])

        dependencies["get_shared"].assert_not_called()
        dependencies["fetch_gate"].assert_not_called()

    def test_router_returns_to_live_sources_after_snapshot_scope(self):
        router, dependencies = self._router()
        snapshot = MarketSnapshot(
            snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
            as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
            candles={"1h": tuple(_candles(3))}, structure={}, levels=(),
            regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
            volume={}, derivatives_context={}, microstructure_context={},
        )
        with use_market_snapshot(snapshot):
            self.assertEqual(len(router.get_candles("BTCUSDT", "1h", 3)), 3)

        self.assertEqual(len(router.get_candles("BTCUSDT", "1h", 5)), 5)
        dependencies["fetch_gate"].assert_called_once()


if __name__ == "__main__":
    unittest.main()
