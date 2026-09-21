from datetime import datetime, timezone
from unittest.mock import Mock

from apex.domain.ids import new_id
from apex.domain.models import MarketRegime, MarketSnapshot
from apex.market.adaptive_indicators import LegacyAdaptiveIndicators
from apex.market.btc_correlation import BtcCorrelationProvider
from apex.market.runtime_cache import get_global_candles, update_global_candles
from apex.market.session_liquidity import SessionLiquidityProvider
from apex.market.snapshot_scope import snapshot_candle_override, use_market_snapshot


def _rows(count: int, *, volume: float = 10.0):
    return tuple({
        "open": float(index + 1), "high": float(index + 2),
        "low": float(index), "close": float(index + 1.5),
        "volume": volume,
    } for index in range(count))


def _snapshot(rows):
    return MarketSnapshot(
        snapshot_id=new_id("snapshot"), symbol="ETHUSDT",
        as_of=datetime(2026, 9, 21, tzinfo=timezone.utc),
        candles={"1h": rows, "4h": rows}, structure={}, levels=(),
        regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"), volume={},
        derivatives_context={}, microstructure_context={},
    )


def test_snapshot_scope_blocks_shared_candles_for_other_symbols():
    update_global_candles("BTCUSDT", "4h", list(_rows(30)))
    assert get_global_candles("BTCUSDT", "4h")
    with use_market_snapshot(_snapshot(_rows(30))):
        assert get_global_candles("BTCUSDT", "4h") == []
        assert get_global_candles("ETHUSDT", "4h") == list(_rows(30))
    assert get_global_candles("BTCUSDT", "4h")


def test_snapshot_evaluation_bypasses_derived_provider_caches():
    live_rows = list(_rows(25, volume=10.0))
    snapshot_rows = _rows(25, volume=30.0)
    live_candles = Mock(return_value=live_rows)

    def candles(symbol, timeframe, limit):
        scoped = snapshot_candle_override(symbol, timeframe, limit)
        return live_candles(symbol, timeframe, limit) if scoped is None else scoped

    indicators = LegacyAdaptiveIndicators(candles, lambda closes, _period: closes[-1])
    liquidity = SessionLiquidityProvider(candles)
    correlation = BtcCorrelationProvider(candles, get_global_candles)

    indicators.get_precomputed_indicators("ETHUSDT", "4h")
    liquidity.check("ETHUSDT", "1h")
    correlation.get("ETHUSDT", live_rows)
    with use_market_snapshot(_snapshot(snapshot_rows)):
        scoped_indicators = indicators.get_precomputed_indicators("ETHUSDT", "4h")
        scoped_liquidity = liquidity.check("ETHUSDT", "1h")
        scoped_correlation = correlation.get("ETHUSDT")

    assert scoped_indicators["avg_vol"] == 30.0
    assert scoped_liquidity["ratio"] == 1.0
    assert scoped_correlation["btc_dir"] == "UNKNOWN"
