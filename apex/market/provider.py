"""Canonical one-call MarketSnapshot provider for the five V3 strategies."""

from __future__ import annotations

from datetime import datetime, timezone
from apex.domain.enums import Strategy
from apex.strategies.specifications import specification_for

from .gate_client import GateMarketClient, normalize_gate_candles
from .candles import confirmed_candles
from .live_context import fetch_live_context
from .orderflow import proxy_cvd
from .regime import classify_regime
from .snapshots import SnapshotBuild, build_snapshot
from .structure import analyze_market_structure
from .volume import volume_features, vwap
from .volume_profile import volume_profile
from .universe import UniverseContext, market_cap_omission


class GateSnapshotProvider:
    def __init__(
        self, client: GateMarketClient, *, candle_limit: int = 500,
        universe_context: UniverseContext | None = None,
    ) -> None:
        self.client = client
        self.candle_limit = min(2000, max(100, int(candle_limit)))
        self.universe_context = universe_context

    def build(
        self,
        strategy: Strategy | str,
        symbol: str,
        *,
        as_of: datetime | None = None,
    ) -> SnapshotBuild:
        key = strategy if isinstance(strategy, Strategy) else Strategy(str(strategy).upper())
        specification = specification_for(key)
        boundary = as_of or datetime.now(timezone.utc)
        if boundary.tzinfo is None:
            boundary = boundary.replace(tzinfo=timezone.utc)
        boundary = boundary.astimezone(timezone.utc)
        required = tuple(dict.fromkeys((specification.working_timeframe, *specification.context_timeframes)))
        candles = {}
        for timeframe in required:
            try:
                response = self.client.candles(symbol, timeframe, limit=self.candle_limit)
                # Every downstream engine consumes exactly the same confirmed
                # point-in-time rows.  Filtering only inside build_snapshot is
                # too late because structure/regime/volume are derived below.
                candles[timeframe] = confirmed_candles(
                    normalize_gate_candles(response.payload),
                    timeframe,
                    as_of=boundary,
                )
            except Exception:
                candles[timeframe] = []

        # Context is optional and cannot change StrategyResult. Late-arriving
        # observations are excluded by PointInTimeContext.
        live = fetch_live_context(self.client, symbol, as_of=int(boundary.timestamp()))
        working_rows = candles.get(specification.working_timeframe, [])
        context_rows = next((candles[tf] for tf in reversed(required) if candles.get(tf)), working_rows)
        volume = volume_features(working_rows)
        volume.update({
            "vwap": vwap(working_rows),
            "volume_profile": volume_profile(working_rows),
            "cvd_proxy": proxy_cvd(working_rows),
        })
        structure = {
            timeframe: analyze_market_structure(rows, swing_lookback=5, max_break_age=1)
            for timeframe, rows in candles.items()
            if rows
        }
        market_context = (
            self.universe_context.for_symbol(symbol, required, as_of=boundary)
            if self.universe_context is not None
            else {
                "authority": "LIVE_CONTEXT", "can_change_strategy_gate": False,
                "status": "UNAVAILABLE", "reason_code": "UNIVERSE_CONTEXT_NOT_LOADED",
                "market_cap": market_cap_omission(),
            }
        )
        return build_snapshot(
            symbol=symbol,
            as_of=boundary,
            raw_candles=candles,
            required_timeframes=required,
            structure=structure,
            regime=classify_regime(context_rows),
            volume=volume,
            derivatives_context=live.derivatives,
            microstructure_context=live.microstructure,
            market_context=market_context,
        )


__all__ = ["GateSnapshotProvider"]
