"""Build one aligned point-in-time MarketSnapshot for a strategy decision."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from apex.domain.ids import new_id
from apex.domain.models import MarketRegime, MarketSnapshot

from .candles import confirmed_candles, epoch_seconds
from .freshness import FreshnessResult, assess_candles, critical_wait_reasons


@dataclass(frozen=True)
class SnapshotBuild:
    snapshot: MarketSnapshot
    freshness: Mapping[str, FreshnessResult]
    wait_reason_codes: tuple[str, ...]


def build_snapshot(
    *,
    symbol: str,
    as_of: datetime,
    raw_candles: Mapping[str, list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]],
    required_timeframes: tuple[str, ...],
    structure: Mapping[str, Any] | None = None,
    levels: tuple[Mapping[str, Any], ...] = (),
    regime: MarketRegime | None = None,
    volume: Mapping[str, Any] | None = None,
    derivatives_context: Mapping[str, Any] | None = None,
    microstructure_context: Mapping[str, Any] | None = None,
    market_context: Mapping[str, Any] | None = None,
) -> SnapshotBuild:
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    as_of = as_of.astimezone(timezone.utc)
    normalized = {
        timeframe: confirmed_candles(rows, timeframe, as_of=as_of)
        for timeframe, rows in raw_candles.items()
    }
    freshness = {
        timeframe: assess_candles(timeframe, rows, as_of=as_of)
        for timeframe, rows in normalized.items()
    }
    for timeframe in required_timeframes:
        if timeframe not in freshness:
            freshness[timeframe] = FreshnessResult(
                timeframe, "UNAVAILABLE", None, None, "GATE_DATA_UNAVAILABLE"
            )
    wait = critical_wait_reasons(required_timeframes, freshness)
    snapshot = MarketSnapshot(
        snapshot_id=new_id("snapshot"), symbol=symbol.upper(), as_of=as_of,
        candles=normalized, structure=structure or {}, levels=levels,
        regime=regime or MarketRegime("RANGE", "NORMAL", "TRANSITION"),
        volume=volume or {}, derivatives_context=derivatives_context or {},
        microstructure_context=microstructure_context or {}, market_context=market_context or {},
    )
    boundary = epoch_seconds(as_of)
    assert boundary is not None
    assert all(row["close_time"] <= boundary for rows in normalized.values() for row in rows)
    return SnapshotBuild(snapshot, freshness, wait)


__all__ = ["SnapshotBuild", "build_snapshot"]
