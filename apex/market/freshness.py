"""Timeframe alignment and freshness decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .candles import candle_close_time, epoch_seconds


FRESHNESS_SLA_SECONDS = {
    "5m": 8 * 60,
    "15m": 20 * 60,
    "1h": 75 * 60,
    "4h": 5 * 60 * 60,
    "1d": 30 * 60 * 60,
    "1w": 8 * 24 * 60 * 60,
}


@dataclass(frozen=True)
class FreshnessResult:
    timeframe: str
    status: str
    age_seconds: float | None
    last_closed_at: float | None
    reason_code: str


def assess_candles(
    timeframe: str,
    candles: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]],
    *,
    as_of: datetime | float | int | None = None,
) -> FreshnessResult:
    now = epoch_seconds(as_of or datetime.now(timezone.utc))
    if not candles or now is None:
        return FreshnessResult(timeframe, "UNAVAILABLE", None, None, "GATE_DATA_UNAVAILABLE")
    closed = candle_close_time(candles[-1], timeframe)
    if closed is None:
        return FreshnessResult(timeframe, "UNAVAILABLE", None, None, "GATE_CLOSE_TIME_UNKNOWN")
    age = max(0.0, now - closed)
    sla = FRESHNESS_SLA_SECONDS.get(timeframe)
    if sla is None:
        raise ValueError(f"unsupported_timeframe:{timeframe}")
    if age <= sla:
        return FreshnessResult(timeframe, "FRESH", age, closed, "OK")
    return FreshnessResult(timeframe, "STALE", age, closed, f"GATE_STALE_{timeframe.upper()}")


def critical_wait_reasons(
    required_timeframes: tuple[str, ...],
    results: Mapping[str, FreshnessResult],
) -> tuple[str, ...]:
    reasons = []
    for timeframe in required_timeframes:
        result = results.get(timeframe)
        if result is None:
            reasons.append("GATE_DATA_UNAVAILABLE")
        elif result.status != "FRESH":
            reasons.append(result.reason_code)
    return tuple(reasons)


__all__ = ["FRESHNESS_SLA_SECONDS", "FreshnessResult", "assess_candles", "critical_wait_reasons"]
