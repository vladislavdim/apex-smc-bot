"""Canonical closed-candle normalization for APEX V3."""

from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
from typing import Any, Iterable, Mapping


TIMEFRAME_SECONDS = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 7 * 86400,
}


def timeframe_seconds(timeframe: str) -> int:
    try:
        return TIMEFRAME_SECONDS[str(timeframe).lower()]
    except KeyError as exc:
        raise ValueError(f"unsupported_timeframe:{timeframe}") from exc


def epoch_seconds(value: Any) -> float | None:
    if isinstance(value, datetime):
        current = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return current.astimezone(timezone.utc).timestamp()
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(number):
        return None
    while number > 100_000_000_000:
        number /= 1000.0
    return number


def candle_open_time(row: Mapping[str, Any]) -> float | None:
    for key in ("open_time", "openTime", "timestamp", "time", "t", "ts"):
        if row.get(key) is not None:
            return epoch_seconds(row[key])
    return None


def candle_close_time(row: Mapping[str, Any], timeframe: str) -> float | None:
    for key in ("close_time", "closeTime", "closed_at"):
        if row.get(key) is not None:
            return epoch_seconds(row[key])
    opened = candle_open_time(row)
    return None if opened is None else opened + timeframe_seconds(timeframe)


def confirmed_candles(
    rows: Iterable[Mapping[str, Any]],
    timeframe: str,
    *,
    as_of: datetime | float | int | None = None,
) -> tuple[Mapping[str, Any], ...]:
    """Return sorted immutable bars whose close was known at ``as_of``.

    Explicit ``is_closed=False`` always wins. Duplicate venue bars are reduced
    to one row by open time. Unknown timestamps are rejected rather than
    guessed, preventing a mutable edge candle from entering a decision.
    """
    boundary = epoch_seconds(as_of or datetime.now(timezone.utc))
    if boundary is None:
        raise ValueError("invalid_as_of")
    normalized: dict[float, Mapping[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping) or raw.get("is_closed") is False:
            continue
        opened = candle_open_time(raw)
        closed = candle_close_time(raw, timeframe)
        if opened is None or closed is None or closed > boundary:
            continue
        try:
            ohlcv = {
                "open": float(raw["open"]),
                "high": float(raw["high"]),
                "low": float(raw["low"]),
                "close": float(raw["close"]),
                "volume": float(raw.get("volume") or 0.0),
            }
        except (KeyError, TypeError, ValueError):
            continue
        if not all(isfinite(value) for value in ohlcv.values()):
            continue
        normalized[opened] = {
            **ohlcv,
            "open_time": opened,
            "close_time": closed,
            "is_closed": True,
        }
    return tuple(normalized[key] for key in sorted(normalized))


__all__ = [
    "TIMEFRAME_SECONDS", "candle_close_time", "candle_open_time",
    "confirmed_candles", "epoch_seconds", "timeframe_seconds",
]
