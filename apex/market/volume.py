"""Deterministic volume and VWAP features from confirmed Gate candles."""

from __future__ import annotations

from math import sqrt
from statistics import mean
from typing import Any, Iterable, Mapping


def _rows(candles: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [row for row in candles if row.get("is_closed") is True]


def volume_features(candles: Iterable[Mapping[str, Any]], *, lookback: int = 20) -> dict[str, Any]:
    rows = _rows(candles)
    if not rows:
        return {"available": False, "source": "gate", "mode": "LIVE_CONTEXT"}
    values = [max(0.0, float(row.get("volume") or 0.0)) for row in rows]
    current = values[-1]
    history = values[max(0, len(values) - lookback - 1):-1]
    baseline = mean(history) if history else None
    deviation = sqrt(mean((value - baseline) ** 2 for value in history)) if history and baseline is not None else None
    percentile = sum(value <= current for value in history) / len(history) if history else None
    previous = values[-2] if len(values) > 1 else None
    return {
        "available": True, "source": "gate", "mode": "LIVE_CONTEXT",
        "raw_volume": current,
        "relative_volume": current / baseline if baseline else None,
        "volume_percentile": percentile,
        "volume_zscore": (current - baseline) / deviation if baseline is not None and deviation else None,
        "volume_acceleration": current / previous - 1 if previous else None,
        "samples": len(history),
    }


def vwap(candles: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = _rows(candles)
    numerator = denominator = 0.0
    values: list[float] = []
    for row in rows:
        volume = max(0.0, float(row.get("volume") or 0.0))
        typical = (float(row["high"]) + float(row["low"]) + float(row["close"])) / 3
        numerator += typical * volume
        denominator += volume
        values.append(numerator / denominator if denominator else typical)
    if not values:
        return {"available": False, "source": "gate"}
    close = float(rows[-1]["close"])
    return {
        "available": True, "source": "gate", "value": values[-1],
        "side": "ABOVE" if close > values[-1] else "BELOW" if close < values[-1] else "AT",
        "slope": values[-1] - values[-2] if len(values) > 1 else None,
    }


__all__ = ["volume_features", "vwap"]
