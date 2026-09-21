"""Three-dimensional V3 regime classifier used as live context only."""

from __future__ import annotations

from statistics import mean
from typing import Any, Iterable, Mapping

from apex.domain.models import MarketRegime


def classify_regime(candles: Iterable[Mapping[str, Any]], *, window: int = 20) -> MarketRegime:
    rows = [row for row in candles if row.get("is_closed") is True][-max(4, window):]
    if len(rows) < 4:
        return MarketRegime("RANGE", "NORMAL", "TRANSITION")
    closes = [float(row["close"]) for row in rows]
    ranges = [max(0.0, float(row["high"]) - float(row["low"])) for row in rows]
    baseline = mean(closes[:-1])
    drift = (closes[-1] - closes[0]) / baseline if baseline else 0.0
    noise = mean(ranges[:-1]) / baseline if baseline else 0.0
    threshold = max(noise, 0.002)
    direction = "UP" if drift > threshold else "DOWN" if drift < -threshold else "RANGE"
    current_range = ranges[-1]
    average_range = mean(ranges[:-1]) or current_range or 1.0
    ratio = current_range / average_range
    volatility = "EXTREME" if ratio > 2.5 else "HIGH" if ratio > 1.5 else "LOW" if ratio < 0.65 else "NORMAL"
    recent = mean(ranges[-3:])
    earlier = mean(ranges[:-3]) if len(ranges) > 3 else recent
    if recent < earlier * 0.7:
        phase = "COMPRESSION"
    elif recent > earlier * 1.35:
        phase = "EXPANSION"
    elif direction != "RANGE" and closes[-1] * (1 if direction == "UP" else -1) < closes[-2] * (1 if direction == "UP" else -1):
        phase = "PULLBACK"
    else:
        phase = "TRANSITION"
    return MarketRegime(direction, volatility, phase)


__all__ = ["classify_regime"]
