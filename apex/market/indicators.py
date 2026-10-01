"""Small deterministic indicator utilities shared by compatibility callers."""

from __future__ import annotations

import math

INDICATOR_VERSION = "wilder-v1"


def true_ranges(candles: list) -> list[float]:
    """One TR per transition; reject corrupt OHLC rather than invent values."""
    values = []
    for candle in candles:
        high, low, close = (float(candle[key]) for key in ("high", "low", "close"))
        if not all(math.isfinite(value) for value in (high, low, close)) or high < low:
            raise ValueError("invalid OHLC")
        values.append((high, low, close))
    return [max(high - low, abs(high - previous[2]), abs(low - previous[2]))
            for previous, (high, low, _close) in zip(values, values[1:])]


def average_true_range(candles: list, period: int = 14) -> float | None:
    """Wilder ATR, seeded with the first period true ranges (TA-Lib convention)."""
    if not candles or period <= 0 or len(candles) < period + 1:
        return None
    ranges = true_ranges(candles)
    value = sum(ranges[:period]) / period
    for current in ranges[period:]:
        value = (value * (period - 1) + current) / period
    return value


def average_directional_index(candles: list, period: int = 14) -> float | None:
    """Wilder ADX with a 2*period-1 lookback and TA-Lib's DM seed."""
    if period < 2 or len(candles) < 2 * period:
        return None
    ranges = true_ranges(candles)
    positive, negative = [], []
    for previous, current in zip(candles, candles[1:]):
        up = float(current["high"]) - float(previous["high"])
        down = float(previous["low"]) - float(current["low"])
        positive.append(up if up > down and up > 0 else 0.0)
        negative.append(down if down > up and down > 0 else 0.0)
    plus, minus, tr = (sum(series[:period - 1]) for series in (positive, negative, ranges))
    dx = []
    for index in range(period - 1, len(ranges)):
        plus = plus - plus / period + positive[index]
        minus = minus - minus / period + negative[index]
        tr = tr - tr / period + ranges[index]
        total = plus + minus
        dx.append(100.0 * abs(plus - minus) / total if tr > 0 and total > 0 else 0.0)
    value = sum(dx[:period]) / period
    for current in dx[period:]:
        value = (value * (period - 1) + current) / period
    return value


def ema_value(values: list, period: int) -> float | None:
    """Return a standard exponentially weighted moving average value."""
    if not values or period <= 0 or len(values) < period:
        return None
    value = sum(values[:period]) / period
    alpha = 2.0 / (period + 1.0)
    for price in values[period:]:
        value = float(price) * alpha + value * (1.0 - alpha)
    return value


__all__ = ["INDICATOR_VERSION", "true_ranges", "average_true_range", "average_directional_index", "ema_value"]
