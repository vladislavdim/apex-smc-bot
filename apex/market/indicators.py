"""Small deterministic indicator utilities shared by compatibility callers."""

from __future__ import annotations


def average_true_range(candles: list, period: int = 14) -> float | None:
    """Return ATR from completed candles without inventing a price level."""
    if not candles or period <= 0 or len(candles) < period + 1:
        return None
    true_ranges = []
    for index in range(1, len(candles)):
        candle = candles[index]
        previous_close = float(candles[index - 1]["close"])
        true_ranges.append(max(
            float(candle["high"]) - float(candle["low"]),
            abs(float(candle["high"]) - previous_close),
            abs(float(candle["low"]) - previous_close),
        ))
    return sum(true_ranges[-period:]) / period


def ema_value(values: list, period: int) -> float | None:
    """Return a standard exponentially weighted moving average value."""
    if not values or period <= 0 or len(values) < period:
        return None
    value = sum(values[:period]) / period
    alpha = 2.0 / (period + 1.0)
    for price in values[period:]:
        value = float(price) * alpha + value * (1.0 - alpha)
    return value


__all__ = ["average_true_range", "ema_value"]
