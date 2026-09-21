"""Deterministic closed-candle pattern helpers."""

from __future__ import annotations


def detect_engulfing(candles: list, direction: str) -> bool:
    """Detect a strict directional engulfing pattern in the latest three bars."""
    if not candles or len(candles) < 2:
        return False
    for index in range(-1, max(-4, -len(candles)), -1):
        current = candles[index]
        previous = (
            candles[index - 1] if abs(index - 1) <= len(candles) else None
        )
        if not previous:
            continue
        current_body = abs(current["close"] - current["open"])
        previous_body = abs(previous["close"] - previous["open"])
        if previous_body == 0:
            continue
        if direction == "BULLISH":
            if (
                current["close"] > current["open"]
                and previous["close"] < previous["open"]
                and current_body > previous_body
                and current["close"] > previous["open"]
                and current["open"] < previous["close"]
            ):
                return True
        elif direction == "BEARISH":
            if (
                current["close"] < current["open"]
                and previous["close"] > previous["open"]
                and current_body > previous_body
                and current["close"] < previous["open"]
                and current["open"] > previous["close"]
            ):
                return True
    return False


__all__ = ["detect_engulfing"]
