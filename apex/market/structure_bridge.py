"""Canonical market-structure compatibility functions."""

from __future__ import annotations

from core.market_structure import (
    analyze_market_structure as _analyze_market_structure,
    classify_swings as _classify_swings,
    events_with_trend_fallback as _events_with_trend_fallback,
    find_swings as _find_swings,
)


def get_bos_choch_event(
    candles: list,
    direction: str,
    lookback: int = 15,
    max_break_age: int = 1,
) -> dict | None:
    """Return a close-confirmed structural event matching ``direction``.

    ``lookback`` keeps its legacy meaning as the minimum amount of history,
    while centred pivots use a small, symmetric radius appropriate for the
    supplied LTF sample. A BOS continues an established HH+HL/LH+LL trend;
    a CHoCH breaks against it. Mixed structure returns ``None``.
    """
    try:
        if direction not in ("BULLISH", "BEARISH"):
            return None
        if not candles or len(candles) < max(lookback + 3, 12):
            return None

        swing_lookback = 3 if len(candles) >= 50 else 2
        structure = _analyze_market_structure(
            candles,
            swing_lookback=swing_lookback,
            max_break_age=max_break_age,
        )
        event = structure.get("event")
        if not event or event.get("direction") != direction:
            return None
        if event.get("type") not in ("BOS", "CHoCH") or not event.get("closed"):
            return None
        return event
    except Exception:
        return None


def detect_bos_choch(
    candles: list,
    direction: str,
    lookback: int = 15,
) -> bool:
    """Compatibility boolean for callers that do not need event metadata."""
    return get_bos_choch_event(candles, direction, lookback=lookback) is not None


def find_equal_highs_lows(
    candles: list,
    lookback: int = 20,
    tolerance: float = 0.002,
) -> tuple[float | None, float | None]:
    """Return the first matching equal-high and equal-low liquidity levels."""
    recent = candles[-lookback:]
    highs = [candle["high"] for candle in recent]
    lows = [candle["low"] for candle in recent]

    equal_high = None
    equal_low = None
    for first in range(len(highs) - 1):
        for second in range(first + 1, len(highs)):
            if abs(highs[first] - highs[second]) / highs[first] <= tolerance:
                equal_high = (highs[first] + highs[second]) / 2
                break
        if equal_high:
            break

    for first in range(len(lows) - 1):
        for second in range(first + 1, len(lows)):
            if abs(lows[first] - lows[second]) / lows[first] <= tolerance:
                equal_low = (lows[first] + lows[second]) / 2
                break
        if equal_low:
            break

    return equal_high, equal_low


def find_swings(candles: list, lookback: int = 8):
    return _find_swings(candles, lookback=lookback)


def classify_swings(highs: list, lows: list):
    return _classify_swings(highs, lows)


def detect_events(candles: list, classified):
    """Return a real BOS/CHoCH or a symmetric HH+HL/LH+LL trend state."""
    if not classified or not candles:
        return []
    return _events_with_trend_fallback(candles, classified, max_break_age=1)


__all__ = [
    "classify_swings",
    "detect_bos_choch",
    "detect_events",
    "find_equal_highs_lows",
    "find_swings",
    "get_bos_choch_event",
]
