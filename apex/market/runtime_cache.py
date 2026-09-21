"""Bounded process-local candle cache used by transition runtime paths."""

from __future__ import annotations

import time


_CANDLES: dict[str, list] = {}
_UPDATED_AT: dict[str, float] = {}
_TTL_SECONDS = 60


def update_global_candles(symbol: str, timeframe: str, candles: list) -> None:
    key = f"{symbol}:{timeframe}"
    _CANDLES[key] = candles
    _UPDATED_AT[key] = time.time()


def get_global_candles(symbol: str, timeframe: str) -> list:
    key = f"{symbol}:{timeframe}"
    if key in _CANDLES and time.time() - _UPDATED_AT.get(key, 0) < _TTL_SECONDS:
        return _CANDLES[key]
    return []


def get_confirmed_candles(candles: list) -> list:
    """Preserve the transition runtime's exchange-last-row contract."""
    if not candles or len(candles) < 2:
        return []
    return candles[:-1]


def last_closed_candle_time(candles: list):
    closed = get_confirmed_candles(candles)
    if not closed or not isinstance(closed[-1], dict):
        return None
    row = closed[-1]
    return row.get("timestamp") or row.get("time") or row.get("open_time") or row.get("t")


__all__ = [
    "get_confirmed_candles", "get_global_candles", "last_closed_candle_time",
    "update_global_candles",
]
