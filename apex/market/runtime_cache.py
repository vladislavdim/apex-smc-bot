"""Bounded process-local candle cache used by transition runtime paths."""

from __future__ import annotations

import time

from .snapshot_scope import snapshot_candle_override


_CANDLES: dict[str, list] = {}
_UPDATED_AT: dict[str, float] = {}
_TTL_SECONDS = 60
_MAX_ENTRIES = 512


def prune_global_candles(*, force: bool = False) -> int:
    """Drop expired optional candle copies without touching durable state."""
    now = time.time()
    stale = [
        key for key, updated_at in _UPDATED_AT.items()
        if force or now - updated_at >= _TTL_SECONDS
    ]
    for key in stale:
        _CANDLES.pop(key, None)
        _UPDATED_AT.pop(key, None)
    if len(_CANDLES) > _MAX_ENTRIES:
        overflow = sorted(_UPDATED_AT, key=_UPDATED_AT.get)[:len(_CANDLES) - _MAX_ENTRIES]
        for key in overflow:
            _CANDLES.pop(key, None)
            _UPDATED_AT.pop(key, None)
        stale.extend(overflow)
    return len(set(stale))


def update_global_candles(symbol: str, timeframe: str, candles: list) -> None:
    prune_global_candles()
    key = f"{symbol}:{timeframe}"
    _CANDLES[key] = candles
    _UPDATED_AT[key] = time.time()


def get_global_candles(symbol: str, timeframe: str) -> list:
    snapshot_rows = snapshot_candle_override(symbol, timeframe, 2000)
    if snapshot_rows is not None:
        return snapshot_rows
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
    "prune_global_candles", "update_global_candles",
]
