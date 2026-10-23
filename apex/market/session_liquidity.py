"""Closed-candle session-liquidity compatibility provider."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .snapshot_scope import snapshot_scope_active


class SessionLiquidityProvider:
    def __init__(
        self,
        get_candles: Callable[[str, str, int], list],
        *,
        cache_ttl: int = 300,
    ):
        self._get_candles = get_candles
        self._cache_ttl = cache_ttl
        self._cache: dict[str, tuple[float, dict]] = {}

    def check(self, symbol: str, timeframe: str = "1h") -> dict:
        """Compare the latest closed candle volume with the prior 20 bars."""
        now = time.time()
        scoped = snapshot_scope_active()
        cache_key = f"{symbol}:{timeframe}"
        cached = self._cache.get(cache_key)
        if not scoped and cached and now - cached[0] < self._cache_ttl:
            return cached[1]

        result = {"ratio": None, "ok": False, "desc": "Liquidity UNKNOWN: insufficient valid closed candles"}
        try:
            candles = self._get_candles(symbol, timeframe, 25)
            if not candles:
                return result
            if any("open_time" in row or "timestamp" in row for row in candles):
                from .candles import confirmed_candles
                closed = list(confirmed_candles(candles, timeframe))
            elif all(row.get("is_closed") is True for row in candles):
                closed = candles
            else:
                closed = candles[:-1]  # legacy mixed tail without timestamps
            if len(closed) < 21:
                return result
            closed_volume = closed[-1].get("volume", 0)
            average_volume = sum(candle.get("volume", 0) for candle in closed[-21:-1]) / 20
            if average_volume <= 0:
                return result

            ratio = round(closed_volume / average_volume, 2)
            ok = ratio >= 0.7
            result = {
                "ratio": ratio,
                "ok": ok,
                "desc": f"Vol ratio: {ratio:.2f}x" + ("" if ok else " (LOW)"),
            }
        except Exception as exc:
            logging.debug("check_session_liquidity %s: %s", symbol, exc)

        if not scoped:
            self._cache[cache_key] = (now, result)
        return result


__all__ = ["SessionLiquidityProvider"]
