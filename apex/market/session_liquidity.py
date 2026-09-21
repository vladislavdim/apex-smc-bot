"""Closed-candle session-liquidity compatibility provider."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable


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
        cache_key = f"{symbol}:{timeframe}"
        cached = self._cache.get(cache_key)
        if cached and now - cached[0] < self._cache_ttl:
            return cached[1]

        result = {"ratio": 1.0, "ok": True, "desc": ""}
        try:
            candles = self._get_candles(symbol, timeframe, 25)
            if not candles or len(candles) < 22:
                return result

            closed_volume = candles[-2].get("volume", 0)
            average_volume = sum(
                candle.get("volume", 0) for candle in candles[-22:-2]
            ) / 20
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

        self._cache[cache_key] = (now, result)
        return result


__all__ = ["SessionLiquidityProvider"]
