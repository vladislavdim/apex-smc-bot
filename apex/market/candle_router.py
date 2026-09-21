"""Gate-only candle routing and bounded concurrent batch reads."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

from .snapshot_scope import snapshot_candle_override


class GateCandleRouter:
    def __init__(
        self,
        *,
        cache: dict,
        get_shared: Callable[[str, str], list],
        update_shared: Callable[[str, str, list], None],
        fetch_gate: Callable[[str, str, int], object],
        gate_available: Callable[[], bool],
        record_health: Callable[..., None],
        last_closed_at: Callable[[list], object],
    ):
        self.cache = cache
        self._get_shared = get_shared
        self._update_shared = update_shared
        self._fetch_gate = fetch_gate
        self._gate_available = gate_available
        self._record_health = record_health
        self._last_closed_at = last_closed_at

    @staticmethod
    def _cache_ttl(interval: str) -> int:
        if interval in ("1m", "3m", "5m"):
            return 60
        if interval in ("15m", "30m"):
            return 180
        if interval in ("1h", "2h"):
            return 300
        return 600

    def get_candles(self, symbol: str, interval: str = "1h", limit: int = 200) -> list:
        requested_limit = max(1, int(limit or 1))
        snapshot_rows = snapshot_candle_override(
            symbol, interval, requested_limit,
        )
        if snapshot_rows is not None:
            return snapshot_rows
        cache_key = f"{symbol}_{interval}"
        errors = []

        if cache_key in self.cache:
            cached, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self._cache_ttl(interval) and len(cached) >= requested_limit:
                self._record_health(
                    symbol, interval, True, source="Gate cache",
                    candle_count=len(cached), cached=True,
                    last_closed_candle_at=self._last_closed_at(cached),
                )
                return cached[-requested_limit:]

        shared = self._get_shared(symbol, interval)
        if shared and len(shared) >= requested_limit:
            self.cache[cache_key] = (shared, time.time())
            self._record_health(
                symbol, interval, True, source="Gate shared cache",
                candle_count=len(shared), cached=True,
                last_closed_candle_at=self._last_closed_at(shared),
            )
            return shared[-requested_limit:]

        if self._gate_available():
            try:
                result = self._fetch_gate(symbol, interval, requested_limit)
                candles = result.get("candles", []) if isinstance(result, dict) else []
                if candles and len(candles) >= 3:
                    self.cache[cache_key] = (candles, time.time())
                    self._update_shared(symbol, interval, candles)
                    self._record_health(
                        symbol, interval, True, source="Gate SMC adapter",
                        candle_count=len(candles),
                        last_closed_candle_at=self._last_closed_at(candles),
                    )
                    return candles[-requested_limit:]
                if isinstance(result, dict) and result.get("error"):
                    errors.append(f"SMC adapter: {result['error']}")
            except Exception as exc:
                errors.append(f"SMC adapter: {type(exc).__name__}: {exc}")
                logging.debug("SMC Gate candles %s %s: %s", symbol, interval, exc)

        reason = " | ".join(errors[-2:]) or "Gate returned no usable candles"
        self._record_health(
            symbol, interval, False, source="Gate", reason=reason, candle_count=0,
        )
        logging.debug("Нет Gate Futures свечей для %s %s", symbol, interval)
        return []

    async def fetch_candles_batch(
        self, symbols: list, timeframe: str = "4h", limit: int = 100,
    ) -> dict:
        async def fetch_one(symbol):
            try:
                snapshot_rows = snapshot_candle_override(
                    symbol, timeframe, limit,
                )
                if snapshot_rows is not None:
                    return symbol, snapshot_rows
                candles = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: self.get_candles(symbol, timeframe, limit),
                )
                return symbol, candles
            except Exception:
                return symbol, []

        results = await asyncio.gather(
            *(fetch_one(symbol) for symbol in symbols), return_exceptions=True,
        )
        return {
            result[0]: result[1]
            for result in results
            if isinstance(result, tuple) and len(result) == 2 and result[1]
        }


__all__ = ["GateCandleRouter"]
