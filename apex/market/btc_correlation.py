"""Legacy BTC correlation context behind injected candle providers."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .snapshot_scope import snapshot_scope_active


class BtcCorrelationProvider:
    def __init__(
        self,
        get_candles: Callable[[str, str, int], list],
        get_shared_candles: Callable[[str, str], list],
        *,
        cache_ttl: int = 300,
    ):
        self._get_candles = get_candles
        self._get_shared_candles = get_shared_candles
        self._cache_ttl = cache_ttl
        self._cache: dict[str, tuple[float, dict]] = {}

    def get(
        self,
        symbol: str,
        btc_candles: list | None = None,
        period: int = 20,
    ) -> dict:
        """Return rolling close-return correlation with BTC."""
        now = time.time()
        scoped = snapshot_scope_active()
        cached = self._cache.get(symbol)
        if not scoped and cached and now - cached[0] < self._cache_ttl:
            return cached[1]

        try:
            if symbol == "BTCUSDT":
                result = {
                    "corr": 1.0,
                    "level": "high",
                    "btc_dir": "BULLISH",
                    "desc": "BTC itself",
                }
                if not scoped:
                    self._cache[symbol] = (now, result)
                return result

            if btc_candles is None:
                btc_candles = self._get_shared_candles("BTCUSDT", "4h")
                if not btc_candles:
                    btc_candles = self._get_candles(
                        "BTCUSDT", "4h", period + 5,
                    )

            alt_candles = self._get_candles(symbol, "4h", period + 5)
            if (
                not alt_candles
                or not btc_candles
                or len(alt_candles) < period
                or len(btc_candles) < period
            ):
                return self._fallback("нет данных")

            alt_returns = [
                alt_candles[-index]["close"]
                / alt_candles[-index - 1]["close"] - 1
                for index in range(1, period + 1)
            ]
            btc_returns = [
                btc_candles[-index]["close"]
                / btc_candles[-index - 1]["close"] - 1
                for index in range(1, period + 1)
            ]
            count = len(alt_returns)
            alt_mean = sum(alt_returns) / count
            btc_mean = sum(btc_returns) / count
            covariance = sum(
                (alt_returns[index] - alt_mean)
                * (btc_returns[index] - btc_mean)
                for index in range(count)
            ) / count
            alt_std = (
                sum((value - alt_mean) ** 2 for value in alt_returns) / count
            ) ** 0.5
            btc_std = (
                sum((value - btc_mean) ** 2 for value in btc_returns) / count
            ) ** 0.5
            correlation = (
                round(covariance / (alt_std * btc_std), 3)
                if alt_std > 0 and btc_std > 0 else 0.7
            )
            btc_direction = (
                "BULLISH"
                if btc_candles[-1]["close"] > btc_candles[-5]["close"]
                else "BEARISH"
            )
            level = (
                "high" if correlation > 0.85
                else "moderate" if correlation > 0.3 else "low"
            )
            result = {
                "corr": correlation,
                "level": level,
                "btc_dir": btc_direction,
                "desc": f"Корр. BTC: {correlation} ({level})",
            }
            if not scoped:
                self._cache[symbol] = (now, result)
            return result
        except Exception as exc:
            logging.warning("get_btc_correlation %s: %s", symbol, exc)
            return self._fallback("ошибка")

    @staticmethod
    def _fallback(description: str) -> dict:
        return {
            "corr": 0.7,
            "level": "moderate",
            "btc_dir": "UNKNOWN",
            "desc": description,
        }


__all__ = ["BtcCorrelationProvider"]
