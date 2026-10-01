"""Legacy market-regime v2 projection outside the market monolith."""

from __future__ import annotations

from collections.abc import Callable
from statistics import median

from .indicators import average_true_range, ema_value, true_ranges
from .runtime_cache import get_confirmed_candles


class LegacyRegimeV2:
    def __init__(self, get_candles: Callable[[str, str, int], list]):
        self._get_candles = get_candles

    def detect(self, symbol: str) -> dict:
        """Return the legacy regime label and compatible strategy allowlist."""
        try:
            candles = get_confirmed_candles(self._get_candles(symbol, "4h", 51))
            if not candles or len(candles) < 50:
                return {"type": "unknown", "enabled": ["MTF", "ZONE"]}

            closes = [candle["close"] for candle in candles]
            highs = [candle["high"] for candle in candles]
            lows = [candle["low"] for candle in candles]

            ema_50 = ema_value(closes, 50)
            ema_20 = ema_value(closes, 20)
            price = closes[-1]

            atr_now = average_true_range(candles, 7)
            atr_median = median(true_ranges(candles)[-20:])
            volatile = atr_now > atr_median * 1.2

            current_range = max(highs[-5:]) - min(lows[-5:])
            previous_range = max(highs[-20:-5]) - min(lows[-20:-5])
            compressed = current_range < previous_range * 0.5

            peak = max(highs[-40:]) if len(highs) >= 40 else max(highs)
            drawdown = (peak - price) / peak * 100

            if compressed and drawdown >= 5:
                return {
                    "type": "accumulation",
                    "enabled": ["WYCKOFF", "ZONE"],
                }
            if (
                (price > ema_50 and ema_20 > ema_50)
                or (price < ema_50 and ema_20 < ema_50)
            ):
                if volatile:
                    return {
                        "type": "trend",
                        "enabled": ["MTF", "FAST", "SWING"],
                    }
                return {"type": "trend_slow", "enabled": ["MTF", "ZONE"]}
            return {"type": "range", "enabled": ["SWING", "ZONE"]}
        except Exception:
            return {
                "type": "unknown",
                "enabled": ["MTF", "ZONE", "SWING"],
            }


__all__ = ["LegacyRegimeV2"]
