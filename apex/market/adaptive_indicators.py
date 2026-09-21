"""Cached legacy indicators used by the production strategy adapters."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable


class LegacyAdaptiveIndicators:
    """Preserve legacy ATR/ADX/EMA calculations behind injected providers."""

    def __init__(self, get_candles: Callable, ema_value: Callable, *, indicator_ttl: int = 60, adaptive_ttl: int = 300):
        self._get_candles = get_candles
        self._ema_value = ema_value
        self._indicator_ttl = indicator_ttl
        self._adaptive_ttl = adaptive_ttl
        self._lock = threading.Lock()
        self._indicator_cache: dict[str, tuple[float, dict]] = {}
        self._adaptive_cache: dict[str, tuple[float, dict]] = {}

    def get_precomputed_indicators(self, symbol: str, timeframe: str = "4h") -> dict:
        """Calculate ATR, ADX and EMA once per indicator TTL."""
        key = f"{symbol}:{timeframe}"
        now = time.time()
        with self._lock:
            cached = self._indicator_cache.get(key)
            if cached and now - cached[0] < self._indicator_ttl:
                return cached[1]

        result = {}
        try:
            candles = self._get_candles(symbol, timeframe, 100)
            if not candles or len(candles) < 20:
                return result
            closes = [candle["close"] for candle in candles]
            highs = [candle["high"] for candle in candles]
            lows = [candle["low"] for candle in candles]

            period_14 = min(14, len(candles))
            result["atr"] = sum(highs[-i] - lows[-i] for i in range(1, period_14 + 1)) / period_14
            period_50 = min(50, len(candles))
            result["atr_med"] = sum(highs[-i] - lows[-i] for i in range(1, period_50 + 1)) / period_50

            for period in (20, 50, 200):
                ema = self._ema_value(closes, period)
                if ema is not None:
                    result[f"ema{period}"] = ema

            result["volatility_factor"] = round(
                max(0.6, min(1.8, result["atr"] / result["atr_med"] if result["atr_med"] > 0 else 1.0)), 2
            )
            try:
                adx_period = min(14, len(candles) - 1)
                plus_dm = minus_dm = true_range_sum = 0
                for i in range(1, adx_period + 1):
                    high_diff = highs[-i] - highs[-i - 1]
                    low_diff = lows[-i - 1] - lows[-i]
                    plus_dm += high_diff if high_diff > low_diff and high_diff > 0 else 0
                    minus_dm += low_diff if low_diff > high_diff and low_diff > 0 else 0
                    true_range_sum += max(
                        highs[-i] - lows[-i],
                        abs(highs[-i] - closes[-i - 1]),
                        abs(lows[-i] - closes[-i - 1]),
                    )
                atr_14 = true_range_sum / adx_period if adx_period > 0 else 1
                positive_di = (plus_dm / adx_period) / atr_14 * 100 if atr_14 > 0 else 0
                negative_di = (minus_dm / adx_period) / atr_14 * 100 if atr_14 > 0 else 0
                di_sum = positive_di + negative_di
                result["adx"] = round(abs(positive_di - negative_di) / di_sum * 100, 1) if di_sum > 0 else 20
            except Exception:
                result["adx"] = 20

            if len(candles) >= 21:
                result["avg_vol"] = sum(candle["volume"] for candle in candles[-20:-1]) / 19
            else:
                result["avg_vol"] = sum(candle["volume"] for candle in candles) / len(candles)
            result["price"] = closes[-1]
            result["hh_hl"] = len(closes) >= 10 and closes[-1] > closes[-5] > closes[-10]
            result["ll_lh"] = len(closes) >= 10 and closes[-1] < closes[-5] < closes[-10]
            result["trend_strength"] = "strong" if result["adx"] > 30 else "normal" if result["adx"] > 20 else "weak"
            result["adx_strong"] = result["adx"] > 30
            result["adx_weak"] = result["adx"] < 20
        except Exception as exc:
            logging.warning("get_precomputed_indicators %s: %s", symbol, exc)

        with self._lock:
            self._indicator_cache[key] = (now, result)
        return result

    def get_adaptive_params(self, symbol: str, candles: list | None = None, timeframe: str = "4h") -> dict:
        """Return the legacy adaptive parameter projection."""
        del candles
        key = f"{symbol}:{timeframe}"
        now = time.time()
        cached = self._adaptive_cache.get(key)
        if cached and now - cached[0] < self._adaptive_ttl:
            return cached[1]

        result = {"volatility_factor": 1.0, "adx": 25.0, "adx_strong": False, "adx_weak": False}
        try:
            indicators = self.get_precomputed_indicators(symbol, timeframe)
            if indicators:
                result["volatility_factor"] = indicators.get("volatility_factor", 1.0)
                result["adx"] = indicators.get("adx", 25.0)
                result["adx_strong"] = indicators.get("adx_strong", False)
                result["adx_weak"] = indicators.get("adx_weak", False)
        except Exception as exc:
            logging.debug("get_adaptive_params %s: %s", symbol, exc)
        self._adaptive_cache[key] = (now, result)
        return result


__all__ = ["LegacyAdaptiveIndicators"]
