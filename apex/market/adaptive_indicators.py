"""Cached, versioned Wilder indicators for production strategy adapters."""

from __future__ import annotations

import logging
import threading
import time
from statistics import median

from .indicators import INDICATOR_VERSION, average_true_range, average_directional_index, true_ranges
from collections.abc import Callable

from .snapshot_scope import snapshot_scope_active
from .runtime_cache import get_confirmed_candles


class LegacyAdaptiveIndicators:
    """Keep the adapter API while using validated Wilder ATR/ADX calculations."""

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
        scoped = snapshot_scope_active()
        with self._lock:
            cached = self._indicator_cache.get(key)
            if not scoped and cached and now - cached[0] < self._indicator_ttl:
                return cached[1]

        result = {}
        try:
            # EMA200 needs 200 closed observations plus the venue's open tail.
            candles = get_confirmed_candles(self._get_candles(symbol, timeframe, 201))
            if not candles or len(candles) < 28:
                return result
            closes = [candle["close"] for candle in candles]

            result["indicator_version"] = INDICATOR_VERSION
            result["atr"] = average_true_range(candles, 14)
            result["atr_med"] = median(true_ranges(candles)[-50:])

            for period in (20, 50, 200):
                ema = self._ema_value(closes, period)
                if ema is not None:
                    result[f"ema{period}"] = ema

            result["volatility_factor"] = round(
                max(0.6, min(1.8, result["atr"] / result["atr_med"] if result["atr_med"] > 0 else 1.0)), 2
            )
            result["adx"] = average_directional_index(candles, 14)

            if len(candles) >= 21:
                result["avg_vol"] = sum(candle["volume"] for candle in candles[-21:-1]) / 20
            else:
                result["avg_vol"] = sum(candle["volume"] for candle in candles) / len(candles)
            result["price"] = closes[-1]
            result["hh_hl"] = len(closes) >= 10 and closes[-1] > closes[-5] > closes[-10]
            result["ll_lh"] = len(closes) >= 10 and closes[-1] < closes[-5] < closes[-10]
            result["trend_strength"] = "strong" if result["adx"] > 30 else "normal" if result["adx"] > 20 else "weak"
            result["adx_strong"] = result["adx"] > 30
            result["adx_weak"] = result["adx"] < 20
        except Exception as exc:
            result = {}
            logging.warning("get_precomputed_indicators %s: %s", symbol, exc)

        if not scoped:
            with self._lock:
                self._indicator_cache[key] = (now, result)
        return result

    def get_adaptive_params(self, symbol: str, candles: list | None = None, timeframe: str = "4h") -> dict:
        """Project adaptive flags; missing readings remain explicitly unknown."""
        del candles
        key = f"{symbol}:{timeframe}"
        now = time.time()
        scoped = snapshot_scope_active()
        cached = self._adaptive_cache.get(key)
        if not scoped and cached and now - cached[0] < self._adaptive_ttl:
            return cached[1]

        result = {"volatility_factor": 1.0, "adx": None, "adx_strong": False, "adx_weak": False,
                  "indicator_version": INDICATOR_VERSION, "indicators_ready": False}
        try:
            indicators = self.get_precomputed_indicators(symbol, timeframe)
            if indicators:
                result["volatility_factor"] = indicators.get("volatility_factor", 1.0)
                result["adx"] = indicators.get("adx")
                result["indicators_ready"] = indicators.get("adx") is not None
                result["adx_strong"] = indicators.get("adx_strong", False)
                result["adx_weak"] = indicators.get("adx_weak", False)
        except Exception as exc:
            logging.debug("get_adaptive_params %s: %s", symbol, exc)
        if not scoped:
            self._adaptive_cache[key] = (now, result)
        return result


__all__ = ["LegacyAdaptiveIndicators"]
