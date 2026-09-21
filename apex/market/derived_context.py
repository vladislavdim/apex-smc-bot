"""Legacy candle-derived context kept outside the market compatibility monolith."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable


class LegacyDerivedContext:
    def __init__(self, get_candles: Callable[[str, str, int], list]):
        self._get_candles = get_candles
        self._higher_timeframe_cache: dict[str, dict] = {}
        self._higher_timeframe_cache_time: dict[str, float] = {}
        self._regime_cache: dict[str, dict] = {}
        self._regime_cache_time: dict[str, float] = {}

    def get_higher_tf_context(self, symbol: str) -> dict:
        if (
            time.time() - self._higher_timeframe_cache_time.get(symbol, 0) < 7200
            and symbol in self._higher_timeframe_cache
        ):
            return self._higher_timeframe_cache[symbol]
        try:
            daily = self._get_candles(symbol, "1d", 14)
            if len(daily) < 7:
                return {"trend": "UNKNOWN", "near_resistance": False, "note": ""}
            closes = [candle["close"] for candle in daily]
            price_now = closes[-1]
            weekly_change = (price_now - closes[-7]) / closes[-7] * 100
            resistance = max(candle["high"] for candle in daily)
            support = min(candle["low"] for candle in daily)
            distance_to_resistance = (resistance - price_now) / price_now * 100
            distance_to_support = (price_now - support) / price_now * 100
            trend = (
                "BULLISH" if weekly_change > 3
                else "BEARISH" if weekly_change < -3 else "NEUTRAL"
            )
            result = {
                "trend": trend,
                "weekly_change": round(weekly_change, 1),
                "dist_to_resistance": round(distance_to_resistance, 1),
                "dist_to_support": round(distance_to_support, 1),
                "near_resistance": distance_to_resistance < 2.0,
                "near_support": distance_to_support < 2.0,
                "note": f"Нед: {trend} ({weekly_change:+.1f}%)",
            }
            self._higher_timeframe_cache[symbol] = result
            self._higher_timeframe_cache_time[symbol] = time.time()
            return result
        except Exception as exc:
            logging.debug("HTF context %s: %s", symbol, exc)
            return {"trend": "UNKNOWN", "near_resistance": False, "note": ""}

    def get_market_regime(self, symbol: str) -> dict:
        now = time.time()
        if (
            symbol in self._regime_cache
            and now - self._regime_cache_time.get(symbol, 0) < 1800
        ):
            return self._regime_cache[symbol]
        try:
            candles = self._get_candles(symbol, "1h", 50)
            if len(candles) < 20:
                return {"mode": "UNKNOWN", "direction": "NONE", "confidence": 0}
            closes = [candle["close"] for candle in candles]
            highs = [candle["high"] for candle in candles]
            lows = [candle["low"] for candle in candles]
            ranges = [highs[index] - lows[index] for index in range(len(candles))]
            average_atr = sum(ranges[-14:]) / 14
            atr_percent = average_atr / closes[-1] * 100
            average_20 = sum(closes[-20:]) / 20
            std_20 = (
                sum((value - average_20) ** 2 for value in closes[-20:]) / 20
            ) ** 0.5
            bb_width = std_20 * 4 / average_20 * 100
            ema_9 = sum(closes[-9:]) / 9
            ema_21 = sum(closes[-21:]) / 21
            direction = "BULLISH" if ema_9 > ema_21 else "BEARISH"
            streak = 1
            for index in range(len(candles) - 2, max(len(candles) - 8, 0), -1):
                if (
                    candles[index]["close"] > candles[index]["open"]
                ) == (candles[-1]["close"] > candles[-1]["open"]):
                    streak += 1
                else:
                    break
            if bb_width < 3 and atr_percent < 1.5:
                mode, confidence = "SIDEWAYS", 80
            elif bb_width > 6 or atr_percent > 3:
                mode, confidence = "VOLATILE", 70
            elif streak >= 3:
                mode, confidence = "TRENDING", 75
            else:
                mode, confidence = "TRENDING", 50
            result = {
                "mode": mode, "direction": direction, "confidence": confidence,
                "bb_width": round(bb_width, 2), "atr_pct": round(atr_percent, 2),
            }
            self._regime_cache[symbol] = result
            self._regime_cache_time[symbol] = now
            return result
        except Exception:
            return {"mode": "UNKNOWN", "direction": "NONE", "confidence": 0}


__all__ = ["LegacyDerivedContext"]
