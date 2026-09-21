"""Read-only legacy historical context and presentation formatter."""

from __future__ import annotations

import logging
from collections.abc import Callable


class HistoricalContextProvider:
    def __init__(self, get_candles: Callable[[str, str, int], list]):
        self._get_candles = get_candles

    def get_historical_context(self, symbol: str, timeframe: str = "1d") -> dict | None:
        try:
            candles = self._get_candles(symbol, "1d", 200)
            if len(candles) < 30:
                candles = self._get_candles(symbol, "4h", 200)
            if len(candles) < 20:
                return None
            closes = [candle["close"] for candle in candles]
            highs = [candle["high"] for candle in candles]
            lows = [candle["low"] for candle in candles]
            current = closes[-1]
            period_high = max(highs)
            period_low = min(lows)
            percent_from_high = round((current - period_high) / period_high * 100, 1)
            percent_from_low = round((current - period_low) / period_low * 100, 1)
            if len(closes) >= 50:
                average_50 = sum(closes[-50:]) / 50
                average_20 = sum(closes[-20:]) / 20
                average_10 = sum(closes[-10:]) / 10
            else:
                average_50 = average_20 = average_10 = current
            if average_10 > average_20 > average_50:
                trend, trend_key = "ВОСХОДЯЩИЙ ↗️", "uptrend"
            elif average_10 < average_20 < average_50:
                trend, trend_key = "НИСХОДЯЩИЙ ↘️", "downtrend"
            elif abs(average_10 - average_50) / average_50 * 100 < 3:
                trend, trend_key = "БОКОВИК ↔️", "sideways"
            else:
                trend, trend_key = "ПЕРЕХОДНЫЙ ⚡️", "transition"
            price_range = period_high - period_low
            zone_size = price_range / 10
            zones: dict[int, int] = {}
            for candle in candles:
                zone = int((candle["close"] - period_low) / zone_size)
                zone = max(0, min(9, zone))
                zones[zone] = zones.get(zone, 0) + 1
            top_zones = sorted(zones.items(), key=lambda item: item[1], reverse=True)[:3]
            key_levels = [
                round(
                    period_low + zone * zone_size + zone_size / 2,
                    4 if current < 10 else 2,
                )
                for zone, _count in top_zones
            ]
            key_levels.sort()
            support = max(
                [level for level in key_levels if level < current],
                default=period_low,
            )
            resistance = min(
                [level for level in key_levels if level > current],
                default=period_high,
            )
            if percent_from_high > -10:
                phase, phase_key = "📈 У ХАЁВ — возможен разворот", "near_high"
            elif percent_from_high > -30:
                phase, phase_key = "💪 СИЛЬНАЯ ЗОНА — выше середины", "strong"
            elif percent_from_high > -60:
                phase, phase_key = "⚖️ СРЕДНЯЯ ЗОНА — середина диапазона", "middle"
            elif percent_from_high > -80:
                phase, phase_key = "🔍 ЗОНА НАКОПЛЕНИЯ — возможен разворот вверх", "accumulation"
            else:
                phase, phase_key = "💎 ГЛУБОКИЙ ЛОУ — экстремальное значение", "deep_low"
            change_5 = round((closes[-1] - closes[-5]) / closes[-5] * 100, 2)
            change_20 = round((closes[-1] - closes[-20]) / closes[-20] * 100, 2)
            return {
                "current": current, "period_high": period_high,
                "period_low": period_low, "pct_from_ath": percent_from_high,
                "pct_from_atl": percent_from_low, "trend": trend,
                "trend_key": trend_key, "phase": phase, "phase_key": phase_key,
                "support": support, "resistance": resistance,
                "key_levels": key_levels, "change_5": change_5,
                "change_20": change_20, "candles_count": len(candles),
            }
        except Exception as exc:
            logging.warning("get_historical_context %s: %s", symbol, exc)
            return None


def format_historical_context(symbol: str, historical: dict | None) -> str:
    if not historical:
        return ""

    def format_price(value):
        if value < 1:
            return f"${value:,.4f}"
        if value < 100:
            return f"${value:,.3f}"
        return f"${value:,.2f}"

    return (
        f"📈 <b>Исторический контекст ({historical['candles_count']} свечей):</b>\n"
        f"🏔 Хай периода: <code>{format_price(historical['period_high'])}</code> "
        f"({historical['pct_from_ath']:+.1f}% от него)\n"
        f"🏔 Лоу периода: <code>{format_price(historical['period_low'])}</code> "
        f"(+{historical['pct_from_atl']:.1f}% от него)\n"
        f"📊 Тренд: <b>{historical['trend']}</b>\n"
        f"🎯 Фаза: {historical['phase']}\n"
        f"🛡 Ближ. поддержка: <code>{format_price(historical['support'])}</code>\n"
        f"⚡️ Ближ. сопротивление: <code>{format_price(historical['resistance'])}</code>\n"
        f"📉 Изм. за 5 свечей: {historical['change_5']:+.2f}% | "
        f"за 20: {historical['change_20']:+.2f}%"
    )


__all__ = ["HistoricalContextProvider", "format_historical_context"]
