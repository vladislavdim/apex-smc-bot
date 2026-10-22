"""Read-only live position analysis for Telegram presentation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Mapping


@dataclass(frozen=True)
class LivePositionService:
    candles: Callable
    find_swings: Callable
    classify_swings: Callable
    detect_events: Callable
    find_ob: Callable
    find_fvg: Callable
    timeframe_labels: Mapping[str, str]

    def analyze(self, symbol: str, timeframe: str = "1h"):
        """Render the legacy read-only market analysis without execution access."""
        try:
            candles = self.candles(symbol, timeframe, 200)
            if len(candles) < 30:
                return None
            price_now = candles[-1]["close"]
            price_open = candles[-1]["open"]
            candle_dir = "🟢" if price_now >= price_open else "🔴"
            highs, lows = self.find_swings(candles, lookback=5)
            classified = self.classify_swings(highs, lows)
            events = self.detect_events(candles, classified)
            trend = events[0]["direction"] if events else "UNCLEAR"
            h_vals = sorted([h[1] for h in highs[-6:]], reverse=True) if highs else []
            l_vals = sorted([l[1] for l in lows[-6:]]) if lows else []
            nearest_res = min(
                [h for h in h_vals if h > price_now * 1.002], default=None,
            )
            nearest_sup = max(
                [low for low in l_vals if low < price_now * 0.998], default=None,
            )
            ob_bull = self.find_ob(candles, "BULLISH")
            ob_bear = self.find_ob(candles, "BEARISH")
            fvg_bull = self.find_fvg(candles, "BULLISH")
            fvg_bear = self.find_fvg(candles, "BEARISH")
            in_bull_ob = ob_bull and ob_bull["bottom"] <= price_now <= ob_bull["top"]
            in_bear_ob = ob_bear and ob_bear["bottom"] <= price_now <= ob_bear["top"]
            in_bull_fvg = fvg_bull and fvg_bull["bottom"] <= price_now <= fvg_bull["top"]
            in_bear_fvg = fvg_bear and fvg_bear["bottom"] <= price_now <= fvg_bear["top"]
            last3 = candles[-3:]
            bulls = sum(1 for candle in last3 if candle["close"] > candle["open"])
            momentum = "🚀 растём" if bulls >= 2 else "💥 падаем" if bulls == 0 else "😐 боковик"
            volumes = [candle["volume"] for candle in candles[-20:]]
            average_volume = sum(volumes[:-1]) / max(len(volumes) - 1, 1)
            volume_ratio = candles[-1]["volume"] / average_volume if average_volume > 0 else 1
            volume_tag = "🔥 высокий" if volume_ratio > 1.5 else "📉 низкий" if volume_ratio < 0.6 else "➡️ средний"
            distance_resistance = ((nearest_res - price_now) / price_now * 100) if nearest_res else None
            distance_support = ((price_now - nearest_sup) / price_now * 100) if nearest_sup else None

            def fmt(price):
                if price is None:
                    return "—"
                return f"${price:,.4f}" if price < 1 else f"${price:,.3f}" if price < 10 else f"${price:,.2f}"

            lines = [
                f"📍 <b>{symbol}</b> [{self.timeframe_labels.get(timeframe, timeframe)}] — СЕЙЧАС",
                f"{'━' * 26}",
                f"{candle_dir} Цена: <code>{fmt(price_now)}</code>",
                f"⚡️ {momentum}  |  📊 Объём: {volume_tag} (×{volume_ratio:.1f})",
                "",
                f"📐 Структура:  {'🟢' if trend == 'BULLISH' else '🔴' if trend == 'BEARISH' else '⚪️'} <b>{trend}</b>",
            ]
            if nearest_res:
                lines.append(f"🔴 Сопротивление: <code>{fmt(nearest_res)}</code> (+{distance_resistance:.1f}%)")
            if nearest_sup:
                lines.append(f"🟢 Поддержка:     <code>{fmt(nearest_sup)}</code> (-{distance_support:.1f}%)")
            lines.append("\n<b>🗺 Зоны:</b>")
            if ob_bull:
                lines.append(f"🟦 Bull OB: <code>{fmt(ob_bull['bottom'])}–{fmt(ob_bull['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bull_ob else ""))
            if ob_bear:
                lines.append(f"🟥 Bear OB: <code>{fmt(ob_bear['bottom'])}–{fmt(ob_bear['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bear_ob else ""))
            if fvg_bull:
                lines.append(f"🔵 Bull FVG: <code>{fmt(fvg_bull['bottom'])}–{fmt(fvg_bull['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bull_fvg else ""))
            if fvg_bear:
                lines.append(f"🟠 Bear FVG: <code>{fmt(fvg_bear['bottom'])}–{fmt(fvg_bear['top'])}</code>" + (" ← ТЫ ЗДЕСЬ" if in_bear_fvg else ""))
            lines += ["\nНаблюдение за структурой и зонами.",
                      "Это не торговый сигнал. Для входа требуется проверенный сигнал APEX."]
            return "\n".join(lines)
        except Exception as error:
            logging.error("live_position_analysis %s: %s", symbol, error)
            return None


_service: LivePositionService | None = None


def configure_live_position_service(service: LivePositionService | None) -> None:
    global _service
    _service = service


def live_position_analysis(symbol: str, timeframe: str = "1h"):
    service = _service
    return service.analyze(symbol, timeframe) if service is not None else None


__all__ = ["LivePositionService", "configure_live_position_service", "live_position_analysis"]
