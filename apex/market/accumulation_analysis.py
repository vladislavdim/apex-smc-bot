"""Legacy accumulation advisory analysis outside the market monolith."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable


class AccumulationAnalysis:
    def __init__(
        self,
        get_candles: Callable[[str, str, int], list],
        get_orderbook: Callable[[str], dict | None],
        ask_groq: Callable[..., str | None],
    ):
        self._get_candles = get_candles
        self._get_orderbook = get_orderbook
        self._ask_groq = ask_groq

    def detect(self, symbol: str) -> dict | None:
        """Return the legacy Wyckoff/volume accumulation advisory."""
        try:
            candles_1h = self._get_candles(symbol, "1h", 48)
            candles_15m = self._get_candles(symbol, "15m", 96)
            if len(candles_1h) < 24 or len(candles_15m) < 48:
                return None

            score = 0
            signals = []
            last_12 = candles_1h[-12:]
            high_max = max(candle["high"] for candle in last_12)
            low_min = min(candle["low"] for candle in last_12)
            price_now = candles_1h[-1]["close"]
            range_percent = (
                (high_max - low_min) / low_min * 100 if low_min > 0 else 0
            )
            if range_percent < 5:
                score += 25
                signals.append(
                    f"✅ Боковик {range_percent:.1f}% за 12ч (накопление)"
                )
            elif range_percent < 8:
                score += 15
                signals.append(
                    f"⚡️ Диапазон {range_percent:.1f}% за 12ч (сжатие)"
                )

            all_volumes = [candle["volume"] for candle in candles_1h[:-3]]
            average_volume = (
                sum(all_volumes) / len(all_volumes) if all_volumes else 1
            )
            recent_volumes = [candle["volume"] for candle in candles_1h[-3:]]
            recent_average = (
                sum(recent_volumes) / len(recent_volumes)
                if recent_volumes else 0
            )
            volume_ratio = (
                recent_average / average_volume if average_volume > 0 else 1
            )
            if volume_ratio < 0.6:
                score += 20
                signals.append(
                    f"✅ Объём в {1 / volume_ratio:.1f}x ниже среднего "
                    "(тихое накопление)"
                )
            elif volume_ratio > 2.0:
                score += 20
                signals.append(
                    f"🔥 Всплеск объёма x{volume_ratio:.1f} (кит заходит!)"
                )

            small_candles = 0
            for candle in last_12:
                body = abs(candle["close"] - candle["open"])
                full_range = (
                    candle["high"] - candle["low"]
                    if candle["high"] != candle["low"] else 0.001
                )
                if body / full_range < 0.3:
                    small_candles += 1
            if small_candles >= 7:
                score += 20
                signals.append(
                    f"✅ {small_candles}/12 свечей с маленьким телом (боковик)"
                )

            orderbook = self._get_orderbook(symbol)
            if orderbook:
                bid_ask_ratio = (
                    orderbook["bids"] / orderbook["asks"]
                    if orderbook["asks"] > 0 else 1
                )
                if bid_ask_ratio > 1.5:
                    score += 20
                    signals.append(
                        f"✅ Биды x{bid_ask_ratio:.1f} больше асков (кит покупает)"
                    )
                elif bid_ask_ratio > 1.2:
                    score += 10
                    signals.append(
                        f"⚡️ Биды немного давят (bid/ask {bid_ask_ratio:.1f})"
                    )

            closes = [candle["close"] for candle in candles_1h[-20:]]
            average_close = sum(closes) / len(closes)
            standard_deviation = (
                sum((value - average_close) ** 2 for value in closes)
                / len(closes)
            ) ** 0.5
            bb_width = standard_deviation * 2 / average_close * 100
            if bb_width < 3:
                score += 15
                signals.append(
                    f"✅ BB сжатие {bb_width:.1f}% (взрыв близко!)"
                )
            elif bb_width < 5:
                score += 8
                signals.append(f"⚡️ BB ширина {bb_width:.1f}% (сжимается)")

            if not any("Биды" in signal for signal in signals):
                score = max(0, score - 15)
            if not signals or score < 55:
                return None

            pump_target = None
            pump_target_percent = None
            pump_logic = ""
            try:
                atr_1h = sum(
                    candle["high"] - candle["low"] for candle in candles_1h[-14:]
                ) / 14
                highs = ", ".join(str(round(candle["high"], 6)) for candle in candles_1h[-12:])
                lows = ", ".join(str(round(candle["low"], 6)) for candle in candles_1h[-12:])
                prompt = (
                    "Ты трейдер SMC. Анализируй накопление после боковика и дай реальную "
                    "цель памп минимум +10% от цены. Ответь СТРОГО JSON:\n"
                    '{"target": число_цены, "target_pct": процент_роста_число, "logic": "причина макс 10 слов"}\n\n'
                    f"Пара: {symbol}\nЦена сейчас: {price_now}\n"
                    f"Диапазон боковика: {round(low_min, 6)} — {round(high_max, 6)} ({range_percent:.1f}%)\n"
                    f"ATR: {round(atr_1h, 6)}\nBB ширина: {bb_width:.1f}%\n"
                    f"Объём ratio: {volume_ratio:.2f}\nМаксимумы 12ч: {highs}\n"
                    f"Минимумы 12ч: {lows}\nПризнаки: {'; '.join(signals)}"
                )
                response = self._ask_groq(prompt, max_tokens=100)
                if response and len(response) > 5:
                    clean = response.strip().replace("```json", "").replace("```", "").strip()
                    match = re.search(r"\{[^}]+\}", clean, re.DOTALL)
                    if match:
                        clean = match.group()
                    parsed = json.loads(clean)
                    if parsed.get("target") and float(parsed["target"]) > price_now:
                        pump_target = float(parsed["target"])
                        pump_target_percent = float(parsed.get(
                            "target_pct",
                            round((pump_target - price_now) / price_now * 100, 1),
                        ))
                    if parsed.get("logic"):
                        pump_logic = str(parsed["logic"]).strip()
            except Exception as exc:
                logging.debug("[AccumGroq] %s: %s", symbol, exc)
                try:
                    fallback_atr = sum(
                        candle["high"] - candle["low"] for candle in candles_1h[-14:]
                    ) / 14
                    pump_target = round(high_max + fallback_atr * 2, 6)
                    pump_target_percent = round(
                        (pump_target - price_now) / price_now * 100, 1
                    )
                    pump_logic = "верхняя граница + ATR×2"
                except Exception:
                    pass

            return {
                "symbol": symbol,
                "score": min(score, 100),
                "price": price_now,
                "range_pct": range_percent,
                "vol_ratio": volume_ratio,
                "bb_width": bb_width,
                "signals": signals,
                "pump_target": pump_target,
                "pump_target_pct": pump_target_percent,
                "pump_logic": pump_logic,
                "high_max": high_max,
                "low_min": low_min,
            }
        except Exception as exc:
            logging.error("Accumulation detect error %s: %s", symbol, exc)
            return None


__all__ = ["AccumulationAnalysis"]
