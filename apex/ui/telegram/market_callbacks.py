"""Navigation-only Telegram callbacks for market analysis views."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, MutableMapping


@dataclass(frozen=True)
class MarketNavigationDependencies:
    edit_message: Callable[..., Any]
    edit_markup: Callable[..., Any]
    pairs_keyboard: Callable[[str, int], Any]
    timeframe_keyboard: Callable[[], Any]
    live_timeframe_keyboard: Callable[[], Any]
    live_position_analysis: Callable[[str, str], Any]
    get_top_pairs: Callable[[int], list[str]]
    full_scan: Callable[..., Any]
    scan_diagnostics: Callable[[str], str]
    get_user_memory: Callable[[int], dict[str, Any]]
    calculate_risk: Callable[..., Any]
    get_crypto_news: Callable[[], list[dict[str, Any]]]
    get_market_news: Callable[[], list[dict[str, Any]]]
    format_news: Callable[[list[dict[str, Any]]], str]
    ask_groq: Callable[..., Any]
    save_news: Callable[..., Any]
    detect_accumulation: Callable[[str], Any]
    scan_all_deals: Callable[[int], list[dict[str, Any]]]
    get_fear_greed: Callable[[], Any]
    get_dxy_signal: Callable[[], Any]
    get_market_regime: Callable[[str], Any]
    get_upcoming_events: Callable[[], Any]
    get_candles: Callable[[str, str, int], Any]
    universe_size: int
    user_states: MutableMapping[int, dict[str, Any]]
    timeframe_labels: dict[str, str]
    button: Callable[..., Any]
    markup: Callable[..., Any]


class MarketNavigationCallbacks:
    """Own market navigation without strategy, execution, or Manager authority."""

    def __init__(self, dependencies: MarketNavigationDependencies) -> None:
        self.dependencies = dependencies

    async def handle(self, callback: Any) -> bool:
        data = str(callback.data or "")
        if data == "menu_market":
            await self._market_summary(callback)
            return True
        if data == "menu_scan":
            await self._scan_menu(callback)
            return True
        if data.startswith("pairs_"):
            await self._pairs(callback, data)
            return True
        if data == "noop":
            return True
        if data == "menu_tf":
            await self._timeframe_menu(callback)
            return True
        if data == "menu_live_select":
            await self._live_menu(callback)
            return True
        if data.startswith("live_"):
            await self._live(callback, data)
            return True
        if data.startswith("tf_"):
            await self._timeframe_scan(callback, data)
            return True
        if data.startswith("scan_"):
            await self._symbol_scan(callback, data)
            return True
        if data == "menu_news":
            await self._news(callback)
            return True
        if data == "menu_pump":
            await self._pump(callback)
            return True
        if data in {"menu_find_deals", "menu_find_deals_refresh"}:
            await self._find_deals(callback)
            return True
        if data.startswith("deal_open_"):
            await self._open_deal(callback, data)
            return True
        if data.startswith(("menu_trade_", "trade_scalp_", "trade_swing_", "trade_long_")):
            await self._legacy_trade(callback)
            return True
        return False

    async def _market_summary(self, callback: Any) -> None:
        await self.dependencies.edit_message(
            callback.message, "📊 Собираю данные рынка..."
        )
        fear_greed, dxy, regime, events = await asyncio.gather(
            asyncio.to_thread(self.dependencies.get_fear_greed),
            asyncio.to_thread(self.dependencies.get_dxy_signal),
            asyncio.to_thread(self.dependencies.get_market_regime, "BTCUSDT"),
            asyncio.to_thread(self.dependencies.get_upcoming_events),
        )
        sentiment = ""
        if fear_greed:
            value = fear_greed["value"]
            bar = "█" * (value // 10) + "░" * (10 - value // 10)
            emoji = "😱" if value < 25 else "😨" if value < 45 else "😐" if value < 55 else "😊" if value < 75 else "🤑"
            sentiment += (
                f"{emoji} <b>Fear & Greed:</b> {value} [{bar}] "
                f"{fear_greed['label']}\n"
            )
        if dxy:
            emoji = "📈" if dxy["signal"] == "STRONG" else "📉" if dxy["signal"] == "WEAK" else "➡️"
            warning = " ⚠️ давит на крипту" if dxy["signal"] == "STRONG" else " ✅ хорошо для крипты" if dxy["signal"] == "WEAK" else ""
            sentiment += (
                f"{emoji} <b>DXY:</b> {dxy['value']} "
                f"({dxy['change']:+.2f}%){warning}\n"
            )
        if regime:
            emoji = "🔥" if regime["mode"] == "TRENDING" else "😴" if regime["mode"] == "SIDEWAYS" else "⚡️"
            sentiment += (
                f"{emoji} <b>Режим BTC:</b> {regime['mode']} "
                f"{regime['direction']}\n"
            )
        if events:
            sentiment += f"\n⚠️ <b>Макро:</b> {events}\n"

        accumulation = ""
        try:
            rows = []
            for symbol in (
                "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
                "TONUSDT", "AVAXUSDT", "LINKUSDT",
            ):
                item = await asyncio.to_thread(
                    self.dependencies.detect_accumulation, symbol
                )
                if item and item.get("score", 0) >= 50:
                    score = item["score"]
                    bar_length = min(10, score // 10)
                    bar = "█" * bar_length + "░" * (10 - bar_length)
                    emoji = "🔥" if score >= 70 else "🟡"
                    rows.append(
                        f"{emoji} <b>{symbol.replace('USDT', '')}</b> "
                        f"[{bar}] {score}/100 {item.get('phase', '')}"
                    )
            if rows:
                accumulation = (
                    "\n🗺 <b>Тепловая карта накоплений:</b>\n"
                    + "\n".join(rows[:5]) + "\n"
                )
        except Exception as exc:
            logging.error("Telegram market accumulation: %s", exc)

        liquidity = ""
        try:
            rows = []
            for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
                candles = await asyncio.to_thread(
                    self.dependencies.get_candles, symbol, "4h", 100
                )
                if not candles or len(candles) <= 20:
                    continue
                volumes = [candle.get("volume", 0) for candle in candles[-50:]]
                average = sum(volumes) / len(volumes) if volumes else 0
                whales = [
                    (candle, volume)
                    for candle, volume in zip(candles[-10:], volumes[-10:])
                    if average > 0 and volume > average * 2
                ]
                if whales:
                    candle, volume = whales[-1]
                    direction = (
                        "🟢 Накопление" if candle["close"] > candle["open"]
                        else "🔴 Сброс"
                    )
                    rows.append(
                        f"🐋 <b>{symbol.replace('USDT', '')}</b>: {direction} "
                        f"(объём ×{volume / average:.1f})"
                    )
            if rows:
                liquidity = (
                    "\n🐋 <b>Крупная ликвидность (4h):</b>\n"
                    + "\n".join(rows) + "\n"
                )
        except Exception as exc:
            logging.error("Telegram market liquidity: %s", exc)

        comment = await asyncio.to_thread(
            self.dependencies.ask_groq,
            f"3 предложения по рынку для трейдера. F&G:{fear_greed}, "
            f"DXY:{dxy}, BTC режим:{regime}. Учти накопления и ликвидность. "
            "Дай конкретный совет — что делать сейчас.",
            max_tokens=150,
        )
        await self.dependencies.edit_message(
            callback.message,
            f"📊 <b>Рынок сейчас</b>\n{'━' * 24}\n\n{sentiment}"
            f"{accumulation}{liquidity}\n💬 <i>{comment or ''}</i>",
            parse_mode="HTML",
            reply_markup=self._refresh_markup("menu_market"),
        )

    async def _scan_menu(self, callback: Any) -> None:
        text = "🔍 <b>Выбери монету</b> (топ-60 по объёму):"
        markup = self.dependencies.pairs_keyboard("scan", 0)
        try:
            await self.dependencies.edit_message(
                callback.message, text, parse_mode="HTML", reply_markup=markup
            )
        except Exception:
            await callback.message.answer(
                text, parse_mode="HTML", reply_markup=markup
            )

    async def _pairs(self, callback: Any, data: str) -> None:
        parts = data.split("_")
        if len(parts) < 2 or not parts[1]:
            return
        try:
            page = int(parts[2]) if len(parts) > 2 else 0
            await self.dependencies.edit_markup(
                callback.message,
                reply_markup=self.dependencies.pairs_keyboard(parts[1], page),
            )
        except (TypeError, ValueError) as exc:
            logging.warning("Invalid Telegram pair navigation %s: %s", data, exc)
        except Exception as exc:
            logging.error("Telegram pair navigation failed: %s", exc)

    async def _timeframe_menu(self, callback: Any) -> None:
        await self.dependencies.edit_message(
            callback.message,
            "⏱ <b>Выбери таймфрейм для анализа</b>\n\n"
            "После выбора бот просканирует все монеты на этом ТФ:",
            parse_mode="HTML",
            reply_markup=self.dependencies.timeframe_keyboard(),
        )

    async def _live_menu(self, callback: Any) -> None:
        await self.dependencies.edit_message(
            callback.message,
            "📍 <b>Живой анализ — где мы сейчас?</b>\n\nВыбери таймфрейм:",
            parse_mode="HTML",
            reply_markup=self.dependencies.live_timeframe_keyboard(),
        )

    async def _live(self, callback: Any, data: str) -> None:
        parts = data.split("_")
        labels = self.dependencies.timeframe_labels
        if len(parts) == 2:
            timeframe = parts[1]
            self.dependencies.user_states[callback.from_user.id] = {
                "action": "live_analysis", "tf": timeframe,
            }
            await self.dependencies.edit_message(
                callback.message,
                f"📍 Анализ на {labels.get(timeframe, timeframe)} — "
                "напиши монету (BTC, SOL, ETHUSDT...):",
            )
            return
        if len(parts) < 4 or parts[1] not in {"now", "refresh"}:
            return
        symbol, timeframe = parts[2], parts[3]
        await self.dependencies.edit_message(
            callback.message,
            f"📍 Обновляю {symbol} {labels.get(timeframe, timeframe)}...",
        )
        result = await asyncio.to_thread(
            self.dependencies.live_position_analysis, symbol, timeframe
        )
        if result:
            rows = [
                [self.dependencies.button(
                    text="🔄 Обновить",
                    callback_data=f"live_refresh_{symbol}_{timeframe}",
                )],
                [self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
            ]
            await self.dependencies.edit_message(
                callback.message, result, parse_mode="HTML",
                reply_markup=self.dependencies.markup(inline_keyboard=rows),
            )
            return
        await self.dependencies.edit_message(
            callback.message, f"Нет данных по {symbol}",
            reply_markup=self.dependencies.markup(inline_keyboard=[[
                self.dependencies.button(text="🔙 Назад", callback_data="menu_back")
            ]]),
        )

    async def _timeframe_scan(self, callback: Any, data: str) -> None:
        timeframe = data.removeprefix("tf_")
        size = self.dependencies.universe_size
        label = self.dependencies.timeframe_labels.get(timeframe, timeframe)
        pairs = await asyncio.to_thread(self.dependencies.get_top_pairs, size)
        await self.dependencies.edit_message(
            callback.message,
            f"🔍 Сканирую {size} пар на {label}...\n"
            "⏳ это может занять несколько минут",
        )
        signals = []
        for symbol in pairs:
            try:
                signal = await asyncio.to_thread(
                    self.dependencies.full_scan, symbol, timeframe
                )
                if signal:
                    signals.append(signal)
                await asyncio.sleep(0.1)
            except Exception as exc:
                logging.error("Telegram timeframe scan %s: %s", symbol, exc)
        if not signals:
            await self.dependencies.edit_message(
                callback.message,
                f"😴 На {label} чётких сетапов нет.\nПопробуй другой таймфрейм.",
                reply_markup=self.dependencies.markup(inline_keyboard=[[
                    self.dependencies.button(text="🔙 Назад", callback_data="menu_tf")
                ]]),
            )
            return
        grade_order = {"МЕГА ТОП": 0, "ТОП СДЕЛКА": 1, "ХОРОШАЯ": 2}
        signals.sort(key=lambda item: grade_order.get(item.get("grade", ""), 3))
        summary_lines = []
        for signal in signals[:8]:
            direction = signal.get("direction", "")
            icon = "🟢" if direction == "BULLISH" else "🔴"
            grade = signal.get("grade", "")
            fire = "🔥🔥🔥" if grade == "МЕГА ТОП" else "🔥🔥" if grade == "ТОП СДЕЛКА" else "✅"
            summary_lines.append(
                f"{fire} {icon} {signal['symbol'].replace('USDT', '')} — {direction}"
            )
        summary_text = "\n".join(summary_lines)
        header = (
            f"⏱ <b>Скан {label}</b> | найдено: {len(signals)}\n"
            f"{'━' * 22}\n\n{summary_text}\n\n{'━' * 22}\n"
            f"<b>Лучший сигнал:</b>\n\n{signals[0]['text']}"
        )
        if len(header) > 4000:
            header = header[:3990] + "..."
        await self.dependencies.edit_message(
            callback.message, header, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="🔄 Обновить", callback_data=data)],
                [self.dependencies.button(text="🔙 Назад", callback_data="menu_tf")],
            ]),
        )

    async def _symbol_scan(self, callback: Any, data: str) -> None:
        symbol = data.removeprefix("scan_")
        await self.dependencies.edit_message(
            callback.message, f"🔍 Анализирую {symbol}..."
        )
        signal = await asyncio.to_thread(
            self.dependencies.full_scan, symbol, "1h", False
        )
        risk_text = ""
        memory = self.dependencies.get_user_memory(callback.from_user.id)
        if memory["deposit"] > 0 and signal:
            risk = self.dependencies.calculate_risk(
                memory["deposit"], memory["risk"],
                signal.get("entry"), signal.get("sl"),
            )
            if risk:
                risk_text = (
                    "\n\n💰 <b>Риск-менеджмент:</b>\n"
                    f"Риск в $: <b>${risk['risk_amount']}</b>\n"
                    f"Размер позиции: <b>${risk['position_size']:.0f}</b>\n"
                    f"Рекомендуемое плечо: <b>x{risk['leverage']}</b>"
                )
        if signal:
            await self.dependencies.edit_message(
                callback.message, signal["text"] + risk_text, parse_mode="HTML",
                reply_markup=self.dependencies.markup(inline_keyboard=[[
                    self.dependencies.button(
                        text="🔙 К монетам", callback_data="menu_scan"
                    )
                ]]),
            )
            return
        diagnostics = await asyncio.to_thread(
            self.dependencies.scan_diagnostics, symbol
        )
        await self.dependencies.edit_message(
            callback.message, diagnostics, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[[
                self.dependencies.button(
                    text="🔄 Повторить", callback_data=f"scan_{symbol}"
                ),
                self.dependencies.button(
                    text="🔙 К монетам", callback_data="menu_scan"
                ),
            ]]),
        )

    async def _news(self, callback: Any) -> None:
        await self.dependencies.edit_message(
            callback.message, "📰 Собираю свежие новости..."
        )
        crypto_news, market_news = await asyncio.gather(
            asyncio.to_thread(self.dependencies.get_crypto_news),
            asyncio.to_thread(self.dependencies.get_market_news),
        )
        crypto_text = self.dependencies.format_news(crypto_news[:6])
        market_text = self.dependencies.format_news(market_news[:4])
        titles = "\n".join(
            item["title"] for item in (crypto_news + market_news)[:10]
        )
        analysis = await asyncio.to_thread(
            self.dependencies.ask_groq,
            "Ты крипто трейдер. Оцени эти новости — что важно для рынка "
            "прямо сейчас? (3-4 пункта, дерзко и кратко):\n" + titles,
            max_tokens=300,
        )
        await asyncio.to_thread(
            self.dependencies.save_news, "crypto news", titles[:500]
        )
        text = (
            "📰 <b>Новости крипторынка</b>\n"
            f"🕐 Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            f"{'━' * 24}\n\n<b>🔥 Крипто:</b>\n{crypto_text}\n\n"
            f"{'━' * 24}\n<b>🌍 Макро (влияет на рынок):</b>\n"
            f"{market_text}\n\n{'━' * 24}\n"
            f"<b>⚡️ APEX анализ:</b>\n{analysis or 'Анализирую...'}"
        )
        if len(text) > 4000:
            text = text[:3990] + "..."
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self._refresh_markup("menu_news"),
        )

    async def _pump(self, callback: Any) -> None:
        await self.dependencies.edit_message(
            callback.message,
            "📦 Сканирую топ-60 на накопление перед пампом...\n⏳ ~30 секунд",
        )
        pairs = await asyncio.to_thread(self.dependencies.get_top_pairs, 50)
        found = []
        for symbol in pairs:
            try:
                accumulation = await asyncio.to_thread(
                    self.dependencies.detect_accumulation, symbol
                )
                if accumulation and accumulation["score"] >= 50:
                    found.append(accumulation)
            except Exception as exc:
                logging.warning("Telegram accumulation scan %s: %s", symbol, exc)
        found.sort(key=lambda item: item["score"], reverse=True)
        if not found:
            await self.dependencies.edit_message(
                callback.message,
                "📦 Накоплений не найдено.\nРынок в движении — боковиков нет.",
                reply_markup=self._refresh_markup("menu_pump"),
            )
            return
        text = (
            "📦 <b>Накопления перед пампом</b>\n"
            f"Найдено: {len(found)} монет\n{'━' * 24}\n\n"
        )
        for accumulation in found[:3]:
            price = accumulation["price"]
            price_text = f"${price:,.4f}" if price < 1 else f"${price:,.2f}"
            score = accumulation["score"]
            bar = "█" * (score // 10) + "░" * (10 - score // 10)
            first_signal = (
                accumulation["signals"][0] if accumulation["signals"] else ""
            )
            text += (
                f"📦 <b>{accumulation['symbol']}</b> | {price_text}\n"
                f"Скор: [{bar}] {score}/100\n{first_signal}\n\n"
            )
        text += "<i>Полный разбор каждой — команда /pump BTCUSDT</i>"
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self._refresh_markup("menu_pump"),
        )

    def _refresh_markup(self, callback_data: str) -> Any:
        return self.dependencies.markup(inline_keyboard=[[
            self.dependencies.button(text="🔄 Обновить", callback_data=callback_data),
            self.dependencies.button(text="🔙 Назад", callback_data="menu_back"),
        ]])

    async def _find_deals(self, callback: Any) -> None:
        await self.dependencies.edit_message(
            callback.message,
            "🎯 <b>Ищу сделки...</b>\n\n"
            "⏳ Сканирую топ-40 пар по SMC: OB, FVG, мультитаймфрейм\n"
            "<i>~20-30 секунд</i>",
            parse_mode="HTML",
        )
        signals = await asyncio.to_thread(self.dependencies.scan_all_deals, 40)
        if not signals:
            await self.dependencies.edit_message(
                callback.message,
                "🎯 <b>Сделок нет</b>\n\n"
                "😴 Прошёлся по топ-40 монетам — чётких сетапов не нашёл.\n"
                "Рынок в боковике или сигналы ещё не сформировались.\n\n"
                "<i>Обычно сигналы появляются после пробоя уровней или выхода новостей</i>",
                parse_mode="HTML",
                reply_markup=self.dependencies.markup(inline_keyboard=[
                    [self.dependencies.button(
                        text="🔄 Попробовать снова",
                        callback_data="menu_find_deals_refresh",
                    )],
                    [self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
                ]),
            )
            return
        grade_icons = {
            "🔥🔥🔥 МЕГА ТОП": "🔥🔥🔥", "🔥🔥 ТОП СДЕЛКА": "🔥🔥",
            "✅ ХОРОШАЯ": "✅",
        }
        lines = [f"🎯 <b>Найдено сделок: {len(signals)}</b>", "━" * 24, ""]
        buttons, row = [], []
        for signal in signals:
            icon = "🟢" if signal["direction"] == "BULLISH" else "🔴"
            lines.append(
                f"{grade_icons.get(signal['grade'], '✅')} {icon} "
                f"<b>{signal['symbol'].replace('USDT', '')}</b> — {signal['direction']}"
            )
        lines += ["", "<i>Выбери монету для полного сигнала 👇</i>"]
        for signal in signals[:12]:
            icon = "🟢" if signal["direction"] == "BULLISH" else "🔴"
            fire = "🔥" if "ТОП" in signal["grade"] else ""
            row.append(self.dependencies.button(
                text=f"{fire}{icon} {signal['symbol'].replace('USDT', '')}",
                callback_data=f"deal_open_{signal['symbol']}",
            ))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([
            self.dependencies.button(
                text="🔄 Обновить", callback_data="menu_find_deals_refresh"
            ),
            self.dependencies.button(text="🔙 Меню", callback_data="menu_back"),
        ])
        await self.dependencies.edit_message(
            callback.message, "\n".join(lines), parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=buttons),
        )

    async def _open_deal(self, callback: Any, data: str) -> None:
        symbol = data.removeprefix("deal_open_")
        await self.dependencies.edit_message(
            callback.message, f"📊 Загружаю сигнал по <b>{symbol}</b>...",
            parse_mode="HTML",
        )
        result = await asyncio.to_thread(
            self.dependencies.full_scan, symbol, "1h"
        )
        if not result:
            await self.dependencies.edit_message(
                callback.message,
                f"😴 <b>{symbol}</b> — сигнал пропал.\n"
                "<i>Рынок изменился пока ты смотрел список</i>",
                parse_mode="HTML",
                reply_markup=self.dependencies.markup(inline_keyboard=[[
                    self.dependencies.button(
                        text="🔙 К списку сделок", callback_data="menu_find_deals"
                    )
                ]]),
            )
            return
        memory = self.dependencies.get_user_memory(callback.from_user.id)
        risk_text = ""
        if memory["deposit"] > 0:
            try:
                risk = self.dependencies.calculate_risk(
                    memory["deposit"], memory["risk"],
                    result.get("entry", 0), result.get("sl", 0),
                )
                if risk:
                    risk_text = (
                        "\n💰 <b>Риск-менеджмент:</b>\n"
                        f"Размер позиции: <b>{risk['position_size']:.2f}</b> USDT\n"
                        f"Риск в $: <b>${risk['risk_amount']:.2f}</b> "
                        f"({memory['risk']}%)\n"
                    )
            except Exception as exc:
                logging.warning("Telegram deal risk unavailable: %s", exc)
        await self.dependencies.edit_message(
            callback.message, result["text"] + risk_text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="🔄 Обновить сигнал", callback_data=data)],
                [self.dependencies.button(
                    text="🔙 К списку сделок", callback_data="menu_find_deals"
                )],
            ]),
        )

    async def _legacy_trade(self, callback: Any) -> None:
        await self.dependencies.edit_message(callback.message, "🔄", parse_mode="HTML")
        await asyncio.sleep(0.1)
        signals = await asyncio.to_thread(self.dependencies.scan_all_deals, 40)
        await self.dependencies.edit_message(
            callback.message,
            f"🎯 Найдено сделок: {len(signals)}" if signals else "😴 Сигналов нет",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(
                    text="🎯 Найти сделки", callback_data="menu_find_deals"
                )],
                [self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
            ]),
        )


__all__ = ["MarketNavigationCallbacks", "MarketNavigationDependencies"]
