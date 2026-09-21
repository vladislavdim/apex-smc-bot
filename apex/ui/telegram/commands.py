"""Dependency-injected Telegram command handlers.

This module contains presentation and user-preference commands only. It has no
access to exchange execution, strategy detection, or process-global state.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Collection


@dataclass(frozen=True)
class CommandDependencies:
    admin_ids: Collection[int]
    get_user_memory: Callable[[int], dict[str, Any]]
    update_user_memory: Callable[..., Any]
    main_menu: Callable[[], Any]
    pairs_keyboard: Callable[[str], Any]
    live_stats: Callable[[], str]


class TelegramCommandHandlers:
    """Small production command surface with explicit dependencies."""

    def __init__(self, dependencies: CommandDependencies) -> None:
        self.dependencies = dependencies

    def _authorized(self, message: Any) -> bool:
        return message.from_user.id in self.dependencies.admin_ids

    async def start(self, message: Any) -> None:
        if not self._authorized(message):
            return
        user_id = message.from_user.id
        name = message.from_user.first_name or "трейдер"
        self.dependencies.update_user_memory(user_id, name=name)
        memory = self.dependencies.get_user_memory(user_id)
        greeting = (
            f"С возвращением, {name}! 👊"
            if memory["messages"] > 1
            else f"Привет, {name}!"
        )
        await message.answer(
            f"⚡️ <b>APEX — AI трейдер по SMC</b>\n\n{greeting}\n\n"
            "Выбирай что нужно 👇",
            parse_mode="HTML",
            reply_markup=self.dependencies.main_menu(),
        )

    async def menu(self, message: Any) -> None:
        if not self._authorized(message):
            return
        await message.answer(
            "Главное меню 👇", reply_markup=self.dependencies.main_menu()
        )

    async def scan(self, message: Any) -> None:
        if not self._authorized(message):
            return
        await message.answer(
            "Выбери монету для скана:",
            reply_markup=self.dependencies.pairs_keyboard("scan"),
        )

    async def risk(self, message: Any) -> None:
        if not self._authorized(message):
            return
        args = message.text.split()
        memory = self.dependencies.get_user_memory(message.from_user.id)
        if len(args) == 2:
            try:
                deposit = float(args[1])
                self.dependencies.update_user_memory(
                    message.from_user.id, deposit=deposit
                )
                await message.answer(
                    f"✅ Депозит сохранён: <b>${deposit:,.2f}</b>\n\n"
                    "Теперь при каждом сигнале я буду считать размер позиции.\n"
                    f"Риск на сделку: {memory['risk']}%\n\n"
                    "Изменить риск: /setrisk 2",
                    parse_mode="HTML",
                )
            except Exception as exc:
                await message.answer(f"Ошибка депозита: {exc}")
            return

        deposit = memory["deposit"]
        if deposit > 0:
            await message.answer(
                "💰 <b>Риск калькулятор</b>\n\n"
                f"Твой депозит: <b>${deposit:,.2f}</b>\n"
                f"Риск на сделку: <b>{memory['risk']}%</b>\n"
                f"Риск в $: <b>${deposit * memory['risk'] / 100:.2f}</b>\n\n"
                "Изменить депозит: /risk 5000\n"
                "Изменить риск %: /setrisk 2",
                parse_mode="HTML",
            )
        else:
            await message.answer(
                "💰 <b>Риск калькулятор</b>\n\nУкажи свой депозит:\n/risk 1000",
                parse_mode="HTML",
            )

    async def setrisk(self, message: Any) -> None:
        if not self._authorized(message):
            return
        args = message.text.split()
        if len(args) != 2:
            return
        try:
            risk = float(args[1])
            if 0.1 <= risk <= 10:
                self.dependencies.update_user_memory(message.from_user.id, risk=risk)
                await message.answer(
                    f"✅ Риск на сделку: <b>{risk}%</b>", parse_mode="HTML"
                )
            else:
                await message.answer("Риск должен быть от 0.1% до 10%")
        except Exception as exc:
            await message.answer(f"Ошибка риска: {exc}")

    async def improve(self, message: Any) -> None:
        if not self._authorized(message):
            return
        await message.answer(
            "🔒 <b>Автоматическое изменение production отключено.</b>\n\n"
            "Предложения можно анализировать отдельно, но код меняется только "
            "через версионированную ветку, обязательные тесты и ручное решение.",
            parse_mode="HTML",
        )

    async def stats(self, message: Any) -> None:
        if not self._authorized(message):
            return
        try:
            text = await asyncio.to_thread(self.dependencies.live_stats)
        except Exception as exc:
            logging.error("Telegram live stats: %s", exc)
            text = "⚠️ Реальная статистика временно недоступна."
        await message.answer(text, parse_mode="HTML")


@dataclass(frozen=True)
class CompatibilityCommandDependencies:
    """Explicit boundary for user-owned data that has not moved to State."""

    admin_ids: Collection[int]
    connect: Callable[[], Any]
    get_live_prices: Callable[[], Any]
    ask_groq: Callable[..., Any]


class CompatibilityCommandHandlers:
    """Legacy-backed commands isolated from the production launcher."""

    def __init__(self, dependencies: CompatibilityCommandDependencies) -> None:
        self.dependencies = dependencies

    def _authorized(self, message: Any) -> bool:
        return message.from_user.id in self.dependencies.admin_ids

    async def alert(self, message: Any) -> None:
        if not self._authorized(message):
            return
        args = message.text.split()
        if len(args) != 3:
            await message.answer(
                "🔔 <b>Алерты на пробой уровня</b>\n\n"
                "Когда цена достигает твоего уровня — пишу сразу.\n\n"
                "Установить: /alert BTCUSDT 70000",
                parse_mode="HTML",
            )
            return
        symbol = args[1].upper()
        try:
            level = float(args[2])
            prices = self.dependencies.get_live_prices()
            if not prices or not isinstance(prices, dict):
                await message.answer("Ошибка получения цен")
                return
            current = prices.get(symbol, {}).get("price", 0)
            direction = "above" if level > current else "below"
            for retry in range(3):
                connection = None
                try:
                    connection = self.dependencies.connect()
                    connection.execute("PRAGMA journal_mode=WAL")
                    connection.execute("PRAGMA busy_timeout=30000")
                    connection.execute(
                        "INSERT INTO alerts VALUES "
                        "(NULL,?,?,?,?,0,CURRENT_TIMESTAMP)",
                        (message.from_user.id, symbol, level, direction),
                    )
                    connection.commit()
                    break
                except Exception as database_error:
                    if retry >= 2:
                        raise
                    logging.warning("DB retry %s: %s", retry + 1, database_error)
                    await asyncio.sleep(1)
                finally:
                    if connection is not None:
                        connection.close()
            arrow = "⬆️" if direction == "above" else "⬇️"
            await message.answer(
                f"🔔 Алерт установлен!\n{arrow} <b>{symbol}</b> → "
                f"<code>{level}</code>\nТекущая цена: <code>{current:.4f}</code>",
                parse_mode="HTML",
            )
        except Exception as exc:
            await message.answer(f"Ошибка алерта: {exc}")

    async def journal(self, message: Any) -> None:
        if not self._authorized(message):
            return
        args = message.text.split(maxsplit=1)
        user_id = message.from_user.id
        if len(args) == 1:
            connection = self.dependencies.connect()
            try:
                rows = connection.execute(
                    "SELECT symbol, direction, entry, exit_price, result, "
                    "pnl_percent, note, created_at FROM journal WHERE user_id=? "
                    "ORDER BY id DESC LIMIT 10",
                    (user_id,),
                ).fetchall()
            finally:
                connection.close()
            if not rows:
                await message.answer(
                    "📓 <b>Дневник сделок</b>\n\nПусто. Добавь сделку:\n"
                    "/journal BTC LONG 65000 67000 win\n\n"
                    "Формат: /journal МОНЕТА НАПРАВЛЕНИЕ ВХОД ВЫХОД win/loss",
                    parse_mode="HTML",
                )
                return
            total = len(rows)
            wins = sum(1 for row in rows if row[4] == "win")
            win_rate = round(wins / total * 100, 1) if total else 0
            text = (
                "📓 <b>Дневник сделок</b> (последние 10)\n"
                f"Win Rate: {win_rate}%\n\n"
            )
            for row in rows:
                emoji = "✅" if row[4] == "win" else "❌"
                text += (
                    f"{emoji} {row[0]} {row[1]}: {row[2]} → {row[3]} "
                    f"({row[5]:+.1f}%)\n"
                )
            losses = [row for row in rows if row[4] == "loss"]
            if len(rows) >= 3 and losses:
                loss_text = "\n".join(
                    f"{row[0]} {row[1]} вход:{row[2]} выход:{row[3]}"
                    for row in losses[:3]
                )
                analysis = self.dependencies.ask_groq(
                    "Проанализируй проигрышные сделки трейдера и дай 2-3 "
                    f"конкретных совета:\n{loss_text}",
                    max_tokens=300,
                )
                if analysis:
                    text += f"\n🧠 <b>Анализ ошибок:</b>\n{analysis}"
            await message.answer(text, parse_mode="HTML")
            return

        try:
            parts = args[1].split()
            if len(parts) < 5:
                await message.answer(
                    "Использование: /journal BTC LONG 65000 68000 win взял на OB"
                )
                return
            symbol, direction = parts[0].upper(), parts[1].upper()
            entry, exit_price = float(parts[2]), float(parts[3])
            result = parts[4].lower()
            note = " ".join(parts[5:]) if len(parts) > 5 else ""
            pnl = (exit_price - entry) / entry * 100
            if direction == "SHORT":
                pnl = -pnl
            connection = self.dependencies.connect()
            try:
                connection.execute(
                    "INSERT INTO journal VALUES "
                    "(NULL,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
                    (
                        user_id, symbol, direction, entry, exit_price, result,
                        note, round(pnl, 2),
                    ),
                )
                connection.commit()
            finally:
                connection.close()
            emoji = "✅" if result == "win" else "❌"
            await message.answer(
                f"{emoji} Сделка добавлена в дневник\n"
                f"{symbol} {direction}: {entry} → {exit_price} ({pnl:+.2f}%)",
                parse_mode="HTML",
            )
        except Exception as exc:
            logging.error("Telegram journal write failed: %s", exc)
            await message.answer(
                "Формат: /journal BTC LONG 65000 67000 win [заметка]\n"
                "Пример: /journal ETH SHORT 3200 3050 win взял на OB"
            )

    async def brain(self, message: Any) -> None:
        try:
            connection = self.dependencies.connect()
            try:
                total = connection.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
                sources = connection.execute(
                    "SELECT source, COUNT(*) FROM knowledge GROUP BY source "
                    "ORDER BY COUNT(*) DESC LIMIT 8"
                ).fetchall()
                recent = connection.execute(
                    "SELECT topic, source, created_at FROM knowledge "
                    "ORDER BY id DESC LIMIT 5"
                ).fetchall()
                reflections = connection.execute(
                    "SELECT COUNT(*) FROM knowledge "
                    "WHERE source='self-reflection'"
                ).fetchone()[0]
                comparisons = connection.execute(
                    "SELECT COUNT(*) FROM knowledge WHERE source='self-compare'"
                ).fetchone()[0]
            finally:
                connection.close()
            sources_text = "\n".join(
                f"• {row[0]}: {row[1]} записей" for row in sources
            )
            recent_text = "\n".join(
                f"• [{row[2][:10]}] {row[0][:40]} ({row[1]})" for row in recent
            )
            await message.answer(
                "🧠 <b>Мозг APEX</b>\n\n"
                f"📚 Всего знаний: <b>{total}</b>\n"
                f"🔄 Само-рефлексий: <b>{reflections}</b>\n"
                f"📊 Сравнений прогнозов: <b>{comparisons}</b>\n\n"
                f"<b>Источники знаний:</b>\n{sources_text}\n\n"
                f"<b>Последние 5 знаний:</b>\n{recent_text}\n\n"
                "<i>Используй /think [тема] — заставить думать над "
                "конкретным вопросом</i>",
                parse_mode="HTML",
            )
        except Exception as exc:
            await message.answer(f"Ошибка: {exc}")


@dataclass(frozen=True)
class MarketCommandDependencies:
    get_crypto_news: Callable[..., list[dict[str, Any]]]
    get_market_impact_news: Callable[..., list[dict[str, Any]]]
    format_news: Callable[[list[dict[str, Any]]], str]
    ask_groq: Callable[..., Any]
    save_news: Callable[..., Any]
    detect_accumulation: Callable[[str], Any]
    format_accumulation: Callable[[Any], str]
    get_top_pairs: Callable[[int], list[str]]
    analyze_trade_type: Callable[[str, str], Any]
    symbol_aliases: dict[str, str]
    timeframe_categories: dict[str, Collection[str]]


class MarketCommandHandlers:
    """Telegram analysis commands with no execution or Manager authority."""

    def __init__(self, dependencies: MarketCommandDependencies) -> None:
        self.dependencies = dependencies

    async def news(self, message: Any) -> None:
        await message.answer("📰 Собираю свежие новости...")
        crypto_news, macro_news = await asyncio.gather(
            asyncio.to_thread(self.dependencies.get_crypto_news),
            asyncio.to_thread(self.dependencies.get_market_impact_news),
        )
        crypto_text = self.dependencies.format_news(crypto_news[:5])
        macro_text = self.dependencies.format_news(macro_news[:3])
        all_titles = "\n".join(
            item["title"] for item in (crypto_news + macro_news)[:10]
        )
        analysis = await asyncio.to_thread(
            self.dependencies.ask_groq,
            "Оцени эти новости для трейдера — что важно прямо сейчас? "
            f"(3 пункта кратко):\n{all_titles}",
            max_tokens=250,
        )
        await asyncio.to_thread(
            self.dependencies.save_news, "crypto news", all_titles[:500]
        )
        message_text = (
            "📰 <b>Новости крипторынка</b>\n"
            f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n{'━' * 24}\n\n"
            f"<b>🔥 Крипто:</b>\n{crypto_text}\n\n"
            f"<b>🌍 Макро:</b>\n{macro_text}\n\n"
            f"<b>⚡️ APEX:</b>\n{analysis or 'Анализирую...'}"
        )
        await message.answer(message_text[:4000], parse_mode="HTML")

    async def pump(self, message: Any) -> None:
        args = message.text.split()
        if len(args) == 2:
            symbol = args[1].upper().replace("USDT", "") + "USDT"
            await message.answer(f"📦 Анализирую накопление {symbol}...")
            accumulation = await asyncio.to_thread(
                self.dependencies.detect_accumulation, symbol
            )
            if accumulation:
                await message.answer(
                    self.dependencies.format_accumulation(accumulation),
                    parse_mode="HTML",
                )
            else:
                await message.answer(f"😴 {symbol} — накоплений не обнаружено.")
            return

        await message.answer("📦 Сканирую топ-20 на накопление...")
        pairs = await asyncio.to_thread(self.dependencies.get_top_pairs, 20)
        found = []
        for symbol in pairs:
            accumulation = await asyncio.to_thread(
                self.dependencies.detect_accumulation, symbol
            )
            if accumulation and accumulation["score"] >= 50:
                found.append(accumulation)
            await asyncio.sleep(0.2)
        found.sort(key=lambda item: item["score"], reverse=True)
        if not found:
            await message.answer("😴 Накоплений не найдено.")
            return
        await message.answer(f"📦 Найдено накоплений: {len(found)}")
        for accumulation in found[:3]:
            await message.answer(
                self.dependencies.format_accumulation(accumulation),
                parse_mode="HTML",
            )
            await asyncio.sleep(0.5)

    async def trade(self, message: Any) -> None:
        args = message.text.split()
        if len(args) < 2:
            await message.answer(
                "📊 <b>Анализ по типу сделки</b>\n\n"
                "Использование:\n"
                "/trade BTC — все типы\n"
                "/trade BTC scalp — скальп (1m/5m/15m)\n"
                "/trade BTC swing — свинг (1h/4h)\n"
                "/trade BTC long — долгосрок (1d/1w/1M)\n\n"
                "<i>Примеры: /trade TON, /trade ETH swing, "
                "/trade SOL long</i>",
                parse_mode="HTML",
            )
            return
        raw_symbol = args[1].lower()
        symbol = self.dependencies.symbol_aliases.get(raw_symbol, raw_symbol.upper())
        if not symbol.endswith("USDT"):
            symbol = symbol.upper() + "USDT"
        trade_type = args[2].lower() if len(args) >= 3 else "all"
        if trade_type not in {"scalp", "swing", "long", "all"}:
            trade_type = "all"
        types_to_run = (
            ["scalp", "swing", "long"] if trade_type == "all" else [trade_type]
        )
        labels = {
            "scalp": "⚡️ Скальп",
            "swing": "🔄 Свинг",
            "long": "📈 Долгосрок",
        }
        await message.answer(
            f"🔍 Анализирую <b>{symbol}</b>\n"
            f"Типы: {' | '.join(labels[value] for value in types_to_run)}\n"
            "⏳ Подожди...",
            parse_mode="HTML",
        )
        found_any = False
        for selected_type in types_to_run:
            result = await asyncio.to_thread(
                self.dependencies.analyze_trade_type, symbol, selected_type
            )
            if result:
                found_any = True
                await message.answer(result["text"], parse_mode="HTML")
                await asyncio.sleep(0.5)
            else:
                timeframes = self.dependencies.timeframe_categories[selected_type]
                await message.answer(
                    f"{labels[selected_type]}: нет чёткого сигнала по {symbol} "
                    f"на таймфреймах {', '.join(timeframes)}"
                )
        if not found_any:
            await message.answer(
                f"😴 <b>{symbol}</b> — нет сигналов ни по одному типу сделки.\n"
                "Рынок, возможно, в боковике или данных недостаточно.",
                parse_mode="HTML",
            )
