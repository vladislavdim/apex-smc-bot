"""State-backed Telegram callback views."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable

@dataclass(frozen=True)
class StateCallbackDependencies:
    edit_message: Callable[..., Any]
    main_menu: Callable[[], Any]
    fetch_manager_trades: Callable[[int], Any]
    fetch_manager_trade: Callable[[int, int], Any]
    format_manager_dashboard: Callable[[Any], str]
    format_manager_trade_detail: Callable[[Any], str]
    manager_trade_buttons: Callable[[Any], Any]
    fetch_trade_rows: Callable[[str, int], Any]
    format_trade_view: Callable[[str, Any], str]
    fetch_watchlist: Callable[[int], Any]
    format_watchlist: Callable[[Any], str]
    rebuild_strategy_risk: Callable[[], Any]
    scanner_dashboard: Callable[[], Any]
    format_scanner_dashboard: Callable[[Any], str]
    setup_evidence_dashboard: Callable[[int, int], Any]
    format_setup_evidence_dashboard: Callable[[Any], str]
    fetch_groq_rejections: Callable[[int, int], Any]
    format_groq_rejections: Callable[[Any], str]
    get_user_memory: Callable[[int], dict[str, Any]]
    live_learning: Callable[[], str]
    current_incidents: Callable[[], Any]
    format_incidents: Callable[[Any], str]
    fetch_strategy_stats: Callable[[], Any]
    format_strategy_stats: Callable[[Any], str]
    system_dashboard: Callable[[], str]
    stats_url: str
    button: Callable[..., Any]
    markup: Callable[..., Any]


class StateCallbackHandlers:
    """Dispatch callback views that read canonical State projections only."""

    def __init__(self, dependencies: StateCallbackDependencies) -> None:
        self.dependencies = dependencies

    async def handle(self, callback: Any) -> bool:
        data = callback.data
        if data == "menu_back":
            await self.dependencies.edit_message(
                callback.message,
                "Главное меню 👇",
                reply_markup=self.dependencies.main_menu(),
            )
            return True
        if data == "menu_trade_manager":
            await self._manager(callback)
            return True
        if data.startswith("manager_trade_"):
            await self._manager_detail(callback, data)
            return True
        if data == "menu_trades":
            await self._trade_menu(callback)
            return True
        if data in {"menu_active_trades", "menu_take_closed", "menu_stop_closed"}:
            await self._trade_rows(callback, data)
            return True
        if data == "menu_watchlist":
            await self._watchlist(callback)
            return True
        if data == "menu_scanners":
            await self._scanners(callback)
            return True
        if data == "menu_setup_evidence":
            await self._setup_evidence(callback)
            return True
        if data == "menu_groq_rejections":
            await self._groq_rejections(callback)
            return True
        if data in {"menu_risk", "menu_journal", "menu_alerts"}:
            await self._preference_view(callback, data)
            return True
        if data in {"menu_stats", "menu_live_learning"}:
            await self._learning(callback, data)
            return True
        if data == "menu_incidents":
            await self._incidents(callback)
            return True
        if data == "menu_strategies":
            await self._strategies(callback)
            return True
        if data == "menu_system":
            await self._system(callback)
            return True
        return False

    async def _system(self, callback: Any) -> None:
        try:
            text = await asyncio.to_thread(self.dependencies.system_dashboard)
        except Exception as exc:
            logging.error("Telegram system dashboard: %s", exc)
            text = "⚠️ Не удалось прочитать каноническое состояние worker."
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="🔄 Обновить", callback_data="menu_system")],
                [self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
            ]),
        )

    async def _manager(self, callback: Any) -> None:
        try:
            items = await asyncio.to_thread(self.dependencies.fetch_manager_trades, 12)
            text = self.dependencies.format_manager_dashboard(items)
            rows = [
                [self.dependencies.button(text=label, callback_data=data)]
                for label, data in self.dependencies.manager_trade_buttons(items)
            ]
            rows.append([
                self.dependencies.button(text="🔄 Обновить", callback_data="menu_trade_manager"),
                self.dependencies.button(text="🔙 Меню", callback_data="menu_back"),
            ])
            markup = self.dependencies.markup(inline_keyboard=rows)
        except Exception as exc:
            logging.error("Telegram Trade Manager dashboard: %s", exc)
            text = "⚠️ Не удалось прочитать состояние менеджера сделок. Сканер продолжает работать."
            markup = self.dependencies.markup(inline_keyboard=[[
                self.dependencies.button(text="🔄 Обновить", callback_data="menu_trade_manager"),
                self.dependencies.button(text="🔙 Меню", callback_data="menu_back"),
            ]])
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML", reply_markup=markup
        )

    async def _manager_detail(self, callback: Any, data: str) -> None:
        try:
            signal_id = int(data.rsplit("_", 1)[-1])
            payload = await asyncio.to_thread(
                self.dependencies.fetch_manager_trade, signal_id, 12
            )
            text = (
                self.dependencies.format_manager_trade_detail(payload)
                if payload else "⚠️ Сделка больше не найдена в памяти Trade Manager."
            )
        except Exception as exc:
            logging.error("Telegram Trade Manager trade detail: %s", exc)
            text = "⚠️ Не удалось прочитать историю этой сделки."
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="🔄 Обновить", callback_data=data)],
                [self.dependencies.button(text="🔙 К менеджеру", callback_data="menu_trade_manager"),
                 self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
            ]),
        )

    async def _trade_menu(self, callback: Any) -> None:
        await self.dependencies.edit_message(
            callback.message, "📊 <b>Сделки APEX</b>\n\nВыберите состояние сделки:",
            parse_mode="HTML", reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="📍 Активные", callback_data="menu_active_trades")],
                [self.dependencies.button(text="✅ Закрыты по тейку", callback_data="menu_take_closed")],
                [self.dependencies.button(text="🛑 Закрыты по стопу", callback_data="menu_stop_closed")],
                [self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
            ]),
        )

    async def _trade_rows(self, callback: Any, data: str) -> None:
        category = {"menu_active_trades": "active", "menu_take_closed": "take", "menu_stop_closed": "stop"}[data]
        try:
            rows = await asyncio.to_thread(self.dependencies.fetch_trade_rows, category, 12)
            text = self.dependencies.format_trade_view(category, rows)
        except Exception as exc:
            logging.error("Telegram trade view %s: %s", category, exc)
            text = "⚠️ Не удалось прочитать историю сделок. Сканер продолжает работать."
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="📍 Активные", callback_data="menu_active_trades"),
                 self.dependencies.button(text="✅ Тейки", callback_data="menu_take_closed"),
                 self.dependencies.button(text="🛑 Стопы", callback_data="menu_stop_closed")],
                [self.dependencies.button(text="🔄 Обновить", callback_data=data),
                 self.dependencies.button(text="🔙 Сделки", callback_data="menu_trades")],
            ]),
        )

    async def _watchlist(self, callback: Any) -> None:
        try:
            items = await asyncio.to_thread(self.dependencies.fetch_watchlist, 20)
            text = self.dependencies.format_watchlist(items)
        except Exception as exc:
            logging.error("Telegram watchlist: %s", exc)
            text = "⚠️ Не удалось прочитать наблюдаемые сделки. Сканер продолжает работать."
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="🚫 Отказы Groq", callback_data="menu_groq_rejections")],
                [self.dependencies.button(text="🔄 Обновить", callback_data="menu_watchlist"),
                 self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
            ]),
        )

    async def _scanners(self, callback: Any) -> None:
        try:
            await asyncio.to_thread(self.dependencies.rebuild_strategy_risk)
            dashboard = await asyncio.to_thread(self.dependencies.scanner_dashboard)
            text = self.dependencies.format_scanner_dashboard(dashboard)
        except Exception as exc:
            logging.error("Telegram scanner dashboard: %s", exc)
            text = "⚠️ Не удалось прочитать состояние сканеров."
        buttons = []
        if self.dependencies.stats_url:
            buttons.append([self.dependencies.button(text="📊 Полная статистика", url=self.dependencies.stats_url)])
        buttons.append([self.dependencies.button(text="🔄 Обновить", callback_data="menu_scanners"), self.dependencies.button(text="🔙 Меню", callback_data="menu_back")])
        await self.dependencies.edit_message(callback.message, text, parse_mode="HTML", reply_markup=self.dependencies.markup(inline_keyboard=buttons))

    async def _setup_evidence(self, callback: Any) -> None:
        try:
            payload = await asyncio.to_thread(self.dependencies.setup_evidence_dashboard, 24, 12)
            text = self.dependencies.format_setup_evidence_dashboard(payload)
        except Exception as exc:
            logging.error("Telegram setup evidence dashboard: %s", exc)
            text = "⚠️ Не удалось прочитать журнал качества сетапов."
        await self.dependencies.edit_message(callback.message, text, parse_mode="HTML", reply_markup=self.dependencies.markup(inline_keyboard=[
            [self.dependencies.button(text="🔄 Обновить", callback_data="menu_setup_evidence"), self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
        ]))

    async def _groq_rejections(self, callback: Any) -> None:
        try:
            payload = await asyncio.to_thread(self.dependencies.fetch_groq_rejections, 24, 30)
            text = self.dependencies.format_groq_rejections(payload)
        except Exception as exc:
            logging.error("Telegram Groq rejection view: %s", exc)
            text = "⚠️ Не удалось прочитать журнал отказов Groq. Сканер продолжает работать."
        await self.dependencies.edit_message(callback.message, text, parse_mode="HTML", reply_markup=self.dependencies.markup(inline_keyboard=[
            [self.dependencies.button(text="🔄 Обновить", callback_data="menu_groq_rejections"), self.dependencies.button(text="👀 Наблюдаемые", callback_data="menu_watchlist")],
            [self.dependencies.button(text="🔙 Меню", callback_data="menu_back")],
        ]))

    async def _preference_view(self, callback: Any, data: str) -> None:
        if data == "menu_risk":
            memory = self.dependencies.get_user_memory(callback.from_user.id)
            if memory["deposit"] > 0:
                text = (
                    "💰 <b>Риск калькулятор</b>\n\n"
                    f"Депозит: <b>${memory['deposit']:,.2f}</b>\n"
                    f"Риск на сделку: <b>{memory['risk']}%</b>\n"
                    f"Макс риск в $: <b>${memory['deposit'] * memory['risk'] / 100:.2f}</b>\n\n"
                    "Изменить: /risk 5000 или /setrisk 2"
                )
            else:
                text = "💰 <b>Риск калькулятор</b>\n\nУкажи депозит командой:\n/risk 1000"
        elif data == "menu_journal":
            text = (
                "📓 <b>Дневник сделок</b>\n\n"
                "/journal — посмотреть историю + анализ ошибок\n\n"
                "Добавить сделку:\n/journal BTC LONG 65000 67000 win"
            )
        else:
            text = (
                "🔔 <b>Алерты на пробой уровня</b>\n\n"
                "Установить:\n/alert BTCUSDT 70000\n\n"
                "Как только цена достигнет уровня — пришлю сразу ⚡️"
            )
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[[
                self.dependencies.button(text="🔙 Назад", callback_data="menu_back")
            ]]),
        )

    async def _learning(self, callback: Any, data: str) -> None:
        try:
            text = await asyncio.to_thread(self.dependencies.live_learning)
        except Exception as exc:
            logging.error("Telegram live learning: %s", exc)
            text = (
                "⚠️ Реальная статистика временно недоступна."
                if data == "menu_stats" else
                "⚠️ Live Learning временно недоступен. Торговый pipeline продолжает работать."
            )
        rows = []
        if data == "menu_stats":
            rows.extend([
                [self.dependencies.button(text="📊 Стратегии", callback_data="menu_strategies")],
                [self.dependencies.button(text="⚠️ Инциденты", callback_data="menu_incidents")],
            ])
        rows.append([self.dependencies.button(text="🔄 Обновить", callback_data=data)])
        rows.append([self.dependencies.button(text="🔙 Назад", callback_data="menu_back")])
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=rows),
        )

    async def _incidents(self, callback: Any) -> None:
        try:
            incidents = await asyncio.to_thread(self.dependencies.current_incidents)
            text = self.dependencies.format_incidents(incidents)
        except Exception as exc:
            logging.error("Telegram incidents: %s", exc)
            text = "⚠️ Список инцидентов временно недоступен."
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="🔄 Обновить", callback_data="menu_incidents")],
                [self.dependencies.button(text="🔙 Назад", callback_data="menu_back")],
            ]),
        )

    async def _strategies(self, callback: Any) -> None:
        try:
            rows = await asyncio.to_thread(self.dependencies.fetch_strategy_stats)
            text = self.dependencies.format_strategy_stats(rows)
        except Exception as exc:
            logging.error("Telegram strategy stats: %s", exc)
            text = "⚠️ Не удалось рассчитать статистику стратегий."
        await self.dependencies.edit_message(
            callback.message, text, parse_mode="HTML",
            reply_markup=self.dependencies.markup(inline_keyboard=[
                [self.dependencies.button(text="🔄 Обновить", callback_data="menu_strategies")],
                [self.dependencies.button(text="🔙 Назад", callback_data="menu_stats")],
            ]),
        )
