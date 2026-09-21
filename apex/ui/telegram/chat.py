"""Dependency-injected free-text and membership handlers."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, MutableMapping


@dataclass(frozen=True)
class ChatDependencies:
    user_states: MutableMapping[int, dict[str, Any]]
    timeframe_labels: dict[str, str]
    live_position_analysis: Callable[[str, str], Any]
    live_markup: Callable[[str, str], Any]
    save_chat_log: Callable[[int, str, str], Any]
    ask_ai: Callable[[int, str, str], Any]
    extract_profile: Callable[[int, str, str, str], Any]
    ask_groq: Callable[..., Any]
    send_message: Callable[..., Any]


class TelegramChatHandlers:
    def __init__(self, dependencies: ChatDependencies) -> None:
        self.dependencies = dependencies

    async def member(self, event: Any) -> None:
        try:
            old_status = (
                event.old_chat_member.status if event.old_chat_member else "left"
            )
            new_status = (
                event.new_chat_member.status if event.new_chat_member else "left"
            )
            if old_status not in {"left", "kicked", "restricted"} or new_status not in {
                "member", "administrator",
            }:
                return
            user = event.new_chat_member.user
            name = user.first_name or "трейдер"
            greeting = None
            try:
                greeting = await asyncio.to_thread(
                    self.dependencies.ask_groq,
                    "Придумай короткое креативное приветствие для нового подписчика "
                    f"трейдингового канала. Имя: {name}. Упомяни профитные сделки "
                    "и удачу. Максимум 2 предложения. Только на русском.",
                    max_tokens=80,
                )
                if not greeting or len(greeting.strip()) <= 10:
                    greeting = None
            except Exception:
                greeting = None
            if not greeting:
                greeting = (
                    f"Привет {name}! Рады видеть тебя — профитных сделок и "
                    "зелёных свечей! 🚀"
                )
            await self.dependencies.send_message(event.chat.id, greeting.strip())
            logging.info("[Welcome] %s (id=%s) joined chat %s", name, user.id, event.chat.id)
        except Exception as exc:
            logging.debug("on_new_member error: %s", exc)

    async def text(self, message: Any) -> None:
        user_id = message.from_user.id
        user_name = message.from_user.first_name or "трейдер"
        text = message.text
        if not text:
            return
        state = self.dependencies.user_states.pop(user_id, None)
        if state and state.get("action") == "live_analysis":
            symbol = text.upper().replace("USDT", "") + "USDT"
            timeframe = state.get("tf", "1h")
            label = self.dependencies.timeframe_labels.get(timeframe, timeframe)
            thinking = await message.answer(f"📍 Анализирую {symbol} {label}...")
            result = await asyncio.to_thread(
                self.dependencies.live_position_analysis, symbol, timeframe
            )
            try:
                await thinking.delete()
            except Exception:
                pass
            if result:
                await message.answer(
                    result,
                    parse_mode="HTML",
                    reply_markup=self.dependencies.live_markup(symbol, timeframe),
                )
            else:
                await message.answer(
                    f"Нет данных по {symbol}. Попробуй: BTC, ETH, SOL, BNB"
                )
            return
        self.dependencies.save_chat_log(user_id, "user", text)
        thinking = await message.answer("⚡️")
        reply = await asyncio.to_thread(
            self.dependencies.ask_ai, user_id, user_name, text
        )
        try:
            await thinking.delete()
        except Exception:
            pass
        if not reply:
            await message.answer("⚡️ Перегружен, попробуй через минуту.")
            return
        self.dependencies.save_chat_log(user_id, "assistant", reply)
        await message.answer(reply)
        asyncio.create_task(
            asyncio.to_thread(
                self.dependencies.extract_profile,
                user_id,
                user_name,
                text,
                reply,
            )
        )
