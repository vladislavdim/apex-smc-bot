"""Telegram transport routing for the production bot.

Handler implementations remain dependency-injected by the launcher while this
module owns the public command surface and registration order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class TelegramHandlers:
    start: Callable[..., Any]
    menu: Callable[..., Any]
    scan: Callable[..., Any]
    risk: Callable[..., Any]
    setrisk: Callable[..., Any]
    alert: Callable[..., Any]
    journal: Callable[..., Any]
    improve: Callable[..., Any]
    stats: Callable[..., Any]
    news: Callable[..., Any]
    pump: Callable[..., Any]
    trade: Callable[..., Any]
    brain: Callable[..., Any]
    callback: Callable[..., Any]
    chat_member: Callable[..., Any]
    text: Callable[..., Any]


COMMAND_ROUTES = (
    ("start", "start"),
    ("menu", "menu"),
    ("scan", "scan"),
    ("risk", "risk"),
    ("setrisk", "setrisk"),
    ("alert", "alert"),
    ("journal", "journal"),
    ("improve", "improve"),
    ("stats", "stats"),
    ("news", "news"),
    ("pump", "pump"),
    ("trade", "trade"),
    ("brain", "brain"),
)


def register_telegram_handlers(
    dispatcher: Any,
    handlers: TelegramHandlers,
    command_filter: Callable[[str], Any],
) -> None:
    """Register the explicit production surface before the catch-all route."""
    for command, attribute in COMMAND_ROUTES:
        dispatcher.message.register(
            getattr(handlers, attribute), command_filter(command)
        )
    dispatcher.callback_query.register(handlers.callback)
    dispatcher.chat_member.register(handlers.chat_member)
    dispatcher.message.register(handlers.text)
