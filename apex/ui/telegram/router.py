"""Telegram transport routing for the production bot.

Handler implementations remain dependency-injected by the launcher while this
module owns the public command surface and registration order.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
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
    *, admin_ids: tuple[int, ...] | frozenset[int] = (),
) -> None:
    """Register the explicit production surface before the catch-all route."""
    permitted = frozenset(admin_ids)

    def protect(handler):
        @wraps(handler)
        async def authorized(event, *args, **kwargs):
            user = getattr(event, "from_user", None)
            if user is None or user.id not in permitted:
                return None
            return await handler(event, *args, **kwargs)
        return authorized

    for command, attribute in COMMAND_ROUTES:
        dispatcher.message.register(
            protect(getattr(handlers, attribute)), command_filter(command)
        )
    dispatcher.callback_query.register(protect(handlers.callback))
    dispatcher.chat_member.register(protect(handlers.chat_member))
    dispatcher.message.register(protect(handlers.text))


# One canonical registration function; legacy/public name is an identity alias.
register_handlers = register_telegram_handlers

__all__ = ["COMMAND_ROUTES", "TelegramHandlers", "register_handlers", "register_telegram_handlers"]
