"""Telegram application boundary.
Transport only: this module must never own trading decisions.
"""
from apex.ui.telegram.router import TelegramHandlers, register_telegram_handlers


def register_handlers(dispatcher, handlers: TelegramHandlers, command_filter):
    """Register the canonical production Telegram surface."""
    return register_telegram_handlers(dispatcher, handlers, command_filter)


__all__=["TelegramHandlers","register_handlers"]
