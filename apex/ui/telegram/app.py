"""Telegram application boundary.

Transport only: trading decisions remain in canonical strategy/risk/execution/manager layers.
"""
from apex.ui.telegram.router import COMMAND_ROUTES, TelegramHandlers, register_telegram_handlers

register_handlers = register_telegram_handlers

__all__ = ["COMMAND_ROUTES", "TelegramHandlers", "register_handlers", "register_telegram_handlers"]
