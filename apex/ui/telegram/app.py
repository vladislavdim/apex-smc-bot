"""Telegram application composition boundary."""
from apex.ui.telegram.router import TelegramHandlers, register_telegram_handlers

__all__ = ["TelegramHandlers", "register_telegram_handlers"]
