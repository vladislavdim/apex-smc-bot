"""Telegram application boundary.
Transport only: this module must never own trading decisions.
"""
from apex.ui.telegram.router import COMMAND_ROUTES,TelegramHandlers,register_telegram_handlers
__all__=["COMMAND_ROUTES","TelegramHandlers","register_telegram_handlers"]
