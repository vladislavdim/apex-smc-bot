"""Telegram application boundary.
Transport only: this module never owns trading decisions.
"""
from apex.ui.telegram.router import TelegramHandlers,register_telegram_handlers
__all__=["TelegramHandlers","register_telegram_handlers"]
