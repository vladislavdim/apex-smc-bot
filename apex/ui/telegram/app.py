"""Telegram application boundary.
Transport only: this module must never own trading decisions.
"""
from apex.ui.telegram.router import TelegramHandlers,register_telegram_handlers
register_handlers=register_telegram_handlers
__all__=["TelegramHandlers","register_handlers","register_telegram_handlers"]
