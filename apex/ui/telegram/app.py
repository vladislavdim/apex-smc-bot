"""Telegram application boundary.
Transport only: trading decisions remain outside UI.
"""
from apex.ui.telegram.router import TelegramHandlers,register_telegram_handlers
__all__=["TelegramHandlers","register_telegram_handlers"]
