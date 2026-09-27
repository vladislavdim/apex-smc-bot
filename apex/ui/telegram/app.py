"""Telegram application boundary.

Transport/presentation only: no trading authority lives here.
"""
from apex.ui.telegram.router import register_telegram_handlers,TelegramHandlers
register_handlers=register_telegram_handlers
__all__=["TelegramHandlers","register_handlers","register_telegram_handlers"]
