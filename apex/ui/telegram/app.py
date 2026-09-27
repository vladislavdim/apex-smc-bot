"""Telegram application boundary.
Transport/presentation only; trading authority remains outside UI.
"""
from apex.ui.telegram.router import TelegramHandlers,register_telegram_handlers,register_handlers
__all__=["TelegramHandlers","register_handlers","register_telegram_handlers"]
