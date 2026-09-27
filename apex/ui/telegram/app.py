"""Telegram application boundary.
Presentation/transport only; trading authority remains outside UI.
"""
from apex.ui.telegram.router import TelegramHandlers,register_handlers,register_telegram_handlers
__all__=["TelegramHandlers","register_handlers","register_telegram_handlers"]
