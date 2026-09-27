"""Telegram application boundary.
Presentation/transport only; trading authority remains outside UI.
"""
from apex.ui.telegram.router import COMMAND_ROUTES,TelegramHandlers,register_handlers
__all__=["COMMAND_ROUTES","TelegramHandlers","register_handlers"]
