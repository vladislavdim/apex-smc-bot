"""Telegram transport boundary for production APEX V3.
Presentation and routing only; never trading authority.
"""
from apex.ui.telegram.router import COMMAND_ROUTES,TelegramHandlers,register_handlers
__all__=["COMMAND_ROUTES","TelegramHandlers","register_handlers"]
