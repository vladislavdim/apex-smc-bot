"""Telegram application boundary.
Transport only: this module never owns trading decisions.
"""
from apex.ui.telegram.router import COMMAND_ROUTES,TelegramHandlers,register_telegram_handlers
__all__=["COMMAND_ROUTES","TelegramHandlers","register_telegram_handlers"]

# Stable transport entry point used by architecture consumers.
register_handlers = register_telegram_handlers
__all__.append("register_handlers")
