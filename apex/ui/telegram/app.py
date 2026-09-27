"""Canonical Telegram application boundary.
Transport only: no trading decision authority lives here.
"""
from apex.ui.telegram.router import (
    COMMAND_ROUTES,
    TelegramHandlers,
    register_handlers,
    register_telegram_handlers,
)
__all__=["COMMAND_ROUTES","TelegramHandlers","register_handlers","register_telegram_handlers"]
