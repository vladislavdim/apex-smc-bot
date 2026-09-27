"""Telegram application boundary; presentation/transport only."""
from apex.ui.telegram.router import TelegramHandlers,register_telegram_handlers

def register_handlers(dispatcher,handlers:TelegramHandlers,command_filter):
    return register_telegram_handlers(dispatcher,handlers,command_filter)
__all__=["TelegramHandlers","register_handlers"]
