"""Telegram application boundary; presentation/transport only."""
from apex.ui.telegram.router import TelegramHandlers,register_handlers
__all__=["TelegramHandlers","register_handlers"]
