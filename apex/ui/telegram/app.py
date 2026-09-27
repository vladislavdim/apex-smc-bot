"""Telegram application boundary; presentation/transport only."""
from apex.ui.telegram.router import TelegramHandlers,register_handlers,register_telegram_handlers
__all__=["TelegramHandlers","register_handlers","register_telegram_handlers"]
