"""Telegram application boundary.
Transport only: this module must never own trading decisions.
"""
from apex.ui.telegram.router import register_telegram_handlers
register_handlers=register_telegram_handlers
__all__=["register_handlers","register_telegram_handlers"]
