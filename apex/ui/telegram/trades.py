"""Trade Telegram presentation boundary.

Legacy public helpers are re-exported during the cutover so production keeps
one trade reader while presentation ownership moves into apex.ui.telegram.
"""
from core.telegram_dashboard import fetch_trades, format_trade_view

def trade_line(symbol,direction,status):
    return f"{symbol} · {direction} · {status}"

__all__=["fetch_trades","format_trade_view","trade_line"]
