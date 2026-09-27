"""Trade Telegram presentation boundary."""
from __future__ import annotations
from typing import Any

def trade_line(symbol,direction,status): return f"{symbol} · {direction} · {status}"

def fetch_trades(*args:Any,**kwargs:Any):
    from core.telegram_dashboard import fetch_trades as impl
    return impl(*args,**kwargs)

def format_trade_view(*args:Any,**kwargs:Any):
    from core.telegram_dashboard import format_trade_view as impl
    return impl(*args,**kwargs)
__all__=["trade_line","fetch_trades","format_trade_view"]
