"""Trade Telegram read-only presentation boundary.

The worker imports this canonical path; the proven legacy renderer remains a bounded
implementation adapter until its internals are physically migrated.
"""
from __future__ import annotations
from typing import Any

def trade_line(symbol:str,direction:str,status:str)->str:
    return f"{symbol} · {direction} · {status}"

def fetch_trades(*args:Any,**kwargs:Any):
    from core.trade_views import fetch_trades as impl
    return impl(*args,**kwargs)

def format_trade_view(*args:Any,**kwargs:Any):
    from core.trade_views import format_trade_view as impl
    return impl(*args,**kwargs)

__all__=["fetch_trades","format_trade_view","trade_line"]
