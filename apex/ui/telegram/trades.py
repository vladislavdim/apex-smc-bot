"""Production trade Telegram presentation boundary.

The worker owns data access during the cutover; this module stays presentation-only.
"""
from __future__ import annotations
from typing import Any,Iterable,Mapping

def fetch_trades(*args:Any,**kwargs:Any)->list[dict[str,Any]]:
    provider=kwargs.pop("provider",None)
    return list(provider(*args,**kwargs)) if callable(provider) else []

def trade_line(symbol:str,direction:str,status:str)->str: return f"{symbol} · {direction} · {status}"

def format_trade_view(rows:Iterable[Mapping[str,Any]])->str:
    data=list(rows)
    if not data:return "Trades · no confirmed positions"
    return "\n".join(trade_line(str(r.get("symbol","?")),str(r.get("direction","?")),str(r.get("status","?"))) for r in data)
__all__=["fetch_trades","format_trade_view","trade_line"]
