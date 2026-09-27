"""Trade Telegram read-only presentation boundary."""
from __future__ import annotations
from typing import Any,Iterable,Mapping

def trade_line(symbol:str,direction:str,status:str)->str: return f"{symbol} · {direction} · {status}"
def fetch_trades(rows:Iterable[Mapping[str,Any]]|None=None)->list[dict[str,Any]]: return [dict(x) for x in (rows or ())]
def format_trade_view(row:Mapping[str,Any])->str:
    return trade_line(str(row.get("symbol","?")),str(row.get("direction","UNKNOWN")),str(row.get("status","UNKNOWN")))
__all__=["fetch_trades","format_trade_view","trade_line"]
