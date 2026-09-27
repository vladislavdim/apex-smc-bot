"""Manager Telegram presentation boundary; no trading authority."""
from __future__ import annotations
from typing import Any,Iterable,Mapping
_STATE_PROVIDER=None

def configure_manager_dashboard_state(provider=None,**_:Any)->None:
    global _STATE_PROVIDER; _STATE_PROVIDER=provider

def fetch_manager_trades(*args:Any,**kwargs:Any)->list[dict[str,Any]]:
    return list(_STATE_PROVIDER(*args,**kwargs)) if callable(_STATE_PROVIDER) else []

def fetch_manager_trade(position_id:str,*_:Any,**__:Any)->dict[str,Any]|None:
    return next((r for r in fetch_manager_trades() if str(r.get("position_id"))==str(position_id)),None)

def manager_line(state=None,position_id=None): return f"Manager {state or 'UNKNOWN'} · {position_id or 'no position'}"
def format_manager_dashboard(rows:Iterable[Mapping[str,Any]])->str:
    data=list(rows); return "Manager · no confirmed positions" if not data else "\n".join(manager_line(r.get("state") or r.get("status"),r.get("position_id")) for r in data)
def format_manager_trade_detail(row:Mapping[str,Any]|None)->str:
    if not row:return "Manager · position not found"
    return manager_line(row.get("state") or row.get("status"),row.get("position_id"))
def format_final_trade_card(row:Mapping[str,Any]|None)->str: return format_manager_trade_detail(row)
def manager_trade_buttons(*_:Any,**__:Any): return None
__all__=["configure_manager_dashboard_state","fetch_manager_trades","fetch_manager_trade","format_manager_dashboard","format_manager_trade_detail","format_final_trade_card","manager_trade_buttons","manager_line"]
