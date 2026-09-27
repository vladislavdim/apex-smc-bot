"""Manager Telegram presentation boundary."""
from __future__ import annotations

def manager_line(state=None,position_id=None): return f"Manager {state or 'UNKNOWN'} · {position_id or 'no position'}"

def _legacy(name):
    from core import telegram_manager_dashboard as mod
    return getattr(mod,name)
def configure_manager_dashboard_state(*a,**k): return _legacy("configure_manager_dashboard_state")(*a,**k)
def fetch_manager_trades(*a,**k): return _legacy("fetch_manager_trades")(*a,**k)
def fetch_manager_trade(*a,**k): return _legacy("fetch_manager_trade")(*a,**k)
def format_manager_dashboard(*a,**k): return _legacy("format_manager_dashboard")(*a,**k)
def format_manager_trade_detail(*a,**k): return _legacy("format_manager_trade_detail")(*a,**k)
def format_final_trade_card(*a,**k): return _legacy("format_final_trade_card")(*a,**k)
def manager_trade_buttons(*a,**k): return _legacy("manager_trade_buttons")(*a,**k)
__all__=["manager_line","configure_manager_dashboard_state","fetch_manager_trades","fetch_manager_trade","format_manager_dashboard","format_manager_trade_detail","format_final_trade_card","manager_trade_buttons"]
