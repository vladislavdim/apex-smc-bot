"""Manager Telegram presentation boundary.

Presentation only. Trading authority remains in apex.manager.
"""
from core.telegram_manager import (
    configure_manager_dashboard_state,
    fetch_manager_trades,
    fetch_manager_trade,
    format_manager_dashboard,
    format_manager_trade_detail,
    format_final_trade_card,
    manager_trade_buttons,
)

def manager_line(state=None,position_id=None):
    return f"Manager {state or 'UNKNOWN'} · {position_id or 'no position'}"

__all__=[
    "configure_manager_dashboard_state","fetch_manager_trades","fetch_manager_trade",
    "format_manager_dashboard","format_manager_trade_detail","format_final_trade_card",
    "manager_trade_buttons","manager_line",
]
