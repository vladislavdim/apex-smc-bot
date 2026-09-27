"""Compatibility facade; canonical implementation lives in apex.ui.telegram.trades."""
from apex.compatibility.trade_views import fetch_trades
from apex.ui.telegram.trades import format_trade_view
__all__=["fetch_trades","format_trade_view"]
