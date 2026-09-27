"""Canonical Binance execution client surface.

Implementation remains co-located with order orchestration during the cutover,
but application/runtime code imports the client only through this Execution
boundary. No strategy, Manager, Learning or UI module may submit orders.
"""
from apex.execution.orders import BinanceAPIError, BinanceFuturesClient, ExecutionConfig
__all__=["BinanceAPIError","BinanceFuturesClient","ExecutionConfig"]
