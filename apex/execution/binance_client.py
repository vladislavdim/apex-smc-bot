"""Binance execution client protocol.
Only the execution layer may implement/use this order-submission boundary.
"""
from __future__ import annotations
from typing import Protocol,Any
class BinanceExecutionClient(Protocol):
    async def place_order(self,**params:Any)->Any: ...
    async def cancel_order(self,**params:Any)->Any: ...
__all__=["BinanceExecutionClient"]
