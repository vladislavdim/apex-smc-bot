"""Gate websocket market-data boundary.
This protocol is market-data only and cannot submit exchange orders.
"""
from __future__ import annotations
from typing import Protocol,AsyncIterator,Mapping,Any
class GateMarketStream(Protocol):
    def candles(self,symbol:str,timeframe:str)->AsyncIterator[Mapping[str,Any]]: ...
__all__=["GateMarketStream"]
