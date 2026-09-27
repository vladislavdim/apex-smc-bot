"""Gate websocket market-data boundary.
Scanning data only; this interface has no execution methods.
"""
from __future__ import annotations
from typing import Any,AsyncIterator,Protocol
class GateMarketStream(Protocol):
    def candles(self,symbol:str,timeframe:str)->AsyncIterator[Any]: ...
__all__=["GateMarketStream"]
