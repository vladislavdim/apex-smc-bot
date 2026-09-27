"""Gate websocket market-data boundary.

Market transport only; order execution is intentionally absent.
"""
from __future__ import annotations
from typing import Any,AsyncIterator,Protocol
class GateMarketStream(Protocol):
    def candles(self,symbol:str,timeframe:str)->AsyncIterator[Any]: ...
__all__=["GateMarketStream"]
