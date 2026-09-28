"""Gate websocket market-data boundary for APEX V3.

The runtime may inject a concrete transport, while strategies consume only
normalized market snapshots.  This module intentionally has no Binance path.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any,AsyncIterator,Protocol

@dataclass(frozen=True)
class GateStreamEvent:
    channel:str
    symbol:str
    payload:Any

class GateStream(Protocol):
    def events(self)->AsyncIterator[GateStreamEvent]: ...

__all__=["GateStream","GateStreamEvent"]
