"""Gate websocket market-data boundary.
The concrete transport is injected; strategies never depend on websocket details.
"""
from __future__ import annotations
from typing import Protocol,AsyncIterator,Mapping,Any
class GateStream(Protocol):
    def __aiter__(self)->AsyncIterator[Mapping[str,Any]]: ...
__all__=["GateStream"]
