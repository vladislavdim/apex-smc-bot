"""Gate WebSocket boundary metadata.

The production HTTP candle path remains authoritative until the websocket
consumer is explicitly wired into RuntimeSupervisor.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class GateWsConfig:
    public_url: str = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    source: str = "GATE"
    execution_allowed: bool = False


__all__ = ["GateWsConfig"]
