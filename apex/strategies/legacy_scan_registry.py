"""Bounded registry for the canonical MTF scanner during legacy cutover."""

from __future__ import annotations

import logging
from typing import Callable


_raw_scan_handler: Callable | None = None


def register_raw_scan_handler(handler) -> None:
    """Register the canonical candidate builder used by every legacy caller."""
    global _raw_scan_handler
    _raw_scan_handler = handler


def raw_scan_handler_registered() -> bool:
    return callable(_raw_scan_handler)


def run_raw_scan(symbol: str, timeframe: str, passive_watch: bool = False):
    """Run the registered scanner, or return ``None`` before composition."""
    handler = _raw_scan_handler
    if not callable(handler):
        return None
    return handler(symbol, timeframe, passive_watch)


def analyze_trade_type(symbol: str, trade_type: str = "swing"):
    """Map the conversational trade type to the canonical MTF scanner."""
    if raw_scan_handler_registered():
        timeframe_by_type = {"scalp": "15m", "swing": "1h", "long": "4h"}
        return run_raw_scan(
            symbol, timeframe_by_type.get(trade_type, "1h"), False,
        )
    logging.warning(
        "analyze_trade_type disabled until canonical scan handler is registered"
    )
    return None


def full_scan(symbol: str, timeframe: str = "1h"):
    """Preserve the legacy facade while delegating to the canonical scanner."""
    if raw_scan_handler_registered():
        return run_raw_scan(symbol, timeframe, False)
    logging.warning("full_scan disabled until canonical scan handler is registered")
    return None


__all__ = [
    "analyze_trade_type",
    "full_scan",
    "raw_scan_handler_registered",
    "register_raw_scan_handler",
    "run_raw_scan",
]
