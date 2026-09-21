"""Static display horizons for production signals."""

from __future__ import annotations


TIMEFRAME_HOURS = {
    "1m": 0.1, "5m": 0.5, "15m": 4, "30m": 8,
    "1h": 12, "2h": 24, "4h": 48,
    "1d": 336, "3d": 360, "1w": 720, "1M": 2880,
}


def get_estimated_time(_symbol: str, timeframe: str) -> tuple[float, str, int]:
    """Return a static UI horizon, never a virtual-performance estimate."""
    return TIMEFRAME_HOURS.get(timeframe, 24), "нет live-выборки", 0


__all__ = ["TIMEFRAME_HOURS", "get_estimated_time"]
