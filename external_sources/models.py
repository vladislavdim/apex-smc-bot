"""Normalization helpers. They intentionally contain no trading-level calculations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def field(value: Any = None, source: str | None = None, status: str = "unknown", age_seconds: int | None = None) -> dict[str, Any]:
    return {"value": value, "source": source, "status": status, "age_seconds": age_seconds}


def empty_context(symbol: str) -> dict[str, Any]:
    return {
        "symbol": symbol.upper(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "open_interest": {"value": None, "change_1h_pct": None, "change_4h_pct": None, "trend": "unknown", "source": None},
        "funding": {"rate": None, "extreme": False, "bias": "neutral", "source": None},
        "liquidations": {"long_usd": 0, "short_usd": 0, "dominance": "unknown", "source": None},
        "large_orders": {"buy_pressure": None, "sell_pressure": None, "bias": "unknown", "source": None},
        "exchange_flow": {"inflow_usd": None, "outflow_usd": None, "bias": "unknown", "source": None},
        "whale_activity": {"buy_usd": None, "sell_usd": None, "bias": "unknown", "confidence": 0, "source": None},
        "smart_money": {"bias": "unknown", "confidence": 0, "top_wallets": [], "source": None},
        "data_quality": {"available_sources": [], "failed_sources": [], "age_seconds": None, "source_status": {}},
        "conflicts": [],
        "external_bias": "unknown",
        "external_confidence": 0,
    }
