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
    return {"value": value, "source": source, "status": status, "age_seconds": age_seconds, "source_values": {}}


def empty_context(symbol: str) -> dict[str, Any]:
    meta = {"source": None, "status": "unknown", "age_seconds": None}
    return {
        "symbol": symbol.upper(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "open_interest": {"value": None, "change_1h_pct": None, "change_4h_pct": None, "trend": "unknown", **meta, "source_values": {}},
        "funding": {"rate": None, "extreme": False, "bias": "neutral", **meta, "source_values": {}},
        "liquidations": {"long_usd": 0, "short_usd": 0, "dominance": "unknown", **meta, "source_values": {}},
        "large_orders": {"buy_pressure": None, "sell_pressure": None, "bias": "unknown", "method": None, **meta, "source_values": {}},
        "exchange_flow": {"inflow_usd": None, "outflow_usd": None, "bias": "unknown", **meta, "source_values": {}},
        "whale_activity": {"buy_usd": None, "sell_usd": None, "bias": "unknown", "confidence": 0, **meta, "source_values": {}},
        "smart_money": {"buy_usd": None, "sell_usd": None, "bias": "unknown", "confidence": 0, "top_wallets": [], "method": None, **meta, "source_values": {}},
        "live_tape": {
            "buy_usd_60s": 0.0, "sell_usd_60s": 0.0,
            "long_liq_usd_300s": 0.0, "short_liq_usd_300s": 0.0,
            "bias": "unknown", "sources": [], "age_seconds": None,
            "status": "unknown", "source_values": {},
        },
        "onchain_activity": {
            "btc_large_transfers_usd": None, "btc_large_transfer_count": 0,
            "oli_labels": [], "status": "unknown", "sources": [], "age_seconds": None,
        },
        "slow_regime": {
            "stablecoin_change_1d_pct": None, "stablecoin_change_7d_pct": None,
            "dex_volume_24h_usd": None, "open_interest_usd": None,
            "status": "unknown", "source": None, "age_seconds": None,
        },
        "options_context": {
            "underlying": None, "underlying_price": None, "contracts": 0,
            "calls_open_interest": None, "puts_open_interest": None,
            "put_call_oi_ratio": None, "put_call_volume_ratio": None,
            "positioning": "unknown", "dvol": None, "dvol_change_1h": None,
            "method": None, "status": "unknown", "source": None,
            "age_seconds": None,
        },
        "network_activity": {
            "asset": None, "time": None, "active_addresses": None,
            "active_addresses_change_1d_pct": None, "transaction_count": None,
            "transaction_count_change_1d_pct": None, "fees_native": None,
            "fees_native_change_1d_pct": None, "adjusted_transfer_usd": None,
            "adjusted_transfer_usd_change_1d_pct": None, "method": None,
            "status": "unknown", "source": None, "age_seconds": None,
        },
        "dex_liquidity": {
            "chain": None, "dex": None, "pair_address": None,
            "liquidity_usd": None, "volume_24h_usd": None,
            "buys_24h": None, "sells_24h": None,
            "price_change_1h_pct": None, "price_change_24h_pct": None,
            "liquidity_risk": "unknown", "method": None,
            "status": "unknown", "source": None, "age_seconds": None,
        },
        "pair_coverage": {},
        "data_quality": {
            "available_sources": [], "failed_sources": [], "age_seconds": None,
            "source_status": {}, "source_ages": {},
        },
        "conflicts": [],
        "external_bias": "unknown",
        "external_confidence": 0,
    }
