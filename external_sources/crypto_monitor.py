"""Adapter for a user-deployed duolaAmengweb3/crypto-monitor instance."""

from __future__ import annotations

import os
from typing import Any

from .cache import cache
from .http_client import http_client
from .models import number


SOURCE = "crypto_monitor"


def _base_url() -> str | None:
    return os.getenv("CRYPTO_MONITOR_API_URL", "").rstrip("/") or None


async def collect(symbol: str) -> dict[str, Any]:
    base = _base_url()
    if not base:
        return {"source": SOURCE, "status": "not_configured", "symbol": symbol}
    key = os.getenv("CRYPTO_MONITOR_API_KEY")
    headers = {"X-API-Key": key} if key else None

    async def fetch() -> dict[str, Any]:
        # These are upstream documented endpoints. Filtering remains local because
        # upstream response shapes differ between deployed versions.
        market, oi, liquidation, orders = await __import__("asyncio").gather(
            http_client.get_json(f"{base}/api/market", headers=headers),
            http_client.get_json(f"{base}/api/oi/overview", headers=headers),
            http_client.get_json(f"{base}/api/liquidation/overview", headers=headers),
            http_client.get_json(f"{base}/api/large-orders/active", headers=headers),
        )
        return {"market": market, "oi": oi, "liquidation": liquidation, "orders": orders}

    try:
        payload, cache_status, age = await cache.get_or_fetch(f"{SOURCE}:{symbol}", 90, 600, fetch)
        return {"source": SOURCE, "status": cache_status, "age_seconds": age, "payload": payload}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}


def normalize(result: dict[str, Any], symbol: str) -> dict[str, Any]:
    """Conservative normalizer for documented but version-variable upstream payloads."""
    if result.get("status") not in {"fresh", "cached", "stale_fallback"}:
        return result
    data = result["payload"]
    def rows(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, list): return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict): return [x for x in value.get("data", value.get("items", []) ) if isinstance(x, dict)]
        return []
    match = lambda items: next((x for x in items if str(x.get("symbol", x.get("contract", ""))).replace("_", "") == symbol), {})
    oi = match(rows(data["oi"]))
    liq = match(rows(data["liquidation"]))
    orders = [x for x in rows(data["orders"]) if str(x.get("symbol", "")).replace("_", "") == symbol]
    buy = sum(number(x.get("value_usd", x.get("value", 0))) or 0 for x in orders if str(x.get("side", "")).lower() == "buy")
    sell = sum(number(x.get("value_usd", x.get("value", 0))) or 0 for x in orders if str(x.get("side", "")).lower() == "sell")
    return {**result, "normalized": {"oi": oi, "liq": liq, "buy": buy or None, "sell": sell or None}}
