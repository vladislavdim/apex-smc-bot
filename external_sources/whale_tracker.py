"""Adapter for a user-deployed Arcan17/crypto-whale-tracker instance."""

from __future__ import annotations

import os
from .cache import cache
from .http_client import http_client

SOURCE = "crypto_whale_tracker"


def ethereum_assets() -> set[str]:
    return {"ETHUSDT", "LINKUSDT", "UNIUSDT", "PEPEUSDT", "SHIBUSDT", "AAVEUSDT", "CRVUSDT", "LDOUSDT", "ENAUSDT", "ONDOUSDT"}


async def collect(symbol: str) -> dict:
    if symbol not in ethereum_assets():
        return {"source": SOURCE, "status": "unsupported_pair", "symbol": symbol}
    base = os.getenv("WHALE_TRACKER_API_URL", "").rstrip("/")
    if not base:
        return {"source": SOURCE, "status": "not_configured", "symbol": symbol}
    key = os.getenv("WHALE_TRACKER_API_KEY")
    headers = {"X-API-Key": key} if key else None
    async def fetch():
        return await http_client.get_json(f"{base}/transactions", params={"symbol": symbol.replace("USDT", ""), "limit": 100}, headers=headers)
    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:{symbol}", 600, 3600, fetch)
        return {"source": SOURCE, "status": status, "age_seconds": age, "payload": payload}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
