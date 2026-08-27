"""DeepBlueAlpha adapter: public Ethereum index now, paid API only when configured."""

from __future__ import annotations

import os
from .cache import cache
from .http_client import http_client

SOURCE = "deepbluealpha"
PUBLIC = "https://deepbluealpha.io/api/v1/public"


async def collect(symbol: str) -> dict:
    # The free endpoints are Ethereum-wide. They are never presented as
    # coin-specific evidence for non-ETH assets.
    if symbol != "ETHUSDT":
        return {"source": SOURCE, "status": "unsupported_pair", "scope": "ethereum_market", "symbol": symbol}
    key = os.getenv("SMART_MONEY_API_KEY")
    base = os.getenv("SMART_MONEY_API_URL", "").rstrip("/")
    async def fetch():
        if key and base:
            return await http_client.get_json(f"{base}/whale-index", headers={"X-API-Key": key})
        index, stats = await __import__("asyncio").gather(
            http_client.get_json(f"{PUBLIC}/whale-index"),
            http_client.get_json(f"{PUBLIC}/stats"),
        )
        return {"index": index, "stats": stats, "tier": "public"}
    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:{symbol}", 900, 3600, fetch)
        return {"source": SOURCE, "status": status, "age_seconds": age, "scope": "ethereum_market", "payload": payload}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "scope": "ethereum_market", "symbol": symbol}
