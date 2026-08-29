"""Public Hyperliquid positioning context; never creates trade levels."""
from __future__ import annotations
from .cache import cache
from .http_client import http_client
from .models import number
from .pair_registry import get_pair

SOURCE = "hyperliquid"

async def collect(symbol: str) -> dict:
    pair = get_pair(symbol)
    if not pair.get("hyperliquid_supported"):
        return {"source": SOURCE, "status": "unsupported_pair", "symbol": symbol}
    async def fetch():
        return await http_client.post_json("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"})
    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:meta", 90, 600, fetch)
        universe = payload[0].get("universe", []) if isinstance(payload, list) and payload else []
        contexts = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
        target = str(pair.get("hyperliquid_symbol"))
        for index, meta in enumerate(universe):
            if isinstance(meta, dict) and meta.get("name") == target and index < len(contexts):
                row = contexts[index] if isinstance(contexts[index], dict) else {}
                mark, units = number(row.get("markPx")), number(row.get("openInterest"))
                return {"source": SOURCE, "status": status, "age_seconds": age, "symbol": symbol,
                        "normalized": {"oi": units * mark if units is not None and mark is not None else None,
                        "funding": number(row.get("funding")), "volume_24h_usd": number(row.get("dayNtlVlm")), "mark_price": mark}}
        return {"source": SOURCE, "status": "unsupported_pair", "symbol": symbol}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
