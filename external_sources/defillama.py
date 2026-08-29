"""Slow global crypto regime context from public DefiLlama endpoints."""
from __future__ import annotations
import asyncio
from .cache import cache
from .http_client import http_client
from .models import number

SOURCE = "defillama"

def _pct(new, old):
    return round((new - old) / old * 100, 3) if new is not None and old else None

async def collect(symbol: str) -> dict:
    async def fetch():
        stable, dexs, oi = await asyncio.gather(
            http_client.get_json("https://stablecoins.llama.fi/stablecoincharts/all"),
            http_client.get_json("https://api.llama.fi/overview/dexs"),
            http_client.get_json("https://api.llama.fi/overview/open-interest"))
        return {"stable": stable, "dexs": dexs, "oi": oi}
    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:global", 1800, 21600, fetch)
        stable = payload.get("stable", []) if isinstance(payload, dict) else []
        supplies = [number(row.get("totalCirculatingUSD", {}).get("peggedUSD")) for row in stable
                    if isinstance(row, dict) and isinstance(row.get("totalCirculatingUSD"), dict)]
        supplies = [value for value in supplies if value is not None]
        current = supplies[-1] if supplies else None
        dexs, oi = payload.get("dexs", {}), payload.get("oi", {})
        return {"source": SOURCE, "status": status, "age_seconds": age, "symbol": symbol,
                "normalized": {"stablecoin_change_1d_pct": _pct(current, supplies[-2] if len(supplies) >= 2 else None),
                "stablecoin_change_7d_pct": _pct(current, supplies[-8] if len(supplies) >= 8 else None),
                "dex_volume_24h_usd": number(dexs.get("total24h")) if isinstance(dexs, dict) else None,
                "open_interest_usd": number(oi.get("total24h")) if isinstance(oi, dict) else None,
                "method": "global_slow_regime_not_directional"}}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
