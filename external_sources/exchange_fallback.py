"""Keyless public-exchange data for supported USDT perpetual pairs."""

from __future__ import annotations

import asyncio
from .cache import cache
from .http_client import http_client
from .models import number

SOURCE = "public_futures"

# Exchanges list some low-price assets in scaled contracts.  Keep the APEX
# symbol unchanged externally; only the provider request is translated.
_SCALED = {"PEPEUSDT": "1000PEPEUSDT", "SHIBUSDT": "1000SHIBUSDT", "BONKUSDT": "1000BONKUSDT", "FLOKIUSDT": "1000FLOKIUSDT", "SATSUSDT": "1000000SATSUSDT"}


def _provider_symbol(symbol: str) -> str:
    return _SCALED.get(symbol, symbol)


async def _safe(coro):
    try:
        return await coro
    except Exception:
        return None


async def collect(symbol: str) -> dict:
    async def fetch():
        binance = "https://fapi.binance.com"
        alternative = _provider_symbol(symbol)
        premium, oi_1h, oi_4h, depth, bybit, gate = await asyncio.gather(
            _safe(http_client.get_json(f"{binance}/fapi/v1/premiumIndex", {"symbol": symbol})),
            _safe(http_client.get_json(f"{binance}/futures/data/openInterestHist", {"symbol": symbol, "period": "1h", "limit": 2})),
            _safe(http_client.get_json(f"{binance}/futures/data/openInterestHist", {"symbol": symbol, "period": "4h", "limit": 2})),
            _safe(http_client.get_json(f"{binance}/fapi/v1/depth", {"symbol": symbol, "limit": 20})),
            _safe(http_client.get_json("https://api.bybit.com/v5/market/tickers", {"category": "linear", "symbol": alternative})),
            _safe(http_client.get_json("https://api.gateio.ws/api/v4/futures/usdt/contract_stats", {"contract": f"{alternative[:-4]}_USDT", "limit": 2})),
        )
        return {"premium": premium, "oi_1h": oi_1h, "oi_4h": oi_4h, "depth": depth, "bybit": bybit, "gate": gate}
    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:{symbol}", 90, 600, fetch)
        if not any(value is not None for value in payload.values()):
            return {"source": SOURCE, "status": "unsupported_pair", "symbol": symbol}
        return {"source": SOURCE, "status": status, "age_seconds": age, "payload": payload}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}


def normalize(result: dict) -> dict:
    if result.get("status") not in {"fresh", "cached", "stale_fallback"}:
        return result
    data = result["payload"]
    premium = data["premium"] if isinstance(data["premium"], dict) else {}
    bybit_rows = data.get("bybit", {}).get("result", {}).get("list", []) if isinstance(data.get("bybit"), dict) else []
    bybit = bybit_rows[0] if bybit_rows else {}
    def change(rows):
        if not isinstance(rows, list) or len(rows) < 2: return None, None
        a, b = number(rows[-2].get("sumOpenInterestValue")), number(rows[-1].get("sumOpenInterestValue"))
        return b, round((b-a)/a*100, 4) if a and b is not None else None
    oi, c1 = change(data["oi_1h"])
    _, c4 = change(data["oi_4h"])
    depth = data["depth"] if isinstance(data["depth"], dict) else {}
    buy = sum((number(p) or 0) * (number(q) or 0) for p, q in depth.get("bids", []))
    sell = sum((number(p) or 0) * (number(q) or 0) for p, q in depth.get("asks", []))
    gate = data["gate"][-1] if isinstance(data["gate"], list) and data["gate"] else {}
    return {**result, "normalized": {
        "oi": oi, "oi_1h": c1, "oi_4h": c4,
        "funding": number(premium.get("lastFundingRate")) if premium else number(bybit.get("fundingRate")), "buy": buy or None, "sell": sell or None,
        "long_liq": number(gate.get("long_liq_size")) or 0, "short_liq": number(gate.get("short_liq_size")) or 0,
    }}
