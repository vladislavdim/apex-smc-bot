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
        gate_contract = f"{alternative[:-4]}_USDT"
        premium, oi_1h, oi_4h, depth, bybit, gate_stats, gate_contract_info, gate_depth = await asyncio.gather(
            _safe(http_client.get_json(f"{binance}/fapi/v1/premiumIndex", {"symbol": symbol})),
            _safe(http_client.get_json(f"{binance}/futures/data/openInterestHist", {"symbol": symbol, "period": "1h", "limit": 2})),
            _safe(http_client.get_json(f"{binance}/futures/data/openInterestHist", {"symbol": symbol, "period": "4h", "limit": 2})),
            _safe(http_client.get_json(f"{binance}/fapi/v1/depth", {"symbol": symbol, "limit": 20})),
            _safe(http_client.get_json("https://api.bybit.com/v5/market/tickers", {"category": "linear", "symbol": alternative})),
            _safe(http_client.get_json("https://api.gateio.ws/api/v4/futures/usdt/contract_stats", {"contract": gate_contract, "limit": 2})),
            _safe(http_client.get_json(f"https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/{gate_contract}")),
            _safe(http_client.get_json("https://api.gateio.ws/api/v4/futures/usdt/order_book", {"contract": gate_contract, "limit": 20})),
        )
        return {"premium": premium, "oi_1h": oi_1h, "oi_4h": oi_4h, "depth": depth, "bybit": bybit, "gate": gate_stats, "gate_contract": gate_contract_info, "gate_depth": gate_depth}
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
    gate_rows = data["gate"] if isinstance(data["gate"], list) else []
    gate = gate_rows[-1] if gate_rows else {}
    gate_previous = gate_rows[-2] if len(gate_rows) >= 2 else {}
    gate_oi = number(gate.get("open_interest"))
    gate_old_oi = number(gate_previous.get("open_interest"))
    gate_change = round((gate_oi - gate_old_oi) / gate_old_oi * 100, 4) if gate_old_oi and gate_oi is not None else None
    gate_info = data.get("gate_contract") if isinstance(data.get("gate_contract"), dict) else {}
    gate_depth = data.get("gate_depth") if isinstance(data.get("gate_depth"), dict) else {}
    gate_buy = sum((number(p) or 0) * (number(q) or 0) for p, q in gate_depth.get("bids", []))
    gate_sell = sum((number(p) or 0) * (number(q) or 0) for p, q in gate_depth.get("asks", []))
    return {**result, "normalized": {
        "oi": gate_oi if gate_oi is not None else oi, "oi_1h": gate_change if gate_change is not None else c1, "oi_4h": c4,
        "funding": number(gate_info.get("funding_rate")) if gate_info else (number(premium.get("lastFundingRate")) if premium else number(bybit.get("fundingRate"))),
        "buy": gate_buy or buy or None, "sell": gate_sell or sell or None,
        "long_liq": number(gate.get("long_liq_size")) or 0, "short_liq": number(gate.get("short_liq_size")) or 0,
    }}
