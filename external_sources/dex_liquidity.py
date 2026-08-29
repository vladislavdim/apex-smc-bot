"""Verified-contract DEX liquidity risk from the public DEX Screener API."""

from __future__ import annotations

from typing import Any

from .cache import cache
from .http_client import http_client
from .models import number
from .pair_registry import get_pair


SOURCE = "dexscreener"
_CHAIN_IDS = {
    "ethereum": "ethereum", "eth": "ethereum", "solana": "solana",
    "base": "base", "arbitrum": "arbitrum", "bsc": "bsc",
    "binance-smart-chain": "bsc", "polygon": "polygon",
}
_STABLES = {"USDT", "USDC", "USD", "DAI", "FDUSD"}


def normalize(rows: list[dict[str, Any]], address: str) -> dict[str, Any] | None:
    verified = address.lower()
    candidates = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        base, quote = row.get("baseToken", {}), row.get("quoteToken", {})
        if not isinstance(base, dict) or not isinstance(quote, dict):
            continue
        base_match = str(base.get("address", "")).lower() == verified
        quote_match = str(quote.get("address", "")).lower() == verified
        counter = str(quote.get("symbol", "")).upper() if base_match else str(base.get("symbol", "")).upper() if quote_match else ""
        if counter not in _STABLES:
            continue
        liquidity = number((row.get("liquidity") or {}).get("usd")) if isinstance(row.get("liquidity"), dict) else None
        if liquidity is not None:
            candidates.append((liquidity, row))
    if not candidates:
        return None
    liquidity, row = max(candidates, key=lambda item: item[0])
    volume = row.get("volume", {}) if isinstance(row.get("volume"), dict) else {}
    txns = row.get("txns", {}).get("h24", {}) if isinstance(row.get("txns"), dict) else {}
    changes = row.get("priceChange", {}) if isinstance(row.get("priceChange"), dict) else {}
    risk = "critical" if liquidity < 100_000 else "thin" if liquidity < 500_000 else "adequate"
    return {
        "chain": row.get("chainId"), "dex": row.get("dexId"), "pair_address": row.get("pairAddress"),
        "liquidity_usd": liquidity, "volume_24h_usd": number(volume.get("h24")),
        "buys_24h": number(txns.get("buys")), "sells_24h": number(txns.get("sells")),
        "price_change_1h_pct": number(changes.get("h1")), "price_change_24h_pct": number(changes.get("h24")),
        "liquidity_risk": risk, "method": "verified_contract_liquidity_risk_not_trade_direction",
    }


async def collect(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper().replace("/", "")
    pair = get_pair(symbol)
    address = str(pair.get("contract_address") or "").strip()
    chain = _CHAIN_IDS.get(str(pair.get("chain") or "").lower())
    if not address:
        return {"source": SOURCE, "status": "not_configured", "symbol": symbol}
    if not chain:
        return {"source": SOURCE, "status": "unsupported_chain", "symbol": symbol}

    async def fetch() -> Any:
        return await http_client.get_json(f"https://api.dexscreener.com/token-pairs/v1/{chain}/{address}")

    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:{chain}:{address.lower()}", 300, 1800, fetch)
        normalized = normalize(payload, address)
        if not normalized:
            return {"source": SOURCE, "status": "no_pair_data", "symbol": symbol}
        return {"source": SOURCE, "status": status, "age_seconds": age, "symbol": symbol, "normalized": normalized}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
