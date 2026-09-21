"""Keyless public-exchange data for supported USDT perpetual pairs."""

from __future__ import annotations

import asyncio
from .cache import cache
from .http_client import http_client
from .models import number
from .pair_registry import get_pair

SOURCE = "public_futures"

# Exchanges list some low-price assets in scaled contracts.  Keep the APEX
# symbol unchanged externally; only the provider request is translated.
_SCALED = {
    "PEPEUSDT": "1000PEPEUSDT",
    "SHIBUSDT": "1000SHIBUSDT",
    "BONKUSDT": "1000BONKUSDT",
    "FLOKIUSDT": "1000FLOKIUSDT",
    "SATSUSDT": "1000SATSUSDT",
}


def _provider_symbol(symbol: str) -> str:
    return _SCALED.get(symbol, symbol)


async def _safe(coro):
    try:
        return await coro
    except Exception:
        return None


async def _provider_get(supported: bool, url: str, params: dict) -> object | None:
    if not supported:
        return None
    return await _safe(http_client.get_json(url, params))


async def collect(symbol: str) -> dict:
    async def fetch():
        pair = get_pair(symbol)
        gate_contract = str(pair.get("gate_symbol") or f"{symbol[:-4]}_USDT")
        gate_supported = bool(pair.get("gate_supported"))
        gate_1h, gate_4h, gate_contract_info, gate_depth = await asyncio.gather(
            _provider_get(gate_supported, "https://api.gateio.ws/api/v4/futures/usdt/contract_stats", {"contract": gate_contract, "interval": "1h", "limit": 2}),
            _provider_get(gate_supported, "https://api.gateio.ws/api/v4/futures/usdt/contract_stats", {"contract": gate_contract, "interval": "4h", "limit": 2}),
            _provider_get(gate_supported, f"https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/{gate_contract}", {}),
            _provider_get(gate_supported, "https://api.gateio.ws/api/v4/futures/usdt/order_book", {"contract": gate_contract, "limit": 20}),
        )
        return {
            "premium": None, "oi_1h": None, "oi_4h": None,
            "depth": None, "bybit": None, "gate_1h": gate_1h,
            "gate_4h": gate_4h, "gate_contract": gate_contract_info,
            "gate_depth": gate_depth,
        }
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
    data = result.get("payload") or {}
    premium = data.get("premium") if isinstance(data.get("premium"), dict) else {}
    bybit_rows = data.get("bybit", {}).get("result", {}).get("list", []) if isinstance(data.get("bybit"), dict) else []
    bybit = bybit_rows[0] if bybit_rows else {}
    def change(rows):
        if not isinstance(rows, list) or len(rows) < 2: return None, None
        a, b = number(rows[-2].get("sumOpenInterestValue")), number(rows[-1].get("sumOpenInterestValue"))
        return b, round((b-a)/a*100, 4) if a and b is not None else None
    oi, c1 = change(data.get("oi_1h"))
    _, c4 = change(data.get("oi_4h"))
    depth = data.get("depth") if isinstance(data.get("depth"), dict) else {}
    buy = sum((number(p) or 0) * (number(q) or 0) for p, q in depth.get("bids", []))
    sell = sum((number(p) or 0) * (number(q) or 0) for p, q in depth.get("asks", []))

    def gate_change(rows):
        if not isinstance(rows, list) or not rows:
            return None, None, {}
        latest = rows[-1] if isinstance(rows[-1], dict) else {}
        previous = rows[-2] if len(rows) >= 2 and isinstance(rows[-2], dict) else {}
        current_oi = number(latest.get("open_interest"))
        previous_oi = number(previous.get("open_interest"))
        change_pct = (
            round((current_oi - previous_oi) / previous_oi * 100, 4)
            if previous_oi and current_oi is not None else None
        )
        return current_oi, change_pct, latest

    gate_rows = data.get("gate_1h") if isinstance(data.get("gate_1h"), list) else []
    gate_rows_4h = data.get("gate_4h") if isinstance(data.get("gate_4h"), list) else []
    gate = gate_rows[-1] if gate_rows else {}
    gate_oi, gate_change_1h, _ = gate_change(gate_rows)
    _, gate_change_4h, _ = gate_change(gate_rows_4h)
    gate_info = data.get("gate_contract") if isinstance(data.get("gate_contract"), dict) else {}
    gate_depth = data.get("gate_depth") if isinstance(data.get("gate_depth"), dict) else {}

    def depth_notional(rows):
        total = 0.0
        for row in rows if isinstance(rows, list) else []:
            if isinstance(row, dict):
                price, size = number(row.get("p", row.get("price"))), number(row.get("s", row.get("size")))
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                price, size = number(row[0]), number(row[1])
            else:
                continue
            total += (price or 0) * abs(size or 0)
        return total

    gate_buy = depth_notional(gate_depth.get("bids", []))
    gate_sell = depth_notional(gate_depth.get("asks", []))
    gate_bids = gate_depth.get("bids", []) if isinstance(gate_depth.get("bids"), list) else []
    gate_asks = gate_depth.get("asks", []) if isinstance(gate_depth.get("asks"), list) else []

    def best_price(rows, *, bid):
        prices = []
        for row in rows:
            if isinstance(row, dict):
                price = number(row.get("p", row.get("price")))
            elif isinstance(row, (list, tuple)) and row:
                price = number(row[0])
            else:
                price = None
            if price is not None and price > 0:
                prices.append(price)
        return (max(prices) if bid else min(prices)) if prices else None

    best_bid = best_price(gate_bids, bid=True)
    best_ask = best_price(gate_asks, bid=False)
    mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    depth_total = gate_buy + gate_sell
    gate_multiplier = number(gate_info.get("quanto_multiplier")) or 0
    gate_mark = number(gate_info.get("mark_price")) or 0
    liquidation_multiplier = gate_multiplier * gate_mark
    long_liq_size = number(gate.get("long_liq_size"))
    short_liq_size = number(gate.get("short_liq_size"))
    long_liq_usd = number(gate.get("long_liq_usd_new", gate.get("long_liq_usd")))
    short_liq_usd = number(gate.get("short_liq_usd_new", gate.get("short_liq_usd")))

    def ratio(explicit, long_value, short_value):
        direct = number(explicit)
        if direct is not None:
            return direct
        numerator, denominator = number(long_value), number(short_value)
        return numerator / denominator if numerator is not None and denominator else None
    bybit_oi = number(bybit.get("openInterestValue"))
    return {**result, "normalized": {
        "oi": gate_oi if gate_oi is not None else (oi if oi is not None else bybit_oi),
        "oi_1h": gate_change_1h if gate_change_1h is not None else c1,
        "oi_4h": gate_change_4h if gate_change_4h is not None else c4,
        "funding": number(gate_info.get("funding_rate")) if gate_info else (number(premium.get("lastFundingRate")) if premium else number(bybit.get("fundingRate"))),
        "buy": gate_buy or buy or None, "sell": gate_sell or sell or None,
        "long_liq": long_liq_usd if long_liq_usd is not None else (
            long_liq_size * liquidation_multiplier if long_liq_size is not None and liquidation_multiplier else None
        ),
        "short_liq": short_liq_usd if short_liq_usd is not None else (
            short_liq_size * liquidation_multiplier if short_liq_size is not None and liquidation_multiplier else None
        ),
        "long_short_ratio": {
            "accounts": ratio(gate.get("lsr_account"), gate.get("long_users"), gate.get("short_users")),
            "takers": ratio(gate.get("lsr_taker"), gate.get("long_taker_size"), gate.get("short_taker_size")),
            "top_accounts": ratio(gate.get("top_lsr_account"), gate.get("top_long_account"), gate.get("top_short_account")),
            "top_positions": ratio(gate.get("top_lsr_size"), gate.get("top_long_size"), gate.get("top_short_size")),
        },
        "microstructure": {
            "source": "gate_rest", "status": "FRESH_REST_SNAPSHOT",
            "sequence_status": "SNAPSHOT_ONLY", "availability": "FORWARD_ONLY",
            "liquidity_kind": "VISIBLE_ORDERBOOK_LIQUIDITY",
            "hidden_stops_claimed": False, "execution_authority": False,
            "best_bid": best_bid, "best_ask": best_ask,
            "spread_bps": ((best_ask - best_bid) / mid * 10000) if mid and best_ask >= best_bid else None,
            "bid_depth_usd": gate_buy or None, "ask_depth_usd": gate_sell or None,
            "depth_imbalance": ((gate_buy - gate_sell) / depth_total) if depth_total else None,
        },
        "order_flow_method": "top20_orderbook_depth_proxy",
        "liquidation_method": "gate_contract_size_x_multiplier_x_mark",
    }}
