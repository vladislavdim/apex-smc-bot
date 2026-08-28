"""Adapter for a user-deployed duolaAmengweb3/crypto-monitor instance."""

from __future__ import annotations

import os
import asyncio
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

    upstream_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol

    async def optional(url: str, params: dict[str, Any] | None = None) -> Any:
        try:
            return await http_client.get_json(url, params=params, headers=headers)
        except Exception:
            # A deployment may legitimately have CoinAnk liquidations disabled
            # while its exchange-derived OI/funding/order data still works.
            return None

    async def fetch() -> dict[str, Any]:
        # Exact routes and response fields are defined by upstream backend/main.py.
        # The detail endpoint is the only documented per-symbol endpoint exposing
        # OI changes and funding; overview endpoints are market-wide aggregates.
        detail, liquidation, orders = await asyncio.gather(
            optional(f"{base}/api/monitor/detail/{upstream_symbol}"),
            optional(
                f"{base}/api/liquidation/orders",
                {"symbol": upstream_symbol, "limit": 100},
            ),
            optional(
                f"{base}/api/large-orders/active",
                {"symbol": upstream_symbol, "min_value": 0},
            ),
        )
        payload = {"detail": detail, "liquidation": liquidation, "orders": orders}
        if not any(value is not None for value in payload.values()):
            raise ValueError("all crypto-monitor components unavailable")
        return payload

    try:
        payload, cache_status, age = await cache.get_or_fetch(f"{SOURCE}:{symbol}", 90, 600, fetch)
        return {"source": SOURCE, "status": cache_status, "age_seconds": age, "payload": payload}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}


def normalize(result: dict[str, Any], symbol: str) -> dict[str, Any]:
    """Normalize the documented crypto-monitor REST payloads for one pair."""
    if result.get("status") not in {"fresh", "cached", "stale_fallback"}:
        return result
    data = result.get("payload") or {}

    def rows(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            for key in ("orders", "data", "items", "transactions", "results"):
                candidate = value.get(key)
                if isinstance(candidate, list):
                    return [x for x in candidate if isinstance(x, dict)]
        return []

    def canonical(value: Any) -> str:
        normalized = str(value or "").upper().replace("_", "").replace("/", "")
        return normalized if normalized.endswith("USDT") else f"{normalized}USDT"

    detail = data.get("detail") if isinstance(data.get("detail"), dict) else {}
    # Some deployments key their DataCenter by BTC while others return BTCUSDT.
    if detail and canonical(detail.get("symbol")) != symbol:
        detail = {}

    liquidations = [
        row for row in rows(data.get("liquidation"))
        if not row.get("symbol") or canonical(row.get("symbol")) == symbol
    ]
    orders = [
        row for row in rows(data.get("orders"))
        if not row.get("symbol") or canonical(row.get("symbol")) == symbol
    ]
    buy = sum(number(x.get("value_usd", x.get("value", 0))) or 0 for x in orders if str(x.get("side", "")).lower() == "buy")
    sell = sum(number(x.get("value_usd", x.get("value", 0))) or 0 for x in orders if str(x.get("side", "")).lower() == "sell")
    long_liq = sum(
        number(x.get("value", x.get("value_usd", 0))) or 0
        for x in liquidations if str(x.get("side", "")).lower() == "long"
    )
    short_liq = sum(
        number(x.get("value", x.get("value_usd", 0))) or 0
        for x in liquidations if str(x.get("side", "")).lower() == "short"
    )
    return {
        **result,
        "normalized": {
            "oi": number(detail.get("open_interest", detail.get("total_oi"))),
            "oi_1h": number(detail.get("oi_change_1h")),
            "oi_4h": number(detail.get("oi_change_4h")),
            "price_change_1h": number(detail.get("price_change_1h")),
            "funding": number(detail.get("funding_rate")),
            "long_liq": long_liq,
            "short_liq": short_liq,
            "buy": buy or None,
            "sell": sell or None,
            "order_flow_method": "upstream_active_large_orders",
        },
        "components": {
            "detail": bool(detail),
            "liquidations": data.get("liquidation") is not None,
            "large_orders": data.get("orders") is not None,
        },
    }
