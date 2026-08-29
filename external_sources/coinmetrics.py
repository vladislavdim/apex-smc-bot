"""Keyless Coin Metrics Community on-chain activity context."""

from __future__ import annotations

import json
import os
from typing import Any

from .cache import cache
from .http_client import http_client
from .models import number


SOURCE = "coinmetrics_community"
_URL = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
_METRICS = ("AdrActCnt", "TxCnt", "FeeTotNtv", "TxTfrValAdjUSD")


def _asset(symbol: str) -> str:
    base = symbol.upper().replace("/", "").removesuffix("USDT").lower()
    try:
        mapping = json.loads(os.environ.get("COINMETRICS_ASSET_MAP_JSON", "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        mapping = {}
    return str(mapping.get(symbol.upper().replace("/", ""), base)) if isinstance(mapping, dict) else base


def _change(current: float | None, previous: float | None) -> float | None:
    return round((current - previous) / previous * 100, 3) if current is not None and previous else None


def normalize(payload: dict[str, Any], asset: str) -> dict[str, Any] | None:
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    rows = [row for row in rows if isinstance(row, dict)]
    if not rows:
        return None
    rows.sort(key=lambda row: str(row.get("time", "")))
    current, previous = rows[-1], rows[-2] if len(rows) >= 2 else {}
    result: dict[str, Any] = {"asset": asset, "time": current.get("time"), "method": "daily_onchain_activity_not_trade_direction"}
    names = {
        "AdrActCnt": "active_addresses", "TxCnt": "transaction_count",
        "FeeTotNtv": "fees_native", "TxTfrValAdjUSD": "adjusted_transfer_usd",
    }
    for metric, name in names.items():
        value, old = number(current.get(metric)), number(previous.get(metric))
        result[name] = value
        result[f"{name}_change_1d_pct"] = _change(value, old)
    return result


async def collect(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper().replace("/", "")
    asset = _asset(symbol)

    async def fetch() -> Any:
        return await http_client.get_json(_URL, {
            "assets": asset, "metrics": ",".join(_METRICS),
            "frequency": "1d", "page_size": 2, "sort": "time",
        })

    try:
        payload, status, age = await cache.get_or_fetch(f"{SOURCE}:{asset}", 1800, 21600, fetch)
        normalized = normalize(payload, asset)
        if not normalized:
            return {"source": SOURCE, "status": "no_pair_data", "symbol": symbol}
        return {"source": SOURCE, "status": status, "age_seconds": age, "symbol": symbol, "normalized": normalized}
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
