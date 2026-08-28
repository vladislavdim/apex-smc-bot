"""DeepBlueAlpha public token-level whale flow adapter.

The upstream sample repository documents ``/api/top-tokens`` as a free,
unauthenticated endpoint.  It returns only the twenty most active tokens in a
window, so absence is represented as ``no_pair_data`` rather than a neutral
zero.  No private endpoint is guessed from an API key.
"""

from __future__ import annotations

import asyncio
from .cache import cache
from .http_client import http_client
from .models import number

SOURCE = "deepbluealpha"
PUBLIC = "https://deepbluealpha.io"


def _row_for(payload: object, symbol: str) -> dict:
    if not isinstance(payload, list):
        return {}
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    return next(
        (
            row for row in payload
            if isinstance(row, dict)
            and str(row.get("token_symbol", "")).upper() == base.upper()
        ),
        {},
    )


async def collect(symbol: str) -> dict:
    async def fetch_windows():
        one_hour, day = await asyncio.gather(
            http_client.get_json(f"{PUBLIC}/api/top-tokens", {"tf": "1H"}),
            http_client.get_json(f"{PUBLIC}/api/top-tokens", {"tf": "24H"}),
        )
        return {"1h": one_hour, "24h": day, "tier": "public_token_flows"}

    try:
        # Both windows are market-wide.  One global cache prevents a scan of 60
        # pairs from making 120 duplicate HTTP requests.
        payload, status, age = await cache.get_or_fetch(
            f"{SOURCE}:top_tokens", 900, 3600, fetch_windows
        )
        row_1h = _row_for(payload.get("1h"), symbol)
        row_24h = _row_for(payload.get("24h"), symbol)
        if not row_1h and not row_24h:
            return {
                "source": SOURCE,
                "status": "no_pair_data",
                "age_seconds": age,
                "scope": "token_level_top20",
                "symbol": symbol,
            }
        return {
            "source": SOURCE,
            "status": status,
            "age_seconds": age,
            "scope": "token_level_top20",
            "payload": {"1h": row_1h, "24h": row_24h, "tier": "public"},
        }
    except Exception as exc:
        return {
            "source": SOURCE,
            "status": "unavailable",
            "error": type(exc).__name__,
            "scope": "token_level_top20",
            "symbol": symbol,
        }


def normalize(result: dict) -> dict:
    if result.get("status") not in {"fresh", "cached", "stale_fallback"}:
        return result
    payload = result.get("payload") if isinstance(result.get("payload"), dict) else {}
    row_1h = payload.get("1h") if isinstance(payload.get("1h"), dict) else {}
    row_24h = payload.get("24h") if isinstance(payload.get("24h"), dict) else {}
    primary = row_1h or row_24h
    buy = number(primary.get("buy_vol"))
    sell = number(primary.get("sell_vol"))
    volume = number(primary.get("volume"))
    count = number(primary.get("txn_count"))
    buy_24h = number(row_24h.get("buy_vol"))
    sell_24h = number(row_24h.get("sell_vol"))
    return {
        **result,
        "normalized": {
            "buy_usd": buy,
            "sell_usd": sell,
            "volume_usd": volume,
            "transaction_count": int(count) if count is not None else None,
            "buy_24h_usd": buy_24h,
            "sell_24h_usd": sell_24h,
            "top_wallets": [],
            "method": "public_top20_token_flow",
        },
    }
