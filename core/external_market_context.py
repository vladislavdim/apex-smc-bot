"""External derivatives and smart-money context for completed APEX candidates.

This module is intentionally isolated from strategy code.  It never creates a
signal and never calculates or changes entry, stop-loss, take-profit or RR.
Public endpoints are best-effort: one unavailable provider must not break the
scan loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SECONDS = 120
_HTTP_TIMEOUT_SECONDS = 7.0


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_json_sync(url: str, params: dict | None = None) -> Any:
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "APEX-SMC-Bot/market-context"})
    with urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


async def _get_json(url: str, params: dict | None = None) -> Any:
    return await asyncio.to_thread(_get_json_sync, url, params)


async def _binance_context(symbol: str) -> dict[str, Any]:
    """Collect public Binance Futures positioning data."""
    base = "https://fapi.binance.com"
    premium, oi_now, oi_hist, ratio = await asyncio.gather(
        _get_json(f"{base}/fapi/v1/premiumIndex", {"symbol": symbol}),
        _get_json(f"{base}/fapi/v1/openInterest", {"symbol": symbol}),
        _get_json(
            f"{base}/futures/data/openInterestHist",
            {"symbol": symbol, "period": "5m", "limit": 2},
        ),
        _get_json(
            f"{base}/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": "5m", "limit": 1},
        ),
    )

    oi_change_pct = None
    if isinstance(oi_hist, list) and len(oi_hist) >= 2:
        old = _number(oi_hist[-2].get("sumOpenInterestValue"))
        new = _number(oi_hist[-1].get("sumOpenInterestValue"))
        if old and new is not None:
            oi_change_pct = round((new - old) / old * 100, 4)

    ratio_row = ratio[-1] if isinstance(ratio, list) and ratio else {}
    return {
        "source": "binance_futures_public",
        "funding_rate": _number(premium.get("lastFundingRate")) if isinstance(premium, dict) else None,
        "mark_price": _number(premium.get("markPrice")) if isinstance(premium, dict) else None,
        "open_interest_contracts": _number(oi_now.get("openInterest")) if isinstance(oi_now, dict) else None,
        "open_interest_value_change_5m_pct": oi_change_pct,
        "global_long_short_ratio": _number(ratio_row.get("longShortRatio")),
        "long_account_pct": _number(ratio_row.get("longAccount")),
        "short_account_pct": _number(ratio_row.get("shortAccount")),
    }


async def _bybit_context(symbol: str) -> dict[str, Any]:
    """Collect an independent public derivatives snapshot from Bybit."""
    payload = await _get_json(
        "https://api.bybit.com/v5/market/tickers",
        {"category": "linear", "symbol": symbol},
    )
    rows = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
    row = rows[0] if rows else {}
    return {
        "source": "bybit_linear_public",
        "funding_rate": _number(row.get("fundingRate")),
        "open_interest_contracts": _number(row.get("openInterest")),
        "open_interest_value": _number(row.get("openInterestValue")),
        "turnover_24h": _number(row.get("turnover24h")),
        "volume_24h": _number(row.get("volume24h")),
        "price_change_24h_pct": (
            round((_number(row.get("price24hPcnt")) or 0.0) * 100, 4)
            if row.get("price24hPcnt") is not None
            else None
        ),
    }


async def _gate_context(symbol: str) -> dict[str, Any]:
    """Collect Gate.io public OI, taker sentiment and liquidation sizes."""
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    contract = f"{base}_USDT"
    payload = await _get_json(
        "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
        {"contract": contract, "limit": 2},
    )
    rows = payload if isinstance(payload, list) else []
    row = rows[-1] if rows else {}
    previous = rows[-2] if len(rows) >= 2 else {}
    current_oi = _number(row.get("open_interest"))
    previous_oi = _number(previous.get("open_interest"))
    oi_change_pct = None
    if previous_oi and current_oi is not None:
        oi_change_pct = round((current_oi - previous_oi) / previous_oi * 100, 4)
    return {
        "source": "gateio_futures_public",
        "contract": contract,
        "open_interest_contracts": current_oi,
        "open_interest_change_pct": oi_change_pct,
        "long_short_account_ratio": _number(row.get("lsr_account")),
        "long_short_taker_ratio": _number(row.get("lsr_taker")),
        "long_liquidation_size": _number(row.get("long_liq_size")),
        "short_liquidation_size": _number(row.get("short_liq_size")),
    }


async def _deepblue_context() -> dict[str, Any]:
    """Use only DeepBlueAlpha's documented free public aggregate endpoints."""
    index, stats = await asyncio.gather(
        _get_json("https://deepbluealpha.io/api/v1/public/whale-index"),
        _get_json("https://deepbluealpha.io/api/v1/public/stats"),
    )
    return {
        "source": "deepbluealpha_public",
        "scope": "ethereum_market",
        "whale_index": index,
        "stats": stats,
    }


async def collect_external_market_context(symbol: str) -> dict[str, Any]:
    """Return cached, best-effort context without ever raising to a scanner."""
    normalized = (symbol or "").upper().replace("/", "")
    now = time.time()
    cached = _CACHE.get(normalized)
    if cached and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    jobs: list[tuple[str, Any]] = [
        ("binance", _binance_context(normalized)),
        ("bybit", _bybit_context(normalized)),
        ("gateio", _gate_context(normalized)),
    ]
    # DeepBlue's public index is Ethereum-wide, so it is useful only for
    # ETH candidates.  Do not present it to Groq as coin-specific data.
    if normalized.startswith("ETH"):
        jobs.append(("deepbluealpha", _deepblue_context()))

    results = await asyncio.gather(*(job for _, job in jobs), return_exceptions=True)

    providers: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for (name, _), result in zip(jobs, results):
        if isinstance(result, Exception):
            errors[name] = type(result).__name__
            logging.debug("[ExternalContext] %s %s unavailable: %s", normalized, name, result)
        else:
            providers[name] = result

    context = {
        "symbol": normalized,
        "collected_at": int(now),
        "providers": providers,
        "available_sources": sorted(providers),
        "unavailable_sources": errors,
    }
    _CACHE[normalized] = (now, context)
    return context
