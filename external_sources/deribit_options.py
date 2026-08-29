"""Public BTC/ETH options positioning and volatility from Deribit."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from .cache import cache
from .http_client import http_client
from .models import number


SOURCE = "deribit_options"
_BASE = "https://www.deribit.com/api/v2"
_SUPPORTED = {"BTCUSDT": "BTC", "ETHUSDT": "ETH"}


def _ratio(numerator: float, denominator: float) -> float | None:
    return round(numerator / denominator, 4) if denominator > 0 else None


def normalize(payload: dict[str, Any], symbol: str) -> dict[str, Any]:
    summaries = payload.get("summaries", {}).get("result", [])
    volatility = payload.get("volatility", {}).get("result", {}).get("data", [])
    calls_oi = puts_oi = calls_volume = puts_volume = 0.0
    contracts = 0
    underlying_prices: list[float] = []
    for row in summaries if isinstance(summaries, list) else []:
        if not isinstance(row, dict):
            continue
        name = str(row.get("instrument_name", "")).upper()
        option_type = name.rsplit("-", 1)[-1]
        if option_type not in {"C", "P"}:
            continue
        oi = number(row.get("open_interest")) or 0.0
        volume = number(row.get("volume")) or 0.0
        underlying = number(row.get("underlying_price"))
        if underlying is not None:
            underlying_prices.append(underlying)
        if option_type == "C":
            calls_oi += oi; calls_volume += volume
        else:
            puts_oi += oi; puts_volume += volume
        contracts += 1
    dvol_rows = sorted(
        (row for row in volatility if isinstance(row, list) and len(row) >= 5),
        key=lambda row: number(row[0]) or 0,
    )
    dvol = number(dvol_rows[-1][4]) if dvol_rows else None
    dvol_previous = number(dvol_rows[-2][4]) if len(dvol_rows) >= 2 else None
    dvol_change = round(dvol - dvol_previous, 3) if dvol is not None and dvol_previous is not None else None
    oi_ratio = _ratio(puts_oi, calls_oi)
    positioning = (
        "put_heavy" if oi_ratio is not None and oi_ratio > 1.25
        else "call_heavy" if oi_ratio is not None and oi_ratio < 0.8
        else "balanced" if oi_ratio is not None else "unknown"
    )
    return {
        "underlying": symbol[:-4], "contracts": contracts,
        "calls_open_interest": round(calls_oi, 4),
        "puts_open_interest": round(puts_oi, 4),
        "put_call_oi_ratio": oi_ratio,
        "put_call_volume_ratio": _ratio(puts_volume, calls_volume),
        "positioning": positioning,
        "dvol": dvol, "dvol_change_1h": dvol_change,
        "underlying_price": round(sum(underlying_prices) / len(underlying_prices), 4) if underlying_prices else None,
        "method": "options_positioning_and_expected_volatility_not_trade_direction",
    }


async def collect(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper().replace("/", "")
    currency = _SUPPORTED.get(symbol)
    if not currency:
        return {"source": SOURCE, "status": "unsupported_pair", "symbol": symbol}

    async def fetch() -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        summaries, volatility = await asyncio.gather(
            http_client.get_json(
                f"{_BASE}/public/get_book_summary_by_currency",
                {"currency": currency, "kind": "option"},
            ),
            http_client.get_json(
                f"{_BASE}/public/get_volatility_index_data",
                {
                    "currency": currency, "start_timestamp": now_ms - 3 * 3600 * 1000,
                    "end_timestamp": now_ms, "resolution": "60",
                },
            ),
        )
        return {"summaries": summaries, "volatility": volatility}

    try:
        payload, status, age = await cache.get_or_fetch(
            f"{SOURCE}:{currency}", 300, 1800, fetch,
        )
        normalized = normalize(payload, symbol)
        if not normalized["contracts"]:
            return {"source": SOURCE, "status": "no_pair_data", "symbol": symbol}
        return {
            "source": SOURCE, "status": status, "age_seconds": age,
            "symbol": symbol, "normalized": normalized,
        }
    except Exception as exc:
        return {"source": SOURCE, "status": "unavailable", "error": type(exc).__name__, "symbol": symbol}
