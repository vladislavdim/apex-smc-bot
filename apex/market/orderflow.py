"""Distinct real-tape and candle-proxy CVD representations."""

from __future__ import annotations

from math import isfinite
from typing import Any, Iterable, Mapping


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def real_cvd(
    trades: Iterable[Mapping[str, Any]], *, source: str = "gate_ws",
) -> dict[str, Any]:
    buy = sell = 0.0
    count = 0
    for row in trades:
        size = _number(row.get("size", row.get("quantity")))
        price = _number(row.get("price"))
        if size is None or price is None or price <= 0:
            continue
        side = str(row.get("side") or ("buy" if size > 0 else "sell")).lower()
        notional = abs(size) * price
        if side in {"buy", "bid", "b"}:
            buy += notional
        elif side in {"sell", "ask", "s"}:
            sell += notional
        else:
            continue
        count += 1
    total = buy + sell
    return {
        "kind": "CVD_REAL", "source": source, "mode": "LIVE_CONTEXT",
        "buy_notional": buy, "sell_notional": sell, "delta_notional": buy - sell,
        "taker_imbalance": (buy - sell) / total if total else None,
        "trades": count, "available": count > 0,
    }


def proxy_cvd(candles: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    delta = 0.0
    count = 0
    for row in candles:
        try:
            high, low = float(row["high"]), float(row["low"])
            close, volume = float(row["close"]), float(row.get("volume") or 0.0)
        except (KeyError, TypeError, ValueError):
            continue
        spread = high - low
        if spread <= 0 or not all(isfinite(x) for x in (high, low, close, volume)):
            continue
        delta += volume * ((2 * close - high - low) / spread)
        count += 1
    return {
        "kind": "CVD_PROXY", "source": "cvd_proxy", "mode": "PROXY",
        "delta_volume": delta if count else None, "candles": count,
        "available": count > 0,
    }


__all__ = ["proxy_cvd", "real_cvd"]
