"""Gate liquidation context provider for compatibility callers."""

from __future__ import annotations

import logging

from external_sources.pair_registry import get_pair


def _http_get(*args, **kwargs):
    import requests
    return requests.get(*args, **kwargs)


def get_liquidation_ratio(symbol: str) -> dict:
    """Return Gate liquidation dominance without Binance market-data reads."""
    try:
        contract = str(
            get_pair(symbol).get("gate_symbol")
            or symbol.replace("USDT", "_USDT")
        )
        response = _http_get(
            "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
            params={"contract": contract, "interval": "1h", "limit": 3},
            headers={"User-Agent": "APEX-SMC/1.0"},
            timeout=8,
        )
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and data:
                latest = data[-1]
                long_liquidations = abs(
                    float(latest.get("long_liq_size") or 0)
                )
                short_liquidations = abs(
                    float(latest.get("short_liq_size") or 0)
                )
                total = long_liquidations + short_liquidations
                long_percent = long_liquidations / total if total else 0.5
                short_percent = short_liquidations / total if total else 0.5
                ratio = (
                    long_liquidations / short_liquidations
                    if short_liquidations > 0
                    else 999.0 if long_liquidations else 1.0
                )
                if ratio > 1.5:
                    signal = "BEARISH"
                    description = (
                        f"Gate long liquidations dominate ({ratio:.2f}x)"
                    )
                elif ratio < 0.67:
                    signal = "BULLISH"
                    inverse = (1 / ratio) if ratio else 999
                    description = (
                        f"Gate short liquidations dominate ({inverse:.2f}x)"
                    )
                else:
                    signal = "NEUTRAL"
                    description = f"Gate liquidations balanced ({ratio:.2f}x)"
                return {
                    "long_pct": long_percent,
                    "short_pct": short_percent,
                    "ratio": ratio,
                    "signal": signal,
                    "desc": description,
                    "ok": True,
                }
    except Exception as exc:
        logging.debug("LiqRatio %s: %s", symbol, exc)
    return {"ratio": 1.0, "signal": "NEUTRAL", "desc": "", "ok": False}


__all__ = ["get_liquidation_ratio"]
