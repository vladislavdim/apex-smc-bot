"""Read-only sentiment and Gate derivatives context adapters."""

from __future__ import annotations

import time


_fear_greed_cache: dict = {}
_fear_greed_cache_time = 0.0


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def get_fear_greed() -> dict | None:
    global _fear_greed_cache, _fear_greed_cache_time
    if time.time() - _fear_greed_cache_time < 3600 and _fear_greed_cache:
        return _fear_greed_cache
    try:
        response = _request_get("https://api.alternative.me/fng/?limit=1", timeout=8)
        data = response.json()["data"][0]
        _fear_greed_cache = {
            "value": int(data["value"]),
            "label": data["value_classification"],
            "updated": data["timestamp"],
        }
        _fear_greed_cache_time = time.time()
        return _fear_greed_cache
    except Exception:
        return None


def _gate_contract(symbol: str) -> str:
    from external_sources.pair_registry import get_pair

    return str(get_pair(symbol).get("gate_symbol") or symbol.replace("USDT", "_USDT"))


def get_funding_rate(symbol: str) -> float | None:
    try:
        response = _request_get(
            f"https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/{_gate_contract(symbol)}",
            timeout=8,
        )
        rate = response.json().get("funding_rate")
        return float(rate) * 100 if rate is not None else None
    except Exception:
        return None


def get_open_interest(symbol: str) -> dict | None:
    """Return the five-hour Gate USD-M open-interest trend."""
    try:
        response = _request_get(
            "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
            params={"contract": _gate_contract(symbol), "interval": "1h", "limit": 5},
            timeout=8,
        )
        response.raise_for_status()
        history = response.json()
        if not isinstance(history, list) or not history:
            return None
        current_oi = float(history[-1].get("open_interest") or 0)
        old_oi = float(history[0].get("open_interest") or 0)
        change_pct = (current_oi - old_oi) / old_oi * 100 if old_oi > 0 else 0
        return {
            "current": current_oi,
            "change_pct": round(change_pct, 2),
            "trend": (
                "GROWING" if change_pct > 2
                else "FALLING" if change_pct < -2 else "FLAT"
            ),
        }
    except Exception:
        return None


__all__ = ["get_fear_greed", "get_funding_rate", "get_open_interest"]
