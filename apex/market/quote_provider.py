"""Read-only current quote helper for conversational market context."""

from __future__ import annotations

from apex.compatibility.market_constants import COINGECKO_IDS


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def get_price_realtime(symbol: str = "BTCUSDT") -> dict | None:
    """Prefer Gate USD-M and use CoinGecko only as a display fallback."""
    try:
        from external_sources.pair_registry import get_pair

        contract = str(
            get_pair(symbol).get("gate_symbol") or symbol.replace("USDT", "_USDT")
        )
        response = _request_get(
            "https://api.gateio.ws/api/v4/futures/usdt/tickers",
            params={"contract": contract},
            headers={"User-Agent": "APEX-SMC/1.0"}, timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        row = payload[0] if isinstance(payload, list) and payload else {}
        price = float(row.get("last") or row.get("mark_price") or 0)
        if price > 0:
            return {
                "price": price,
                "change": round(float(row.get("change_percentage") or 0), 2),
                "source": "Gate.io Futures",
            }
    except Exception:
        pass

    coin_id = COINGECKO_IDS.get(symbol)
    try:
        if not coin_id:
            return None
        response = _request_get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": coin_id, "vs_currencies": "usd",
                "include_24hr_change": "true",
            },
            headers={"User-Agent": "Mozilla/5.0"}, timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        if coin_id in payload:
            return {
                "price": payload[coin_id]["usd"],
                "change": round(payload[coin_id].get("usd_24h_change", 0), 2),
                "source": "CoinGecko",
            }
    except Exception:
        pass
    return None


__all__ = ["get_price_realtime"]
