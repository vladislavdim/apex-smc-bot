"""Read-only Gate USD-M order-book adapter."""

from __future__ import annotations


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def get_orderbook(symbol: str) -> dict | None:
    """Return notional bid/ask totals for the registered Gate contract."""
    try:
        from external_sources.pair_registry import get_pair

        pair = get_pair(symbol)
        contract = str(pair.get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        multiplier = float(pair.get("gate_multiplier") or 1)
        response = _request_get(
            "https://api.gateio.ws/api/v4/futures/usdt/order_book",
            params={"contract": contract, "limit": 20}, timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        bids = sum(
            float(row.get("p", 0)) * abs(float(row.get("s", 0))) * multiplier
            for row in payload.get("bids", [])
        )
        asks = sum(
            float(row.get("p", 0)) * abs(float(row.get("s", 0))) * multiplier
            for row in payload.get("asks", [])
        )
        return {"bids": bids, "asks": asks, "bias": "BUY" if bids > asks else "SELL"}
    except Exception:
        return None


__all__ = ["get_orderbook"]
