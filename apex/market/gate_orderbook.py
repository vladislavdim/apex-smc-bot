"""Read-only Gate USD-M order-book adapter."""

from __future__ import annotations


def _request_get(url: str, **kwargs):
    import requests
    return requests.get(url, **kwargs)


def get_orderbook(symbol: str) -> dict | None:
    """Return notional bid/ask totals for the registered Gate contract."""
    from .snapshot_scope import active_snapshot
    snapshot = active_snapshot()
    if snapshot is not None:
        return snapshot.microstructure_context.get("strategy_orderbook")
    try:
        from external_sources.pair_registry import get_pair

        pair = get_pair(symbol)
        contract = str(pair.get("gate_symbol") or symbol.replace("USDT", "_USDT"))
        raw_multiplier = pair.get("gate_multiplier")
        if raw_multiplier is None:
            return None
        multiplier = float(raw_multiplier)
        import math
        if not math.isfinite(multiplier) or multiplier <= 0:
            return None
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
