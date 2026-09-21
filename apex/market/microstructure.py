"""Sequence-safe Gate visible order-book context."""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
import time
from typing import Any, Iterable, Mapping


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def _levels(values: Any) -> list[tuple[float, float]]:
    output: list[tuple[float, float]] = []
    if isinstance(values, Mapping):
        values = values.items()
    if not isinstance(values, Iterable) or isinstance(values, (str, bytes, Mapping)):
        return output
    for row in values:
        if isinstance(row, Mapping):
            price = _number(row.get("price", row.get("p")))
            size = _number(row.get("size", row.get("amount", row.get("s"))))
        elif isinstance(row, (tuple, list)) and len(row) >= 2:
            price, size = _number(row[0]), _number(row[1])
        else:
            continue
        if price is not None and size is not None and price > 0 and size >= 0:
            output.append((price, size))
    return output


@dataclass
class GateOrderBook:
    symbol: str
    contract_multiplier: float = 1.0
    last_update_id: int | None = None
    status: str = "UNAVAILABLE"
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    observed_at: float | None = None
    resync_count: int = 0

    def snapshot(
        self, bids: Any, asks: Any, update_id: Any, *, observed_at: float | None = None,
    ) -> bool:
        parsed = _number(update_id)
        self.bids = {p: s for p, s in _levels(bids) if s > 0}
        self.asks = {p: s for p, s in _levels(asks) if s > 0}
        self.last_update_id = int(parsed) if parsed is not None else None
        self.status = "FRESH" if self.bids and self.asks and parsed is not None else "UNAVAILABLE"
        self.observed_at = time.time() if observed_at is None else float(observed_at)
        return self.status == "FRESH"

    def delta(
        self, bids: Any, asks: Any, first_id: Any, final_id: Any,
        *, observed_at: float | None = None,
    ) -> bool:
        start, end = _number(first_id), _number(final_id)
        if self.status != "FRESH" or self.last_update_id is None or start is None or end is None:
            return False
        if int(end) <= self.last_update_id:
            return True
        if int(start) != self.last_update_id + 1 or int(end) < int(start):
            self.status = "RESYNC_REQUIRED"
            self.resync_count += 1
            return False
        for target, rows in ((self.bids, bids), (self.asks, asks)):
            for price, size in _levels(rows):
                target.pop(price, None) if size == 0 else target.__setitem__(price, size)
        self.last_update_id = int(end)
        self.observed_at = time.time() if observed_at is None else float(observed_at)
        self.status = "FRESH" if self.bids and self.asks else "UNAVAILABLE"
        return self.status == "FRESH"

    def features(self, *, depth: int = 20, now: float | None = None, max_age_seconds: float = 5.0) -> dict[str, Any]:
        age = None if self.observed_at is None else max(0.0, (now or time.time()) - self.observed_at)
        usable = self.status == "FRESH" and age is not None and age <= max_age_seconds
        if not usable:
            return {
                "source": "gate_ws", "mode": "LIVE_CONTEXT", "status": self.status,
                "available": False, "age_seconds": age, "resync_count": self.resync_count,
                "liquidity_kind": "VISIBLE_ORDERBOOK_LIQUIDITY",
            }
        bids = sorted(self.bids.items(), reverse=True)[:depth]
        asks = sorted(self.asks.items())[:depth]
        best_bid, best_ask = bids[0][0], asks[0][0]
        bid_depth = sum(p * s * self.contract_multiplier for p, s in bids)
        ask_depth = sum(p * s * self.contract_multiplier for p, s in asks)
        total = bid_depth + ask_depth
        mid = (best_bid + best_ask) / 2
        micro = ((best_ask * bid_depth) + (best_bid * ask_depth)) / total if total else mid
        return {
            "source": "gate_ws", "mode": "LIVE_CONTEXT", "status": "FRESH",
            "available": True, "age_seconds": age, "update_id": self.last_update_id,
            "spread_bps": (best_ask - best_bid) / mid * 10_000 if mid else None,
            "mid_price": mid, "microprice": micro,
            "bid_depth_usd": bid_depth, "ask_depth_usd": ask_depth,
            "depth_imbalance": (bid_depth - ask_depth) / total if total else None,
            "resync_count": self.resync_count,
            "liquidity_kind": "VISIBLE_ORDERBOOK_LIQUIDITY",
            "hidden_stops_claimed": False,
        }


__all__ = ["GateOrderBook"]
