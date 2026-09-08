"""Gate WebSocket microstructure reducer (shadow/context only).

This module consumes already-received Gate order-book and trade messages.  It
does not open a socket and it never changes a strategy decision.  A sequence
gap puts the reducer into ``RESYNC_REQUIRED``; callers must obtain a fresh Gate
snapshot before applying more deltas.  Features are probabilistic exchange
microstructure observations, not proof of stop hunting, market-maker intent or
institutional flow.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
import time
from typing import Any, Iterable, Mapping

SOURCE = "gate_ws"


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
    for item in values:
        if isinstance(item, Mapping):
            price = _number(item.get("price", item.get("p")))
            size = _number(item.get("size", item.get("amount", item.get("s"))))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price, size = _number(item[0]), _number(item[1])
        else:
            continue
        if price is not None and size is not None and price > 0 and size >= 0:
            output.append((price, size))
    return output


@dataclass
class OrderBookReducer:
    """Apply Gate snapshot/delta messages with strict sequence continuity."""

    symbol: str
    depth: int = 20
    contract_multiplier: float = 1.0
    last_update_id: int | None = None
    status: str = "EMPTY"
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    resync_count: int = 0
    observed_at: float | None = None

    def apply_snapshot(self, bids: Any, asks: Any, update_id: Any = None) -> bool:
        parsed_id = _number(update_id)
        self.bids = {price: size for price, size in _levels(bids) if size > 0}
        self.asks = {price: size for price, size in _levels(asks) if size > 0}
        self.last_update_id = int(parsed_id) if parsed_id is not None else None
        self.status = "FRESH" if self.bids and self.asks else "EMPTY"
        self.observed_at = time.time()
        return self.status == "FRESH"

    def apply_delta(self, bids: Any, asks: Any, first_id: Any, final_id: Any) -> bool:
        if self.status != "FRESH" or self.last_update_id is None:
            return False
        start, end = _number(first_id), _number(final_id)
        if start is None or end is None or end < start:
            self.status = "RESYNC_REQUIRED"
            self.resync_count += 1
            return False
        start_id, end_id = int(start), int(end)
        if end_id <= self.last_update_id:
            return True  # replayed/overlapping delta already incorporated
        if start_id != self.last_update_id + 1:
            self.status = "RESYNC_REQUIRED"
            self.resync_count += 1
            return False
        for price, size in _levels(bids):
            if size <= 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = size
        for price, size in _levels(asks):
            if size <= 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = size
        self.last_update_id = end_id
        self.status = "FRESH" if self.bids and self.asks else "EMPTY"
        self.observed_at = time.time()
        return self.status == "FRESH"

    def apply_message(self, message: Mapping[str, Any]) -> bool:
        """Accept common Gate v4 ``result``/flat order-book shapes."""
        payload: Mapping[str, Any] = message
        result = message.get("result")
        if isinstance(result, Mapping):
            payload = result
        bids = payload.get("bids", payload.get("b"))
        asks = payload.get("asks", payload.get("a"))
        if payload.get("type") == "snapshot" or payload.get("snapshot") or payload.get("full") is True:
            return self.apply_snapshot(bids, asks, payload.get("lastUpdateId", payload.get("u")))
        first_id = payload.get("U", payload.get("firstUpdateId"))
        final_id = payload.get("u", payload.get("lastUpdateId"))
        return self.apply_delta(bids, asks, first_id, final_id)

    def features(self, levels: int | None = None, *, now: float | None = None, freshness_seconds: int = 5) -> dict[str, Any]:
        result = summarize_order_book(
            self.bids, self.asks, levels or self.depth, self.status,
            self.last_update_id, contract_multiplier=self.contract_multiplier,
        )
        age = None if self.observed_at is None else max(0.0, (now or time.time()) - self.observed_at)
        result.update({
            "age_seconds": round(age, 3) if age is not None else None,
            "freshness_status": "UNKNOWN" if age is None else "FRESH" if age <= freshness_seconds else "STALE",
            "resync_count": self.resync_count,
        })
        return result


def summarize_order_book(
    bids: Any, asks: Any, depth: int = 20, status: str = "FRESH", update_id: int | None = None,
    *, contract_multiplier: float = 1.0,
) -> dict[str, Any]:
    """Create bounded, normalized Gate-only depth features."""
    bid_rows = sorted(_levels(bids), key=lambda x: x[0], reverse=True)[:max(1, int(depth))]
    ask_rows = sorted(_levels(asks), key=lambda x: x[0])[:max(1, int(depth))]
    if not bid_rows or not ask_rows:
        return {
            "source": SOURCE, "status": status, "update_id": update_id,
            "spread_bps": None, "mid_price": None, "microprice": None,
            "bid_depth": 0.0, "ask_depth": 0.0, "depth_imbalance": None,
            "bid_depth_usd": 0.0, "ask_depth_usd": 0.0,
            "levels": 0, "scope": "SHADOW_CONTEXT", "institutional_intent": False,
        }
    bid, ask = bid_rows[0][0], ask_rows[0][0]
    bid_depth = sum(size for _, size in bid_rows)
    ask_depth = sum(size for _, size in ask_rows)
    total = bid_depth + ask_depth
    multiplier = max(0.0, _number(contract_multiplier) or 0.0)
    bid_depth_usd = sum(price * size * multiplier for price, size in bid_rows)
    ask_depth_usd = sum(price * size * multiplier for price, size in ask_rows)
    total_usd = bid_depth_usd + ask_depth_usd
    mid = (bid + ask) / 2
    micro = ((ask * bid_depth) + (bid * ask_depth)) / total if total else mid
    return {
        "source": SOURCE, "status": status, "update_id": update_id,
        "spread_bps": round((ask - bid) / mid * 10000, 6) if mid else None,
        "mid_price": mid, "microprice": micro,
        "bid_depth": bid_depth, "ask_depth": ask_depth,
        "bid_depth_usd": bid_depth_usd, "ask_depth_usd": ask_depth_usd,
        "depth_imbalance": (bid_depth_usd - ask_depth_usd) / total_usd if total_usd else ((bid_depth - ask_depth) / total if total else None),
        "levels": min(len(bid_rows), len(ask_rows)), "scope": "SHADOW_CONTEXT",
        "institutional_intent": False, "venue_normalized": True,
    }


@dataclass
class TradeFlow:
    """Bounded taker-flow accumulator for one Gate symbol/time window."""

    buy_volume: float = 0.0
    sell_volume: float = 0.0
    trades: int = 0

    def add(self, side: str, size: Any) -> None:
        amount = _number(size)
        if amount is None or amount < 0:
            return
        normalized = str(side or "").lower()
        if normalized in {"buy", "bid", "b"}:
            self.buy_volume += amount
        elif normalized in {"sell", "ask", "s"}:
            self.sell_volume += amount
        else:
            return
        self.trades += 1

    def snapshot(self) -> dict[str, Any]:
        total = self.buy_volume + self.sell_volume
        return {
            "source": SOURCE, "buy_volume": self.buy_volume,
            "sell_volume": self.sell_volume, "trades": self.trades,
            "taker_imbalance": (self.buy_volume - self.sell_volume) / total if total else None,
            "scope": "SHADOW_CONTEXT", "institutional_intent": False,
        }


__all__ = ["OrderBookReducer", "TradeFlow", "SOURCE", "summarize_order_book"]
