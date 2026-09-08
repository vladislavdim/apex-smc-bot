"""Offline execution model for replay and shadow analysis.

It estimates fills from Gate-derived spread/depth and configured latency.  The
simulator is never imported by the live Binance executor, so a changed model
cannot place or modify an order.  It is useful for comparing manager actions
under conservative assumptions and for exposing fee/slippage tail risk.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Mapping


@dataclass(frozen=True)
class ExecutionModel:
    fee_bps: float = 4.0
    slippage_bps: float = 1.0
    latency_ms: int = 250
    max_depth_levels: int = 5


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result and abs(result) != float("inf") else default


def simulate_fill(
    side: str, quantity: float, reference_price: float, *, bid: float | None = None,
    ask: float | None = None, available_quantity: float | None = None,
    model: ExecutionModel | None = None,
) -> dict[str, Any]:
    """Return a deterministic paper fill; reject malformed/under-depth orders."""
    config = model or ExecutionModel()
    normalized_side = str(side or "").upper()
    qty, reference = _num(quantity), _num(reference_price)
    if normalized_side not in {"BUY", "SELL", "LONG", "SHORT"}:
        return {"status": "REJECTED", "reason": "invalid_side", "model": asdict(config)}
    if qty is None or qty <= 0 or reference is None or reference <= 0:
        return {"status": "REJECTED", "reason": "invalid_quantity_or_price", "model": asdict(config)}
    if available_quantity is not None and qty > max(0.0, float(available_quantity)):
        return {"status": "REJECTED", "reason": "insufficient_depth", "model": asdict(config)}
    bid_price = _num(bid, reference)
    ask_price = _num(ask, reference)
    if bid_price is None or ask_price is None or bid_price <= 0 or ask_price <= 0 or ask_price < bid_price:
        return {"status": "REJECTED", "reason": "invalid_book", "model": asdict(config)}
    is_buy = normalized_side in {"BUY", "LONG"}
    base = ask_price if is_buy else bid_price
    direction = 1.0 if is_buy else -1.0
    fill_price = base * (1.0 + direction * max(0.0, config.slippage_bps) / 10000)
    notional = fill_price * qty
    fee = notional * max(0.0, config.fee_bps) / 10000
    return {
        "status": "FILLED", "side": normalized_side, "requested_quantity": qty,
        "filled_quantity": qty, "reference_price": reference, "bid": bid_price,
        "ask": ask_price, "fill_price": fill_price, "notional": notional,
        "fee_quote": fee, "latency_ms": max(0, int(config.latency_ms)),
        "slippage_bps": max(0.0, config.slippage_bps), "model": asdict(config),
        "source": "Gate_book_shadow", "execution_scope": "REPLAY_ONLY",
    }


def validate_protection_replace(current: Mapping[str, Any] | None, replacement: Mapping[str, Any] | None) -> dict[str, Any]:
    """Model the safe order-replace sequence: place/confirm new before old."""
    old_id = str((current or {}).get("order_id") or "")
    new_id = str((replacement or {}).get("order_id") or "")
    if not new_id:
        return {"status": "REJECTED", "reason": "replacement_missing_order_id", "old_order_id": old_id}
    if old_id and old_id == new_id:
        return {"status": "NOOP", "reason": "same_protection_order", "old_order_id": old_id, "new_order_id": new_id}
    return {
        "status": "PLACE_THEN_CONFIRM_THEN_CANCEL",
        "old_order_id": old_id or None, "new_order_id": new_id,
        "cancel_old": bool(old_id), "preserve_old_until_confirmed": True,
    }


__all__ = ["ExecutionModel", "simulate_fill", "validate_protection_replace"]
