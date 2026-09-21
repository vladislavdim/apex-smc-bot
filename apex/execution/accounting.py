"""Fill-weighted accounting for real Binance executions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from apex.domain.enums import Direction


@dataclass(frozen=True)
class LedgerFill:
    order_id: str
    role: str
    price: float
    quantity: float
    commission_quote: float
    filled_at: datetime


@dataclass(frozen=True)
class ActualAccounting:
    weighted_entry: float
    weighted_exit: float | None
    entry_quantity: float
    exited_quantity: float
    remaining_quantity: float
    remaining_fraction: float
    gross_pnl_quote: float
    fees_quote: float
    funding_quote: float
    net_pnl_quote: float
    net_r: float | None


def _weighted(rows: list[LedgerFill]) -> float | None:
    quantity = sum(row.quantity for row in rows)
    return sum(row.price * row.quantity for row in rows) / quantity if quantity else None


def actual_accounting(
    fills: Iterable[LedgerFill],
    *,
    direction: Direction,
    initial_stop: float,
    exchange_position_quantity: float,
    funding_quote: float = 0.0,
) -> ActualAccounting:
    rows = list(fills)
    entries = [row for row in rows if row.role.upper() == "ENTRY" and row.quantity > 0]
    exits = [row for row in rows if row.role.upper() in {"EXIT", "TP", "SL", "PARTIAL_EXIT"} and row.quantity > 0]
    entry_quantity = sum(row.quantity for row in entries)
    exited_quantity = sum(row.quantity for row in exits)
    weighted_entry = _weighted(entries)
    weighted_exit = _weighted(exits)
    if weighted_entry is None or entry_quantity <= 0:
        raise ValueError("confirmed_entry_fill_required")
    actual_remaining = max(0.0, abs(float(exchange_position_quantity)))
    # Binance positionAmt is authoritative. Never infer the remainder from a
    # configured TP fraction or from requested order quantity.
    remaining_fraction = min(1.0, actual_remaining / entry_quantity)
    multiplier = 1.0 if direction is Direction.LONG else -1.0
    gross = sum((row.price - weighted_entry) * row.quantity * multiplier for row in exits)
    fees = sum(max(0.0, row.commission_quote) for row in rows)
    net = gross - fees - float(funding_quote)
    initial_risk_quote = abs(weighted_entry - float(initial_stop)) * entry_quantity
    net_r = net / initial_risk_quote if initial_risk_quote else None
    return ActualAccounting(
        weighted_entry=weighted_entry,
        weighted_exit=weighted_exit,
        entry_quantity=entry_quantity,
        exited_quantity=exited_quantity,
        remaining_quantity=actual_remaining,
        remaining_fraction=remaining_fraction,
        gross_pnl_quote=gross,
        fees_quote=fees,
        funding_quote=float(funding_quote),
        net_pnl_quote=net,
        net_r=net_r,
    )


__all__ = ["ActualAccounting", "LedgerFill", "actual_accounting"]
