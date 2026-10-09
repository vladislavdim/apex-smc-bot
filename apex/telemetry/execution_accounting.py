"""Financial telemetry comes only from the canonical confirmed-fill ledger."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping


def confirmed_execution_events(snapshot: Any, accounting: Mapping[str, Any]) -> list[dict[str, Any]]:
    if accounting.get("entry_state") != "FILLED" or not accounting.get("entry_time"):
        return []
    quantity = float(accounting.get("quantity") or 0)
    if not math.isfinite(quantity) or quantity <= 0:
        return []
    common = {"signal_id": snapshot.signal_id, "symbol": snapshot.symbol,
              "strategy": snapshot.strategy, "direction": snapshot.direction,
              "entry": snapshot.entry, "quantity": quantity}
    opened_at = datetime.fromtimestamp(float(accounting["entry_time"]) / 1000, timezone.utc).isoformat()
    events = [{**common, "action": "OPEN", "occurred_at": opened_at,
               "accounting_basis": "CONFIRMED_ENTRY_FILLS"}]
    if (accounting.get("status") != "CLOSED"
            or accounting.get("accounting_basis") != "confirmed_fills_after_commissions_and_funding"
            or accounting.get("net_r") is None or not accounting.get("exit_time")):
        return events
    net_r = float(accounting["net_r"])
    if not math.isfinite(net_r):
        return events
    closed_at = datetime.fromtimestamp(float(accounting["exit_time"]) / 1000, timezone.utc).isoformat()
    # Ledger's realized_pct excludes funding; include it explicitly in net %.
    net_pct = accounting.get("realized_pct")
    if net_pct is not None and accounting.get("funding_quote") is not None:
        value = float(accounting.get("entry") or snapshot.entry) * quantity
        net_pct = float(net_pct) + float(accounting["funding_quote"]) / value * 100 if value > 0 else None
    events.append({**common, "action": "CLOSE", "occurred_at": closed_at,
                   "accounting_basis": accounting["accounting_basis"],
                   "result": "win" if net_r > 0 else "loss" if net_r < 0 else "breakeven",
                   "exit_reason": accounting.get("exit_reason"), "realized_r": net_r,
                   "pnl_pct": net_pct, "fees_quote": accounting.get("fees_quote"),
                   "funding_quote": accounting.get("funding_quote")})
    return events
