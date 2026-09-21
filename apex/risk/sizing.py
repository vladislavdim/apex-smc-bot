"""Deterministic position sizing from stop geometry."""
from __future__ import annotations

from math import isfinite


def quantity_for_risk(equity_quote: float, risk_pct: float, entry: float, stop: float) -> float:
    distance = abs(float(entry) - float(stop))
    equity = float(equity_quote)
    pct = float(risk_pct)
    if not all(isfinite(v) for v in (distance, equity, pct)) or distance <= 0 or equity <= 0 or pct <= 0:
        return 0.0
    return (equity * pct / 100.0) / distance


__all__ = ["quantity_for_risk"]
