"""Read-only position sizing calculator used by the user interface."""

from __future__ import annotations


def calc_risk(deposit, risk_percent, entry, sl):
    """Preserve the legacy informational position-size calculation."""
    risk_amount = deposit * (risk_percent / 100)
    sl_distance_pct = abs(entry - sl) / entry * 100
    if sl_distance_pct == 0:
        return None
    position_size = risk_amount / (sl_distance_pct / 100)
    leverage = round(position_size / deposit, 1)
    return {
        "risk_amount": round(risk_amount, 2),
        "position_size": round(position_size, 2),
        "sl_distance": round(sl_distance_pct, 2),
        "leverage": min(leverage, 20),
        "contracts": round(position_size / entry, 4),
    }


__all__ = ["calc_risk"]
