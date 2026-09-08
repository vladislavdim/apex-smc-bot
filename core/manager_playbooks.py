"""Measurable Manager 2.0 shadow playbooks.

Concept attribution only (no copied book text):
- Ruben Villahermosa, *The Wyckoff Methodology in Depth*: phase events,
  effort-versus-result, SOS/SOW, Spring/UTAD tests, LPS/LPSY and invalidation.
- Gavin Holmes, *Trading in the Shadow of the Smart Money, Vol. 1*:
  relative volume versus spread/close, no-demand/no-supply and follow-through.

Gate volume is venue-specific relative volume. These rules do not identify
institutional orders and never participate in entry qualification.
"""
from __future__ import annotations

from statistics import median
from typing import Any, Iterable


BOOK_RULES = {
    "VILLAHERMOSA_EFFORT_RESULT": {
        "strategies": {"MTF", "ZONE", "WYCKOFF"},
        "description": "Flag high relative effort with weak directional result for review.",
    },
    "VILLAHERMOSA_TEST_FOLLOW_THROUGH": {
        "strategies": {"WYCKOFF"},
        "description": "Require a closed-candle test and subsequent SOS/SOW follow-through.",
    },
    "HOLMES_VSA_RELATIVE_VOLUME": {
        "strategies": {"FAST", "MTF", "ZONE"},
        "description": "Compare normalized Gate volume with spread and close location.",
    },
    "HOLMES_NO_DEMAND_SUPPLY": {
        "strategies": {"FAST", "MTF", "ZONE"},
        "description": "Flag narrow-spread low-volume bars only after confirming reaction.",
    },
}


def shadow_features(closed: list[dict[str, Any]], direction: str) -> dict[str, Any]:
    """Return deterministic, venue-qualified observations; never live actions."""
    if len(closed) < 20:
        return {"eligible": False, "reason": "insufficient_closed_gate_candles"}
    recent = closed[-20:]
    volumes = [max(0.0, float(c.get("volume") or 0)) for c in recent]
    spreads = [max(0.0, float(c["high"]) - float(c["low"])) for c in recent]
    volume_base = median(volumes[:-1]) or 1.0
    spread_base = median(spreads[:-1]) or 1.0
    bar = recent[-1]
    spread = max(float(bar["high"]) - float(bar["low"]), 1e-12)
    close_location = (float(bar["close"]) - float(bar["low"])) / spread
    volume_ratio = float(bar.get("volume") or 0) / volume_base
    spread_ratio = spread / spread_base
    bullish = str(direction).upper() == "BULLISH"
    weak_close = close_location < 0.4 if bullish else close_location > 0.6
    return {
        "eligible": True,
        "source": "Gate_relative_volume",
        "volume_ratio": round(volume_ratio, 4),
        "spread_ratio": round(spread_ratio, 4),
        "close_location": round(close_location, 4),
        "effort_without_result": volume_ratio >= 1.5 and spread_ratio <= 0.8,
        "no_demand_supply_candidate": volume_ratio <= 0.7 and spread_ratio <= 0.8 and weak_close,
        "execution_scope": "PLAYBOOK_ONLY_SHADOW",
    }


def promotion_assessment(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Evidence gate may propose activation but can never activate a rule."""
    data = list(rows)
    deltas = [float(row.get("delta_r") or 0) for row in data]
    actual = [float(row.get("new_r") or 0) for row in data]
    old = [float(row.get("old_r") or 0) for row in data]
    mean_delta = sum(deltas) / len(deltas) if deltas else 0.0
    median_delta = median(deltas) if deltas else 0.0
    outlier_dependent = bool(len(deltas) >= 2 and sum(deltas) > 0 and max(deltas) > sum(deltas) * 0.5)
    drawdown_worse = min(actual or [0]) < min(old or [0])
    safety = any(bool(row.get("safety_violation")) for row in data)
    eligible = (
        len(data) >= 30 and mean_delta > 0 and median_delta > 0
        and not drawdown_worse and not outlier_dependent and not safety
    )
    return {
        "eligible_closed_trades": len(data), "mean_delta_r": mean_delta,
        "median_delta_r": median_delta, "drawdown_worse": drawdown_worse,
        "outlier_dependent": outlier_dependent, "safety_violation": safety,
        "promotion_proposed": eligible, "auto_activated": False,
    }
