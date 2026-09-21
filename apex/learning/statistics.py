"""Deterministic performance summaries of real closed positions."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

from .confidence import confidence_label


def _max_drawdown(values: list[float]) -> float:
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def strategy_statistics(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT strategy,net_r,closed_at FROM live_trade_outcomes ORDER BY closed_at,outcome_id"
    ).fetchall()
    grouped: dict[str, list[float]] = defaultdict(list)
    for strategy, net_r, _closed_at in rows:
        grouped[str(strategy)].append(float(net_r))
    output = []
    for strategy, values in sorted(grouped.items()):
        wins = sum(value > 0 for value in values)
        gross_profit = sum(value for value in values if value > 0)
        gross_loss = -sum(value for value in values if value < 0)
        output.append({
            "strategy": strategy,
            "samples": len(values),
            "confidence": confidence_label(len(values)),
            "win_rate": wins / len(values) if values else 0.0,
            "net_expectancy_r": sum(values) / len(values) if values else 0.0,
            "profit_factor": gross_profit / gross_loss if gross_loss else None,
            "max_drawdown_r": _max_drawdown(values),
        })
    return output


__all__ = ["strategy_statistics"]
