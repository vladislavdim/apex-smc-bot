"""Transparent similarity over actual live outcomes."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Mapping


WEIGHTS = {
    "strategy": 4.0,
    "direction": 2.0,
    "setup_type": 2.0,
    "session": 1.0,
    "volatility": 1.0,
    "btc_state": 1.0,
    "symbol": 0.5,
}


def similar_cases(
    conn: sqlite3.Connection,
    query: Mapping[str, Any],
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT outcome_id,strategy,symbol,direction,net_r,mfe_r,mae_r,
                  setup_type,session,volatility,btc_state,context_json,closed_at
             FROM live_trade_outcomes ORDER BY closed_at DESC LIMIT 1000"""
    ).fetchall()
    names = (
        "outcome_id", "strategy", "symbol", "direction", "net_r", "mfe_r", "mae_r",
        "setup_type", "session", "volatility", "btc_state", "context_json", "closed_at",
    )
    maximum = sum(WEIGHTS.values())
    output = []
    for raw in rows:
        row = dict(zip(names, raw))
        score = sum(
            weight for name, weight in WEIGHTS.items()
            if query.get(name) is not None
            and str(query.get(name)).upper() == str(row.get(name)).upper()
        )
        row["similarity"] = score / maximum
        try:
            row["context"] = json.loads(row.pop("context_json") or "{}")
        except (TypeError, ValueError):
            row["context"] = {}
            row.pop("context_json", None)
        output.append(row)
    output.sort(key=lambda item: (item["similarity"], item["closed_at"]), reverse=True)
    return output[:max(1, min(int(limit), 100))]


__all__ = ["WEIGHTS", "similar_cases"]
