"""Bounded read-only LIVE_CONTEXT projection for production telemetry."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from apex.config.settings import ApexConfig
from apex.db.connection import connect_memory
from external_sources.live_tape import telemetry_snapshot as live_tape_snapshot


def _memory_context(limit: int = 200) -> list[dict[str, Any]]:
    """Read the latest persisted context observations without mutating Memory DB."""
    try:
        conn = connect_memory(read_only=True)
    except (OSError, sqlite3.Error):
        return []
    try:
        rows = conn.execute(
            """SELECT symbol,context_type,event_time,received_at,value_json,status,
                      source,freshness_seconds,quality
                 FROM live_context_observations
                ORDER BY received_at DESC
                LIMIT ?""",
            (max(20, min(int(limit), 1000)),),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()

    seen: set[tuple[str, str]] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        key = (str(row["symbol"]).upper(), str(row["context_type"]).upper())
        if key in seen:
            continue
        seen.add(key)
        try:
            value = json.loads(row["value_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            value = {}
        result.append({
            "symbol": key[0],
            "context_type": key[1],
            "event_time": row["event_time"],
            "received_at": row["received_at"],
            "status": row["status"],
            "source": row["source"],
            "freshness_seconds": row["freshness_seconds"],
            "quality": row["quality"],
            "value": value if isinstance(value, dict) else {},
        })
    return result


def market_context_snapshot(limit: int = 20) -> list[dict[str, Any]]:
    """Merge live Gate tape/depth with latest persisted V3 context by symbol."""
    bounded = max(1, min(int(limit), 50))
    merged: dict[str, dict[str, Any]] = {}

    try:
        tape_rows = live_tape_snapshot(bounded)
    except Exception:
        tape_rows = []
    for row in tape_rows:
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            merged[symbol] = {**row, "observations": {}}

    for row in _memory_context():
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        target = merged.setdefault(symbol, {
            "symbol": symbol,
            "source": "apex_memory",
            "status": "MEMORY_ONLY",
            "age_seconds": None,
            "gate": {},
            "orderbook": {},
            "observations": {},
        })
        target.setdefault("observations", {})[str(row["context_type"])] = row

    rows = list(merged.values())
    rows.sort(key=lambda item: (
        0 if str(item.get("status") or "").upper() == "FRESH" else 1,
        str(item.get("symbol") or ""),
    ))
    return rows[:bounded]


__all__ = ["market_context_snapshot"]
