"""Forward-only persistence for canonical production level transitions."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping

from apex.config.settings import ApexConfig
from apex.db.connection import connect_memory
from apex.db.memory_db import migrate_memory


def _iso_time(value: Any) -> str:
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        timestamp = datetime.now(timezone.utc).timestamp()
    if timestamp > 10_000_000_000:
        timestamp /= 1000.0
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def persist_level_events(
    events: Iterable[Mapping[str, Any]], *, config: ApexConfig | None = None,
) -> int:
    """Persist deterministic level transitions without granting trade authority."""
    rows = []
    for event in events:
        symbol = str(event.get("symbol") or "").upper()
        timeframe = str(event.get("timeframe") or "")
        event_type = str(event.get("event_type") or "").upper()
        level_id = str(event.get("level_id") or "")
        if not all((symbol, timeframe, event_type, level_id)):
            continue
        event_time = _iso_time(event.get("event_time"))
        payload = {
            **dict(event),
            "authority": "LIVE_CONTEXT",
            "can_change_strategy_gate": False,
        }
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        event_id = hashlib.sha256(
            f"{symbol}:{timeframe}:{level_id}:{event_type}:{event_time}".encode("utf-8")
        ).hexdigest()
        rows.append((
            event_id, symbol, timeframe, f"LEVEL_{event_type}", event_time,
            payload_json, "GATE_DERIVED", "PRIMARY_MARKET",
        ))
    if not rows:
        return 0
    with connect_memory(config) as conn:
        migrate_memory(conn)
        before = conn.total_changes
        conn.executemany(
            """INSERT OR IGNORE INTO live_market_events
               (event_id,symbol,timeframe,event_type,event_time,payload_json,source,provenance)
               VALUES(?,?,?,?,?,?,?,?)""",
            rows,
        )
        return conn.total_changes - before


__all__ = ["persist_level_events"]
