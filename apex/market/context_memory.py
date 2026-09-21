"""Persist forward-only production context observations in V3 Memory DB."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Mapping

from apex.config.settings import ApexConfig
from apex.db.connection import connect_memory
from apex.db.memory_db import migrate_memory


_FIELDS = {
    "open_interest": "OPEN_INTEREST",
    "funding": "FUNDING",
    "long_short_ratio": "LONG_SHORT_RATIO",
    "liquidations": "LIQUIDATIONS",
    "live_tape": "CVD_REAL",
    "microstructure": "VISIBLE_ORDERBOOK_LIQUIDITY",
}


def _received_at(context: Mapping[str, Any]) -> datetime:
    raw = context.get("timestamp")
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _status(field: Mapping[str, Any]) -> str:
    value = str(field.get("freshness_status") or field.get("freshness") or field.get("status") or "UNKNOWN").upper()
    if value in {"FRESH", "CACHED", "FRESH_REST_SNAPSHOT", "BBO_TRADE_ONLY"}:
        return "FRESH"
    if "STALE" in value or "RESYNC" in value:
        return "STALE"
    return "UNAVAILABLE"


def persist_live_context(
    context: Mapping[str, Any], *, config: ApexConfig | None = None,
) -> int:
    """Append known observations; missing values remain in the candidate audit."""
    symbol = str(context.get("symbol") or "").upper()
    if not symbol:
        return 0
    received = _received_at(context)
    rows = []
    for field_name, context_type in _FIELDS.items():
        field = context.get(field_name)
        if not isinstance(field, Mapping):
            continue
        source = str(field.get("source") or "").strip()
        if not source:
            continue
        try:
            age_seconds = max(0.0, float(field.get("age_seconds") or 0.0))
        except (TypeError, ValueError):
            age_seconds = 0.0
        event_time = received - timedelta(seconds=age_seconds)
        status = _status(field)
        quality = "VALID" if status == "FRESH" else "STALE" if status == "STALE" else "UNKNOWN"
        payload = json.dumps(dict(field), ensure_ascii=False, sort_keys=True, default=str)
        observation_id = hashlib.sha256(
            f"{symbol}:{context_type}:{source}:{event_time.isoformat()}:{payload}".encode("utf-8")
        ).hexdigest()
        rows.append((
            observation_id, symbol, context_type, event_time.isoformat(),
            received.isoformat(), payload, status, source, age_seconds, quality,
        ))
    if not rows:
        return 0
    with connect_memory(config) as conn:
        migrate_memory(conn)
        before = conn.total_changes
        conn.executemany(
            """INSERT OR IGNORE INTO live_context_observations
               (observation_id,symbol,context_type,event_time,received_at,value_json,
                status,source,freshness_seconds,quality)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        return conn.total_changes - before


__all__ = ["persist_live_context"]
