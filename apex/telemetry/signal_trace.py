"""Read-only per-signal evidence, without equating a signal to a Binance fill."""
from __future__ import annotations

import json
from apex.db.connection import connect_compatibility
from contextlib import closing
from decimal import Decimal, InvalidOperation
from pathlib import Path


def signal_trace(state_path: str, signal_id: int, *, memory_path: str | None = None,
                 brain_path: str | None = None) -> dict:
    result = {"signal_id": int(signal_id), "sources": {}, "evidence": {},
              "exchange_execution_confirmed": False}
    canonical_id = None
    specs = {
        "state": (state_path, ("executions", "trade_correlation", "signal_lifecycle", "execution_orders", "execution_fills", "execution_actions")),
        "memory": (memory_path, ("live_candidates",)),
        "brain": (brain_path, ("signals",)),
    }
    for source, (path, tables) in specs.items():
        if not path or not Path(path).is_file():
            result["sources"][source] = "UNAVAILABLE"
            continue
        with closing(connect_compatibility(path, read_only=True)) as conn:
            result["sources"][source] = "READ_ONLY"
            for table in tables:
                columns = {str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')}
                key = ("id" if table == "signals" else "legacy_signal_id" if "legacy_signal_id" in columns else "signal_id")
                if key not in columns:
                    result["evidence"][table] = {"available": False, "rows": []}
                    continue
                lookup = canonical_id if table in {"trade_correlation", "live_candidates"} else int(signal_id)
                if lookup is None:
                    result["evidence"][table] = {"available": True, "mapping_available": False, "rows": []}
                    continue
                rows = [dict(row) for row in conn.execute(f'SELECT * FROM "{table}" WHERE "{key}"=?', (lookup,))]
                if table == "executions" and rows:
                    canonical_id = rows[0].get("signal_entity_id")
                for row in rows:
                    for name in list(row):
                        if name.endswith("_json") and isinstance(row[name], str):
                            try:
                                row[name] = json.loads(row[name])
                            except (ValueError, TypeError):
                                pass
                result["evidence"][table] = {"available": True, "rows": rows}
    fills = result["evidence"].get("execution_fills", {}).get("rows", [])
    for fill in fills:
        try:
            quantity = Decimal(str(fill.get("qty") or "0"))
            if quantity.is_finite() and quantity > 0:
                result["exchange_execution_confirmed"] = True
        except InvalidOperation:
            pass
    return result
