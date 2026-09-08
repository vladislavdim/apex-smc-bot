"""Gate-return dependency diagnostics for portfolio awareness.

This is an observation layer only.  It estimates pairwise Pearson correlation
and BTC beta from closed Gate returns, groups highly correlated symbols and
reports concentration.  It does not alter the existing portfolio risk limits
or approve/deny an order; Manager V2 remains the sole execution authority.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from math import sqrt
from typing import Any, Iterable, Mapping


DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.environ.get(
        "APEX_BRAIN_DB_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
    ),
)


def _corr(left: list[float], right: list[float]) -> float | None:
    n = min(len(left), len(right))
    if n < 2:
        return None
    x, y = left[-n:], right[-n:]
    mx, my = sum(x) / n, sum(y) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y))
    vx, vy = sum((a - mx) ** 2 for a in x), sum((b - my) ** 2 for b in y)
    if vx <= 0 or vy <= 0:
        return None
    return cov / sqrt(vx * vy)


def _normalized_series(values: Any) -> dict[str, float]:
    """Return event-time keyed returns; positional input remains compatible.

    Production callers should pass ``{closed_candle_time: return}`` or
    ``[(closed_candle_time, return), ...]``. Synthetic positional keys exist
    only for legacy/tests and are labelled in the result metadata.
    """
    if isinstance(values, Mapping):
        return {str(key): float(value) for key, value in values.items()}
    output: dict[str, float] = {}
    for index, item in enumerate(values):
        if isinstance(item, Mapping):
            timestamp = item.get("closed_at") or item.get("close_time") or item.get("timestamp")
            value = item.get("return") if item.get("return") is not None else item.get("value")
            if timestamp is None or value is None:
                continue
            output[str(timestamp)] = float(value)
        elif isinstance(item, (tuple, list)) and len(item) == 2:
            output[str(item[0])] = float(item[1])
        else:
            output[f"legacy:{index:08d}"] = float(item)
    return output


def _aligned(left: Mapping[str, float], right: Mapping[str, float]) -> tuple[list[float], list[float]]:
    keys = sorted(set(left).intersection(right))
    return [left[key] for key in keys], [right[key] for key in keys]


def _beta(values: list[float], benchmark: list[float]) -> float | None:
    n = min(len(values), len(benchmark))
    if n < 2:
        return None
    v, b = values[-n:], benchmark[-n:]
    mb = sum(b) / n
    variance = sum((item - mb) ** 2 for item in b) / n
    if variance <= 0:
        return None
    mv = sum(v) / n
    return sum((a - mv) * (z - mb) for a, z in zip(v, b)) / n / variance


def build_dependency_graph(
    returns_by_symbol: Mapping[str, Iterable[float]], *, threshold: float = 0.75,
    benchmark: str = "BTCUSDT", min_samples: int = 20,
) -> dict[str, Any]:
    """Build a deterministic graph from normalized closed Gate returns."""
    series = {str(symbol).upper(): _normalized_series(values) for symbol, values in returns_by_symbol.items()}
    symbols = sorted(series)
    edges: list[dict[str, Any]] = []
    parent = {symbol: symbol for symbol in symbols}

    def find(symbol: str) -> str:
        while parent[symbol] != symbol:
            parent[symbol] = parent[parent[symbol]]
            symbol = parent[symbol]
        return symbol

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[b] = a

    for index, left in enumerate(symbols):
        for right in symbols[index + 1:]:
            aligned_left, aligned_right = _aligned(series[left], series[right])
            value = _corr(aligned_left, aligned_right)
            if value is None:
                continue
            edge = {"from": left, "to": right, "correlation": round(value, 6), "samples": len(aligned_left)}
            edges.append(edge)
            if abs(value) >= max(0.0, float(threshold)) and edge["samples"] >= max(2, int(min_samples)):
                union(left, right)
    clusters: dict[str, list[str]] = {}
    for symbol in symbols:
        clusters.setdefault(find(symbol), []).append(symbol)
    btc = series.get(str(benchmark).upper())
    betas = {}
    for symbol, values in series.items():
        if symbol == str(benchmark).upper():
            betas[symbol] = 1.0
        elif btc is None:
            betas[symbol] = None
        else:
            aligned_values, aligned_btc = _aligned(values, btc)
            betas[symbol] = _beta(aligned_values, aligned_btc)
    return {
        "source": "Gate_closed_returns", "scope": "OBSERVATION_ONLY", "threshold": float(threshold),
        "alignment": "closed_candle_event_time_intersection",
        "benchmark": str(benchmark).upper(), "symbols": symbols,
        "edges": sorted(edges, key=lambda x: abs(x["correlation"]), reverse=True),
        "clusters": sorted((sorted(value) for value in clusters.values()), key=lambda x: (len(x), x), reverse=True),
        "btc_beta": {key: round(value, 6) if value is not None else None for key, value in betas.items()},
        "sample_counts": {key: len(value) for key, value in series.items()},
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_dependency_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS apex_v2_dependency_snapshots (
            snapshot_key TEXT PRIMARY KEY, snapshot_hash TEXT NOT NULL,
            snapshot_json TEXT NOT NULL, observed_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_apex_v2_dependency_recent ON apex_v2_dependency_snapshots(observed_at DESC)")
    conn.commit(); conn.close()


def persist_dependency_snapshot(snapshot: Mapping[str, Any], db_path: str = DB_PATH) -> str:
    ensure_dependency_schema(db_path)
    encoded = json.dumps(dict(snapshot), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    key = str(snapshot.get("snapshot_key") or f"dependency:{digest[:20]}")
    observed = str(snapshot.get("observed_at") or datetime.now(timezone.utc).isoformat(timespec="seconds"))
    conn = _connect(db_path)
    conn.execute("INSERT OR IGNORE INTO apex_v2_dependency_snapshots(snapshot_key,snapshot_hash,snapshot_json,observed_at) VALUES(?,?,?,?)", (key, digest, encoded, observed))
    conn.commit(); conn.close()
    return key


def latest_dependency_snapshot(db_path: str = DB_PATH) -> dict[str, Any]:
    ensure_dependency_schema(db_path)
    conn = _connect(db_path)
    row = conn.execute("SELECT snapshot_json FROM apex_v2_dependency_snapshots ORDER BY observed_at DESC LIMIT 1").fetchone()
    conn.close()
    if not row:
        return {}
    try:
        return json.loads(row[0] or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}


__all__ = ["build_dependency_graph", "ensure_dependency_schema", "latest_dependency_snapshot", "persist_dependency_snapshot"]
