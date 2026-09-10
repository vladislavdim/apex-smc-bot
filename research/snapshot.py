"""Fail-closed validation and manifest helpers for BTC Research artifacts.

The Research workflow publishes two compressed artifacts (the SQLite database
and a compact dashboard JSON) plus a manifest.  The manifest is deliberately
written last by the workflow: the dashboard reader refuses to serve a mixed
generation when either data artifact is replaced without its matching manifest.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SNAPSHOT_VERSION = "research-snapshot-v2"
EXPECTED_SYMBOL = "BTCUSDT"
EXPECTED_TIMEFRAMES = ("15m", "1h", "4h", "1d")


class SnapshotValidationError(RuntimeError):
    """Raised when a Research snapshot is incomplete or internally unsafe."""


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(payload: bytes) -> str:
    """Return the content hash used by the public snapshot reader."""
    return hashlib.sha256(payload).hexdigest()


def _json(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as exc:
        raise SnapshotValidationError(f"invalid_dashboard_json:{type(exc).__name__}") from exc
    if not isinstance(value, dict):
        raise SnapshotValidationError("dashboard_payload_not_object")
    return value


def _rows(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return list(conn.execute(sql, tuple(params)).fetchall())


def validate_database(
    db_path: str | Path,
    *,
    expected_symbol: str = EXPECTED_SYMBOL,
    expected_timeframes: Iterable[str] = EXPECTED_TIMEFRAMES,
    require_completed_run: bool = True,
) -> dict[str, Any]:
    """Validate SQLite integrity, coverage, run status and open data errors."""
    path = Path(db_path)
    if not path.is_file() or path.stat().st_size <= 0:
        raise SnapshotValidationError("database_missing_or_empty")
    expected_symbol = str(expected_symbol).upper()
    timeframes = tuple(str(x) for x in expected_timeframes)
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    except sqlite3.Error as exc:
        raise SnapshotValidationError(f"database_open_failed:{type(exc).__name__}") from exc
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise SnapshotValidationError(f"sqlite_integrity:{integrity}")
        counts = _rows(conn, """SELECT timeframe,COUNT(*) AS count,
            MIN(open_time) AS first_open,MAX(open_time) AS last_open,
            MAX(close_time) AS last_close
            FROM market_candles WHERE source='GATE' AND symbol=? AND is_closed=1
            GROUP BY timeframe""", (expected_symbol,))
        by_tf = {str(row["timeframe"]): dict(row) for row in counts}
        missing = [tf for tf in timeframes if int(by_tf.get(tf, {}).get("count") or 0) <= 0]
        if missing:
            raise SnapshotValidationError("missing_timeframes:" + ",".join(missing))
        # A completed run is the only state allowed to become a published
        # snapshot.  PAUSED/FAILED runs remain resumable but never publish.
        runs = _rows(conn, """SELECT research_run_id,status,progress,range_start,range_end,
            universe_json,config_json FROM research_runs ORDER BY started_at DESC LIMIT 1""")
        latest_run = dict(runs[0]) if runs else None
        if require_completed_run and (not latest_run or latest_run.get("status") != "COMPLETED"
                                      or float(latest_run.get("progress") or 0) < 100):
            status = (latest_run or {}).get("status") or "MISSING"
            raise SnapshotValidationError(f"run_not_completed:{status}")
        errors = _rows(conn, """SELECT issue_type,COUNT(*) AS count
            FROM market_quality_issues WHERE status='OPEN' AND severity='ERROR'
            GROUP BY issue_type""")
        if errors:
            raise SnapshotValidationError("open_quality_errors:" + ",".join(str(x["issue_type"]) for x in errors))
        # A rolling snapshot is allowed to retain diagnostics for candles
        # outside its current window.  Only defects which could affect the
        # published cohort are fail-closed; the full diagnostic list remains
        # visible in the Research dashboard.
        range_start = latest_run.get("range_start") if latest_run else None
        range_end = latest_run.get("range_end") if latest_run else None
        issue_window = ""
        issue_params: list[Any] = []
        if range_start is not None and range_end is not None:
            issue_window = " AND (open_time IS NULL OR (open_time>=? AND open_time<=?))"
            issue_params.extend([int(range_start), int(range_end)])
        incomplete = _rows(conn, f"""SELECT issue_type,COUNT(*) AS count
            FROM market_quality_issues WHERE status='OPEN'
              AND issue_type IN ('EMPTY_HISTORY_PAGE','MISSING_CANDLES','TIMESTAMP_ORDER','DUPLICATE')
              {issue_window}
            GROUP BY issue_type""", issue_params)
        if incomplete:
            raise SnapshotValidationError("open_history_gaps:" + ",".join(str(x["issue_type"]) for x in incomplete))
        coverage = _rows(conn, """SELECT feature,symbol,timeframe,coverage_start,coverage_end,
            quality,availability,samples FROM market_feature_coverage
            WHERE source='GATE' AND (symbol=? OR symbol='*')
            ORDER BY feature,timeframe""", (expected_symbol,))
        return {
            "symbol": expected_symbol,
            "timeframes": list(timeframes),
            "counts": {tf: by_tf[tf] for tf in timeframes},
            "latest_run": latest_run,
            "coverage": [dict(row) for row in coverage],
        }
    except sqlite3.Error as exc:
        if isinstance(exc, SnapshotValidationError):
            raise
        raise SnapshotValidationError(f"database_query_failed:{type(exc).__name__}") from exc
    finally:
        conn.close()


def validate_dashboard(
    dashboard_path: str | Path,
    *,
    expected_symbol: str = EXPECTED_SYMBOL,
    expected_timeframes: Iterable[str] = EXPECTED_TIMEFRAMES,
) -> dict[str, Any]:
    value = _json(dashboard_path)
    storage = value.get("storage") or {}
    if str(storage.get("symbol") or "").upper() != str(expected_symbol).upper():
        raise SnapshotValidationError("dashboard_symbol_mismatch")
    observed = tuple(str(x) for x in (storage.get("timeframes") or []))
    missing = [tf for tf in expected_timeframes if tf not in observed]
    if missing:
        raise SnapshotValidationError("dashboard_timeframes_missing:" + ",".join(missing))
    runs = value.get("runs") or []
    latest = runs[0] if runs and isinstance(runs[0], dict) else None
    if not latest:
        raise SnapshotValidationError("dashboard_run_missing")
    if latest.get("status") != "COMPLETED" or float(latest.get("progress") or 0) < 100:
        raise SnapshotValidationError("dashboard_run_not_completed")
    storage_run = storage.get("research_run_id")
    if storage_run and latest.get("research_run_id") and storage_run != latest.get("research_run_id"):
        raise SnapshotValidationError("dashboard_run_identity_mismatch")
    return value


def build_manifest(
    db_path: str | Path,
    dashboard_path: str | Path,
    *,
    db_gz_path: str | Path | None = None,
    dashboard_gz_path: str | Path | None = None,
    expected_symbol: str = EXPECTED_SYMBOL,
    expected_timeframes: Iterable[str] = EXPECTED_TIMEFRAMES,
    history_days: int = 365,
) -> dict[str, Any]:
    db_info = validate_database(db_path, expected_symbol=expected_symbol,
                                expected_timeframes=expected_timeframes)
    dashboard = validate_dashboard(dashboard_path, expected_symbol=expected_symbol,
                                   expected_timeframes=expected_timeframes)
    latest = db_info.get("latest_run") or {}
    manifest: dict[str, Any] = {
        "snapshot_version": SNAPSHOT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": str(expected_symbol).upper(),
        "history_days": int(history_days),
        "history_days_semantics": "REQUESTED_TARGET_NOT_PER_TIMEFRAME_GUARANTEE",
        "coverage_policy": "GATE_CANONICAL_ONLY_NO_VENUE_SUBSTITUTION",
        "timeframes": list(expected_timeframes),
        "database": {"name": Path(db_path).name, "sha256": sha256_file(db_path),
                     "size_bytes": Path(db_path).stat().st_size},
        "dashboard": {"name": Path(dashboard_path).name, "sha256": sha256_file(dashboard_path),
                      "size_bytes": Path(dashboard_path).stat().st_size},
        "latest_run": latest,
        "counts": db_info["counts"],
        "coverage": db_info["coverage"],
        "dashboard_schema_version": dashboard.get("schema_version"),
        "no_real_execution": True,
        "live_activation": "FORBIDDEN",
    }
    if db_gz_path:
        manifest["database_gz"] = {"name": Path(db_gz_path).name, "sha256": sha256_file(db_gz_path),
                                    "size_bytes": Path(db_gz_path).stat().st_size}
    if dashboard_gz_path:
        manifest["dashboard_gz"] = {"name": Path(dashboard_gz_path).name,
                                     "sha256": sha256_file(dashboard_gz_path),
                                     "size_bytes": Path(dashboard_gz_path).stat().st_size}
    return manifest


def verify_compressed_assets(
    db_gz_path: str | Path,
    dashboard_gz_path: str | Path,
    manifest_path: str | Path,
) -> dict[str, Any]:
    try:
        manifest = _json(manifest_path)
    except (OSError, SnapshotValidationError) as exc:
        if isinstance(exc, SnapshotValidationError):
            raise
        raise SnapshotValidationError("manifest_missing") from exc
    if manifest.get("snapshot_version") != SNAPSHOT_VERSION:
        raise SnapshotValidationError("snapshot_manifest_version_mismatch")
    if str(manifest.get("symbol") or "").upper() != EXPECTED_SYMBOL:
        raise SnapshotValidationError("snapshot_manifest_symbol_mismatch")
    if tuple(manifest.get("timeframes") or ()) != EXPECTED_TIMEFRAMES:
        raise SnapshotValidationError("snapshot_manifest_timeframes_mismatch")
    if manifest.get("no_real_execution") is not True or manifest.get("live_activation") != "FORBIDDEN":
        raise SnapshotValidationError("snapshot_live_execution_flag_invalid")
    for key, path in (("database_gz", db_gz_path), ("dashboard_gz", dashboard_gz_path)):
        expected = ((manifest.get(key) or {}).get("sha256") or "").lower()
        try:
            actual = sha256_file(path).lower()
        except OSError as exc:
            raise SnapshotValidationError(f"{key}_missing") from exc
        if not expected or expected != actual:
            raise SnapshotValidationError(f"{key}_checksum_mismatch")
    return manifest


__all__ = [
    "EXPECTED_SYMBOL", "EXPECTED_TIMEFRAMES", "SNAPSHOT_VERSION", "SnapshotValidationError",
    "build_manifest", "sha256_bytes", "sha256_file", "validate_dashboard", "validate_database",
    "verify_compressed_assets",
]
