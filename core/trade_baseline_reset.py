"""One-time, recoverable reset of legacy trade-derived learning.

The strategy-integrity release changed which signals are safe training
examples. Old Telegram statistics and adaptive rules must therefore not be
mixed with the new baseline. This module archives the affected rows inside
``brain.db`` before deleting them and records an idempotent migration marker.

Market/news feeds, API health history, user settings and curated SMC rules are
deliberately outside the reset scope.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any, Iterable


MIGRATION_ID = "2026-08-29-reset-pre-integrity-trade-learning-v1"

# These tables contain Telegram trade history or learning derived from trade
# outcomes. Missing tables are expected on older databases and are skipped.
_FULL_RESET_TABLES = (
    "market_memory_path",
    "market_memory_snapshots",
    "signal_execution_state",
    "trade_executions",
    "ai_signal_reviews",
    "signals",
    "signal_log",
    "signal_stats",
    "signal_learning",
    "symbol_stats",
    "pattern_history",
    "pattern_memory",
    "error_patterns",
    "auto_rules",
    "session_stats",
    "self_analysis",
    "grade_accuracy",
    "streak_log",
    "news_impact",
    "bot_errors",
    "signal_cooldown",
    "timing_queue",
    "trade_history",
    "market_model",
    "observations",
    "brain_log",
    "learning_history",
    "router_signal_history",
    "router_market_memory",
    "router_groq_insights",
    "router_contradiction_log",
)

_RETAINED_RULE_SOURCES = ("smc_seed", "manual", "user")
_ACTIVE_LIVE_STATUSES = (
    "ENTRY_PENDING",
    "PROTECTED",
    "PROTECTED_NO_TP",
    "CLEANUP_PENDING",
    "UNPROTECTED_POSITION",
)
_TRADE_KNOWLEDGE_WHERE = """
    lower(COALESCE(source, '')) IN (
        'history_groq', 'self-reflection', 'error-analysis',
        'trade-analysis', 'groq_trade_analysis', 'stats_analysis'
    )
    OR lower(COALESCE(topic, '')) LIKE 'reflection_%'
    OR lower(COALESCE(topic, '')) LIKE 'trade_history_analysis_%'
"""


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')]


def _archive_rows(
    conn: sqlite3.Connection,
    table: str,
    where: str = "1=1",
    params: Iterable[Any] = (),
) -> int:
    if not _table_exists(conn, table):
        return 0
    cursor = conn.execute(f'SELECT * FROM "{table}" WHERE {where}', tuple(params))
    columns = [str(item[0]) for item in (cursor.description or [])]
    rows = cursor.fetchall()
    if not rows:
        return 0

    primary_keys = [
        str(row[1])
        for row in conn.execute(f'PRAGMA table_info("{table}")')
        if int(row[5] or 0) > 0
    ]
    payloads = []
    for index, row in enumerate(rows, start=1):
        payload = dict(zip(columns, row))
        reference = (
            {key: payload.get(key) for key in primary_keys}
            if primary_keys
            else {"archive_index": index}
        )
        payloads.append((
            MIGRATION_ID,
            table,
            json.dumps(reference, ensure_ascii=False, default=str, sort_keys=True),
            json.dumps(payload, ensure_ascii=False, default=str, sort_keys=True),
        ))
    conn.executemany(
        """INSERT INTO trade_reset_archive
           (migration_id, source_table, source_reference, payload_json)
           VALUES (?, ?, ?, ?)""",
        payloads,
    )
    return len(payloads)


def _delete_all(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    cursor = conn.execute(f'DELETE FROM "{table}"')
    return max(0, int(cursor.rowcount or 0))


def _active_live_execution_count(conn: sqlite3.Connection) -> int:
    if not _table_exists(conn, "trade_executions"):
        return 0
    columns = set(_columns(conn, "trade_executions"))
    if not {"mode", "status"}.issubset(columns):
        return 0
    placeholders = ",".join("?" for _ in _ACTIVE_LIVE_STATUSES)
    row = conn.execute(
        f"""SELECT COUNT(*) FROM trade_executions
            WHERE lower(COALESCE(mode, ''))='live'
              AND upper(COALESCE(status, '')) IN ({placeholders})""",
        _ACTIVE_LIVE_STATUSES,
    ).fetchone()
    return int(row[0] if row else 0)


def apply_trade_baseline_reset(db_path: str) -> dict[str, Any]:
    """Archive and clear the pre-integrity trade-learning baseline once.

    The operation is atomic. It refuses to run while an active live exchange
    execution exists, because deleting its local reconciliation state could
    leave a position or protective order unmanaged.
    """
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("""CREATE TABLE IF NOT EXISTS system_migrations (
            name TEXT PRIMARY KEY,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP,
            details_json TEXT NOT NULL DEFAULT '{}'
        )""")
        existing = conn.execute(
            "SELECT details_json FROM system_migrations WHERE name=?", (MIGRATION_ID,)
        ).fetchone()
        if existing:
            conn.rollback()
            return {
                "applied": False,
                "already_applied": True,
                "migration_id": MIGRATION_ID,
            }

        active_live = _active_live_execution_count(conn)
        if active_live:
            conn.rollback()
            logging.error(
                "[TradeBaselineReset] blocked: %s active live execution(s)", active_live,
            )
            return {
                "applied": False,
                "blocked": True,
                "active_live_executions": active_live,
                "migration_id": MIGRATION_ID,
            }

        conn.execute("""CREATE TABLE IF NOT EXISTS trade_reset_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration_id TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_reference TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            archived_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_reset_archive_migration "
            "ON trade_reset_archive(migration_id, source_table)"
        )

        removed: dict[str, int] = {}
        archived: dict[str, int] = {}
        for table in _FULL_RESET_TABLES:
            count = _archive_rows(conn, table)
            if count:
                archived[table] = count
            deleted = _delete_all(conn, table)
            if deleted:
                removed[table] = deleted

        # Archive the complete former rule state, then retain only curated SMC
        # and explicitly manual/user rules. Their outcome counters are reset.
        if _table_exists(conn, "self_rules"):
            count = _archive_rows(conn, "self_rules")
            if count:
                archived["self_rules"] = count
            placeholders = ",".join("?" for _ in _RETAINED_RULE_SOURCES)
            cursor = conn.execute(
                f"""DELETE FROM self_rules
                    WHERE lower(COALESCE(source, '')) NOT IN ({placeholders})""",
                _RETAINED_RULE_SOURCES,
            )
            if cursor.rowcount:
                removed["self_rules"] = int(cursor.rowcount)
            columns = set(_columns(conn, "self_rules"))
            updates = []
            if "confirmed_by" in columns:
                updates.append("confirmed_by=0")
            if "contradicted_by" in columns:
                updates.append("contradicted_by=0")
            if "active" in columns:
                updates.append("active=1")
            if updates:
                conn.execute(
                    f"""UPDATE self_rules SET {', '.join(updates)}
                        WHERE lower(COALESCE(source, '')) IN ({placeholders})""",
                    _RETAINED_RULE_SOURCES,
                )

        # General/manual research remains available. Only conclusions tied to
        # the discarded trade outcomes are removed from the knowledge store.
        if _table_exists(conn, "knowledge"):
            columns = set(_columns(conn, "knowledge"))
            if {"source", "topic"}.issubset(columns):
                count = _archive_rows(conn, "knowledge", _TRADE_KNOWLEDGE_WHERE)
                if count:
                    archived["knowledge"] = count
                cursor = conn.execute(f"DELETE FROM knowledge WHERE {_TRADE_KNOWLEDGE_WHERE}")
                if cursor.rowcount:
                    removed["knowledge"] = int(cursor.rowcount)

        details = {
            "migration_id": MIGRATION_ID,
            "removed": removed,
            "archived": archived,
            "preserved": [
                "user settings and journal",
                "curated/manual SMC rules",
                "general knowledge",
                "external and news context",
                "API/source reliability history",
            ],
        }
        conn.execute(
            "INSERT INTO system_migrations(name, details_json) VALUES (?, ?)",
            (MIGRATION_ID, json.dumps(details, ensure_ascii=False, sort_keys=True)),
        )
        conn.commit()
        logging.warning(
            "[TradeBaselineReset] applied: removed=%s archived=%s",
            sum(removed.values()), sum(archived.values()),
        )
        return {
            "applied": True,
            "migration_id": MIGRATION_ID,
            "removed": removed,
            "archived": archived,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
