"""Initial V3 live-only analytical memory schema."""

from __future__ import annotations

import sqlite3

from .migrations import Migration, MigrationRunner


def _migration_001(conn: sqlite3.Connection) -> None:
    statements = (
        """CREATE TABLE IF NOT EXISTS live_trade_outcomes (
            outcome_id TEXT PRIMARY KEY,
            position_id TEXT NOT NULL UNIQUE,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            net_r REAL NOT NULL,
            fees REAL NOT NULL DEFAULT 0,
            funding REAL NOT NULL DEFAULT 0,
            regime_json TEXT NOT NULL DEFAULT '{}',
            context_json TEXT NOT NULL DEFAULT '{}',
            closed_at TEXT NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS live_market_events (
            event_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_time TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            source TEXT NOT NULL,
            provenance TEXT NOT NULL,
            UNIQUE(symbol, timeframe, event_type, event_time)
        )""",
        """CREATE TABLE IF NOT EXISTS live_context_observations (
            observation_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            context_type TEXT NOT NULL,
            event_time TEXT NOT NULL,
            received_at TEXT NOT NULL,
            value_json TEXT,
            status TEXT NOT NULL,
            source TEXT NOT NULL,
            freshness_seconds REAL,
            quality TEXT NOT NULL,
            UNIQUE(symbol, context_type, source, event_time)
        )""",
    )
    for statement in statements:
        conn.execute(statement)


def _migration_002(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS live_candidates (
        candidate_id TEXT PRIMARY KEY,
        signal_id TEXT UNIQUE,
        strategy TEXT NOT NULL,
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        candidate_json TEXT NOT NULL,
        groq_json TEXT,
        risk_json TEXT,
        executed INTEGER NOT NULL DEFAULT 0,
        release_sha TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""")
    additions = (
        ("candidate_id", "TEXT"),
        ("execution_id", "TEXT"),
        ("gross_r", "REAL"),
        ("mfe_r", "REAL"),
        ("mae_r", "REAL"),
        ("duration_seconds", "REAL"),
        ("setup_type", "TEXT"),
        ("session", "TEXT"),
        ("volatility", "TEXT"),
        ("btc_state", "TEXT"),
        ("release_sha", "TEXT"),
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(live_trade_outcomes)")}
    for name, definition in additions:
        if name not in columns:
            conn.execute(f"ALTER TABLE live_trade_outcomes ADD COLUMN {name} {definition}")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_live_outcome_candidate ON live_trade_outcomes(candidate_id) WHERE candidate_id IS NOT NULL")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_live_outcome_execution ON live_trade_outcomes(execution_id) WHERE execution_id IS NOT NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_live_outcome_similarity ON live_trade_outcomes(strategy,direction,setup_type,closed_at)")


def _migration_003(conn: sqlite3.Connection) -> None:
    """Allow independent levels to emit the same transition on one candle."""
    conn.execute("ALTER TABLE live_market_events RENAME TO live_market_events_v2")
    conn.execute("""CREATE TABLE live_market_events (
        event_id TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_time TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        source TEXT NOT NULL,
        provenance TEXT NOT NULL
    )""")
    conn.execute("""INSERT OR IGNORE INTO live_market_events
        (event_id,symbol,timeframe,event_type,event_time,payload_json,source,provenance)
        SELECT event_id,symbol,timeframe,event_type,event_time,payload_json,source,provenance
        FROM live_market_events_v2""")
    conn.execute("DROP TABLE live_market_events_v2")
    conn.execute(
        "CREATE INDEX idx_live_market_event_lookup "
        "ON live_market_events(symbol,timeframe,event_type,event_time)"
    )


MEMORY_MIGRATIONS = (
    Migration(1, "live_memory_core", _migration_001),
    Migration(2, "live_only_learning", _migration_002),
    Migration(3, "independent_level_events", _migration_003),
)


def migrate_memory(conn: sqlite3.Connection) -> tuple[int, ...]:
    return MigrationRunner(MEMORY_MIGRATIONS).run(conn)


__all__ = ["MEMORY_MIGRATIONS", "migrate_memory"]
