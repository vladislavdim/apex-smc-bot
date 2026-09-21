"""Initial V3 production state schema."""

from __future__ import annotations

import sqlite3

from apex.domain.ids import derived_id, is_id

from .migrations import Migration, MigrationRunner


def _migration_001(conn: sqlite3.Connection) -> None:
    statements = (
        """CREATE TABLE IF NOT EXISTS runtime_state (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS runtime_lease (
            lease_key TEXT PRIMARY KEY,
            instance_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS trade_correlation (
            candidate_id TEXT PRIMARY KEY,
            signal_id TEXT UNIQUE,
            execution_id TEXT UNIQUE,
            position_id TEXT UNIQUE,
            outcome_id TEXT UNIQUE,
            release_sha TEXT NOT NULL,
            versions_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS incidents (
            incident_id TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            severity TEXT NOT NULL,
            component TEXT NOT NULL,
            started_at TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 1,
            details_json TEXT NOT NULL DEFAULT '{}',
            resolved_at TEXT
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_incidents_one_active
             ON incidents(code, component) WHERE resolved_at IS NULL""",
    )
    for statement in statements:
        conn.execute(statement)


def _migration_002(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS job_runs (
        run_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        duration_ms REAL,
        rss_before INTEGER,
        rss_after INTEGER,
        cpu_before REAL,
        cpu_after REAL,
        items_processed INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        error_code TEXT NOT NULL DEFAULT ''
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_job_runs_job_started ON job_runs(job_id,started_at DESC)")


def _migration_003(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS release_manifests (
        release_sha TEXT PRIMARY KEY,
        manifest_json TEXT NOT NULL,
        config_hash TEXT NOT NULL,
        strategy_config_hash TEXT NOT NULL,
        deployed_at TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")


def _migration_004(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS incident_notifications (
        notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_key TEXT NOT NULL UNIQUE,
        incident_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        delivered_at TEXT
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_incident_notifications_pending "
        "ON incident_notifications(delivered_at,notification_id)"
    )


def _migration_005(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS runtime_instances (
        instance_id TEXT PRIMARY KEY,
        release_sha TEXT NOT NULL,
        started_at TEXT NOT NULL,
        stopped_at TEXT,
        shutdown_reason TEXT NOT NULL DEFAULT '',
        previous_instance TEXT NOT NULL DEFAULT ''
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_runtime_instances_started "
        "ON runtime_instances(started_at DESC)"
    )


def _migration_006(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS delivery_claims (
        cache_key TEXT PRIMARY KEY,
        claimed_at REAL NOT NULL,
        delivered_at REAL,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_delivery_claims_delivered "
        "ON delivery_claims(delivered_at,claimed_at)"
    )


def _migration_007(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS setup_audit_events (
        event_key TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        strategy TEXT,
        symbol TEXT,
        occurred_at TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        synced INTEGER NOT NULL DEFAULT 0,
        sync_attempts INTEGER NOT NULL DEFAULT 0,
        last_sync_error TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_setup_audit_recent "
        "ON setup_audit_events(occurred_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_setup_audit_lookup "
        "ON setup_audit_events(strategy,symbol,occurred_at DESC)"
    )


def _migration_008(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS runtime_heartbeats (
        heartbeat_id INTEGER PRIMARY KEY AUTOINCREMENT,
        instance_id TEXT NOT NULL,
        release_sha TEXT NOT NULL,
        observed_at TEXT NOT NULL
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_runtime_heartbeats_observed "
        "ON runtime_heartbeats(observed_at DESC)"
    )


def _migration_009(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_account_cache (
        exchange TEXT PRIMARY KEY,
        wallet_balance REAL,
        available_balance REAL,
        cross_unrealized_pnl REAL,
        fetched_at_epoch REAL,
        attempted_at_epoch REAL NOT NULL,
        last_error TEXT,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")


def _migration_010(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS trade_manager_messages (
        signal_id INTEGER NOT NULL,
        chat_id INTEGER NOT NULL,
        thread_id INTEGER NOT NULL DEFAULT 0,
        message_id INTEGER NOT NULL,
        content_hash TEXT NOT NULL DEFAULT '',
        is_final INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(signal_id,chat_id,thread_id)
    )""")


def _migration_011(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS strategy_decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        timeframe TEXT,
        direction TEXT,
        structure_direction TEXT,
        structure_event TEXT,
        outcome TEXT NOT NULL,
        stage TEXT NOT NULL,
        reason TEXT,
        groq_decision TEXT,
        groq_confidence REAL,
        evidence_json TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_strategy_decisions_created "
        "ON strategy_decisions(created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_strategy_decisions_lookup "
        "ON strategy_decisions(strategy,direction,outcome)"
    )


def _migration_012(conn: sqlite3.Connection) -> None:
    """Create production-owned Manager state without legacy cross-table joins."""
    conn.execute("""CREATE TABLE IF NOT EXISTS manager_positions (
        signal_id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL,
        strategy TEXT NOT NULL,
        direction TEXT NOT NULL,
        management_tf TEXT NOT NULL,
        initial_entry REAL NOT NULL,
        initial_sl REAL NOT NULL,
        initial_tp1 REAL NOT NULL,
        initial_tp2 REAL,
        initial_tp3 REAL,
        initial_rr REAL,
        manager_version INTEGER NOT NULL,
        thesis_json TEXT NOT NULL DEFAULT '{}',
        snapshot_json TEXT NOT NULL,
        snapshot_hash TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'ACTIVE',
        manager_state TEXT NOT NULL DEFAULT 'PROTECTED',
        position_fraction REAL NOT NULL DEFAULT 1.0,
        partial_exit_done INTEGER NOT NULL DEFAULT 0,
        last_price REAL,
        best_price REAL,
        current_r REAL NOT NULL DEFAULT 0,
        tp1_seen INTEGER NOT NULL DEFAULT 0,
        tp2_seen INTEGER NOT NULL DEFAULT 0,
        tp3_seen INTEGER NOT NULL DEFAULT 0,
        manager_target REAL,
        confirmed_protect_level REAL,
        proposed_protect_level REAL,
        last_event TEXT,
        last_action TEXT,
        last_confidence REAL,
        last_reviewed_candle TEXT,
        no_progress_bars INTEGER NOT NULL DEFAULT 0,
        progress_anchor_r REAL NOT NULL DEFAULT 0,
        last_progress_candle TEXT,
        reconciliation_reason TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        closed_at TEXT,
        close_result TEXT,
        exit_price REAL,
        realized_pct REAL,
        realized_r REAL
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manager_positions_active "
        "ON manager_positions(status,updated_at DESC)"
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS manager_events (
        manager_event_id TEXT PRIMARY KEY,
        signal_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        action TEXT,
        confidence REAL,
        price REAL,
        r_multiple REAL,
        manager_target REAL,
        confirmed_protect_level REAL,
        facts_json TEXT NOT NULL DEFAULT '{}',
        reason_codes_json TEXT NOT NULL DEFAULT '[]',
        summary TEXT NOT NULL DEFAULT '',
        execution_status TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(signal_id) REFERENCES manager_positions(signal_id)
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manager_events_signal "
        "ON manager_events(signal_id,created_at DESC)"
    )


def _migration_013(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS executions (
        signal_id INTEGER PRIMARY KEY,
        execution_id TEXT UNIQUE,
        candidate_id TEXT,
        position_id TEXT UNIQUE,
        mode TEXT NOT NULL,
        exchange TEXT NOT NULL DEFAULT 'binance_futures',
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        status TEXT NOT NULL,
        entry REAL,
        sl REAL,
        tp1 REAL,
        tp2 REAL,
        tp3 REAL,
        quantity REAL,
        risk_usdt REAL,
        balance_usdt REAL,
        leverage INTEGER,
        entry_order_id TEXT,
        stop_order_id TEXT,
        tp1_order_id TEXT,
        tp2_order_id TEXT,
        active_stop_price REAL,
        pending_stop_order_id TEXT,
        previous_stop_order_id TEXT,
        last_error TEXT,
        plan_json TEXT NOT NULL,
        plan_hash TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_executions_status "
        "ON executions(mode,status,updated_at DESC)"
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_actions (
        action_key TEXT PRIMARY KEY,
        signal_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        status TEXT NOT NULL,
        requested_level REAL,
        exchange_order_id TEXT,
        error TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(signal_id) REFERENCES executions(signal_id)
    )""")


def _migration_014(conn: sqlite3.Connection) -> None:
    """Complete Manager availability state before State read cutover."""
    conn.execute(
        "ALTER TABLE manager_positions ADD COLUMN data_failure_count INTEGER NOT NULL DEFAULT 0"
    )
    conn.execute(
        "ALTER TABLE manager_positions ADD COLUMN data_failure_notified INTEGER NOT NULL DEFAULT 0"
    )
    conn.execute("ALTER TABLE manager_positions ADD COLUMN last_data_error TEXT")
    conn.execute("ALTER TABLE manager_positions ADD COLUMN pre_degraded_state TEXT")


def _migration_015(conn: sqlite3.Connection) -> None:
    """Create the State-owned projection of delivered-signal lifecycle."""
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_lifecycle(
        signal_id INTEGER PRIMARY KEY,
        status TEXT NOT NULL,
        result TEXT NOT NULL DEFAULT 'pending',
        activated_at TEXT,
        last_checked_at TEXT,
        closed_at TEXT,
        cancel_reason TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_signal_lifecycle_status "
        "ON signal_lifecycle(status,updated_at DESC)"
    )


def _migration_016(conn: sqlite3.Connection) -> None:
    """Create the canonical confirmed Binance execution ledger."""
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_orders(
        signal_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        kind TEXT NOT NULL,
        remote_id TEXT NOT NULL,
        is_algo INTEGER NOT NULL,
        expected_side TEXT NOT NULL,
        standard_id TEXT,
        complete INTEGER NOT NULL DEFAULT 0,
        checked_at REAL NOT NULL DEFAULT 0,
        error TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(signal_id,kind,remote_id),
        FOREIGN KEY(signal_id) REFERENCES executions(signal_id)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_fills(
        symbol TEXT NOT NULL,
        trade_id TEXT NOT NULL,
        signal_id INTEGER NOT NULL,
        order_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        qty TEXT NOT NULL,
        price TEXT NOT NULL,
        commission TEXT NOT NULL,
        commission_asset TEXT NOT NULL,
        time_ms INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(symbol,trade_id),
        FOREIGN KEY(signal_id) REFERENCES executions(signal_id)
    )""")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_signal_time "
        "ON execution_fills(signal_id,time_ms,trade_id)"
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_funding(
        signal_id INTEGER NOT NULL,
        tran_id TEXT NOT NULL UNIQUE,
        symbol TEXT NOT NULL,
        income TEXT NOT NULL,
        asset TEXT NOT NULL,
        time_ms INTEGER NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(signal_id,tran_id),
        FOREIGN KEY(signal_id) REFERENCES executions(signal_id)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_funding_coverage(
        signal_id INTEGER PRIMARY KEY,
        start_ms INTEGER NOT NULL,
        end_ms INTEGER NOT NULL,
        checked_at REAL NOT NULL,
        status TEXT NOT NULL,
        FOREIGN KEY(signal_id) REFERENCES executions(signal_id)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS execution_ledger_poll(
        poll_kind TEXT PRIMARY KEY,
        attempted_at REAL NOT NULL
    )""")


def _migration_017(conn: sqlite3.Connection) -> None:
    """Give every canonical execution a typed signal identity.

    The integer key remains only as the compatibility/exchange lookup key.
    """
    conn.execute("ALTER TABLE executions ADD COLUMN signal_entity_id TEXT")
    rows = conn.execute(
        """SELECT e.signal_id,e.candidate_id,c.signal_id
             FROM executions e
             LEFT JOIN trade_correlation c ON c.candidate_id=e.candidate_id"""
    ).fetchall()
    for legacy_signal_id, candidate_id, correlated_signal_id in rows:
        signal_entity_id = str(correlated_signal_id or "")
        if not is_id(signal_entity_id, "signal"):
            signal_entity_id = derived_id(
                "signal", "legacy-execution", int(legacy_signal_id)
            )
        conn.execute(
            "UPDATE executions SET signal_entity_id=? WHERE signal_id=?",
            (signal_entity_id, int(legacy_signal_id)),
        )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_signal_entity "
        "ON executions(signal_entity_id)"
    )


def _migration_018(conn: sqlite3.Connection) -> None:
    """Attach Manager state and events to the canonical typed signal."""
    conn.execute("ALTER TABLE manager_positions ADD COLUMN signal_entity_id TEXT")
    conn.execute("ALTER TABLE manager_events ADD COLUMN signal_entity_id TEXT")
    rows = conn.execute(
        """SELECT m.signal_id,e.signal_entity_id
             FROM manager_positions m
             LEFT JOIN executions e ON e.signal_id=m.signal_id"""
    ).fetchall()
    for legacy_signal_id, execution_signal_id in rows:
        signal_entity_id = str(execution_signal_id or "")
        if not is_id(signal_entity_id, "signal"):
            signal_entity_id = derived_id(
                "signal", "legacy-execution", int(legacy_signal_id)
            )
        conn.execute(
            "UPDATE manager_positions SET signal_entity_id=? WHERE signal_id=?",
            (signal_entity_id, int(legacy_signal_id)),
        )
    conn.execute(
        """UPDATE manager_events
              SET signal_entity_id=(
                  SELECT p.signal_entity_id FROM manager_positions p
                   WHERE p.signal_id=manager_events.signal_id
              )"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_manager_positions_signal_entity "
        "ON manager_positions(signal_entity_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manager_events_signal_entity "
        "ON manager_events(signal_entity_id,created_at DESC)"
    )


def _migration_019(conn: sqlite3.Connection) -> None:
    """Attach execution actions and confirmed ledger evidence to typed signals."""
    tables = (
        "execution_actions", "execution_orders", "execution_fills",
        "execution_funding", "execution_funding_coverage",
    )
    for table in tables:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN signal_entity_id TEXT")
        conn.execute(
            f"""UPDATE {table}
                   SET signal_entity_id=(
                       SELECT e.signal_entity_id FROM executions e
                        WHERE e.signal_id={table}.signal_id
                   )"""
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_signal_entity "
            f"ON {table}(signal_entity_id)"
        )


STATE_MIGRATIONS = (
    Migration(1, "production_core", _migration_001),
    Migration(2, "job_telemetry", _migration_002),
    Migration(3, "release_manifest", _migration_003),
    Migration(4, "incident_notifications", _migration_004),
    Migration(5, "runtime_restart_history", _migration_005),
    Migration(6, "production_delivery_claims", _migration_006),
    Migration(7, "strategy_check_audit_queue", _migration_007),
    Migration(8, "runtime_heartbeats", _migration_008),
    Migration(9, "execution_account_cache", _migration_009),
    Migration(10, "manager_message_identity", _migration_010),
    Migration(11, "strategy_decision_journal", _migration_011),
    Migration(12, "manager_position_state", _migration_012),
    Migration(13, "execution_state", _migration_013),
    Migration(14, "manager_data_availability", _migration_014),
    Migration(15, "signal_lifecycle_state", _migration_015),
    Migration(16, "confirmed_execution_ledger", _migration_016),
    Migration(17, "typed_execution_signal_identity", _migration_017),
    Migration(18, "typed_manager_signal_identity", _migration_018),
    Migration(19, "typed_execution_ledger_signal_identity", _migration_019),
)


def migrate_state(conn: sqlite3.Connection) -> tuple[int, ...]:
    return MigrationRunner(STATE_MIGRATIONS).run(conn)


__all__ = ["STATE_MIGRATIONS", "migrate_state"]
