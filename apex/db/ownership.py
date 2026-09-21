"""Executable ownership contract for the three APEX persistence domains."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


MIGRATION_METADATA_TABLES = frozenset({"apex_schema_migrations"})

STATE_TABLES = frozenset({
    "delivery_claims",
    "execution_account_cache",
    "execution_actions",
    "execution_fills",
    "execution_funding",
    "execution_funding_coverage",
    "execution_ledger_poll",
    "execution_orders",
    "executions",
    "incident_notifications",
    "incidents",
    "job_runs",
    "manager_events",
    "manager_positions",
    "release_manifests",
    "runtime_heartbeats",
    "runtime_instances",
    "runtime_lease",
    "runtime_state",
    "setup_audit_events",
    "signal_lifecycle",
    "strategy_decisions",
    "trade_correlation",
    "trade_manager_messages",
})

MEMORY_TABLES = frozenset({
    "live_candidates",
    "live_context_observations",
    "live_market_events",
    "live_trade_outcomes",
})

# These names may be read by bounded startup importers.  They never become
# canonical names and must not appear in State or Live Memory.
LEGACY_IMPORT_TABLES = frozenset({
    "signals",
    "signal_execution_state",
    "trade_executions",
    "manager_execution_actions",
    "trade_manager_state",
    "trade_manager_events",
    "confirmed_execution_orders",
    "confirmed_execution_fills",
    "confirmed_execution_funding",
    "confirmed_execution_funding_coverage",
    "confirmed_execution_poll",
    "confirmed_execution_funding_poll",
})


class SchemaOwnershipError(RuntimeError):
    pass


@dataclass(frozen=True)
class SchemaOwnershipReport:
    state_tables: tuple[str, ...]
    memory_tables: tuple[str, ...]
    errors: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0]) for row in conn.execute(
            """SELECT name FROM sqlite_master
                 WHERE type='table' AND name NOT LIKE 'sqlite_%'"""
        ).fetchall()
    }


def inspect_schema_ownership(
    state: sqlite3.Connection,
    memory: sqlite3.Connection,
) -> SchemaOwnershipReport:
    state_tables = _tables(state)
    memory_tables = _tables(memory)
    expected_state = set(STATE_TABLES | MIGRATION_METADATA_TABLES)
    expected_memory = set(MEMORY_TABLES | MIGRATION_METADATA_TABLES)
    errors: list[str] = []

    for label, actual, expected in (
        ("state", state_tables, expected_state),
        ("memory", memory_tables, expected_memory),
    ):
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        if missing:
            errors.append(f"{label}_missing:" + ",".join(missing))
        if unexpected:
            errors.append(f"{label}_unexpected:" + ",".join(unexpected))

    overlap = sorted((state_tables & memory_tables) - MIGRATION_METADATA_TABLES)
    if overlap:
        errors.append("cross_store_overlap:" + ",".join(overlap))
    legacy_overlap = sorted(
        LEGACY_IMPORT_TABLES & (STATE_TABLES | MEMORY_TABLES)
    )
    if legacy_overlap:
        errors.append("legacy_canonical_overlap:" + ",".join(legacy_overlap))

    return SchemaOwnershipReport(
        state_tables=tuple(sorted(state_tables)),
        memory_tables=tuple(sorted(memory_tables)),
        errors=tuple(errors),
    )


def assert_schema_ownership(
    state: sqlite3.Connection,
    memory: sqlite3.Connection,
) -> SchemaOwnershipReport:
    report = inspect_schema_ownership(state, memory)
    if not report.ok:
        raise SchemaOwnershipError("schema_ownership_failed:" + ";".join(report.errors))
    return report


__all__ = [
    "LEGACY_IMPORT_TABLES",
    "MEMORY_TABLES",
    "MIGRATION_METADATA_TABLES",
    "STATE_TABLES",
    "SchemaOwnershipError",
    "SchemaOwnershipReport",
    "assert_schema_ownership",
    "inspect_schema_ownership",
]
