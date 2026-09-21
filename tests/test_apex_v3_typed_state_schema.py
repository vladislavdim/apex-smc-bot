import sqlite3

from apex.db.migrations import MigrationRunner
from apex.db.repositories.executions import ExecutionRepository
from apex.db.state_db import STATE_MIGRATIONS, migrate_state
from apex.domain.ids import derived_id


def _factory(connection: sqlite3.Connection):
    class _BorrowedConnection:
        def __getattr__(self, name):
            return getattr(connection, name)

        def close(self):
            pass

    return _BorrowedConnection


def test_migration_020_rebuilds_canonical_typed_relationships_with_data():
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys=ON")
    MigrationRunner(STATE_MIGRATIONS[:19]).run(connection)
    factory = _factory(connection)
    signal_entity_id = derived_id("signal", "migration-contract", 41)
    assert ExecutionRepository(factory).register({
        "signal_id": 41,
        "signal_entity_id": signal_entity_id,
        "mode": "live",
        "symbol": "BTCUSDT",
        "direction": "BULLISH",
        "status": "ENTRY_PENDING",
    })
    connection.execute(
        "INSERT INTO signal_lifecycle(signal_id,status) VALUES(41,'waiting_entry')"
    )
    connection.commit()

    assert migrate_state(connection) == (20,)
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert connection.execute(
        "SELECT signal_entity_id FROM executions WHERE signal_id=41"
    ).fetchone()[0] == signal_entity_id
    assert connection.execute(
        "SELECT signal_entity_id FROM signal_lifecycle WHERE signal_id=41"
    ).fetchone()[0] == signal_entity_id

    primary_keys = {
        table: tuple(
            row[1] for row in connection.execute(f"PRAGMA table_info({table})")
            if row[5]
        )
        for table in (
            "executions", "manager_positions", "execution_orders",
            "execution_funding", "execution_funding_coverage",
            "signal_lifecycle",
        )
    }
    assert primary_keys["executions"] == ("signal_entity_id",)
    assert primary_keys["manager_positions"] == ("signal_entity_id",)
    assert primary_keys["execution_orders"] == (
        "signal_entity_id", "kind", "remote_id",
    )
    assert primary_keys["execution_funding"] == (
        "signal_entity_id", "tran_id",
    )
    assert primary_keys["execution_funding_coverage"] == ("signal_entity_id",)
    assert primary_keys["signal_lifecycle"] == ("signal_entity_id",)

    for table in (
        "manager_positions", "manager_events", "execution_actions",
        "execution_orders", "execution_fills", "execution_funding",
        "execution_funding_coverage",
    ):
        foreign_keys = connection.execute(
            f"PRAGMA foreign_key_list({table})"
        ).fetchall()
        assert any(row[3] == "signal_entity_id" and row[4] == "signal_entity_id"
                   for row in foreign_keys)
