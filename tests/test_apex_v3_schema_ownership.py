from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path

from apex.db.memory_db import migrate_memory
from apex.db.ownership import (
    LEGACY_IMPORT_TABLES,
    MEMORY_TABLES,
    STATE_TABLES,
    SchemaOwnershipError,
    assert_schema_ownership,
    inspect_schema_ownership,
)
from apex.db.state_db import migrate_state


class SchemaOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.state = sqlite3.connect(":memory:")
        self.memory = sqlite3.connect(":memory:")
        self.addCleanup(self.state.close)
        self.addCleanup(self.memory.close)
        migrate_state(self.state)
        migrate_memory(self.memory)

    def test_fresh_v3_schemas_have_exact_disjoint_owners(self):
        report = assert_schema_ownership(self.state, self.memory)
        self.assertTrue(report.ok)
        self.assertEqual(set(report.state_tables) - {"apex_schema_migrations"}, STATE_TABLES)
        self.assertEqual(set(report.memory_tables) - {"apex_schema_migrations"}, MEMORY_TABLES)
        self.assertFalse(LEGACY_IMPORT_TABLES & (STATE_TABLES | MEMORY_TABLES))

    def test_missing_owned_table_fails_closed(self):
        self.state.execute("DROP TABLE manager_events")
        report = inspect_schema_ownership(self.state, self.memory)
        self.assertFalse(report.ok)
        self.assertIn("state_missing:manager_events", report.errors)
        with self.assertRaisesRegex(SchemaOwnershipError, "state_missing:manager_events"):
            assert_schema_ownership(self.state, self.memory)

    def test_cross_store_table_fails_closed(self):
        self.state.execute("CREATE TABLE live_candidates(candidate_id TEXT)")
        report = inspect_schema_ownership(self.state, self.memory)
        self.assertIn("state_unexpected:live_candidates", report.errors)
        self.assertIn("cross_store_overlap:live_candidates", report.errors)

    def test_runtime_invokes_gate_after_both_migrations(self):
        source = Path("apex", "compatibility", "legacy_bot_runtime.py").read_text(encoding="utf-8")
        prepare = source.index("def _v3_prepare_databases():")
        state = source.index("_v3_migrate_state(state)", prepare)
        memory = source.index("_v3_migrate_memory(memory)", state)
        ownership = source.index("_v3_assert_schema_ownership(state, memory)", memory)
        manifest = source.index("_v3_build_release_manifest", ownership)
        self.assertLess(state, memory)
        self.assertLess(memory, ownership)
        self.assertLess(ownership, manifest)


if __name__ == "__main__":
    unittest.main()
