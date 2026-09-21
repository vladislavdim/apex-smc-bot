from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from apex.db.connection import compatibility_connection, connect_compatibility
from apex.db.compatibility_runtime import get_db_conn


class CompatibilityConnectionTests(unittest.TestCase):
    def test_market_runtime_connection_is_owned_by_db_layer(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "runtime.db")
            conn = get_db_conn(path=path, timeout=1)
            try:
                conn.execute("CREATE TABLE sample(value TEXT)")
                self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
                self.assertEqual(conn.execute("PRAGMA busy_timeout").fetchone()[0], 30000)
            finally:
                conn.close()

    def test_explicit_bridge_does_not_monkey_patch_sqlite(self):
        original = sqlite3.connect
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "legacy.db")
            conn = connect_compatibility(path, timeout=1, check_same_thread=True)
            try:
                conn.execute("CREATE TABLE sample(value TEXT)")
                self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
                self.assertEqual(conn.execute("PRAGMA busy_timeout").fetchone()[0], 30000)
            finally:
                conn.close()
        self.assertIs(sqlite3.connect, original)

    def test_commits_and_applies_canonical_pragmas(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "legacy.db")
            with compatibility_connection(path=path) as conn:
                conn.execute("CREATE TABLE sample(value TEXT)")
                conn.execute("INSERT INTO sample VALUES ('kept')")
                self.assertEqual(conn.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                self.assertEqual(conn.execute("PRAGMA busy_timeout").fetchone()[0], 30000)

            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("SELECT value FROM sample").fetchone()[0], "kept")

    def test_rolls_back_failed_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "legacy.db")
            with compatibility_connection(path=path) as conn:
                conn.execute("CREATE TABLE sample(value TEXT)")

            with self.assertRaises(RuntimeError):
                with compatibility_connection(path=path) as conn:
                    conn.execute("INSERT INTO sample VALUES ('discarded')")
                    raise RuntimeError("stop")

            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM sample").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
