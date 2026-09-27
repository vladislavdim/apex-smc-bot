"""New entries must fail closed when pair ownership cannot be established."""
import sqlite3
import unittest

from apex.strategies.pending_thesis import has_pending_thesis


class PairOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.state = sqlite3.connect(":memory:")
        self.state.execute("CREATE TABLE executions(signal_entity_id TEXT,symbol TEXT,mode TEXT,position_id TEXT)")
        self.state.execute("CREATE TABLE signal_lifecycle(signal_entity_id TEXT,status TEXT)")
        self.legacy = sqlite3.connect(":memory:")
        self.legacy.execute("CREATE TABLE signals(symbol TEXT,result TEXT)")

    def tearDown(self):
        self.state.close()
        self.legacy.close()

    def _factory(self, source):
        class Borrowed:
            def execute(self, *args):
                return source.execute(*args)
            def close(self):
                pass
        return Borrowed

    def check(self, pair="BTCUSDT"):
        return has_pending_thesis(pair, self._factory(self.state), self._factory(self.legacy))

    def test_state_position_and_legacy_pending_each_block_duplicate(self):
        self.state.execute("INSERT INTO executions VALUES('signal_1','BTCUSDT','live','position_1')")
        self.state.execute("INSERT INTO signal_lifecycle VALUES('signal_1','active')")
        self.assertTrue(self.check())
        self.state.execute("DELETE FROM executions")
        self.legacy.execute("INSERT INTO signals VALUES('BTCUSDT','pending')")
        self.assertTrue(self.check())
        self.legacy.execute("UPDATE signals SET result='tp1'")
        self.assertFalse(self.check())

    def test_unreadable_store_cannot_report_pair_free(self):
        self.state.execute("DROP TABLE executions")
        with self.assertRaises(sqlite3.OperationalError):
            self.check()
        self.state.execute("CREATE TABLE executions(signal_entity_id TEXT,symbol TEXT,mode TEXT,position_id TEXT)")
        self.legacy.execute("DROP TABLE signals")
        with self.assertRaises(sqlite3.OperationalError):
            self.check()
        with self.assertRaises(ValueError):
            self.check("")
