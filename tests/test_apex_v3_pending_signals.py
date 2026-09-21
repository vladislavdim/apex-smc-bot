import sqlite3
import tempfile
import unittest

from apex.db.legacy_pending_signals import (
    check_pending_signals,
    configure_pending_signal_monitor,
)
from core.signal_lifecycle import barrier_hits, entry_touched


class PendingSignalMonitorTests(unittest.TestCase):
    def test_ambiguous_bullish_bar_preserves_stop_first_result(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as handle:
            conn = sqlite3.connect(handle.name)
            conn.execute(
                """CREATE TABLE signals(
                    id INTEGER PRIMARY KEY,symbol TEXT,direction TEXT,entry REAL,
                    tp1 REAL,tp2 REAL,tp3 REAL,sl REAL,timeframe TEXT,grade TEXT,
                    created_at TEXT,signal_type TEXT,estimated_hours REAL,
                    tp1_hit INTEGER,trailing_sl REAL,best_price REAL,
                    confluence INTEGER,regime TEXT,learning_id INTEGER,
                    result TEXT,closed_at TEXT)"""
            )
            conn.execute(
                """INSERT INTO signals VALUES(
                    1,'BTCUSDT','BULLISH',100,110,120,130,95,'1h','A',
                    '2026-01-01T00:00:00','MTF',72,0,NULL,NULL,8,'TREND',NULL,
                    'pending',NULL)"""
            )
            conn.commit()
            conn.close()
            emitted = []

            def connect(path=handle.name, **kwargs):
                return sqlite3.connect(path, **kwargs)

            configure_pending_signal_monitor(
                get_db_conn_fn=lambda **kwargs: connect(**kwargs),
                get_live_prices_fn=lambda: {"BTCUSDT": {"price": 100}},
                get_candles_fn=lambda *_args: [{"low": 94, "high": 111}],
                connector=connect,
                database_path=handle.name,
                lifecycle_available=True,
                lifecycle_active="active",
                lifecycle_cancelled="cancelled",
                lifecycle_waiting="waiting_entry",
                lifecycle_state_for=lambda conn, signal_id: "active",
                lifecycle_activated_at_for=lambda conn, signal_id: "2026-01-01T00:00:00",
                lifecycle_touch=lambda conn, signal_id: None,
                lifecycle_entry_touched=entry_touched,
                lifecycle_mark_active=lambda conn, signal_id: None,
                lifecycle_barrier_hits=barrier_hits,
                lifecycle_mark_finished=lambda conn, signal_id, state, result: None,
                emit_trade_stats_event=lambda *args, **kwargs: emitted.append((args, kwargs)),
            )

            closed = check_pending_signals()

            self.assertEqual(closed[0]["result"], "sl")
            self.assertEqual(closed[0]["exit_price"], 95)
            conn = sqlite3.connect(handle.name)
            result = conn.execute("SELECT result FROM signals WHERE id=1").fetchone()[0]
            conn.close()
            self.assertEqual(result, "sl")
            self.assertEqual(emitted[0][0][0], "CLOSE")


if __name__ == "__main__":
    unittest.main()
