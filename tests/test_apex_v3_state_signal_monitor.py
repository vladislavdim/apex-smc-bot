import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta

from apex.db.repositories.signal_lifecycle import SignalLifecycleRepository
from apex.db.legacy_pending_signals import check_pending_signals, configure_pending_signal_monitor
from apex.db.state_db import migrate_state
from apex.strategies.state_signal_monitor import StateSignalMonitor
from core.signal_lifecycle import (
    activated_at_for, barrier_hits, entry_touched, mark_active, mark_finished,
    state_for, touch,
)


class StateSignalMonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        path = os.path.join(self.temp.name, "state.db")
        conn = sqlite3.connect(path); migrate_state(conn); conn.close()
        self.repository = SignalLifecycleRepository(lambda: sqlite3.connect(path))
        self.events = []

    def seed(self, *, signal_id=1, status="active", strategy="MTF", tp1_hit=0,
             trailing_sl=None, best_price=None, activated_at=None):
        before = (datetime.now() - timedelta(hours=1)).isoformat()
        self.repository.import_row({
            "signal_id": signal_id, "symbol": "BTCUSDT", "direction": "BULLISH",
            "signal_type": strategy, "timeframe": "1h", "entry": 100,
            "sl": 95, "tp1": 110, "tp2": 120, "tp3": 130,
            "grade": "A", "status": status, "result": "pending",
            "created_at": before,
            "activated_at": activated_at or (before if status == "active" else None),
            "tp1_hit": tp1_hit, "trailing_sl": trailing_sl,
            "best_price": best_price, "estimated_hours": 72,
        })

    def monitor(self, price, candle):
        return StateSignalMonitor(
            self.repository,
            lambda: {"BTCUSDT": {"price": price}},
            lambda *_: [candle],
            lambda *args, **kwargs: self.events.append((args, kwargs)),
        )

    def test_ambiguous_active_bar_closes_stop_first_once(self):
        self.seed()
        monitor = self.monitor(100, {"low": 94, "high": 111})
        first = monitor.check()
        self.assertEqual(first[0]["result"], "sl")
        self.assertEqual(first[0]["exit_price"], 95)
        self.assertEqual(monitor.check(), [])
        self.assertEqual([event[0][0] for event in self.events], ["CLOSE"])

    def test_waiting_entry_cancels_without_inferred_fill(self):
        self.seed(status="waiting_entry")
        monitor = self.monitor(94, {"low": 94, "high": 111})
        self.assertEqual(monitor.check()[0]["reason"], "stop_reached_before_confirmed_entry")
        self.assertEqual(self.repository.get(1)["status"], "cancelled")
        self.assertEqual(self.events, [])

    def test_fast_stop_wins_even_when_tp2_touched(self):
        self.seed(strategy="FAST")
        monitor = self.monitor(100, {"low": 94, "high": 121})
        self.assertEqual(monitor.check()[0]["result"], "sl")
        self.assertEqual(monitor.check(), [])

    def test_waiting_target_does_not_become_a_trade(self):
        self.seed(status="waiting_entry")
        monitor = self.monitor(111, {"low": 99, "high": 112})
        self.assertEqual(monitor.check()[0]["reason"], "target_reached_without_entry")
        self.assertEqual(self.events, [])

    def test_tp1_trailing_then_tp2_matches_legacy_sequence(self):
        self.seed()
        first = self.monitor(110, {"low": 105, "high": 111})
        self.assertEqual(first.check()[0]["result"], "tp1_hit")
        self.assertEqual(self.repository.get(1)["trailing_sl"], 104.0)
        second = self.monitor(120, {"low": 110, "high": 121})
        self.assertEqual(second.check()[0]["result"], "tp2")
        self.assertEqual(second.check(), [])

    def test_legacy_and_state_ambiguous_bar_outcomes_match(self):
        self.seed()
        legacy_path = os.path.join(self.temp.name, "legacy.db")
        conn = sqlite3.connect(legacy_path)
        conn.executescript("""CREATE TABLE signals(
            id INTEGER PRIMARY KEY,symbol TEXT,direction TEXT,entry REAL,
            tp1 REAL,tp2 REAL,tp3 REAL,sl REAL,timeframe TEXT,grade TEXT,
            created_at TEXT,signal_type TEXT,estimated_hours REAL,tp1_hit INTEGER,
            trailing_sl REAL,best_price REAL,confluence INTEGER,regime TEXT,
            learning_id INTEGER,result TEXT,closed_at TEXT);
            CREATE TABLE signal_execution_state(
                signal_id INTEGER PRIMARY KEY,status TEXT,activated_at TEXT,
                last_checked_at TEXT,closed_at TEXT,cancel_reason TEXT);
        """)
        previous = (datetime.now() - timedelta(hours=1)).isoformat()
        conn.execute(
            """INSERT INTO signals VALUES(
                1,'BTCUSDT','BULLISH',100,110,120,130,95,'1h','A',
                ?,'MTF',72,0,NULL,NULL,8,'TREND',NULL,'pending',NULL)""",
            (previous,),
        )
        conn.execute(
            "INSERT INTO signal_execution_state VALUES(1,'active',?,NULL,NULL,NULL)",
            (previous,),
        )
        conn.commit(); conn.close()
        legacy_events = []
        connector = lambda path=legacy_path, **kwargs: sqlite3.connect(path, **kwargs)
        configure_pending_signal_monitor(
            get_db_conn_fn=lambda **kwargs: connector(**kwargs),
            get_live_prices_fn=lambda: {"BTCUSDT": {"price": 100}},
            get_candles_fn=lambda *_: [{"low": 94, "high": 111}],
            connector=connector, database_path=legacy_path,
            lifecycle_available=True, lifecycle_active="active",
            lifecycle_cancelled="cancelled", lifecycle_waiting="waiting_entry",
            lifecycle_state_for=state_for, lifecycle_activated_at_for=activated_at_for,
            lifecycle_touch=touch, lifecycle_entry_touched=entry_touched,
            lifecycle_mark_active=mark_active, lifecycle_barrier_hits=barrier_hits,
            lifecycle_mark_finished=mark_finished,
            emit_trade_stats_event=lambda *args, **kwargs: legacy_events.append((args, kwargs)),
        )
        legacy = check_pending_signals()
        state = self.monitor(100, {"low": 94, "high": 111}).check()
        for field in ("signal_id", "symbol", "result", "is_win", "exit_price"):
            self.assertEqual(state[0][field], legacy[0][field])
        self.assertEqual(self.events[0][0][0], legacy_events[0][0][0])


if __name__ == "__main__":
    unittest.main()
