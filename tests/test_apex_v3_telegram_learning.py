from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path

from apex.ui.telegram.learning import format_live_learning


class TelegramLiveLearningTests(unittest.TestCase):
    def test_stats_ui_uses_live_memory_not_legacy_signal_tables(self):
        launcher = Path("apex", "compatibility", "legacy_bot_runtime.py").read_text()
        commands = Path("apex/ui/telegram/commands.py").read_text()
        self.assertIn("async def stats", commands)
        self.assertIn("_format_live_learning", launcher)
        self.assertIn("live_stats=lambda: _format_live_learning", launcher)
        self.assertNotIn(
            "SELECT symbol, win_rate, total, avg_hours_to_tp FROM signal_learning",
            launcher + commands,
        )
        self.assertNotIn('Command("errors")', launcher + commands)

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE live_candidates(candidate_id TEXT, executed INTEGER);
            CREATE TABLE live_trade_outcomes(
                outcome_id TEXT, strategy TEXT, net_r REAL, closed_at TEXT
            );
            INSERT INTO live_candidates VALUES('cand-1',1),('cand-2',0);
            INSERT INTO live_trade_outcomes VALUES
                ('out-1','FAST',2.0,'2026-01-01T00:00:00+00:00'),
                ('out-2','FAST',-1.0,'2026-01-02T00:00:00+00:00');
            """
        )

    def tearDown(self):
        self.conn.close()

    def test_view_uses_only_live_tables_and_is_advisory(self):
        text = format_live_learning(lambda: self.conn)
        self.assertIn("Кандидаты: <b>2</b>", text)
        self.assertIn("Подтверждённые Binance-позиции: <b>1</b>", text)
        self.assertIn("FAST: N=2", text)
        self.assertIn("E=+0.50R", text)
        self.assertIn("ADVISORY", text)
        self.assertNotIn("Shadow", text)
        self.assertNotIn("Backtest", text)


if __name__ == "__main__":
    unittest.main()
