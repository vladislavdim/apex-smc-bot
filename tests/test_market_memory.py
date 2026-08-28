import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from core import market_memory as memory


CANDLES = [
    {"open": 100, "high": 102, "low": 99, "close": 101, "volume": 10},
    {"open": 101, "high": 103, "low": 100, "close": 102, "volume": 11},
    {"open": 102, "high": 102, "low": 97, "close": 98, "volume": 13},
    {"open": 98, "high": 101, "low": 98, "close": 100, "volume": 12},
    {"open": 100, "high": 105, "low": 99, "close": 104, "volume": 16},
    {"open": 104, "high": 106, "low": 103, "close": 105, "volume": 15},
]


class MarketMemoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.previous_db_path = memory.DB_PATH
        memory.DB_PATH = os.path.join(self.tmp.name, "brain.db")
        memory.init_market_memory()

    def tearDown(self):
        memory.DB_PATH = self.previous_db_path
        self.tmp.cleanup()

    def _capture(self, signal_id, direction="BULLISH"):
        memory.capture_snapshot(
            signal_id, "BTCUSDT", "MTF", direction, "1h",
            100, 95, 110, 120, 130, 85, "TREND",
            {"1h": CANDLES, "4h": CANDLES},
        )

    def test_snapshot_keeps_candles_and_zones_without_trade_calculation(self):
        self._capture(1)
        with memory._connect() as conn:
            row = conn.execute(
                "SELECT entry,sl,tp1,snapshot_json,zones_json FROM market_memory_snapshots WHERE signal_id=1"
            ).fetchone()
        self.assertEqual(row[:3], (100.0, 95.0, 110.0))
        self.assertIn('"candles"', row[3])
        self.assertIn('"supports"', row[4])

    def test_price_path_creates_objective_outcome_label(self):
        self._capture(2)
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        memory.record_price(2, 100, start)
        memory.record_price(2, 112, start + timedelta(minutes=6))
        memory.close_snapshot(2, "tp1", 112)
        with memory._connect() as conn:
            row = conn.execute(
                "SELECT outcome,outcome_label,max_favorable_pct,max_adverse_pct "
                "FROM market_memory_snapshots WHERE signal_id=2"
            ).fetchone()
        self.assertEqual(row[0], "tp1")
        self.assertEqual(row[1], "continuation")
        self.assertGreater(row[2], 0)

    def test_context_is_compact_and_never_claims_a_trade(self):
        for signal_id, outcome in ((3, "tp1"), (4, "sl"), (5, "tp2")):
            self._capture(signal_id)
            start = datetime(2026, 1, signal_id, tzinfo=timezone.utc)
            memory.record_price(signal_id, 100, start)
            memory.record_price(signal_id, 108 if outcome != "sl" else 94, start + timedelta(minutes=6))
            memory.close_snapshot(signal_id, outcome)
        context = memory.build_memory_context("BTCUSDT", "MTF", "BULLISH", "1h")
        rendered = memory.format_market_memory_context(context)
        self.assertTrue(context["available"])
        self.assertEqual(context["samples"], 3)
        self.assertIn("MARKET MEMORY", rendered)
        self.assertIn("must not create a trade", rendered)


if __name__ == "__main__":
    unittest.main()
