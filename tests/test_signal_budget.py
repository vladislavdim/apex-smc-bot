import os
import tempfile
import unittest
from unittest.mock import patch

from core import signal_budget


class SignalBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp.name, "budget.db")

    def tearDown(self):
        self.temp.cleanup()

    def test_non_fast_is_limited_and_fast_wyckoff_are_exempt(self):
        mtf = {"symbol": "BTCUSDT", "direction": "BULLISH", "timeframe": "1h", "grade": "MTF"}
        with patch.object(signal_budget, "DB_PATH", self.db_path), patch.dict(os.environ, {"NON_FAST_WEEKLY_SIGNAL_LIMIT": "2"}):
            self.assertTrue(signal_budget.weekly_budget_status(mtf)["allowed"])
            signal_budget.record_signal_delivery(mtf)
            signal_budget.record_signal_delivery({**mtf, "symbol": "ETHUSDT"})
            self.assertFalse(signal_budget.weekly_budget_status(mtf)["allowed"])
            self.assertTrue(signal_budget.weekly_budget_status({**mtf, "grade": "FAST"})["allowed"])
            self.assertTrue(signal_budget.weekly_budget_status({**mtf, "grade": "WYCKOFF"})["allowed"])
