import concurrent.futures
import sqlite3
import tempfile
import unittest
from pathlib import Path

from core.signal_delivery import (
    claim_signal_delivery,
    release_signal_delivery_claim,
    signal_delivery_key,
)


class SignalDeliveryTests(unittest.TestCase):
    def test_delivery_key_ignores_display_grade(self):
        base = {
            "symbol": "aaveusdt",
            "direction": "BULLISH",
            "timeframe": "4H",
            "grade": "ZONE 3/8",
        }
        changed = dict(base, grade="ZONE 8/8")
        self.assertEqual(
            signal_delivery_key(base, "ZONE"),
            signal_delivery_key(changed, "ZONE"),
        )

    def test_only_one_concurrent_delivery_claim_wins(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "brain.db")
            key = "AAVEUSDT:ZONE:BULLISH:4h"
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(
                    lambda _: claim_signal_delivery(db_path, key, 1000.0, 4 * 3600),
                    range(8),
                ))
            self.assertEqual(results.count(True), 1)
            self.assertEqual(results.count(False), 7)

    def test_failed_delivery_releases_only_its_own_claim(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "brain.db")
            key = "AAVEUSDT:ZONE:BULLISH:4h"
            self.assertTrue(claim_signal_delivery(db_path, key, 1000.0, 4 * 3600))
            release_signal_delivery_claim(db_path, key, 999.0)
            self.assertFalse(claim_signal_delivery(db_path, key, 1001.0, 4 * 3600))
            release_signal_delivery_claim(db_path, key, 1000.0)
            self.assertTrue(claim_signal_delivery(db_path, key, 1002.0, 4 * 3600))
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(conn.execute(
                    "SELECT sent_at FROM signal_cooldown WHERE cache_key=?", (key,)
                ).fetchone(), (1002.0,))


if __name__ == "__main__":
    unittest.main()
