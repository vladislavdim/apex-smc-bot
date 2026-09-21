from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from apex.market.context_memory import persist_live_context


class LiveContextMemoryTests(unittest.TestCase):
    def test_forward_context_is_idempotent_and_unknown_is_not_written_as_zero(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "memory.db")
            context = {
                "symbol": "BTCUSDT",
                "timestamp": datetime(2026, 9, 11, tzinfo=timezone.utc).isoformat(),
                "open_interest": {
                    "source": "gate_contract_stats", "status": "fresh",
                    "age_seconds": 12, "value": 100,
                },
                "funding": {"source": None, "status": "unknown", "rate": None},
                "long_short_ratio": {
                    "source": "gate_contract_stats", "status": "fresh",
                    "age_seconds": 12, "accounts": 1.2,
                },
                "live_tape": {
                    "source": "gate_ws", "status": "fresh", "age_seconds": 1,
                    "cvd_real_delta_usd_60s": 250, "trade_count_60s": 5,
                },
            }
            with patch.dict(os.environ, {"APEX_MEMORY_DB_PATH": path}):
                self.assertEqual(persist_live_context(context), 3)
                self.assertEqual(persist_live_context(context), 0)
            with sqlite3.connect(path) as conn:
                rows = conn.execute(
                    "SELECT context_type,value_json FROM live_context_observations ORDER BY context_type"
                ).fetchall()
        self.assertEqual([row[0] for row in rows], ["CVD_REAL", "LONG_SHORT_RATIO", "OPEN_INTEREST"])
        values = {kind: json.loads(payload) for kind, payload in rows}
        self.assertEqual(values["CVD_REAL"]["cvd_real_delta_usd_60s"], 250)
        self.assertNotIn("FUNDING", values)


if __name__ == "__main__":
    unittest.main()
