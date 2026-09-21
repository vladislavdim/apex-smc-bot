from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from apex.db.memory_db import migrate_memory
from apex.telemetry.market_context import market_context_snapshot


class MarketContextProjectionTests(unittest.TestCase):
    def test_merges_live_gate_tape_with_latest_memory_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "memory.db")
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            migrate_memory(conn)
            conn.execute(
                """INSERT INTO live_context_observations
                   (observation_id,symbol,context_type,event_time,received_at,value_json,
                    status,source,freshness_seconds,quality)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    "obs-1", "BTCUSDT", "LONG_SHORT_RATIO",
                    "2026-09-21T20:00:00+00:00", "2026-09-21T20:00:01+00:00",
                    json.dumps({"value": {"accounts": 1.2}, "source": "gate_contract_stats"}),
                    "FRESH", "gate_contract_stats", 1.0, "VALID",
                ),
            )
            conn.commit()
            conn.close()

            def connect_memory(*, read_only=False):
                target = f"file:{path}?mode=ro" if read_only else path
                value = sqlite3.connect(target, uri=read_only)
                value.row_factory = sqlite3.Row
                return value

            tape = [{
                "symbol": "BTCUSDT", "source": "live_market_tape", "status": "FRESH",
                "age_seconds": 1, "cvd_real_delta_usd_60s": 500,
                "gate": {"oi": 1000, "funding": 0.0001},
                "orderbook": {
                    "source": "gate_ws", "freshness_status": "FRESH",
                    "liquidity_kind": "VISIBLE_ORDERBOOK_LIQUIDITY",
                    "heatmap_levels": [{"side": "BID", "price": 100, "size": 2}],
                },
            }]

            with patch("apex.telemetry.market_context.connect_memory", side_effect=connect_memory), \
                 patch("apex.telemetry.market_context.live_tape_snapshot", return_value=tape):
                rows = market_context_snapshot(5)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["gate"]["oi"], 1000)
        self.assertEqual(
            rows[0]["orderbook"]["liquidity_kind"],
            "VISIBLE_ORDERBOOK_LIQUIDITY",
        )
        self.assertEqual(
            rows[0]["observations"]["LONG_SHORT_RATIO"]["value"]["value"]["accounts"],
            1.2,
        )


if __name__ == "__main__":
    unittest.main()
