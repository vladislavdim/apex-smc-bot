import gzip
import json
from pathlib import Path
import tempfile
import unittest

from research.snapshot import (
    SNAPSHOT_VERSION,
    SnapshotValidationError,
    build_manifest,
    validate_database,
    verify_compressed_assets,
)
from research.store import ResearchStore


def _candle(symbol: str, timeframe: str, open_time: int) -> dict:
    period = {"15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}[timeframe]
    return {"symbol": symbol, "timeframe": timeframe, "open_time": open_time,
            "close_time": open_time + period, "open": 100, "high": 101,
            "low": 99, "close": 100.5, "volume": 10, "is_closed": True}


def _artifacts(tmp_path: Path):
    db = tmp_path / "BTCUSDT.research.db"
    store = ResearchStore(str(db)); store.ensure_schema()
    for index, timeframe in enumerate(("15m", "1h", "4h", "1d"), 1):
        store.upsert_candles([_candle("BTCUSDT", timeframe, index * 100000)])
    run_id = store.save_run({"research_run_id": "run-1", "run_type": "TEST",
        "dataset_version": "test", "strategy_version": "test", "feature_version": "test",
        "code_sha": "test", "range_start": 1, "range_end": 200000,
        "universe": ["BTCUSDT"], "config": {}, "status": "COMPLETED",
        "progress": 100, "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T01:00:00+00:00"})
    dashboard = tmp_path / "BTCUSDT.dashboard.json"
    dashboard.write_text(json.dumps({"schema_version": 3,
        "runs": [{"research_run_id": run_id, "status": "COMPLETED", "progress": 100}],
        "storage": {"symbol": "BTCUSDT", "timeframes": ["15m", "1h", "4h", "1d"],
                    "snapshot_version": SNAPSHOT_VERSION, "research_run_id": run_id}}), encoding="utf-8")
    db_gz = tmp_path / "BTCUSDT.research.db.gz"
    dashboard_gz = tmp_path / "BTCUSDT.dashboard.json.gz"
    with db.open("rb") as src, gzip.open(db_gz, "wb") as dst: dst.write(src.read())
    with dashboard.open("rb") as src, gzip.open(dashboard_gz, "wb") as dst: dst.write(src.read())
    return db, dashboard, db_gz, dashboard_gz


class ResearchSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_snapshot_manifest_is_complete_and_idempotently_verifiable(self):
        db, dashboard, db_gz, dashboard_gz = _artifacts(self.tmp_path)
        manifest = build_manifest(db, dashboard, db_gz_path=db_gz, dashboard_gz_path=dashboard_gz)
        self.assertEqual(manifest["snapshot_version"], SNAPSHOT_VERSION)
        self.assertIs(manifest["no_real_execution"], True)
        self.assertEqual(manifest["coverage_policy"], "GATE_CANONICAL_ONLY_NO_VENUE_SUBSTITUTION")
        manifest_path = self.tmp_path / "BTCUSDT.manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.assertEqual(
            verify_compressed_assets(db_gz, dashboard_gz, manifest_path)["symbol"],
            "BTCUSDT",
        )
        self.assertEqual(validate_database(db)["counts"]["15m"]["count"], 1)

    def test_snapshot_rejects_paused_run_and_open_gap(self):
        db, _dashboard, _db_gz, _dashboard_gz = _artifacts(self.tmp_path)
        store = ResearchStore(str(db))
        store.save_run({"research_run_id": "run-1", "run_type": "TEST",
            "dataset_version": "test", "strategy_version": "test", "feature_version": "test",
            "code_sha": "test", "range_start": 1, "range_end": 200000,
            "universe": ["BTCUSDT"], "config": {}, "status": "PAUSED", "progress": 50,
            "started_at": "2026-01-01T00:00:00+00:00"})
        with self.assertRaisesRegex(SnapshotValidationError, "run_not_completed"):
            validate_database(db)

        # Rebuild a completed run, then introduce a repairable quality marker.
        store.save_run({"research_run_id": "run-1", "run_type": "TEST",
            "dataset_version": "test", "strategy_version": "test", "feature_version": "test",
            "code_sha": "test", "range_start": 1, "range_end": 200000,
            "universe": ["BTCUSDT"], "config": {}, "status": "COMPLETED", "progress": 100,
            "started_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T01:00:00+00:00"})
        store.save_quality_issue("BTCUSDT", "15m", "MISSING_CANDLES", open_time=100000)
        with self.assertRaisesRegex(SnapshotValidationError, "open_history_gaps"):
            validate_database(db)


if __name__ == "__main__":
    unittest.main()
