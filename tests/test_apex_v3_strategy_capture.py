from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from apex.domain.enums import Strategy
from apex.domain.ids import new_id
from apex.domain.models import MarketRegime, MarketSnapshot
from apex.market.snapshots import SnapshotBuild
from apex.strategies.capture import (
    DEFAULT_CASE_KWARGS, capture_gate_case, capture_gate_corpus,
)
from apex.strategies.parity_corpus import load_corpus_directory, load_frozen_case


NOW = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)


def snapshot_build(*, wait=()) -> SnapshotBuild:
    rows = ({
        "open_time": NOW.timestamp() - 1800, "close_time": NOW.timestamp() - 900,
        "open": 99, "high": 101, "low": 98, "close": 100,
        "volume": 10, "is_closed": True,
    },)
    market_snapshot = MarketSnapshot(
        snapshot_id=new_id("snapshot"), symbol="BTCUSDT", as_of=NOW,
        candles={"15m": rows}, structure={}, levels=(),
        regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
        volume={}, derivatives_context={}, microstructure_context={},
    )
    return SnapshotBuild(market_snapshot, {}, tuple(wait))


class StrategyCaptureTests(unittest.TestCase):
    def test_capture_pins_complete_gate_snapshot_with_strategy_defaults(self):
        provider = Mock()
        provider.build.return_value = snapshot_build()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fast.json"
            case = capture_gate_case(
                provider, Strategy.MTF, "btcusdt", target,
                as_of=NOW, clock=lambda: NOW,
            )
            loaded = load_frozen_case(target)
        provider.build.assert_called_once_with(Strategy.MTF, "BTCUSDT", as_of=NOW)
        self.assertEqual(case, loaded)
        self.assertEqual(case.kwargs, DEFAULT_CASE_KWARGS[Strategy.MTF])
        self.assertEqual(case.validation_error(), None)

    def test_capture_writes_nothing_when_gate_snapshot_is_not_ready(self):
        provider = Mock()
        provider.build.return_value = snapshot_build(wait=("GATE_STALE_15M",))
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fast.json"
            with self.assertRaisesRegex(RuntimeError, "capture_market_data_not_ready"):
                capture_gate_case(
                    provider, Strategy.FAST, "BTCUSDT", target,
                    as_of=NOW, clock=lambda: NOW,
                )
            self.assertFalse(target.exists())

    def test_capture_never_replaces_an_existing_case(self):
        provider = Mock()
        provider.build.return_value = snapshot_build()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fast.json"
            capture_gate_case(
                provider, Strategy.FAST, "BTCUSDT", target,
                as_of=NOW, clock=lambda: NOW,
            )
            with self.assertRaises(FileExistsError):
                capture_gate_case(
                    provider, Strategy.FAST, "BTCUSDT", target,
                    as_of=NOW, clock=lambda: NOW,
                )

    def test_full_corpus_is_published_only_after_all_five_cases(self):
        provider = Mock()
        provider.build.side_effect = [snapshot_build() for _ in Strategy]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "corpus"
            cases = capture_gate_corpus(
                provider, "BTCUSDT", target,
                as_of=NOW, clock=lambda: NOW,
            )
            self.assertTrue((target / "corpus.json").is_file())
            self.assertEqual(
                {path.name for path in target.glob("*.json")},
                {"corpus.json", "fast.json", "mtf.json", "swing.json",
                 "zone.json", "wyckoff.json"},
            )
            loaded = load_corpus_directory(target)
        self.assertEqual([case.strategy for case in cases], list(Strategy))
        self.assertEqual([case.strategy for case in loaded], list(Strategy))

    def test_failed_batch_leaves_no_partial_corpus(self):
        provider = Mock()
        provider.build.side_effect = [
            snapshot_build(), snapshot_build(wait=("GATE_DATA_UNAVAILABLE",)),
        ]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "corpus"
            with self.assertRaisesRegex(RuntimeError, "capture_market_data_not_ready"):
                capture_gate_corpus(
                    provider, "BTCUSDT", target,
                    as_of=NOW, clock=lambda: NOW,
                )
            self.assertFalse(target.exists())
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_manifest_drift_is_rejected(self):
        provider = Mock()
        provider.build.side_effect = [snapshot_build() for _ in Strategy]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "corpus"
            capture_gate_corpus(
                provider, "BTCUSDT", target,
                as_of=NOW, clock=lambda: NOW,
            )
            manifest = target / "corpus.json"
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            payload["cases"][0]["sha256"] = "0" * 64
            manifest.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "manifest_case_mismatch"):
                load_corpus_directory(target)


if __name__ == "__main__":
    unittest.main()
