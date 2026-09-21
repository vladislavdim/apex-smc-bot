from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile

from apex.domain.enums import Strategy
from apex.domain.ids import new_id
from apex.domain.models import MarketRegime, MarketSnapshot
from apex.strategies.fast import FastStrategy
from apex.strategies.parity_corpus import (
    FrozenParityCase,
    REAL_MARKET_SOURCE,
    evaluate_activation_corpus,
    fixture_digest,
    load_frozen_case,
    write_frozen_case,
)


def snapshot() -> MarketSnapshot:
    return MarketSnapshot(
        snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
        as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
        candles={"15m": ({"open": 99, "high": 101, "low": 98,
                           "close": 100, "volume": 10},)},
        structure={}, levels=(),
        regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
        volume={}, derivatives_context={}, microstructure_context={},
    )


def detector(symbol):
    return {"symbol": symbol, "direction": "BULLISH", "entry": 100,
            "sl": 90, "tp": 120, "tp1": 110, "tp2": 120,
            "tp3": 130, "rr": 2}


def frozen_case() -> FrozenParityCase:
    market_snapshot = snapshot()
    captured_at = datetime(2026, 9, 20, 0, 1, tzinfo=timezone.utc)
    values = dict(
        case_id="gate-fast-btc-20260920-0000", strategy=Strategy.FAST,
        snapshot=market_snapshot, kwargs={}, source=REAL_MARKET_SOURCE,
        captured_at=captured_at,
    )
    return FrozenParityCase(
        **values, sha256=fixture_digest(**values),
    )


class StrategyParityCorpusTests(unittest.TestCase):
    def test_digest_detects_any_frozen_market_case_mutation(self):
        case = frozen_case()
        changed = replace(case, kwargs={"timeframe": "1h"})
        self.assertEqual(changed.validation_error(), "fixture_digest_mismatch")

    def test_non_real_market_provenance_is_rejected(self):
        case = replace(frozen_case(), source="SYNTHETIC")
        self.assertEqual(case.validation_error(), "real_market_source_required")

    def test_activation_fails_closed_when_any_strategy_is_not_covered(self):
        case = frozen_case()
        adapters = {Strategy.FAST: FastStrategy(detector, lambda snap: detector(snap.symbol))}
        result = evaluate_activation_corpus((case,), adapters, adapters)
        self.assertFalse(result.ready)
        self.assertEqual(len(result.reports), 1)
        self.assertTrue(result.reports[0].matched)
        self.assertEqual(
            set(result.reasons),
            {f"strategy_not_covered:{strategy.value}" for strategy in Strategy if strategy != Strategy.FAST},
        )

    def test_duplicate_case_ids_fail_closed(self):
        case = frozen_case()
        adapters = {Strategy.FAST: FastStrategy(detector, lambda snap: detector(snap.symbol))}
        result = evaluate_activation_corpus((case, case), adapters, adapters)
        self.assertIn(f"duplicate_case_id:{case.case_id}", result.reasons)

    def test_canonical_fixture_round_trip_preserves_digest(self):
        case = frozen_case()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fast.json"
            write_frozen_case(target, case)
            loaded = load_frozen_case(target)
        self.assertEqual(loaded, case)
        self.assertEqual(loaded.validation_error(), None)

    def test_capture_writer_never_overwrites_existing_fixture(self):
        case = frozen_case()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fast.json"
            write_frozen_case(target, case)
            original = target.read_bytes()
            with self.assertRaises(FileExistsError):
                write_frozen_case(target, case)
            self.assertEqual(target.read_bytes(), original)

    def test_loader_rejects_tampered_fixture(self):
        case = frozen_case()
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "fast.json"
            write_frozen_case(target, case)
            payload = json.loads(target.read_text(encoding="utf-8"))
            payload["snapshot"]["candles"]["15m"][0]["close"] = 999
            target.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "fixture_digest_mismatch"):
                load_frozen_case(target)


if __name__ == "__main__":
    unittest.main()
