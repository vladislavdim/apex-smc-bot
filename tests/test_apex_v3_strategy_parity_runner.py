from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from apex.domain.enums import Strategy
from apex.strategies.base import StrategyTrace
from apex.strategies.capture import capture_gate_corpus
from apex.strategies.parity_corpus import load_corpus_directory
from apex.strategies.parity_runner import load_activation_verdict, run_corpus
from apex.strategies.registry import StrategyRegistry
from tests.test_apex_v3_strategy_capture import NOW, snapshot_build


class StaticAdapter:
    def __init__(self, strategy: Strategy, *, snapshot_outcome: str = "FILTERED"):
        self.strategy = strategy
        self.snapshot_outcome = snapshot_outcome

    def _trace(self, symbol: str, outcome: str) -> tuple[StrategyTrace, ...]:
        return (StrategyTrace(
            strategy=self.strategy, symbol=symbol, outcome=outcome,
            raw_result=None, checks=(), stop=None, attempt_key=None,
        ),)

    def evaluate(self, symbol: str, **kwargs):
        return self._trace(symbol, "FILTERED")

    def evaluate_snapshot(self, snapshot, **kwargs):
        return self._trace(snapshot.symbol, self.snapshot_outcome)


def registry(*, mismatch: Strategy | None = None) -> StrategyRegistry:
    return StrategyRegistry({
        strategy: StaticAdapter(
            strategy,
            snapshot_outcome="CANDIDATE" if strategy == mismatch else "FILTERED",
        )
        for strategy in Strategy
    })


class StrategyParityRunnerTests(unittest.TestCase):
    def corpus(self, root: Path) -> Path:
        provider = Mock()
        provider.build.side_effect = [snapshot_build() for _ in Strategy]
        target = root / "corpus"
        capture_gate_corpus(
            provider, "BTCUSDT", target,
            as_of=NOW, clock=lambda: NOW,
        )
        return target

    def test_matching_corpus_creates_hashed_ready_verdict(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            verdict = root / "verdict.json"
            result = run_corpus(self.corpus(root), verdict, registry())
            payload = json.loads(verdict.read_text(encoding="utf-8"))
        digest = payload.pop("verdict_sha256")
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
        self.assertTrue(result.ready)
        self.assertTrue(payload["ready"])
        self.assertEqual(digest, hashlib.sha256(encoded).hexdigest())
        self.assertEqual(len(payload["cases"]), 5)

    def test_mismatch_creates_blocked_verdict_and_never_claims_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            verdict = root / "verdict.json"
            result = run_corpus(
                self.corpus(root), verdict, registry(mismatch=Strategy.ZONE),
            )
            payload = json.loads(verdict.read_text(encoding="utf-8"))
        self.assertFalse(result.ready)
        self.assertFalse(payload["ready"])
        self.assertIn("parity_mismatch:gate-zone-btcusdt-1789905600", payload["reasons"])

    def test_release_loader_rejects_blocked_or_tampered_verdict(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = self.corpus(root)
            cases = load_corpus_directory(corpus)
            blocked = root / "blocked.json"
            run_corpus(corpus, blocked, registry(mismatch=Strategy.ZONE))
            with self.assertRaisesRegex(ValueError, "verdict_not_ready"):
                load_activation_verdict(blocked, cases)

            ready = root / "ready.json"
            run_corpus(corpus, ready, registry())
            payload = json.loads(ready.read_text(encoding="utf-8"))
            payload["cases"][0]["fixture_sha256"] = "0" * 64
            ready.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "digest_mismatch"):
                load_activation_verdict(ready, cases)

    def test_release_loader_rejects_ready_verdict_for_another_corpus(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.corpus(root)
            verdict = root / "verdict.json"
            run_corpus(first, verdict, registry())
            cases = list(load_corpus_directory(first))
            cases[0] = replace(cases[0], sha256="f" * 64)
            with self.assertRaisesRegex(ValueError, "case_mismatch"):
                load_activation_verdict(verdict, tuple(cases))

    def test_existing_verdict_is_never_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            verdict = root / "verdict.json"
            corpus = self.corpus(root)
            run_corpus(corpus, verdict, registry())
            original = verdict.read_bytes()
            with self.assertRaises(FileExistsError):
                run_corpus(corpus, verdict, registry())
            self.assertEqual(verdict.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
