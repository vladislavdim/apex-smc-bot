from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from apex.domain.enums import Strategy
from apex.app.runtime import RuntimeSupervisor
from apex.strategies.activation import (
    SnapshotEvaluationBlocked, StrategyActivationSwitch,
)
from apex.strategies.capture import capture_gate_corpus
from apex.strategies.parity_runner import run_corpus
from tests.test_apex_v3_strategy_capture import NOW, snapshot_build
from tests.test_apex_v3_strategy_parity_runner import registry


class StrategyActivationTests(unittest.TestCase):
    def proof(self, root: Path) -> tuple[Path, Path]:
        provider = Mock()
        provider.build.side_effect = [snapshot_build() for _ in Strategy]
        corpus = root / "corpus"
        verdict = root / "verdict.json"
        capture_gate_corpus(
            provider, "BTCUSDT", corpus,
            as_of=NOW, clock=lambda: NOW,
        )
        run_corpus(corpus, verdict, registry())
        return corpus, verdict

    def test_not_requested_uses_legacy_without_building_snapshot(self):
        switch = StrategyActivationSwitch.from_proof(
            requested=False, corpus_directory="missing", verdict_path="missing",
        )
        provider = Mock()
        traces = switch.evaluate(
            registry(), provider, Strategy.FAST, "BTCUSDT",
        )
        self.assertFalse(switch.active)
        self.assertEqual(traces[0].outcome, "FILTERED")
        provider.build.assert_not_called()

    def test_runtime_exposes_strategy_activation_component(self):
        supervisor = RuntimeSupervisor()
        supervisor.activate()
        snapshot = supervisor.snapshot()
        self.assertIn("strategy_activation", snapshot["components"])

    def test_requested_without_valid_proof_stays_legacy(self):
        switch = StrategyActivationSwitch.from_proof(
            requested=True, corpus_directory="missing", verdict_path="missing",
        )
        provider = Mock()
        switch.evaluate(registry(), provider, Strategy.FAST, "BTCUSDT")
        self.assertFalse(switch.active)
        self.assertIn("SNAPSHOT_PROOF_INVALID", switch.reason)
        provider.build.assert_not_called()

    def test_ready_proof_uses_snapshot_evaluation(self):
        with tempfile.TemporaryDirectory() as directory:
            corpus, verdict = self.proof(Path(directory))
            switch = StrategyActivationSwitch.from_proof(
                requested=True, corpus_directory=corpus, verdict_path=verdict,
            )
        provider = Mock()
        provider.build.return_value = snapshot_build()
        traces = switch.evaluate(
            registry(), provider, Strategy.FAST, "BTCUSDT",
        )
        self.assertTrue(switch.active)
        self.assertEqual(switch.reason, "SNAPSHOT_PROOF_READY")
        self.assertEqual(traces[0].outcome, "FILTERED")
        provider.build.assert_called_once_with(Strategy.FAST, "BTCUSDT")

    def test_active_switch_never_falls_back_on_stale_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            corpus, verdict = self.proof(Path(directory))
            switch = StrategyActivationSwitch.from_proof(
                requested=True, corpus_directory=corpus, verdict_path=verdict,
            )
        provider = Mock()
        provider.build.return_value = snapshot_build(wait=("GATE_STALE_15M",))
        live_registry = registry()
        legacy = Mock(wraps=live_registry.evaluate)
        live_registry.evaluate = legacy
        with self.assertRaisesRegex(SnapshotEvaluationBlocked, "GATE_STALE_15M"):
            switch.evaluate(
                live_registry, provider, Strategy.FAST, "BTCUSDT",
            )
        legacy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
