from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from apex.domain.enums import Strategy
from apex.domain.ids import new_id
from apex.domain.models import MarketRegime, MarketSnapshot
from apex.strategies.base import trace_payload
from apex.strategies.fast import FastStrategy
from apex.strategies.mtf import MtfStrategy
from apex.strategies.registry import StrategyRegistry
from apex.strategies.swing import SwingStrategy
from apex.strategies.wyckoff import WyckoffStrategy
from apex.strategies.zone import ZoneStrategy
from core.setup_audit import audit_fail, audit_strategy, audit_test


def accepted(strategy: str, calls: list | None = None):
    @audit_strategy(strategy)
    def detector(symbol, *args, **kwargs):
        if calls is not None:
            calls.append((symbol, args, kwargs))
        audit_test(f"{strategy}.CORE", False, "core", "not core")
        audit_test(f"{strategy}.TRIGGER", False, "trigger", "not trigger")
        return {
            "symbol": symbol, "scan_type": strategy.lower(), "direction": "BULLISH",
            "entry": 100, "sl": 90, "tp1": 110, "tp2": 120, "rr": 2,
        }
    return detector


def rejected(strategy: str):
    @audit_strategy(strategy)
    def detector(symbol, *args, **kwargs):
        if audit_test(f"{strategy}.CORE", True, "core", "not core"):
            return audit_fail(f"{strategy}.NO_CORE", "core", {"symbol": symbol}, "not core")
        return None
    return detector


def snapshot(symbol="BTCUSDT"):
    return MarketSnapshot(
        snapshot_id=new_id("snapshot"), symbol=symbol,
        as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
        candles={"15m": ()}, structure={}, levels=(),
        regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
        volume={}, derivatives_context={}, microstructure_context={},
    )


class ApexV3StrategyTests(unittest.TestCase):
    def setUp(self):
        self.emit = patch("core.setup_audit.emit_event", return_value="event").start()

    def tearDown(self):
        patch.stopall()

    def test_fast_preserves_exact_legacy_result_and_chronology(self):
        trace = FastStrategy(accepted("FAST")).evaluate("btcusdt")[0]
        self.assertEqual(trace.outcome, "CANDIDATE")
        self.assertEqual(trace.raw_result["entry"], 100)
        self.assertEqual(trace.first_check.code, "FAST.CORE")
        self.assertEqual(trace.last_check.code, "FAST.TRIGGER")
        self.assertEqual([row.sequence for row in trace.checks], [1, 2])
        payload = trace_payload(trace)
        self.assertEqual(payload["first_check"]["check_id"], "FAST.CORE")
        self.assertEqual(payload["last_check"]["check_id"], "FAST.TRIGGER")
        self.assertEqual([row["sequence"] for row in payload["checks"]], [1, 2])

    def test_blocking_check_is_identified_without_guessing(self):
        trace = FastStrategy(rejected("FAST")).evaluate("BTCUSDT")[0]
        self.assertEqual(trace.outcome, "FILTERED")
        self.assertIsNone(trace.raw_result)
        self.assertTrue(trace.last_check.blocking)
        self.assertEqual(trace.stop["code"], "FAST.NO_CORE")

    def test_check_journal_carries_observed_operands_and_requirement(self):
        @audit_strategy("FAST")
        def detector(symbol):
            rr_value = 1.5
            if audit_test("FAST.RR", rr_value < 2.0, "RR minimum", "rr_value < 2.0"):
                return audit_fail("FAST.RR_LOW", "RR minimum", locals(), "rr_value < 2.0")
            return {"symbol": symbol}

        trace = FastStrategy(detector).evaluate("BTCUSDT")[0]
        payload = trace_payload(trace)
        check = payload["checks"][0]
        self.assertEqual(check["actual_value"]["operands"]["rr_value"], 1.5)
        self.assertTrue(check["actual_value"]["failure_predicate"])
        self.assertFalse(check["required_value"]["failure_predicate"])
        self.assertEqual(check["required_value"]["condition"], "rr_value < 2.0")

    def test_manifest_separates_hard_gates_from_context_and_removed_legacy_authority(self):
        @audit_strategy("FAST")
        def detector(symbol):
            audit_test("FAST_LTF_CONTEXT_DATA", False, "closed data", "not data_ok")
            audit_test("FAST_IMPULSE_VOLUME_CONTEXT", False, "volume context", "not volume_ok")
            audit_test("FAST_DETECT_FAST_DEAL_G9172", False, "legacy", "legacy_skip")
            return {"symbol": symbol, "entry": 100, "sl": 90, "tp1": 120, "rr": 2}

        trace = FastStrategy(detector).evaluate("BTCUSDT")[0]
        roles = {check.code: check.role for check in trace.checks}
        self.assertEqual(roles["FAST_LTF_CONTEXT_DATA"], "HARD_GATE")
        self.assertEqual(roles["FAST_IMPULSE_VOLUME_CONTEXT"], "LIVE_CONTEXT")
        self.assertEqual(roles["FAST_DETECT_FAST_DEAL_G9172"], "OBSERVED_CHECK")

    def test_adapter_arguments_match_existing_live_calls(self):
        mtf_calls, swing_calls, zone_calls = [], [], []
        MtfStrategy(accepted("MTF", mtf_calls)).evaluate(
            "AAVEUSDT", timeframe="1h", auto=True, passive_watch=True,
        )
        SwingStrategy(accepted("SWING", swing_calls)).evaluate("AAVEUSDT", timeframe="4h")
        ZoneStrategy(accepted("ZONE", zone_calls)).evaluate(
            "AAVEUSDT", timeframe="4h", passive_watch=True,
        )
        self.assertEqual(mtf_calls, [("AAVEUSDT", ("1h",), {"auto": True, "passive_watch": True})])
        self.assertEqual(swing_calls, [("AAVEUSDT", ("4h",), {})])
        self.assertEqual(zone_calls, [("AAVEUSDT", ("4h",), {"passive_watch": True})])

    def test_wyckoff_preserves_three_live_subtype_attempts(self):
        traces = WyckoffStrategy((
            rejected("WYCKOFF"), accepted("WYCKOFF"), rejected("WYCKOFF"),
        )).evaluate("BTCUSDT")
        self.assertEqual(len(traces), 3)
        self.assertEqual([trace.outcome for trace in traces], ["FILTERED", "CANDIDATE", "FILTERED"])

    def test_one_registry_serves_manual_and_scheduled_paths(self):
        adapters = {
            Strategy.FAST: FastStrategy(accepted("FAST")),
            Strategy.MTF: MtfStrategy(accepted("MTF")),
            Strategy.SWING: SwingStrategy(accepted("SWING")),
            Strategy.ZONE: ZoneStrategy(accepted("ZONE")),
            Strategy.WYCKOFF: WyckoffStrategy((accepted("WYCKOFF"), rejected("WYCKOFF"), rejected("WYCKOFF"))),
        }
        registry = StrategyRegistry(adapters)
        self.assertTrue(registry.complete())
        manual = registry.evaluate("FAST", "BTCUSDT")[0].raw_result
        scheduled = registry.evaluate(Strategy.FAST, "BTCUSDT")[0].raw_result
        stable = lambda row: {key: value for key, value in row.items() if key != "_audit_attempt_key"}
        self.assertEqual(stable(manual), stable(scheduled))

    def test_duplicate_strategy_registration_is_rejected(self):
        registry = StrategyRegistry()
        registry.register(FastStrategy(accepted("FAST")))
        with self.assertRaisesRegex(ValueError, "strategy_already_registered"):
            registry.register(FastStrategy(accepted("FAST")))

    def test_snapshot_path_fails_closed_without_snapshot_detector(self):
        registry = StrategyRegistry({
            Strategy.FAST: FastStrategy(accepted("FAST")),
        })
        with self.assertRaisesRegex(
            RuntimeError, "snapshot_detector_not_registered:FAST",
        ):
            registry.evaluate_snapshot(Strategy.FAST, snapshot())

    def test_snapshot_path_passes_exact_immutable_snapshot(self):
        received = []

        @audit_strategy("FAST")
        def detector(market_snapshot):
            received.append(market_snapshot)
            audit_test("FAST.SNAPSHOT", False, "snapshot", "not snapshot")
            return {
                "symbol": market_snapshot.symbol,
                "direction": "BULLISH", "entry": 100, "sl": 90,
                "tp1": 120, "rr": 2,
            }

        market_snapshot = snapshot("ETHUSDT")
        registry = StrategyRegistry({
            Strategy.FAST: FastStrategy(accepted("FAST"), detector),
        })

        trace = registry.evaluate_snapshot("FAST", market_snapshot)[0]

        self.assertIs(received[0], market_snapshot)
        self.assertEqual(trace.symbol, "ETHUSDT")
        self.assertEqual(trace.raw_result["entry"], 100)
        self.assertEqual(trace.first_check.code, "FAST.SNAPSHOT")


if __name__ == "__main__":
    unittest.main()
