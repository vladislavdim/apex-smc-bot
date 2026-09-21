from __future__ import annotations

import copy
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from unittest.mock import Mock

from apex.domain.enums import Strategy
from apex.domain.ids import new_id
from apex.domain.models import MarketRegime, MarketSnapshot
from apex.market.candle_router import GateCandleRouter
from apex.strategies.fast import FastStrategy
from apex.strategies.legacy_bridge import snapshot_symbol_detector
from apex.strategies.mtf import MtfStrategy
from apex.strategies.swing import SwingStrategy
from apex.strategies.zone import ZoneStrategy
from apex.strategies.parity import (
    StrategyParityError,
    compare_traces,
    evaluate_parity,
    evaluate_snapshot_parity,
    require_parity,
)
from apex.strategies.wyckoff import WyckoffStrategy
from core.setup_audit import audit_fail, audit_strategy, audit_test


def candidate_detector(strategy: str, *, subtype: str = "", sl: float = 90):
    @audit_strategy(strategy, subtype)
    def detector(symbol, *args, **kwargs):
        volume = 1.8
        audit_test(f"{strategy}.CORE", False, "core", "not core")
        audit_test(f"{strategy}.VOLUME", volume < 1.6, "volume", "volume < 1.6")
        return {
            "symbol": symbol,
            "direction": "BULLISH",
            "entry": 100,
            "sl": sl,
            "tp": 120,
            "tp1": 110,
            "tp2": 120,
            "tp3": 130,
            "rr": 2,
        }
    return detector


def rejected_detector(strategy: str, *, subtype: str = ""):
    @audit_strategy(strategy, subtype)
    def detector(symbol, *args, **kwargs):
        if audit_test(f"{strategy}.CORE", True, "core", "not core"):
            return audit_fail(f"{strategy}.NO_CORE", "core", locals(), "not core")
        return None
    return detector


class StrategyParityTests(unittest.TestCase):
    def setUp(self):
        patch("core.setup_audit.emit_event", return_value="event").start()

    def tearDown(self):
        patch.stopall()

    def test_identical_behavior_passes_despite_runtime_attempt_ids(self):
        report = evaluate_parity(
            "fast-closed-candle-001",
            FastStrategy(candidate_detector("FAST")),
            FastStrategy(candidate_detector("FAST")),
            "BTCUSDT",
        )
        self.assertTrue(report.matched)
        self.assertEqual(report.mismatches, ())
        require_parity(report)

    def test_geometry_change_fails_closed(self):
        report = evaluate_parity(
            "fast-closed-candle-002",
            FastStrategy(candidate_detector("FAST", sl=90)),
            FastStrategy(candidate_detector("FAST", sl=91)),
            "BTCUSDT",
        )
        self.assertFalse(report.matched)
        self.assertEqual([row.path for row in report.mismatches], ["traces[0].geometry"])
        with self.assertRaisesRegex(StrategyParityError, "strategy_parity_failed"):
            require_parity(report)

    def test_terminal_reason_change_is_detected(self):
        legacy = FastStrategy(rejected_detector("FAST")).evaluate("BTCUSDT")
        replacement = copy.deepcopy(legacy)
        changed = dict(replacement[0].stop or {})
        changed["code"] = "FAST.DIFFERENT_STOP"
        replacement = (replacement[0].__class__(
            strategy=replacement[0].strategy,
            symbol=replacement[0].symbol,
            outcome=replacement[0].outcome,
            raw_result=replacement[0].raw_result,
            checks=replacement[0].checks,
            stop=changed,
            attempt_key="different-runtime-id",
            subtype=replacement[0].subtype,
        ),)
        report = compare_traces("fast-reject-001", legacy, replacement)
        self.assertEqual([row.path for row in report.mismatches], ["traces[0].stop"])

    def test_wyckoff_subtype_and_order_are_part_of_contract(self):
        legacy = WyckoffStrategy((
            candidate_detector("WYCKOFF", subtype="SPRING"),
            rejected_detector("WYCKOFF", subtype="DISTRIBUTION"),
            rejected_detector("WYCKOFF", subtype="REACCUMULATION"),
        )).evaluate("BTCUSDT")
        self.assertEqual(
            [trace.subtype for trace in legacy],
            ["SPRING", "DISTRIBUTION", "REACCUMULATION"],
        )
        replacement = (legacy[1], legacy[0], legacy[2])
        report = compare_traces("wyckoff-sequence-001", legacy, replacement)
        paths = {row.path for row in report.mismatches}
        self.assertIn("traces[0].subtype", paths)
        self.assertIn("traces[1].subtype", paths)

    def test_mismatched_strategy_adapters_are_rejected_before_run(self):
        class Other:
            strategy = Strategy.MTF

            def evaluate(self, symbol, **kwargs):
                return ()

        with self.assertRaisesRegex(ValueError, "parity_strategy_mismatch"):
            evaluate_parity(
                "bad-strategy",
                FastStrategy(candidate_detector("FAST")),
                Other(),
                "BTCUSDT",
            )

    def test_fast_snapshot_parity_uses_fixture_without_second_gate_read(self):
        candles = [
            {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
             "volume": 10.0}
            for _ in range(30)
        ]
        fetch_gate = Mock(return_value={"candles": candles})
        router = GateCandleRouter(
            cache={}, get_shared=lambda *_args: [],
            update_shared=lambda *_args: None, fetch_gate=fetch_gate,
            gate_available=lambda: True, record_health=lambda *_args, **_kwargs: None,
            last_closed_at=lambda rows: None,
        )

        @audit_strategy("FAST")
        def detector(symbol):
            rows = router.get_candles(symbol, "15m", 30)
            close = rows[-1]["close"] if rows else None
            if audit_test("FAST.DATA", not rows, "closed candles", "not rows"):
                return audit_fail("FAST.NO_DATA", "closed candles", locals(), "not rows")
            return {
                "symbol": symbol, "direction": "BULLISH", "entry": close,
                "sl": 90, "tp": 120, "tp1": 120, "tp2": 130,
                "tp3": 140, "rr": 2,
            }

        market_snapshot = MarketSnapshot(
            snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
            as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
            candles={"15m": tuple(candles)}, structure={}, levels=(),
            regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
            volume={}, derivatives_context={}, microstructure_context={},
        )
        report = evaluate_snapshot_parity(
            "fast-snapshot-fixture-001",
            FastStrategy(detector),
            FastStrategy(detector, snapshot_symbol_detector(detector)),
            market_snapshot,
        )

        self.assertTrue(report.matched)
        fetch_gate.assert_called_once_with("BTCUSDT", "15m", 30)

    def test_mtf_snapshot_parity_preserves_timeframe_and_avoids_second_gate_reads(self):
        candles = {
            timeframe: [
                {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 10.0}
                for _ in range(30)
            ]
            for timeframe in ("15m", "1h", "4h", "1d")
        }
        fetch_gate = Mock(side_effect=lambda symbol, timeframe, limit: {
            "candles": candles[timeframe][-limit:],
        })
        router = GateCandleRouter(
            cache={}, get_shared=lambda *_args: [],
            update_shared=lambda *_args: None, fetch_gate=fetch_gate,
            gate_available=lambda: True, record_health=lambda *_args, **_kwargs: None,
            last_closed_at=lambda rows: None,
        )

        @audit_strategy("MTF")
        def detector(symbol, timeframe="1h", auto=False, passive_watch=False):
            rows = {
                interval: router.get_candles(symbol, interval, 30)
                for interval in ("15m", "1h", "4h", "1d")
            }
            missing = [interval for interval, values in rows.items() if not values]
            if audit_test("MTF.DATA", bool(missing), "all closed candles", "missing"):
                return audit_fail("MTF.NO_DATA", "all closed candles", locals(), "missing")
            close = rows[timeframe][-1]["close"]
            return {
                "symbol": symbol, "direction": "BULLISH", "entry": close,
                "sl": 90, "tp": 120, "tp1": 120, "tp2": 130,
                "tp3": 140, "rr": 2, "timeframe": timeframe,
                "auto": auto, "passive_watch": passive_watch,
            }

        market_snapshot = MarketSnapshot(
            snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
            as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
            candles={key: tuple(value) for key, value in candles.items()},
            structure={}, levels=(),
            regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
            volume={}, derivatives_context={}, microstructure_context={},
        )
        report = evaluate_snapshot_parity(
            "mtf-snapshot-fixture-001",
            MtfStrategy(detector),
            MtfStrategy(detector, snapshot_symbol_detector(detector)),
            market_snapshot,
            timeframe="4h", auto=True, passive_watch=True,
        )

        self.assertTrue(report.matched)
        self.assertEqual(fetch_gate.call_count, 4)
        self.assertEqual(
            [call.args for call in fetch_gate.call_args_list],
            [("BTCUSDT", interval, 30) for interval in ("15m", "1h", "4h", "1d")],
        )

    def test_swing_snapshot_parity_preserves_timeframe_without_second_gate_read(self):
        candles = [
            {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
             "volume": 10.0}
            for _ in range(30)
        ]
        fetch_gate = Mock(return_value={"candles": candles})
        router = GateCandleRouter(
            cache={}, get_shared=lambda *_args: [],
            update_shared=lambda *_args: None, fetch_gate=fetch_gate,
            gate_available=lambda: True, record_health=lambda *_args, **_kwargs: None,
            last_closed_at=lambda rows: None,
        )

        @audit_strategy("SWING")
        def detector(symbol, timeframe="4h"):
            rows = router.get_candles(symbol, timeframe, 30)
            if audit_test("SWING.DATA", not rows, "closed candles", "not rows"):
                return audit_fail("SWING.NO_DATA", "closed candles", locals(), "not rows")
            return {
                "symbol": symbol, "direction": "BEARISH", "entry": rows[-1]["close"],
                "sl": 110, "tp": 80, "tp1": 90, "tp2": 80,
                "tp3": 70, "rr": 2, "timeframe": timeframe,
            }

        market_snapshot = MarketSnapshot(
            snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
            as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
            candles={"1d": tuple(candles)}, structure={}, levels=(),
            regime=MarketRegime("TREND", "NORMAL", "IMPULSE"),
            volume={}, derivatives_context={}, microstructure_context={},
        )
        report = evaluate_snapshot_parity(
            "swing-snapshot-fixture-001",
            SwingStrategy(detector),
            SwingStrategy(detector, snapshot_symbol_detector(detector)),
            market_snapshot,
            timeframe="1d",
        )

        self.assertTrue(report.matched)
        fetch_gate.assert_called_once_with("BTCUSDT", "1d", 30)

    def test_zone_snapshot_parity_preserves_timeframe_and_passive_watch(self):
        candles = {
            timeframe: [
                {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 10.0}
                for _ in range(30)
            ]
            for timeframe in ("1h", "4h")
        }
        fetch_gate = Mock(side_effect=lambda symbol, timeframe, limit: {
            "candles": candles[timeframe][-limit:],
        })
        router = GateCandleRouter(
            cache={}, get_shared=lambda *_args: [],
            update_shared=lambda *_args: None, fetch_gate=fetch_gate,
            gate_available=lambda: True, record_health=lambda *_args, **_kwargs: None,
            last_closed_at=lambda rows: None,
        )

        @audit_strategy("ZONE")
        def detector(symbol, timeframe="4h", passive_watch=False):
            zone_rows = router.get_candles(symbol, timeframe, 30)
            confirmation_rows = router.get_candles(symbol, "1h", 30)
            missing = not zone_rows or not confirmation_rows
            if audit_test("ZONE.DATA", missing, "4h zone and closed 1h confirmation", "missing"):
                return audit_fail("ZONE.NO_DATA", "4h zone and closed 1h confirmation", locals(), "missing")
            return {
                "symbol": symbol, "direction": "BULLISH",
                "entry": confirmation_rows[-1]["close"], "sl": 90,
                "tp": 120, "tp1": 110, "tp2": 120, "tp3": 130,
                "rr": 2, "timeframe": timeframe,
                "passive_watch": passive_watch,
            }

        market_snapshot = MarketSnapshot(
            snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
            as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
            candles={key: tuple(value) for key, value in candles.items()},
            structure={}, levels=(),
            regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
            volume={}, derivatives_context={}, microstructure_context={},
        )
        report = evaluate_snapshot_parity(
            "zone-snapshot-fixture-001",
            ZoneStrategy(detector),
            ZoneStrategy(detector, snapshot_symbol_detector(detector)),
            market_snapshot,
            timeframe="4h", passive_watch=True,
        )

        self.assertTrue(report.matched)
        self.assertEqual(fetch_gate.call_count, 2)
        self.assertEqual(
            [call.args for call in fetch_gate.call_args_list],
            [("BTCUSDT", "4h", 30), ("BTCUSDT", "1h", 30)],
        )

    def test_wyckoff_snapshot_parity_preserves_all_subtypes_and_order(self):
        subtype_timeframes = (
            ("SPRING", "1h"),
            ("DISTRIBUTION", "4h"),
            ("REACCUMULATION", "1d"),
        )
        candles = {
            timeframe: [
                {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0,
                 "volume": 10.0}
                for _ in range(30)
            ]
            for _subtype, timeframe in subtype_timeframes
        }
        fetch_gate = Mock(side_effect=lambda symbol, timeframe, limit: {
            "candles": candles[timeframe][-limit:],
        })
        router = GateCandleRouter(
            cache={}, get_shared=lambda *_args: [],
            update_shared=lambda *_args: None, fetch_gate=fetch_gate,
            gate_available=lambda: True, record_health=lambda *_args, **_kwargs: None,
            last_closed_at=lambda rows: None,
        )

        def make_detector(subtype, timeframe):
            @audit_strategy("WYCKOFF", subtype)
            def detector(symbol):
                rows = router.get_candles(symbol, timeframe, 30)
                if audit_test(
                    f"WYCKOFF.{subtype}.DATA", not rows,
                    "closed candles", "not rows",
                ):
                    return audit_fail(
                        f"WYCKOFF.{subtype}.NO_DATA", "closed candles",
                        locals(), "not rows",
                    )
                return {
                    "symbol": symbol, "direction": "BULLISH",
                    "entry": rows[-1]["close"], "sl": 90,
                    "tp": 120, "tp1": 110, "tp2": 120, "tp3": 130,
                    "rr": 2, "subtype": subtype,
                }
            return detector

        detectors = tuple(
            make_detector(subtype, timeframe)
            for subtype, timeframe in subtype_timeframes
        )
        market_snapshot = MarketSnapshot(
            snapshot_id=new_id("snapshot"), symbol="BTCUSDT",
            as_of=datetime(2026, 9, 20, tzinfo=timezone.utc),
            candles={key: tuple(value) for key, value in candles.items()},
            structure={}, levels=(),
            regime=MarketRegime("RANGE", "NORMAL", "TRANSITION"),
            volume={}, derivatives_context={}, microstructure_context={},
        )
        report = evaluate_snapshot_parity(
            "wyckoff-snapshot-fixture-001",
            WyckoffStrategy(detectors),
            WyckoffStrategy(
                detectors,
                tuple(snapshot_symbol_detector(detector) for detector in detectors),
            ),
            market_snapshot,
        )

        self.assertTrue(report.matched)
        self.assertEqual(fetch_gate.call_count, 3)
        self.assertEqual(
            [call.args for call in fetch_gate.call_args_list],
            [("BTCUSDT", timeframe, 30) for _subtype, timeframe in subtype_timeframes],
        )


if __name__ == "__main__":
    unittest.main()
