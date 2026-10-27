from __future__ import annotations

import copy
import json
import os
import socket
import sqlite3
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from apex.domain.enums import Strategy
from apex.strategies import native
from apex.strategies.parity_corpus import load_corpus_directory
from apex.strategies.registry import StrategyRegistry
from apex.strategies.wyckoff import WyckoffStrategy
from apex.strategies.wyckoff_sequence import strict_sequence_policy
from tests.wyckoff_sequence_fixture import END, MODES, fixture


ROOT = Path(native.__file__).resolve().parents[2]
CASES = [
    (kind, mode, scale)
    for kind in ("SPRING", "DISTRIBUTION")
    for scale in (0.5, 1.0, 2.0)
    for mode in MODES
]


def _adapter(policy):
    return WyckoffStrategy(
        (lambda symbol: None, lambda symbol: None, lambda symbol: None),
        (
            native.detect_wyckoff_spring,
            native.detect_wyckoff_distribution,
            native.detect_wyckoff_reaccumulation,
        ),
        sequence_policy=policy,
    )


def _snapshot(case):
    base = load_corpus_directory(ROOT / "tests/fixtures/apex_v3_strategy_parity")[
        0
    ].snapshot
    return replace(
        base,
        symbol="BTCUSDT",
        as_of=datetime.fromtimestamp(END, timezone.utc),
        candles={"1d": case["daily"], "4h": case["bars"]},
        market_context={},
        derivatives_context={},
        microstructure_context={},
    )


def _trace(registry, case):
    traces = registry.evaluate_snapshot(Strategy.WYCKOFF, _snapshot(case))
    return next(trace for trace in traces if trace.subtype == case["kind"])


def _stable_trace(trace):
    raw = dict(trace.raw_result) if trace.raw_result is not None else None
    if raw is not None:
        raw.pop("_audit_attempt_key", None)
    return replace(trace, raw_result=raw, attempt_key=None)


@pytest.mark.parametrize("kind,mode,scale", CASES)
def test_actual_registry_filters_only_unordered_native_candidates(kind, mode, scale):
    case = fixture(kind, mode, scale)
    before = copy.deepcopy(case)
    baseline = StrategyRegistry({Strategy.WYCKOFF: _adapter(None)})
    proposal = StrategyRegistry({Strategy.WYCKOFF: _adapter(strict_sequence_policy)})
    with (
        patch.object(
            socket.socket, "connect", side_effect=AssertionError("network forbidden")
        ) as network,
        patch.object(
            sqlite3, "connect", side_effect=AssertionError("DB forbidden")
        ) as database,
    ):
        old = _trace(baseline, case)
        new = _trace(proposal, case)
    network.assert_not_called()
    database.assert_not_called()
    assert bool(old.raw_result) == (mode not in {"no_trigger", "no_confirmation"})
    assert bool(new.raw_result) == case["oracle"]["expected_sequence"]
    if case["oracle"]["expected_sequence"]:
        assert _stable_trace(new) == _stable_trace(old)
    elif old.raw_result:
        assert new.outcome == "FILTERED" and new.raw_result is None
        assert new.stop == {
            "code": "WYCKOFF_SEQUENCE_ORDER",
            "reason": "INCOMPLETE_OR_ORDER_CONFLICT",
        }
        assert new.last_check and new.last_check.code == "WYCKOFF_SEQUENCE_ORDER"
        assert new.last_check.blocking and new.last_check.outcome == "FAIL"
    else:
        assert _stable_trace(new) == _stable_trace(old)
    assert case == before


@pytest.mark.parametrize(
    "kind,scale,bad",
    [
        (kind, scale, bad)
        for kind in ("SPRING", "DISTRIBUTION")
        for scale in (0.5, 1.0, 2.0)
        for bad in ("future", "missing_time")
    ],
)
def test_policy_fails_closed_on_unknown_candle_timing(kind, scale, bad):
    case = fixture(kind, "ordered", scale)
    if bad == "future":
        case["bars"][-1]["close_time"] = END + 14400
    elif bad == "unclosed":
        case["bars"][-1]["is_closed"] = False
    else:
        case["bars"][-1].pop("open_time")
    proof = strict_sequence_policy(_snapshot(case), kind)
    assert proof["applies"]
    assert not proof["sequence_complete"]
    assert proof["status"] == "TIMING_UNKNOWN"


@pytest.mark.parametrize("kind", ["SPRING", "DISTRIBUTION"])
def test_explicitly_unclosed_rows_are_excluded_like_native_detector(kind):
    case = fixture(kind, "ordered", 1.0)
    case["bars"][-1]["is_closed"] = False
    proof = strict_sequence_policy(_snapshot(case), kind)
    assert proof["applies"]
    assert proof["sequence_complete"]
    assert proof["status"] == "ORDERED"


def test_reaccumulation_is_explicitly_outside_the_policy():
    case = fixture("SPRING", "ordered", 1.0)
    proof = strict_sequence_policy(_snapshot(case), "REACCUMULATION")
    assert proof == {
        "applies": False,
        "sequence_complete": True,
        "status": "NOT_APPLICABLE",
    }


def test_frozen_33_registry_outputs_are_unchanged():
    environment = os.environ.copy()
    environment["TELEGRAM_TOKEN"] = "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijk"
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("r0028_frozen_compare.py"))],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    assert payload == {"cases": 33, "matched": 33, "ready": True, "reasons": []}
