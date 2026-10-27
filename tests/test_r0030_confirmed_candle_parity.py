from __future__ import annotations

import copy

import pytest

from apex.domain.enums import Strategy
from apex.strategies.registry import StrategyRegistry
from apex.strategies.wyckoff_sequence import strict_sequence_policy
from tests.wyckoff_sequence_expanded import expanded
from tests.test_r0028_wyckoff_registry_sequence import (
    _adapter,
    _snapshot,
    _stable_trace,
    _trace,
)


def _poison_unclosed(case, source: str) -> None:
    rows = case[source]
    row = rows[10] if source == "daily" else rows[-30]
    row.clear()
    row.update({"is_closed": False, "volume": "not-a-number"})


@pytest.mark.parametrize("kind", ["SPRING", "DISTRIBUTION"])
@pytest.mark.parametrize("scale", [0.5, 1.0, 2.0])
@pytest.mark.parametrize("source", ["daily", "bars"])
def test_policy_uses_same_confirmed_rows_as_native(kind, scale, source):
    case = expanded(kind, scale, 0)
    _poison_unclosed(case, source)
    before = copy.deepcopy(case)
    baseline = StrategyRegistry({Strategy.WYCKOFF: _adapter(None)})
    proposal = StrategyRegistry({Strategy.WYCKOFF: _adapter(strict_sequence_policy)})
    native_trace = _trace(baseline, case)
    guarded_trace = _trace(proposal, case)
    assert native_trace.outcome == "CANDIDATE" and native_trace.raw_result
    assert _stable_trace(guarded_trace) == _stable_trace(native_trace)
    assert strict_sequence_policy(_snapshot(case), kind)["status"] == "ORDERED"
    assert case == before


@pytest.mark.parametrize("kind", ["SPRING", "DISTRIBUTION"])
@pytest.mark.parametrize("scale", [0.5, 1.0, 2.0])
def test_unannotated_rows_follow_native_drop_last_rule(kind, scale):
    from tests.wyckoff_sequence_fixture import fixture

    case = fixture(kind, "no_confirmation", scale)
    last = case["bars"][-1]
    if kind == "SPRING":
        last.update(
            open=100 * scale,
            close=200 * scale,
            high=201 * scale,
            low=99 * scale,
            volume=1e9,
        )
    else:
        last.update(
            open=120 * scale,
            close=20 * scale,
            high=121 * scale,
            low=19 * scale,
            volume=1e9,
        )
    for row in case["daily"] + case["bars"]:
        row.pop("is_closed", None)
    baseline = StrategyRegistry({Strategy.WYCKOFF: _adapter(None)})
    proposal = StrategyRegistry({Strategy.WYCKOFF: _adapter(strict_sequence_policy)})
    assert _stable_trace(_trace(proposal, case)) == _stable_trace(
        _trace(baseline, case)
    )
    proof = strict_sequence_policy(_snapshot(case), kind)
    assert not proof["sequence_complete"]
    assert proof["status"] == "INCOMPLETE_OR_ORDER_CONFLICT"
