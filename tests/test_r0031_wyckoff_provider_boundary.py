from __future__ import annotations

import copy
from datetime import datetime, timezone
from types import SimpleNamespace as NS
from unittest.mock import patch

import pytest

from apex.domain.enums import Strategy
from apex.market.candles import TIMEFRAME_SECONDS
from apex.market.provider import GateSnapshotProvider
from apex.strategies.registry import StrategyRegistry
from apex.strategies.wyckoff_sequence import strict_sequence_policy
from tests.wyckoff_sequence_expanded import expanded
from tests.test_r0028_wyckoff_registry_sequence import _adapter, _stable_trace


END = 1_792_972_800


class Client:
    def __init__(self, case: dict, mode: str, source: str) -> None:
        self.case, self.mode, self.source = case, mode, source

    def candles(self, symbol: str, timeframe: str, limit: int):
        assert symbol == "BTCUSDT"
        if timeframe == "4h":
            rows = copy.deepcopy(self.case["bars"])
        elif timeframe == "1d":
            rows = copy.deepcopy(self.case["daily"])
        else:
            step = TIMEFRAME_SECONDS["1w"]
            rows = [
                {
                    "open_time": END - step * (100 - index),
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 100,
                }
                for index in range(100)
            ]
        target = "1d" if self.source == "daily" else "4h"
        if timeframe == target and self.mode != "baseline":
            step = TIMEFRAME_SECONDS[timeframe]
            if self.mode == "unclosed":
                rows.append(
                    {
                        "open_time": END - 2 * step,
                        "open": 1,
                        "high": 1e12,
                        "low": 1e-12,
                        "close": 1,
                        "volume": 1e30,
                        "is_closed": False,
                    }
                )
            elif self.mode == "future":
                rows.append(
                    {
                        "open_time": END + 5 * step,
                        "open": 1,
                        "high": 1e12,
                        "low": 1e-12,
                        "close": 1,
                        "volume": 1e30,
                        "is_closed": True,
                    }
                )
            elif self.mode == "malformed_unclosed":
                rows.append({"is_closed": False, "volume": "not-a-number"})
        return NS(payload=rows[-limit:], received_at=END + 10)

    def __getattr__(self, name: str):
        def unavailable(*args, **kwargs):
            raise RuntimeError(f"optional context unavailable:{name}")

        return unavailable


def build(case: dict, mode: str, source: str):
    with patch(
        "apex.market.provider.fetch_live_context",
        return_value=NS(derivatives={}, microstructure={}),
    ):
        return GateSnapshotProvider(Client(case, mode, source), candle_limit=500).build(
            Strategy.WYCKOFF,
            "BTCUSDT",
            as_of=datetime.fromtimestamp(END, timezone.utc),
        )


def trace(build_result, kind: str):
    traces = StrategyRegistry(
        {
            Strategy.WYCKOFF: _adapter(strict_sequence_policy),
        }
    ).evaluate_snapshot(Strategy.WYCKOFF, build_result.snapshot)
    return next(item for item in traces if item.subtype == kind)


def signature(build_result):
    snapshot = build_result.snapshot
    return {
        field: getattr(snapshot, field)
        for field in (
            "candles",
            "structure",
            "regime",
            "volume",
            "market_context",
            "derivatives_context",
            "microstructure_context",
        )
    }


@pytest.mark.parametrize("kind", ["SPRING", "DISTRIBUTION"])
@pytest.mark.parametrize("scale", [0.5, 1.0, 2.0])
@pytest.mark.parametrize("source", ["daily", "bars"])
def test_actual_provider_defines_reachability_boundary(kind, scale, source):
    case = expanded(kind, scale, 0)
    before = copy.deepcopy(case)
    baseline = build(case, "baseline", source)
    baseline_trace = trace(baseline, kind)
    assert not baseline.wait_reason_codes
    assert baseline_trace.outcome == "CANDIDATE" and baseline_trace.raw_result

    for mode in ("unclosed", "future"):
        candidate = build(case, mode, source)
        assert signature(candidate) == signature(baseline)
        assert candidate.wait_reason_codes == baseline.wait_reason_codes
        assert _stable_trace(trace(candidate, kind)) == _stable_trace(baseline_trace)

    malformed = build(case, "malformed_unclosed", source)
    target = "1d" if source == "daily" else "4h"
    assert not malformed.snapshot.candles[target]
    assert "GATE_DATA_UNAVAILABLE" in malformed.wait_reason_codes
    malformed_trace = trace(malformed, kind)
    assert malformed_trace.outcome == "FILTERED"
    assert malformed_trace.raw_result is None
    assert case == before
