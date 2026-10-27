"""Research regression tests: one proof-gated technical admission route.

AST extraction runs the unmodified production function bodies with isolated
boundary doubles; no Telegram, model, database or exchange calls are allowed.
"""

import ast
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import logging
from pathlib import Path
import time
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from apex.domain.enums import Strategy
from apex.strategies.activation import (
    StrategyActivationSwitch,
    SnapshotEvaluationBlocked,
)


def function(name, namespace):
    source = ast.parse(Path("apex/app/worker.py").read_text())
    node = next(
        n
        for n in source.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
    )
    node.decorator_list = []
    exec(
        compile(ast.Module(body=[node], type_ignores=[]), "actual_worker_body", "exec"),
        namespace,
    )
    return namespace[name]


def watcher_namespace(result=None, error=None):
    canonical = Mock(return_value=result, side_effect=error)
    legacy = Mock(
        return_value=dict(
            symbol="BTCUSDT", direction="BULLISH", entry=100, sl=95, tp1=110
        )
    )
    namespace = dict(
        asyncio=asyncio,
        logging=logging,
        DB_PATH="unused",
        _due_ltf_watches=Mock(return_value=[dict(strategy="MTF", symbol="BTCUSDT")]),
        _control_scan_scope=AsyncMock(),
        _control_scan_pair=AsyncMock(),
        _v3_strategy_candidate=canonical,
        full_scan_raw=legacy,
        _touch_ltf_watch=Mock(),
        _is_entry_still_valid=Mock(return_value=True),
        _send_signal=AsyncMock(return_value=True),
    )
    return namespace, canonical, legacy


def test_ltf_snapshot_data_block_cannot_fall_through_to_legacy_delivery():
    ns, canonical, legacy = watcher_namespace(
        error=SnapshotEvaluationBlocked("snapshot_market_data_not_ready:GATE_STALE_15M")
    )
    asyncio.run(function("_auto_ltf_watch_scan_impl", ns)())
    legacy.assert_not_called()
    canonical.assert_called_once_with(
        "MTF", "BTCUSDT", timeframe="1h", auto=True, passive_watch=True
    )
    ns["_send_signal"].assert_not_awaited()
    assert ns["_touch_ltf_watch"].call_args.args[3] is False


def test_pending_ltf_result_remains_a_watch_and_never_reaches_sender():
    ns, canonical, legacy = watcher_namespace(
        result=dict(_pending_ltf=True, reason="wait fresh BOS")
    )
    asyncio.run(function("_auto_ltf_watch_scan_impl", ns)())
    legacy.assert_not_called()
    ns["_send_signal"].assert_not_awaited()
    assert ns["_touch_ltf_watch"].call_args.args[3] is False


def test_ready_ltf_candidate_preserves_snapshot_identity_for_quality_sender():
    candidate = dict(
        symbol="BTCUSDT",
        direction="BULLISH",
        entry=100,
        sl=95,
        tp1=110,
        snapshot_id="snap_research_identity",
        snapshot_as_of="2026-10-24T09:00:00+00:00",
    )
    ns, canonical, legacy = watcher_namespace(result=candidate)
    asyncio.run(function("_auto_ltf_watch_scan_impl", ns)())
    legacy.assert_not_called()
    ns["_send_signal"].assert_awaited_once_with(candidate)
    assert (
        ns["_send_signal"].call_args.args[0]["snapshot_id"] == "snap_research_identity"
    )
    assert ns["_touch_ltf_watch"].call_args.args[3] is True


def test_ltf_watch_is_not_resolved_when_delivery_is_deferred():
    candidate = dict(symbol="BTCUSDT", direction="BULLISH", entry=100, sl=95, tp1=110)
    ns, canonical, legacy = watcher_namespace(result=candidate)
    ns["_send_signal"] = AsyncMock(return_value=False)
    asyncio.run(function("_auto_ltf_watch_scan_impl", ns)())
    ns["_send_signal"].assert_awaited_once_with(candidate)
    assert ns["_touch_ltf_watch"].call_args.args[3] is False
    assert "retained" in ns["_touch_ltf_watch"].call_args.args[2]


def test_ltf_watch_survives_sender_cancellation():
    candidate = dict(symbol="BTCUSDT", direction="BULLISH", entry=100, sl=95, tp1=110)
    ns, canonical, legacy = watcher_namespace(result=candidate)
    ns["_send_signal"] = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(function("_auto_ltf_watch_scan_impl", ns)())
    ns["_touch_ltf_watch"].assert_not_called()


@pytest.mark.parametrize("strategy", list(Strategy))
def test_requested_but_invalid_proof_never_falls_back_to_legacy(strategy):
    registry = Mock()
    registry.evaluate.return_value = []
    provider = Mock()
    ns = dict(
        _get_v3_live_strategy_registry=lambda: registry,
        _get_v3_strategy_activation=lambda: StrategyActivationSwitch(
            True, False, "SNAPSHOT_PROOF_INVALID:fixture_digest"
        ),
        _get_v3_strategy_snapshot_provider=provider,
        _V3_SNAPSHOT_EVALUATION_BLOCKED=SnapshotEvaluationBlocked,
    )
    evaluate = function("_v3_strategy_candidates", ns)
    with pytest.raises(SnapshotEvaluationBlocked, match="snapshot_activation_blocked"):
        evaluate(strategy.value, "BTCUSDT")
    registry.evaluate.assert_not_called()
    provider.assert_not_called()


def test_explicitly_disabled_snapshot_mode_retains_legacy_compatibility():
    registry = Mock()
    registry.evaluate.return_value = []
    ns = dict(
        _get_v3_live_strategy_registry=lambda: registry,
        _get_v3_strategy_activation=lambda: StrategyActivationSwitch(
            False, False, "SNAPSHOT_ACTIVATION_NOT_REQUESTED"
        ),
    )
    assert function("_v3_strategy_candidates", ns)("MTF", "BTCUSDT") == []
    registry.evaluate.assert_called_once_with("MTF", "BTCUSDT")


def test_manual_deep_scan_uses_canonical_admission_in_bounded_executor():
    canonical = Mock(return_value=None)
    legacy = Mock(return_value=None)
    with ThreadPoolExecutor(max_workers=2) as executor:
        ns = dict(
            asyncio=asyncio,
            time=time,
            logging=logging,
            functools=functools,
            get_all_market_pairs=lambda: ["BTCUSDT"],
            _manual_scan_executor=executor,
            _v3_strategy_candidate=canonical,
            full_scan_raw=legacy,
            detect_accumulation=lambda _: None,
        )
        signals, accumulations, summary = asyncio.run(
            function("deep_market_scan", ns)(limit=1, with_summary=True)
        )
    legacy.assert_not_called()
    canonical.assert_called_once_with("MTF", "BTCUSDT", timeframe="1h")
    assert signals == [] and accumulations == []
    assert summary == dict(scope=1, attempted=1, checked=1, failed=0)


@pytest.mark.parametrize(
    "grade,pending,expected",
    [
        ("СДЕЛКА", False, True),
        ("ПРЕМИУМ СИГНАЛ", False, True),
        ("СДЕЛКА", True, False),
        ("СЛАБАЯ", False, False),
    ],
)
def test_manual_summary_keeps_current_complete_grades_but_not_watch_or_weak(
    grade, pending, expected
):
    candidate = dict(
        symbol="BTCUSDT", grade=grade, _pending_ltf=pending, entry=100, sl=95, tp1=110
    )
    # Both old/new entrypoints receive the same detector result. This test
    # independently checks result selection rather than routing assertions.
    with ThreadPoolExecutor(max_workers=2) as executor:
        ns = dict(
            asyncio=asyncio,
            time=time,
            logging=logging,
            functools=functools,
            get_all_market_pairs=lambda: ["BTCUSDT"],
            _manual_scan_executor=executor,
            _v3_strategy_candidate=lambda *a, **k: candidate,
            full_scan_raw=lambda *a, **k: candidate,
            detect_accumulation=lambda _: None,
        )
        signals, _, _ = asyncio.run(
            function("deep_market_scan", ns)(limit=1, with_summary=True)
        )
    assert bool(signals) is expected


def test_deep_scan_summary_does_not_imply_model_approval():
    candidate = dict(
        symbol="BTCUSDT", direction="BULLISH", grade="СДЕЛКА", entry=100, sl=95, tp1=110
    )
    text = function("format_deep_scan_result", {})([candidate], [], 1)
    assert "Технических кандидатов: 1" in text
    assert "Проверка Groq: не выполнялась" in text
    assert "Groq: одобрено" not in text
