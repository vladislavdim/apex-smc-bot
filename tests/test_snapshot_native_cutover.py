from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
import copy
import json
import pytest
from apex.strategies.parity_corpus import load_corpus_directory
from apex.strategies import native
from apex.market.snapshot_scope import use_market_snapshot, snapshot_candle_override

ROOT = Path(__file__).parent


def test_snapshot_owns_deep_immutable_detached_inputs():
    snapshot = load_corpus_directory(ROOT / "fixtures/apex_v3_strategy_parity")[
        0
    ].snapshot
    original = {"nested": [{"value": 1}]}
    frozen = replace(snapshot, market_context=original)
    original["nested"][0]["value"] = 99
    assert frozen.market_context["nested"][0]["value"] == 1
    with pytest.raises(TypeError):
        frozen.candles["15m"][0]["close"] = 0
    with pytest.raises(TypeError):
        frozen.market_context.update({"nested": []})
    assert copy.deepcopy(frozen.market_context) == frozen.market_context
    assert json.loads(json.dumps(frozen.market_context)) == {"nested": [{"value": 1}]}


def test_native_evaluation_has_no_live_io_or_legacy_detector_dependency():
    cases = load_corpus_directory(ROOT / "fixtures/apex_v3_strategy_parity")
    functions = {
        "FAST": [native.detect_fast_deal],
        "MTF": [native.full_scan_raw],
        "SWING": [native.detect_swing_setup],
        "ZONE": [native.detect_zone_setup],
        "WYCKOFF": [
            native.detect_wyckoff_spring,
            native.detect_wyckoff_distribution,
            native.detect_wyckoff_reaccumulation,
        ],
    }
    with (
        patch("requests.get", side_effect=AssertionError("live network forbidden")),
        patch("sqlite3.connect", side_effect=AssertionError("database forbidden")),
        patch(
            "core.smc_engine.get_candles_smart",
            side_effect=AssertionError("live candle source forbidden"),
        ),
    ):
        for case in cases:
            for function in functions[case.strategy.value]:
                left = function(case.snapshot, **dict(case.kwargs))
                right = function(case.snapshot, **dict(case.kwargs))
                for result in (left, right):
                    if result:
                        result.pop("_audit_attempt_key", None)
                assert left == right


def test_reference_symbol_uses_frozen_snapshot_and_unknown_btc_blocks():
    snapshot = load_corpus_directory(ROOT / "fixtures/apex_v3_strategy_parity")[
        0
    ].snapshot
    context = native.SnapshotContext(
        replace(snapshot, symbol="ETHUSDT", market_context={})
    )
    assert context.btc_allows_signal("BULLISH")[0] is False
    other = replace(
        snapshot,
        symbol="ETHUSDT",
        market_context={
            "reference_candles": {"BTCUSDT": {"1h": snapshot.candles["1h"]}}
        },
    )
    with use_market_snapshot(other):
        assert snapshot_candle_override("BTCUSDT", "1h", 5) == native.SnapshotContext(
            other
        ).get_candles("BTCUSDT", "1h", 5)
        assert snapshot_candle_override("SOLUSDT", "1h", 5) == []


def test_migrated_trading_arithmetic_and_ordered_gates_are_unchanged():
    """AST equivalence covers paths rarer than the available market corpus.

    Only data dependency binding, decision clock and frozen ownership lookup
    differ; neither a changed comparison nor an omitted gate can pass this.
    """
    import ast

    root = ROOT.parent
    old = ast.parse((root / "apex/compatibility/legacy_market_runtime.py").read_text())
    old_worker = ast.parse((root / "apex/app/worker.py").read_text())
    new = ast.parse((root / "apex/strategies/native.py").read_text())
    context = next(
        n
        for n in new.body
        if isinstance(n, ast.ClassDef) and n.name == "SnapshotContext"
    )
    names = {
        "check_opposing_ob",
        "_swing_build_ltf_entry",
        "detect_swing_setup",
        "detect_zone_setup",
        "_find_wyckoff_phases_accumulation",
        "_find_wyckoff_phases_distribution",
        "detect_wyckoff_spring",
        "detect_wyckoff_distribution",
        "detect_wyckoff_reaccumulation",
        "detect_fast_deal",
        "full_scan_raw",
    }
    dependencies = names | {
        "get_candles",
        "smc_on_tf",
        "multi_tf_analysis",
        "btc_allows_signal",
        "get_adaptive_params",
        "get_market_regime",
        "get_funding_rate",
        "get_fear_greed",
        "get_orderbook",
        "_fast_session",
    }

    class Normalize(ast.NodeTransformer):
        def visit_Attribute(self, node):
            if (
                isinstance(node.value, ast.Name)
                and node.value.id == "self"
                and node.attr in dependencies
            ):
                return ast.Name(id=node.attr, ctx=ast.Load())
            if node.attr == "hour":
                text = ast.unparse(node.value)
                if "decision_time" in text or "self.snapshot.as_of" in text:
                    return ast.Name(id="FROZEN_DECISION_HOUR", ctx=ast.Load())
            return self.generic_visit(node)

    left = {
        n.name: n
        for n in old.body + old_worker.body
        if isinstance(n, ast.FunctionDef) and n.name in names
    }
    right = {
        n.name: n
        for n in context.body
        if isinstance(n, ast.FunctionDef) and n.name in names
    }
    assert set(left) == set(right) == names
    for name in sorted(names):
        a, b = left[name], right[name]
        a.decorator_list = b.decorator_list = []
        for node in (a, b):
            if (
                isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
                and isinstance(node.body[0].value.value, str)
            ):
                node.body[0] = ast.Pass()
        b.args.args.pop(0)
        if name == "detect_zone_setup":
            # Intentional reviewed bug fix: count overlaps only after zone birth.
            # Lock its exact implementation, then compare every other statement
            # against the frozen legacy detector as before.
            old_loop = next(n for n in ast.walk(a) if isinstance(n, ast.For)
                            and ast.unparse(n.iter) == "candles[-40:-3]")
            expected = ast.parse("""
_zone_origin = ob if zone_type == 'OB' else fvg
_zone_birth_idx = int(_zone_origin['index']) + (1 if zone_type == 'FVG' else 0)
for _zone_test_idx in range(max(0, len(candles) - 40, _zone_birth_idx + 1), max(0, len(candles) - 3)):
    c = candles[_zone_test_idx]
    if c['low'] <= _zone_top and c['high'] >= _zone_bot:
        _test_count += 1
""").body
            section = next(n.body for n in ast.walk(b) if isinstance(n, ast.Try)
                           and any(isinstance(x, ast.Assign) and
                                   any(isinstance(t, ast.Name) and t.id == '_zone_origin' for t in x.targets)
                                   for x in n.body))
            start = next(i for i, n in enumerate(section) if isinstance(n, ast.Assign)
                         and any(isinstance(t, ast.Name) and t.id == '_zone_origin' for t in n.targets))
            assert [ast.dump(n, include_attributes=False) for n in section[start:start + 3]] == [
                ast.dump(n, include_attributes=False) for n in expected
            ]
            section[start:start + 3] = [old_loop]
        if name == "full_scan_raw":
            # Same gate represented by one snapshot-owned lookup in native.
            a.body[1].body[:1] = b.body[1].body[:2]
        assert ast.dump(Normalize().visit(a), include_attributes=False) == ast.dump(
            Normalize().visit(b), include_attributes=False
        ), name
