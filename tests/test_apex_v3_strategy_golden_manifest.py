from __future__ import annotations

import ast
from pathlib import Path

from apex.strategies.golden_manifest import GOLDEN_GATE_MANIFESTS


ROOT = Path(__file__).resolve().parents[1]


def _source_order(file_name: str, function_name: str) -> tuple[str, ...]:
    tree = ast.parse((ROOT / file_name).read_text(encoding="utf-8"))
    functions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == function_name
    ]
    assert len(functions) == 1, (file_name, function_name, len(functions))
    checks: list[tuple[int, int, str]] = []
    for node in ast.walk(functions[0]):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "_audit_test":
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        checks.append((node.lineno, node.col_offset, str(node.args[0].value)))
    return tuple(item[2] for item in sorted(checks))


def test_golden_manifests_cover_every_legacy_audit_point_in_source_order():
    for name, manifest in GOLDEN_GATE_MANIFESTS.items():
        actual = _source_order(manifest.source_file, manifest.function_name)
        assert actual == manifest.source_order, name


def test_golden_manifests_have_unique_ids_and_valid_role_classifications():
    for name, manifest in GOLDEN_GATE_MANIFESTS.items():
        all_ids = frozenset(manifest.source_order)
        assert len(all_ids) == len(manifest.source_order), name
        assert manifest.classified_ids() <= all_ids, name
        assert not (manifest.context_only & manifest.legacy_authority), name
        assert not (manifest.context_only & manifest.post_candidate_review), name
        assert not (manifest.legacy_authority & manifest.post_candidate_review), name


def test_manifest_covers_all_five_strategies_and_wyckoff_subtypes():
    assert set(GOLDEN_GATE_MANIFESTS) == {
        "FAST",
        "MTF",
        "SWING",
        "ZONE",
        "WYCKOFF_SPRING",
        "WYCKOFF_DISTRIBUTION",
        "WYCKOFF_REACCUMULATION",
    }


def test_manifest_assigns_each_source_check_one_explicit_role():
    allowed = {"HARD_GATE", "LIVE_CONTEXT", "LEGACY_AUTHORITY", "POST_CANDIDATE_REVIEW"}
    for name, manifest in GOLDEN_GATE_MANIFESTS.items():
        roles = {manifest.role_for(check_id) for check_id in manifest.source_order}
        assert roles <= allowed, name
        assert "HARD_GATE" in roles, name
        for check_id in manifest.context_only:
            assert manifest.role_for(check_id) == "LIVE_CONTEXT"
        for check_id in manifest.legacy_authority:
            assert manifest.role_for(check_id) == "LEGACY_AUTHORITY"
        for check_id in manifest.post_candidate_review:
            assert manifest.role_for(check_id) == "POST_CANDIDATE_REVIEW"
