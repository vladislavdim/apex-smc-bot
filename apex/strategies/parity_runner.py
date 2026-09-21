"""Offline runner and immutable verdict for the real-market parity corpus."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from .parity_corpus import (
    CorpusParityResult,
    FrozenParityCase,
    evaluate_activation_corpus,
    load_corpus_directory,
)
from .registry import StrategyRegistry


VERDICT_SCHEMA_VERSION = 1


def build_activation_verdict(
    cases: tuple[FrozenParityCase, ...],
    registry: StrategyRegistry,
) -> tuple[dict[str, Any], CorpusParityResult]:
    adapters = registry.registered_adapters()
    result = evaluate_activation_corpus(cases, adapters, adapters)
    reports_by_case = {report.case_id: report for report in result.reports}
    payload: dict[str, Any] = {
        "schema_version": VERDICT_SCHEMA_VERSION,
        "ready": result.ready,
        "reasons": list(result.reasons),
        "cases": [
            {
                "case_id": case.case_id,
                "strategy": case.strategy.value,
                "fixture_sha256": case.sha256,
                "matched": bool(
                    reports_by_case.get(case.case_id)
                    and reports_by_case[case.case_id].matched
                ),
                "mismatch_paths": [
                    row.path
                    for row in (
                        reports_by_case[case.case_id].mismatches
                        if case.case_id in reports_by_case else ()
                    )
                ],
            }
            for case in cases
        ],
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    payload["verdict_sha256"] = hashlib.sha256(encoded).hexdigest()
    return payload, result


def write_activation_verdict(path: str | Path, payload: dict[str, Any]) -> Path:
    """Atomically create a verdict; an existing proof is never replaced."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(
        payload, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False,
    ) + "\n").encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, target)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    return target


def load_activation_verdict(
    path: str | Path,
    cases: tuple[FrozenParityCase, ...],
) -> dict[str, Any]:
    """Verify that a READY verdict belongs exactly to the supplied corpus."""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError("activation_verdict_missing") from exc
    required = {
        "schema_version", "ready", "reasons", "cases", "verdict_sha256",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError("invalid_activation_verdict_schema")
    pinned_digest = str(payload["verdict_sha256"])
    digest_payload = dict(payload)
    digest_payload.pop("verdict_sha256")
    encoded = json.dumps(
        digest_payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    if hashlib.sha256(encoded).hexdigest() != pinned_digest:
        raise ValueError("activation_verdict_digest_mismatch")
    if payload["schema_version"] != VERDICT_SCHEMA_VERSION:
        raise ValueError("activation_verdict_version_mismatch")
    if payload["ready"] is not True or payload["reasons"] != []:
        raise ValueError("activation_verdict_not_ready")
    rows = payload["cases"]
    if not isinstance(rows, list) or len(rows) != len(cases):
        raise ValueError("activation_verdict_coverage_mismatch")
    by_id = {case.case_id: case for case in cases}
    if len(by_id) != len(cases):
        raise ValueError("activation_verdict_duplicate_fixture")
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "case_id", "strategy", "fixture_sha256", "matched",
            "mismatch_paths",
        }:
            raise ValueError("invalid_activation_verdict_case")
        case_id = str(row["case_id"])
        case = by_id.get(case_id)
        if case is None or case_id in seen:
            raise ValueError("activation_verdict_case_mismatch")
        seen.add(case_id)
        if (
            row["strategy"] != case.strategy.value
            or row["fixture_sha256"] != case.sha256
            or row["matched"] is not True
            or row["mismatch_paths"] != []
        ):
            raise ValueError(f"activation_verdict_case_mismatch:{case_id}")
    if seen != set(by_id):
        raise ValueError("activation_verdict_coverage_mismatch")
    return payload


def run_corpus(
    corpus_directory: str | Path,
    output: str | Path,
    registry: StrategyRegistry,
) -> CorpusParityResult:
    cases = load_corpus_directory(corpus_directory)
    payload, result = build_activation_verdict(cases, registry)
    write_activation_verdict(output, payload)
    return result


__all__ = [
    "VERDICT_SCHEMA_VERSION", "build_activation_verdict",
    "load_activation_verdict", "run_corpus", "write_activation_verdict",
]
