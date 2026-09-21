"""Fail-closed admission gate for frozen real-market strategy fixtures."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Mapping

from apex.domain.enums import Strategy
from apex.domain.models import MarketRegime, MarketSnapshot
from apex.market.snapshot_scope import use_market_snapshot

from .base import StrategyAdapter
from .parity import ParityReport, compare_traces


REAL_MARKET_SOURCE = "GATE_REAL_MARKET"


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _case_payload(
    *, case_id: str, strategy: Strategy, snapshot: MarketSnapshot,
    kwargs: Mapping[str, Any], source: str, captured_at: datetime,
) -> dict[str, Any]:
    return _json_value({
        "case_id": case_id,
        "strategy": strategy.value,
        "source": source,
        "captured_at": captured_at,
        "snapshot": {
            "snapshot_id": snapshot.snapshot_id,
            "symbol": snapshot.symbol,
            "as_of": snapshot.as_of,
            "candles": snapshot.candles,
            "structure": snapshot.structure,
            "levels": snapshot.levels,
            "regime": {
                "direction": snapshot.regime.direction,
                "volatility": snapshot.regime.volatility,
                "phase": snapshot.regime.phase,
            },
            "volume": snapshot.volume,
            "derivatives_context": snapshot.derivatives_context,
            "microstructure_context": snapshot.microstructure_context,
            "market_context": snapshot.market_context,
        },
        "kwargs": kwargs,
    })


def fixture_digest(
    *, case_id: str, strategy: Strategy, snapshot: MarketSnapshot,
    kwargs: Mapping[str, Any], source: str, captured_at: datetime,
) -> str:
    payload = _case_payload(
        case_id=case_id, strategy=strategy, snapshot=snapshot, kwargs=kwargs,
        source=source, captured_at=captured_at,
    )
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class FrozenParityCase:
    case_id: str
    strategy: Strategy
    snapshot: MarketSnapshot
    kwargs: Mapping[str, Any]
    source: str
    captured_at: datetime
    sha256: str

    def validation_error(self) -> str | None:
        if not self.case_id.strip():
            return "case_id_required"
        if self.source != REAL_MARKET_SOURCE:
            return "real_market_source_required"
        if self.captured_at.tzinfo is None:
            return "captured_at_timezone_required"
        if self.captured_at < self.snapshot.as_of:
            return "captured_before_snapshot"
        expected = fixture_digest(
            case_id=self.case_id, strategy=self.strategy,
            snapshot=self.snapshot, kwargs=self.kwargs,
            source=self.source, captured_at=self.captured_at,
        )
        if self.sha256 != expected:
            return "fixture_digest_mismatch"
        return None


@dataclass(frozen=True)
class CorpusParityResult:
    ready: bool
    reports: tuple[ParityReport, ...]
    reasons: tuple[str, ...]


def write_frozen_case(path: str | Path, case: FrozenParityCase) -> Path:
    """Atomically create one immutable fixture; never replace an existing case."""
    error = case.validation_error()
    if error:
        raise ValueError(f"invalid_frozen_case:{error}")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _case_payload(
        case_id=case.case_id, strategy=case.strategy, snapshot=case.snapshot,
        kwargs=case.kwargs, source=case.source, captured_at=case.captured_at,
    )
    payload["sha256"] = case.sha256
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


def load_frozen_case(path: str | Path) -> FrozenParityCase:
    """Load a canonical fixture and verify its pinned digest before use."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    required = {
        "case_id", "strategy", "source", "captured_at", "snapshot",
        "kwargs", "sha256",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError("invalid_fixture_schema")
    raw_snapshot = payload["snapshot"]
    snapshot_required = {
        "snapshot_id", "symbol", "as_of", "candles", "structure", "levels",
        "regime", "volume", "derivatives_context", "microstructure_context",
        "market_context",
    }
    if not isinstance(raw_snapshot, dict) or set(raw_snapshot) != snapshot_required:
        raise ValueError("invalid_snapshot_schema")
    raw_regime = raw_snapshot["regime"]
    if not isinstance(raw_regime, dict) or set(raw_regime) != {
        "direction", "volatility", "phase",
    }:
        raise ValueError("invalid_regime_schema")
    market_snapshot = MarketSnapshot(
        snapshot_id=str(raw_snapshot["snapshot_id"]),
        symbol=str(raw_snapshot["symbol"]),
        as_of=datetime.fromisoformat(str(raw_snapshot["as_of"])),
        candles={
            str(timeframe): tuple(rows)
            for timeframe, rows in dict(raw_snapshot["candles"]).items()
        },
        structure=dict(raw_snapshot["structure"]),
        levels=tuple(raw_snapshot["levels"]),
        regime=MarketRegime(**raw_regime),
        volume=dict(raw_snapshot["volume"]),
        derivatives_context=dict(raw_snapshot["derivatives_context"]),
        microstructure_context=dict(raw_snapshot["microstructure_context"]),
        market_context=dict(raw_snapshot["market_context"]),
    )
    case = FrozenParityCase(
        case_id=str(payload["case_id"]),
        strategy=Strategy(str(payload["strategy"])),
        snapshot=market_snapshot,
        kwargs=dict(payload["kwargs"]),
        source=str(payload["source"]),
        captured_at=datetime.fromisoformat(str(payload["captured_at"])),
        sha256=str(payload["sha256"]),
    )
    error = case.validation_error()
    if error:
        raise ValueError(f"invalid_frozen_case:{error}")
    return case


def load_corpus_directory(path: str | Path) -> tuple[FrozenParityCase, ...]:
    """Load a complete five-strategy manifest and reject any drift."""
    root = Path(path)
    manifest_path = root / "corpus.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError("corpus_manifest_missing") from exc
    required = {"schema_version", "source", "symbol", "as_of", "cases"}
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise ValueError("invalid_corpus_manifest_schema")
    if manifest["schema_version"] != 1 or manifest["source"] != REAL_MARKET_SOURCE:
        raise ValueError("invalid_corpus_manifest_identity")
    rows = manifest["cases"]
    if not isinstance(rows, list) or len(rows) != len(Strategy):
        raise ValueError("corpus_strategy_coverage_incomplete")
    expected_files = {"corpus.json"}
    loaded: dict[Strategy, FrozenParityCase] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "strategy", "file", "case_id", "sha256",
        }:
            raise ValueError("invalid_corpus_case_manifest")
        strategy = Strategy(str(row["strategy"]))
        filename = str(row["file"])
        if Path(filename).name != filename or not filename.endswith(".json"):
            raise ValueError("invalid_corpus_case_path")
        if strategy in loaded:
            raise ValueError(f"duplicate_corpus_strategy:{strategy.value}")
        case = load_frozen_case(root / filename)
        if (
            case.strategy != strategy
            or case.case_id != str(row["case_id"])
            or case.sha256 != str(row["sha256"])
        ):
            raise ValueError(f"corpus_manifest_case_mismatch:{strategy.value}")
        if case.snapshot.symbol != str(manifest["symbol"]):
            raise ValueError(f"corpus_symbol_mismatch:{strategy.value}")
        if case.snapshot.as_of.isoformat() != str(manifest["as_of"]):
            raise ValueError(f"corpus_as_of_mismatch:{strategy.value}")
        expected_files.add(filename)
        loaded[strategy] = case
    if set(loaded) != set(Strategy):
        raise ValueError("corpus_strategy_coverage_incomplete")
    actual_files = {item.name for item in root.glob("*.json")}
    if actual_files != expected_files:
        raise ValueError("corpus_file_set_mismatch")
    return tuple(loaded[strategy] for strategy in Strategy)


def evaluate_activation_corpus(
    cases: tuple[FrozenParityCase, ...],
    legacy: Mapping[Strategy, StrategyAdapter],
    replacements: Mapping[Strategy, StrategyAdapter],
) -> CorpusParityResult:
    """Evaluate a pinned corpus; any missing proof keeps activation disabled."""
    reasons: list[str] = []
    reports: list[ParityReport] = []
    case_ids: set[str] = set()
    covered: set[Strategy] = set()

    for case in cases:
        if case.case_id in case_ids:
            reasons.append(f"duplicate_case_id:{case.case_id}")
            continue
        case_ids.add(case.case_id)
        error = case.validation_error()
        if error:
            reasons.append(f"invalid_case:{case.case_id}:{error}")
            continue
        left = legacy.get(case.strategy)
        right = replacements.get(case.strategy)
        if left is None or right is None:
            reasons.append(f"adapter_missing:{case.strategy.value}")
            continue
        if left.strategy != case.strategy or right.strategy != case.strategy:
            reasons.append(f"adapter_strategy_mismatch:{case.strategy.value}")
            continue
        evaluator = getattr(right, "evaluate_snapshot", None)
        if not callable(evaluator):
            reasons.append(f"snapshot_adapter_missing:{case.strategy.value}")
            continue
        # Both interfaces must see the exact same frozen point-in-time market.
        # The legacy symbol entry point is intentionally exercised, but its
        # candle boundary is fenced so an offline proof can never drift with
        # current Gate/cache state.
        with use_market_snapshot(case.snapshot):
            legacy_traces = left.evaluate(
                case.snapshot.symbol, **dict(case.kwargs)
            )
        replacement_traces = evaluator(case.snapshot, **dict(case.kwargs))
        report = compare_traces(case.case_id, legacy_traces, replacement_traces)
        reports.append(report)
        covered.add(case.strategy)
        if not report.matched:
            reasons.append(f"parity_mismatch:{case.case_id}")

    for strategy in Strategy:
        if strategy not in covered:
            reasons.append(f"strategy_not_covered:{strategy.value}")
    return CorpusParityResult(
        ready=not reasons and covered == set(Strategy),
        reports=tuple(reports), reasons=tuple(reasons),
    )


__all__ = [
    "CorpusParityResult", "FrozenParityCase", "REAL_MARKET_SOURCE",
    "evaluate_activation_corpus", "fixture_digest", "load_frozen_case",
    "load_corpus_directory", "write_frozen_case",
]
