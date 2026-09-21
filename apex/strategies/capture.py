"""Controlled capture of immutable Gate snapshots for strategy parity."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Callable, Mapping, Protocol

from apex.domain.enums import Strategy
from apex.market.snapshots import SnapshotBuild

from .parity_corpus import (
    FrozenParityCase,
    REAL_MARKET_SOURCE,
    fixture_digest,
    write_frozen_case,
)


class SnapshotProvider(Protocol):
    def build(
        self, strategy: Strategy | str, symbol: str, *, as_of: datetime | None = None,
    ) -> SnapshotBuild: ...


DEFAULT_CASE_KWARGS: Mapping[Strategy, Mapping[str, Any]] = {
    Strategy.FAST: {},
    Strategy.MTF: {"timeframe": "1h", "auto": False, "passive_watch": False},
    Strategy.SWING: {"timeframe": "4h"},
    Strategy.ZONE: {"timeframe": "4h", "passive_watch": False},
    Strategy.WYCKOFF: {},
}


def capture_gate_case(
    provider: SnapshotProvider,
    strategy: Strategy | str,
    symbol: str,
    output: str | Path,
    *,
    as_of: datetime | None = None,
    kwargs: Mapping[str, Any] | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> FrozenParityCase:
    """Capture one complete fresh Gate case or fail without writing a file."""
    key = strategy if isinstance(strategy, Strategy) else Strategy(str(strategy).upper())
    normalized_symbol = str(symbol).strip().upper()
    if not normalized_symbol:
        raise ValueError("capture_symbol_required")
    boundary = as_of or clock()
    if boundary.tzinfo is None:
        raise ValueError("capture_as_of_timezone_required")
    boundary = boundary.astimezone(timezone.utc)
    build = provider.build(key, normalized_symbol, as_of=boundary)
    if build.wait_reason_codes:
        raise RuntimeError(
            "capture_market_data_not_ready:" + ",".join(build.wait_reason_codes)
        )
    if not build.snapshot.candles or any(not rows for rows in build.snapshot.candles.values()):
        raise RuntimeError("capture_market_data_incomplete")
    captured_at = clock()
    if captured_at.tzinfo is None:
        raise ValueError("capture_clock_timezone_required")
    captured_at = captured_at.astimezone(timezone.utc)
    if captured_at < build.snapshot.as_of:
        captured_at = build.snapshot.as_of
    case_kwargs = dict(DEFAULT_CASE_KWARGS[key] if kwargs is None else kwargs)
    case_id = (
        f"gate-{key.value.lower()}-{normalized_symbol.lower()}-"
        f"{int(build.snapshot.as_of.timestamp())}"
    )
    values = dict(
        case_id=case_id, strategy=key, snapshot=build.snapshot,
        kwargs=case_kwargs, source=REAL_MARKET_SOURCE,
        captured_at=captured_at,
    )
    case = FrozenParityCase(**values, sha256=fixture_digest(**values))
    write_frozen_case(output, case)
    return case


def capture_gate_corpus(
    provider: SnapshotProvider,
    symbol: str,
    output_directory: str | Path,
    *,
    as_of: datetime | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> tuple[FrozenParityCase, ...]:
    """Publish all five strategy cases atomically or leave no corpus behind."""
    target = Path(output_directory)
    if target.exists():
        raise FileExistsError(str(target))
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    boundary = as_of or clock()
    cases: list[FrozenParityCase] = []
    try:
        for strategy in Strategy:
            case = capture_gate_case(
                provider, strategy, symbol,
                temporary / f"{strategy.value.lower()}.json",
                as_of=boundary, clock=clock,
            )
            cases.append(case)
        manifest = {
            "schema_version": 1,
            "source": REAL_MARKET_SOURCE,
            "symbol": str(symbol).strip().upper(),
            "as_of": cases[0].snapshot.as_of.isoformat(),
            "cases": [
                {
                    "strategy": case.strategy.value,
                    "file": f"{case.strategy.value.lower()}.json",
                    "case_id": case.case_id,
                    "sha256": case.sha256,
                }
                for case in cases
            ],
        }
        manifest_path = temporary / "corpus.json"
        with manifest_path.open("x", encoding="utf-8") as output:
            json.dump(manifest, output, sort_keys=True, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.rename(temporary, target)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return tuple(cases)


__all__ = [
    "DEFAULT_CASE_KWARGS", "SnapshotProvider", "capture_gate_case",
    "capture_gate_corpus",
]
