"""Advisory multi-source agreement without strategy authority."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from statistics import median
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class SourceValue:
    source: str
    value: float | None
    event_time: int | None
    received_at: int | None
    freshness: str

    @property
    def usable(self) -> bool:
        return (
            self.freshness == "FRESH"
            and self.value is not None
            and isfinite(float(self.value))
        )


@dataclass(frozen=True)
class SourceConsensus:
    status: str
    value: float | None
    dispersion_pct: float | None
    sources: tuple[SourceValue, ...]
    can_block_strategy: bool = False


def numeric_consensus(
    rows: Iterable[SourceValue],
    *,
    high_agreement_pct: float = 0.05,
    conflict_pct: float = 0.20,
) -> SourceConsensus:
    sources = tuple(rows)
    usable = tuple(row for row in sources if row.usable)
    if not usable:
        return SourceConsensus("UNKNOWN", None, None, sources)
    values = [float(row.value) for row in usable if row.value is not None]
    centre = median(values)
    if len(values) == 1:
        return SourceConsensus("SINGLE_SOURCE", centre, None, sources)
    scale = max(abs(centre), 1e-12)
    dispersion = (max(values) - min(values)) / scale
    status = (
        "HIGH" if dispersion <= high_agreement_pct
        else "CONTEXT_CONFLICT" if dispersion >= conflict_pct
        else "MEDIUM"
    )
    return SourceConsensus(status, centre, dispersion, sources)


def from_context(feature_rows: Mapping[str, Mapping[str, Any]], value_key: str) -> SourceConsensus:
    normalized = []
    for source, row in feature_rows.items():
        value = row.get("value")
        raw = value.get(value_key) if isinstance(value, Mapping) else None
        try:
            parsed = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            parsed = None
        normalized.append(SourceValue(
            source=str(source), value=parsed,
            event_time=int(row["event_time"]) if row.get("event_time") is not None else None,
            received_at=int(row["received_at"]) if row.get("received_at") is not None else None,
            freshness=str(row.get("freshness") or "UNKNOWN").upper(),
        ))
    return numeric_consensus(normalized)


__all__ = ["SourceConsensus", "SourceValue", "from_context", "numeric_consensus"]
