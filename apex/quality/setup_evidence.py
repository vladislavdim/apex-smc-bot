"""Domain-grouped setup evidence without double-counting correlated facts."""

from __future__ import annotations

from typing import Mapping

from apex.domain.models import SetupEvidence


DOMAINS = ("CORE", "LOCATION", "TRIGGER", "PARTICIPATION", "GEOMETRY", "CONTEXT")


def assess_evidence(
    evidence: Mapping[str, str],
    *,
    required_domains: tuple[str, ...] = ("CORE", "LOCATION", "TRIGGER", "GEOMETRY"),
) -> SetupEvidence:
    normalized = {name: str(evidence.get(name, "UNKNOWN")).upper() for name in DOMAINS}
    invalid = tuple(name for name in required_domains if normalized[name] != "PASS")
    reasons = tuple(f"EVIDENCE_{name}_{normalized[name]}" for name in invalid)
    return SetupEvidence(not invalid, normalized, reasons)


def participation_group(*, volume: bool | None, cvd: bool | None, taker: bool | None) -> str:
    """Return one participation domain, never three confluence votes."""
    values = [value for value in (volume, cvd, taker) if value is not None]
    if not values:
        return "UNKNOWN"
    return "PASS" if any(values) else "FAIL"


__all__ = ["DOMAINS", "assess_evidence", "participation_group"]
