"""Strict Manager Groq response constrained by deterministic eligibility."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .eligibility import ManagerFacts, enforce_action


@dataclass(frozen=True)
class ManagerReview:
    action: str
    confidence: float
    reason_codes: tuple[str, ...]
    summary: str


def parse_manager_review(payload: Mapping[str, Any] | None, facts: ManagerFacts) -> ManagerReview:
    allowed = {"action", "confidence", "reason_codes", "summary"}
    if not isinstance(payload, Mapping) or set(payload) != allowed:
        return ManagerReview("HOLD", 0.0, ("MANAGER_GROQ_BAD_SCHEMA",), "")
    try:
        requested = str(payload["action"]).upper()
        confidence = float(payload["confidence"])
        reasons = payload["reason_codes"]
        summary = payload["summary"]
        if not 0 <= confidence <= 1 or not isinstance(reasons, (list, tuple)):
            raise ValueError
        if not all(isinstance(item, str) and item for item in reasons) or not isinstance(summary, str):
            raise ValueError
    except (TypeError, ValueError):
        return ManagerReview("HOLD", 0.0, ("MANAGER_GROQ_BAD_SCHEMA",), "")
    action = enforce_action(requested, facts)
    if action != requested:
        return ManagerReview("HOLD", confidence, tuple(reasons) + ("ACTION_NOT_ELIGIBLE",), summary[:500])
    return ManagerReview(action, confidence, tuple(reasons), summary[:500])


__all__ = ["ManagerReview", "parse_manager_review"]
