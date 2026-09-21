"""Strict fail-safe Groq candidate-review schema."""

from __future__ import annotations

from typing import Any, Mapping

from apex.domain.enums import Decision
from apex.domain.models import GroqReview


def wait_review(reason_code: str) -> GroqReview:
    return GroqReview(Decision.WAIT, 0.0, (reason_code,), "")


def parse_review(payload: Mapping[str, Any] | None) -> GroqReview:
    if not isinstance(payload, Mapping):
        return wait_review("GROQ_BAD_SCHEMA")
    if set(payload) != {"decision", "confidence", "reason_codes", "short_summary"}:
        return wait_review("GROQ_BAD_SCHEMA")
    try:
        decision = Decision(str(payload["decision"]).upper())
        confidence = float(payload["confidence"])
        reasons = payload["reason_codes"]
        summary = payload["short_summary"]
        if not 0 <= confidence <= 1:
            raise ValueError
        if not isinstance(reasons, (list, tuple)) or not all(isinstance(item, str) and item for item in reasons):
            raise ValueError
        if not isinstance(summary, str):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        return wait_review("GROQ_BAD_SCHEMA")
    return GroqReview(decision, confidence, tuple(reasons), summary[:500])


__all__ = ["parse_review", "wait_review"]
