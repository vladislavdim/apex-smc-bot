"""Strict fail-safe Groq candidate-review schema."""

from __future__ import annotations

from typing import Any, Mapping
from math import isfinite

from apex.config.settings import ApexConfig
from apex.domain.enums import Decision
from apex.domain.models import GroqReview


DEFAULT_GROQ_MODELS = ("openai/gpt-oss-20b", "openai/gpt-oss-120b")


def configured_groq_models(env=None) -> tuple[str, ...]:
    """Return a de-duplicated configured model list, with safe defaults."""
    settings = ApexConfig.from_env(env).integrations
    configured = [model.strip() for model in settings.groq_model.split(",") if model.strip()]
    configured.extend(settings.groq_fallback_models)
    configured.extend(DEFAULT_GROQ_MODELS)
    return tuple(dict.fromkeys(configured))


def is_model_unavailable_error(error: object) -> bool:
    """True for a model-level 404/deprecation error, never a key quota error."""
    text = str(error).lower()
    return any(marker in text for marker in (
        "model_not_found", "does not exist", "model not found",
        "model_decommissioned", "decommissioned",
    ))


def wait_review(reason_code: str) -> GroqReview:
    return GroqReview(Decision.WAIT, 0.0, (reason_code,), "")


def approval_error(review: object, minimum_confidence: float) -> str:
    """Revalidate stored and fresh approvals at every irreversible boundary."""
    if not isinstance(review, Mapping):
        return "missing Groq quality review"
    if review.get("degraded"):
        return "degraded Groq quality review"
    if str(review.get("decision") or "").upper() != "APPROVE":
        return "Groq decision is not APPROVE"
    if isinstance(review.get("confidence"), bool):
        return "invalid Groq confidence"
    try:
        confidence = float(review.get("confidence"))
        minimum = float(minimum_confidence)
    except (TypeError, ValueError, OverflowError):
        return "invalid Groq confidence"
    if (not isfinite(confidence) or not isfinite(minimum)
            or not 0 <= minimum <= confidence <= 1):
        return "Groq confidence outside current approval bounds"
    return ""


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
        if isinstance(payload["confidence"], bool) or not 0 <= confidence <= 1:
            raise ValueError
        if not isinstance(reasons, (list, tuple)) or not all(isinstance(item, str) and item for item in reasons):
            raise ValueError
        if not isinstance(summary, str):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        return wait_review("GROQ_BAD_SCHEMA")
    return GroqReview(decision, confidence, tuple(reasons), summary[:500])


__all__ = [
    "DEFAULT_GROQ_MODELS", "configured_groq_models", "is_model_unavailable_error",
    "parse_review", "wait_review",
    "approval_error",
]
