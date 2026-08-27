"""Shared Groq model selection.

Keep model names in one place: retired models return HTTP 404 and must not be
treated as an exhausted API key.
"""
from __future__ import annotations

import os


# Groq's current developer-tier replacements for the retired Llama 3.x names.
DEFAULT_GROQ_MODELS = ("openai/gpt-oss-20b", "openai/gpt-oss-120b")


def configured_groq_models() -> tuple[str, ...]:
    """Return a de-duplicated configured model list, with safe defaults."""
    configured = []
    for variable in ("GROQ_MODEL", "GROQ_FALLBACK_MODELS"):
        configured.extend(
            model.strip() for model in os.environ.get(variable, "").split(",")
            if model.strip()
        )
    configured.extend(DEFAULT_GROQ_MODELS)
    return tuple(dict.fromkeys(configured))


def is_model_unavailable_error(error: object) -> bool:
    """True for a model-level 404/deprecation error, never a key quota error."""
    text = str(error).lower()
    return (
        "model_not_found" in text
        or "does not exist" in text
        or "model not found" in text
        or "model_decommissioned" in text
        or "decommissioned" in text
    )
