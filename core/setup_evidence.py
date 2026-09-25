"""Deprecated compatibility facade for canonical V3 setup evidence."""

from apex.quality.setup_evidence import (
    _geometry,
    assess_candidate,
    bind_assessment_to_signal,
    candidate_key,
    ensure_setup_evidence_schema,
    persist_assessment,
    setup_evidence_dashboard,
)

__all__ = [
    "assess_candidate", "bind_assessment_to_signal", "candidate_key",
    "ensure_setup_evidence_schema", "persist_assessment",
    "setup_evidence_dashboard",
]
