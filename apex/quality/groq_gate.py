"""Bounded Groq critique gate for immutable candidates.

Groq may approve/reject/hold a setup, but it cannot rewrite strategy geometry.
"""
from __future__ import annotations
from apex.domain.models import Candidate, GroqReview

Geometry = tuple[float, float, float, float, float | None, float]

def geometry(candidate: Candidate) -> Geometry:
    return (candidate.entry, candidate.initial_sl, candidate.tp1, candidate.tp2, candidate.tp3, candidate.rr)

def assert_geometry_unchanged(candidate: Candidate, before: Geometry) -> None:
    if geometry(candidate) != before:
        raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")

def review_without_mutation(candidate: Candidate, review: GroqReview) -> GroqReview:
    before = geometry(candidate)
    assert_geometry_unchanged(candidate, before)
    return review

__all__=["Geometry","assert_geometry_unchanged","geometry","review_without_mutation"]
