"""Bounded Groq critique gate for immutable candidates.

Groq may approve/reject a candidate, but candidate Entry/SL/TP/RR geometry is
owned by the strategy and is immutable across the critique boundary.
"""
from __future__ import annotations
from typing import Callable
from apex.domain.models import Candidate, GroqReview

Geometry=tuple[float,float,float,float,float|None,float]

def geometry(candidate:Candidate)->Geometry:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)

def critique(candidate:Candidate, reviewer:Callable[[Candidate],GroqReview])->GroqReview:
    before=geometry(candidate)
    review=reviewer(candidate)
    if not isinstance(review,GroqReview):
        raise TypeError("groq_review_schema_invalid")
    if geometry(candidate)!=before:
        raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")
    return review

def assert_geometry_unchanged(candidate:Candidate,before:Geometry)->None:
    if geometry(candidate)!=before:
        raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")

__all__=["Geometry","assert_geometry_unchanged","critique","geometry"]
