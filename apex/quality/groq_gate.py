"""Bounded Groq critique gate for immutable V3 candidates.
Groq may approve/reject/hold a candidate but can never rewrite geometry.
"""
from __future__ import annotations
from apex.domain.models import Candidate,GroqReview
Geometry=tuple[float,float,float,float,float|None,float]
def geometry(candidate:Candidate)->Geometry:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)
def assert_geometry_unchanged(candidate:Candidate,before:Geometry)->None:
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")
def review_without_geometry_mutation(candidate:Candidate,review:GroqReview,before:Geometry)->GroqReview:
    assert_geometry_unchanged(candidate,before); return review
__all__=["Geometry","assert_geometry_unchanged","geometry","review_without_geometry_mutation"]
