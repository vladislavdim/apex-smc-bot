"""Bounded Groq critique gate for immutable candidates.

Groq may approve/reject/hold a setup, but candidate Entry/SL/TP/RR are
immutable and are verified before and after critique.
"""
from __future__ import annotations
from typing import Awaitable,Callable
from apex.domain.models import Candidate,GroqReview

Geometry=tuple[float,float,float,float,float|None,float]

def geometry(candidate:Candidate)->Geometry:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)

def assert_geometry_unchanged(candidate:Candidate,before:Geometry)->None:
    if geometry(candidate)!=before:
        raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")

async def critique(candidate:Candidate,reviewer:Callable[[Candidate],Awaitable[GroqReview]])->GroqReview:
    before=geometry(candidate)
    review=await reviewer(candidate)
    if not isinstance(review,GroqReview):
        raise TypeError("invalid_groq_review")
    assert_geometry_unchanged(candidate,before)
    return review

__all__=["Geometry","assert_geometry_unchanged","critique","geometry"]
