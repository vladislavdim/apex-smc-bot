"""Bounded Groq critique gate for immutable candidates.

Groq may review a candidate but must never rewrite Entry/SL/TP/RR.
"""
from __future__ import annotations
from apex.domain.models import Candidate, GroqReview
Geometry = tuple[float,float,float,float,float|None,float]
def geometry(candidate:Candidate)->Geometry:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)
def verify_review(candidate:Candidate,before:Geometry,review:GroqReview)->GroqReview:
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")
    return review
__all__=["Geometry","geometry","verify_review"]
