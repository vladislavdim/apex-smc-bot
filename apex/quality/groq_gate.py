"""Bounded Groq critique gate for immutable candidate geometry."""
from __future__ import annotations
from apex.domain.models import Candidate,GroqReview

def geometry(candidate:Candidate)->tuple[float,float,float,float,float|None,float]:
    return candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr

def validate_review(candidate:Candidate,before:tuple[float,float,float,float,float|None,float],review:GroqReview)->GroqReview:
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")
    return review
__all__=["geometry","validate_review"]
