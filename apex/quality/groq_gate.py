"""Bounded Groq critique gate for immutable strategy geometry."""
from __future__ import annotations
from typing import Any
from apex.domain.models import Candidate

def geometry(candidate:Candidate)->tuple[float,float,float,float,float|None,float]:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)

def assert_geometry_unchanged(candidate:Candidate,before:tuple[float,float,float,float,float|None,float])->None:
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")

def critique(candidate:Candidate, reviewer:Any)->Any:
    before=geometry(candidate)
    review=reviewer(candidate)
    assert_geometry_unchanged(candidate,before)
    return review
__all__=["assert_geometry_unchanged","critique","geometry"]
