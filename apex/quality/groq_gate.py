"""Bounded Groq critique gate for immutable candidates."""
from __future__ import annotations
from typing import Callable,TypeVar
from apex.domain.models import Candidate
T=TypeVar("T")
def geometry(candidate:Candidate)->tuple[float,float,float,float,float|None,float]:
    return candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr
def review_immutable(candidate:Candidate,reviewer:Callable[[Candidate],T])->T:
    before=geometry(candidate); result=reviewer(candidate)
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")
    return result
def assert_geometry_unchanged(candidate:Candidate,before:tuple[float,float,float,float,float|None,float])->None:
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")
__all__=["assert_geometry_unchanged","geometry","review_immutable"]
