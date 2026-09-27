"""Bounded Groq critique gate for immutable candidate geometry."""
from __future__ import annotations
from apex.domain.models import Candidate
Geometry=tuple[float,float,float,float,float|None,float]
def geometry(candidate:Candidate)->Geometry:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)
def assert_geometry_unchanged(candidate:Candidate,before:Geometry)->None:
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")
__all__=["Geometry","assert_geometry_unchanged","geometry"]
