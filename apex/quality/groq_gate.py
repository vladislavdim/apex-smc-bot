"""Bounded Groq critique gate for immutable V3 candidates.

The gate snapshots strategy-owned geometry before an external critique and
verifies it afterwards.  Groq may approve/reject/hold; it never rewrites
Entry, SL, TP or RR.
"""
from __future__ import annotations
from apex.domain.models import Candidate

Geometry=tuple[float,float,float,float,float|None,float]

def geometry(candidate:Candidate)->Geometry:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)

def assert_geometry_unchanged(candidate:Candidate,before:Geometry)->None:
    if geometry(candidate)!=before:
        raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")

__all__=["Geometry","assert_geometry_unchanged","geometry"]
