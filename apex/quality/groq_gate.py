"""Bounded Groq critique gate for immutable candidates.

Groq may approve/reject/hold a setup, but candidate Entry/SL/TP/RR are owned by
the strategy and are never rewritten here.
"""
from __future__ import annotations
from apex.domain.models import Candidate

def geometry(candidate: Candidate) -> tuple[float,float,float,float,float|None,float]:
    return (candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr)

def assert_geometry_unchanged(candidate: Candidate, before: tuple[float,float,float,float,float|None,float]) -> None:
    if geometry(candidate) != before:
        raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")

__all__=["assert_geometry_unchanged","geometry"]
