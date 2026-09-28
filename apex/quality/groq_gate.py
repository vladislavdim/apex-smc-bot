"""Canonical bounded Groq critique gate.

The legacy implementation is temporarily hidden behind this Quality boundary;
callers outside Quality no longer import it directly. Candidate geometry is
snapshotted and can be asserted immutable around every critique.
"""
from __future__ import annotations
from apex.domain.models import Candidate
from core.signal_quality_gate import review_signal_candidate

def geometry(candidate:Candidate)->tuple[float,float,float,float,float|None,float]:
    return candidate.entry,candidate.initial_sl,candidate.tp1,candidate.tp2,candidate.tp3,candidate.rr

def assert_geometry_unchanged(candidate:Candidate,before:tuple[float,float,float,float,float|None,float])->None:
    if geometry(candidate)!=before: raise RuntimeError("GROQ_GEOMETRY_MUTATION_FORBIDDEN")

__all__=["assert_geometry_unchanged","geometry","review_signal_candidate"]
