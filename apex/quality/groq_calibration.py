"""Read-only Groq calibration summaries from real reviewed outcomes."""
from __future__ import annotations
from typing import Iterable,Mapping,Any

def approval_rate(rows:Iterable[Mapping[str,Any]])->float|None:
    values=list(rows)
    if not values:return None
    return sum(str(x.get("decision","")).upper()=="APPROVE" for x in values)/len(values)
__all__=["approval_rate"]
