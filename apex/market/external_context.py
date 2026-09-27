"""Canonical optional external-context boundary.

External providers enrich evidence only; UNKNOWN is distinct from zero and
provider failures cannot become strategy authority.
"""
from __future__ import annotations
from typing import Any,Mapping
from external_sources.aggregator import collect_external_context

def external_context(values:Mapping[str,Any]|None,*,fresh:bool,source:str)->dict[str,Any]:
    return {"source":str(source),"fresh":bool(fresh),"state":"REAL" if fresh and values else "UNKNOWN","values":dict(values or {})}
__all__=["collect_external_context","external_context"]
