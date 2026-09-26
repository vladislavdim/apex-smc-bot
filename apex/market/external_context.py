"""Canonical optional external-context projection.
Optional sources enrich evidence and never become strategy authority.
"""
from __future__ import annotations
from typing import Any,Mapping

def external_context(values:Mapping[str,Any]|None,*,fresh:bool,source:str)->dict[str,Any]:
    return {"source":str(source),"fresh":bool(fresh),"state":"REAL" if fresh and values else "UNKNOWN","values":dict(values or {})}
__all__=["external_context"]
