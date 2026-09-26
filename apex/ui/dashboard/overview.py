"""Overview tab projection helpers."""
from __future__ import annotations
from typing import Mapping,Any

def project(payload:Mapping[str,Any])->dict[str,Any]:
    return {"system":payload.get("system",{}),"summary":payload.get("summary",{}),"execution":payload.get("execution",{})}
__all__=["project"]
