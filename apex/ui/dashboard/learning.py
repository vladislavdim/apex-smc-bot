"""Learning tab projection helpers."""
from __future__ import annotations
from typing import Mapping,Any

def project(payload:Mapping[str,Any])->dict[str,Any]: return dict(payload.get("learning") or {})
__all__=["project"]
