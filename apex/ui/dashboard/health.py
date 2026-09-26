"""Health tab projection helpers."""
from __future__ import annotations
from typing import Mapping,Any

def project(payload:Mapping[str,Any])->dict[str,Any]: return dict(payload.get("health") or payload.get("system") or {})
__all__=["project"]
