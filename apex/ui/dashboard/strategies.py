"""Strategies tab projection."""
from typing import Any,Mapping
def project(payload:Mapping[str,Any])->dict[str,Any]: return dict(payload.get("strategies") or {})
__all__=["project"]
