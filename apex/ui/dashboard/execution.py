"""Execution tab projection."""
from typing import Any,Mapping
def project(payload:Mapping[str,Any])->dict[str,Any]: return dict(payload.get("execution") or {})
__all__=["project"]
