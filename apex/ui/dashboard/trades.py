"""Trades tab projection."""
from typing import Any,Mapping
def project(payload:Mapping[str,Any])->dict[str,Any]: return dict(payload.get("trades") or {})
__all__=["project"]
