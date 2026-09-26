"""Manager tab projection."""
from typing import Any,Mapping
def project(payload:Mapping[str,Any])->dict[str,Any]: return dict(payload.get("manager") or {})
__all__=["project"]
