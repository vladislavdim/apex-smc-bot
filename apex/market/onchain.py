"""Optional on-chain context; never required for core strategy evaluation."""
from __future__ import annotations
from typing import Any

def onchain_context(value:Any=None,*,source:str="")->dict[str,Any]:
    return {"source":source,"state":"REAL" if value is not None else "UNKNOWN","value":value}
__all__=["onchain_context"]
