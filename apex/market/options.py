"""Optional options-market context; UNKNOWN is distinct from zero."""
from __future__ import annotations
from typing import Any

def options_context(value:Any=None,*,source:str="")->dict[str,Any]:
    return {"source":source,"state":"REAL" if value is not None else "UNKNOWN","value":value}
__all__=["options_context"]
