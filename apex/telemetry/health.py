"""Read-only V3 runtime health projection."""
from __future__ import annotations
from typing import Any

def runtime_health(supervisor:Any)->dict[str,Any]:
    snapshot=supervisor.snapshot()
    return dict(snapshot) if isinstance(snapshot,dict) else {"status":str(snapshot)}
__all__=["runtime_health"]
