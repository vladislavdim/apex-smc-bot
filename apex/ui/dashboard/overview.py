"""Overview projection from the production dashboard snapshot."""
from __future__ import annotations
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "system": payload.get("system_overview", payload.get("system", {})),
        "summary": payload.get("summary", {}),
        "execution": payload.get("execution_mode", payload.get("execution", {})),
    }


__all__ = ["project"]
