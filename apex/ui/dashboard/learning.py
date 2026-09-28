"""Read-only live-learning projection."""
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "learning_v2" not in payload:
        return dict(payload.get("learning") or {})
    return {"live": payload.get("learning_v2") or {}, "authority": "ADVISORY"}


__all__ = ["project"]
