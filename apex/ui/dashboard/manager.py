"""Manager projection from production telemetry."""
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "manager_db" not in payload and "manager_v2" not in payload:
        return dict(payload.get("manager") or {})
    return {"positions": payload.get("manager_db") or {},
            "reviews": payload.get("manager_v2") or {}}


__all__ = ["project"]
