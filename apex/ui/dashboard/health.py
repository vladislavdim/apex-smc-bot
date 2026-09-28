"""Production runtime health and incident projection."""
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "function_health" not in payload and "integration_health" not in payload:
        return dict(payload.get("health") or payload.get("system") or {})
    return {"functions": payload.get("function_health") or {},
            "integrations": payload.get("integration_health") or {},
            "incidents": payload.get("incidents") or [],
            "versions": payload.get("versions") or {},
            "dashboard_cache": payload.get("dashboard_cache") or {}}


__all__ = ["project"]
