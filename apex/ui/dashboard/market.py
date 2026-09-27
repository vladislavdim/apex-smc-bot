"""Gate market and source-health projection."""
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "market_data" not in payload and "source_registry" not in payload:
        return dict(payload.get("market") or {})
    return {"market_data": payload.get("market_data") or {},
            "source_registry": payload.get("source_registry") or [],
            "gate_microstructure": payload.get("gate_microstructure") or []}


__all__ = ["project"]
