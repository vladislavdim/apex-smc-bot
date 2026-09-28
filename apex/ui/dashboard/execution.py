"""Execution projection from production telemetry."""
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "execution_mode" not in payload and "execution_health" not in payload:
        return dict(payload.get("execution") or {})
    return {
        "mode": payload.get("execution_mode") or {},
        "health": payload.get("execution_health") or {},
        "trade_stats": payload.get("trade_stats") or {},
    }


__all__ = ["project"]
