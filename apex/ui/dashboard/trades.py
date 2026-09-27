"""Trades projection from production telemetry."""
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "trade_stats" not in payload and "rows" not in payload:
        return dict(payload.get("trades") or {})
    return {"stats": payload.get("trade_stats") or {},
            "rows": payload.get("rows") or [],
            "pagination": payload.get("pagination") or {}}


__all__ = ["project"]
