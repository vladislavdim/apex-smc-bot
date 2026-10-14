"""Strategy funnel and check projection."""
from typing import Any, Mapping


def project(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "funnels" not in payload and "strategy_counts" not in payload:
        return dict(payload.get("strategies") or {})
    return {key: payload.get(key) or ([] if key in {"funnels", "criterion_stats", "failures"} else {})
            for key in ("strategy_counts", "funnels", "criterion_stats", "failures")}


__all__ = ["project"]
