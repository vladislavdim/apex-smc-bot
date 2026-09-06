"""Small correctness fixups for passive observability instrumentation."""
from __future__ import annotations

from typing import Any

_APPLIED = False


def apply() -> None:
    """Keep normal FAST 15m context timing inclusive of BOS/CHoCH detection.

    The early DATA audit point is useful only when candle loading itself fails.
    On a normal pass we let the later STRUCTURE audit point close the 15m stage,
    so time spent detecting the two directional structure events is not lost
    between stage buckets. Trading values and predicates are untouched.
    """
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True
    try:
        import core.setup_audit as audit
    except Exception:
        return

    underlying = audit.audit_test

    def audit_test(code: str, value: Any, label: str = "", condition: str = "", line: int | None = None) -> Any:
        context = None
        previous_anchor = None
        had_anchor = False
        previous_marker = None
        had_marker = False
        is_fast_data_pass = False
        try:
            context = audit._current()
            is_fast_data_pass = bool(
                isinstance(context, dict)
                and str(context.get("strategy") or "").upper() == "FAST"
                and str(code) == "FAST_LTF_CONTEXT_DATA"
                and not bool(value)
            )
            if is_fast_data_pass:
                had_anchor = "_fast_stage_anchor" in context
                previous_anchor = context.get("_fast_stage_anchor")
                timing = context.setdefault("telemetry", {}).setdefault("fast_stage_timing", {})
                had_marker = "context_15m_ms" in timing
                previous_marker = timing.get("context_15m_ms")
        except Exception:
            is_fast_data_pass = False

        result = underlying(code, value, label, condition, line)

        if is_fast_data_pass and isinstance(context, dict):
            try:
                timing = context.setdefault("telemetry", {}).setdefault("fast_stage_timing", {})
                if had_marker:
                    timing["context_15m_ms"] = previous_marker
                else:
                    timing.pop("context_15m_ms", None)
                if had_anchor:
                    context["_fast_stage_anchor"] = previous_anchor
                else:
                    context.pop("_fast_stage_anchor", None)
            except Exception:
                pass
        return result

    audit.audit_test = audit_test
