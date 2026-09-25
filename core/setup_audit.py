"""Deprecated compatibility facade for the canonical V3 telemetry event log."""

from apex.telemetry.event_log import (
    audit_fail,
    audit_observe,
    audit_strategy,
    audit_test,
    emit_decision_event,
    emit_event,
    emit_groq_review_event,
    emit_scan_event,
    take_last_completed_attempt,
)

__all__ = [
    "audit_fail", "audit_observe", "audit_strategy", "audit_test",
    "emit_decision_event", "emit_event", "emit_groq_review_event",
    "emit_scan_event", "take_last_completed_attempt",
]
