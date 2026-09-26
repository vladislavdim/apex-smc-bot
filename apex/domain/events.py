"""Dependency-free APEX V3 domain event envelope."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

@dataclass(frozen=True)
class DomainEvent:
    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: datetime=field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Mapping[str,Any]=field(default_factory=dict)
    def __post_init__(self)->None:
        if not self.event_id or not self.event_type or not self.aggregate_id: raise ValueError("domain_event_identity_required")
        if self.occurred_at.tzinfo is None: raise ValueError("domain_event_time_must_be_timezone_aware")
__all__=["DomainEvent"]
