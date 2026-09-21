"""Typed domain-event envelope used across V3 boundaries."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


@dataclass(frozen=True)
class DomainEvent:
    kind: str
    payload: Mapping[str, Any]
    entity_id: str = ""
    event_id: str = field(default_factory=lambda: f"evt_{uuid.uuid4().hex}")
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


__all__ = ["DomainEvent"]
