"""Manager event domain object."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping


@dataclass(frozen=True)
class ManagerEvent:
    position_id: str
    action: str
    payload: Mapping[str, Any]
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


__all__ = ["ManagerEvent"]
