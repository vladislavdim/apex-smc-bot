"""Manager event domain object."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


@dataclass(frozen=True)
class ManagerEvent:
    position_id: str
    action: str
    payload: Mapping[str, Any]
    occurred_at: datetime = datetime.now(timezone.utc)


__all__ = ["ManagerEvent"]
