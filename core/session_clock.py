"""Timezone-aware session windows used by the FAST strategy."""

from __future__ import annotations

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo


LONDON = ZoneInfo("Europe/London")
NEW_YORK = ZoneInfo("America/New_York")


def _within(local_dt: datetime, start: time, end: time) -> bool:
    value = local_dt.time().replace(tzinfo=None)
    return start <= value <= end


def fast_session(now_utc: datetime | None = None) -> str | None:
    """Return the active liquid session while respecting DST transitions."""
    current = now_utc or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)

    london = current.astimezone(LONDON)
    if _within(london, time(8, 0), time(11, 30)):
        return "LONDON"

    new_york = current.astimezone(NEW_YORK)
    if _within(new_york, time(9, 30), time(12, 30)):
        return "NEW_YORK"
    return None


def is_fast_session(now_utc: datetime | None = None) -> bool:
    return fast_session(now_utc) is not None
