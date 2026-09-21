"""Structured event logging boundary."""
from __future__ import annotations

import json
import logging
from typing import Any, Mapping


def emit(kind: str, payload: Mapping[str, Any]) -> None:
    logging.info("[APEX_EVENT] %s %s", str(kind), json.dumps(dict(payload), sort_keys=True, default=str))


__all__ = ["emit"]
