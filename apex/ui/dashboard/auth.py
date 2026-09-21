"""Dashboard authentication helpers."""
from __future__ import annotations

import hmac


def authorized(provided: str, expected: str) -> bool:
    return bool(expected) and hmac.compare_digest(str(provided or ""), str(expected))


__all__ = ["authorized"]
