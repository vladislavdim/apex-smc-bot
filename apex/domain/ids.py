"""Typed, non-overlapping production entity identifiers."""

from __future__ import annotations

import uuid


PREFIXES = {
    "candidate": "cand",
    "signal": "sig",
    "execution": "exec",
    "position": "pos",
    "manager_event": "mgr",
    "delivery": "del",
    "incident": "inc",
    "outcome": "out",
    "snapshot": "snap",
}


def new_id(entity: str) -> str:
    try:
        prefix = PREFIXES[str(entity)]
    except KeyError as exc:
        raise ValueError(f"unknown entity id type: {entity}") from exc
    return f"{prefix}_{uuid.uuid4().hex}"


def derived_id(entity: str, *identity: object) -> str:
    """Return a stable typed ID for an imported external identity."""
    try:
        prefix = PREFIXES[str(entity)]
    except KeyError as exc:
        raise ValueError(f"unknown entity id type: {entity}") from exc
    material = "\x1f".join(str(part) for part in identity)
    if not material:
        raise ValueError("derived identity is required")
    return f"{prefix}_{uuid.uuid5(uuid.NAMESPACE_URL, 'apex-v3:' + str(entity) + ':' + material).hex}"


def is_id(value: object, entity: str) -> bool:
    prefix = PREFIXES.get(str(entity), "")
    text = str(value or "")
    if not prefix or not text.startswith(prefix + "_"):
        return False
    suffix = text[len(prefix) + 1:]
    return len(suffix) == 32 and all(char in "0123456789abcdef" for char in suffix)


__all__ = ["PREFIXES", "derived_id", "is_id", "new_id"]
