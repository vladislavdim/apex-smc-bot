"""Pure Telegram formatting helpers."""
from __future__ import annotations

import html
from typing import Any


def escape(value: Any) -> str:
    return html.escape(str(value), quote=False)


def code(value: Any) -> str:
    return f"<code>{escape(value)}</code>"


__all__ = ["code", "escape"]
