"""Deprecated import facade for the production market runtime.

New code must use the explicit modules under :mod:`apex.market` or one of the
launcher boundaries under :mod:`apex.compatibility`.  Attribute forwarding is
kept only for external operators and old integrations during the V3 cutover.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


_runtime = import_module("apex.compatibility.legacy_market_runtime")
__all__ = tuple(name for name in dir(_runtime) if not name.startswith("_"))
globals().update({name: getattr(_runtime, name) for name in __all__})


def __getattr__(name: str) -> Any:
    return getattr(_runtime, name)
