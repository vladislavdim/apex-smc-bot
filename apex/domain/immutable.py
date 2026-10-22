"""JSON-compatible deeply immutable values for point-in-time market inputs."""

from __future__ import annotations
from collections.abc import Mapping


class FrozenDict(dict):
    """A detached dict that ordinary consumers can serialize but never mutate."""

    def _blocked(self, *args, **kwargs):
        raise TypeError("market_snapshot_is_immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = (
        __ior__
    ) = _blocked

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def freeze(value):
    if isinstance(value, Mapping):
        return FrozenDict((key, freeze(item)) for key, item in value.items())
    if isinstance(value, (tuple, list)):
        return tuple(freeze(item) for item in value)
    return value
