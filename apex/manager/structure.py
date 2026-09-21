"""Structural protection helpers."""
from __future__ import annotations

from apex.domain.enums import Direction


def improves_stop(direction: Direction, current_stop: float, proposed_stop: float, current_price: float) -> bool:
    if direction is Direction.LONG:
        return current_stop < proposed_stop < current_price
    return current_price < proposed_stop < current_stop


__all__ = ["improves_stop"]
