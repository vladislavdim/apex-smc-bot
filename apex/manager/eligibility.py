"""Deterministic action eligibility before a Manager Groq call."""

from __future__ import annotations

from dataclasses import dataclass

from apex.domain.enums import Direction


MANAGER_ACTIONS = ("HOLD", "PROTECT", "PARTIAL_EXIT", "LET_RUN", "CLOSE")


@dataclass(frozen=True)
class ManagerFacts:
    direction: Direction
    current_price: float
    confirmed_stop: float
    proposed_stop: float | None = None
    structural_protection: bool = False
    tp1_confirmed: bool = False
    continuation_confirmed: bool = False
    invalidation: bool = False
    reversal_confirmed: bool = False
    emergency: bool = False
    external_conflict: bool = False
    remaining_quantity: float = 0.0


def stop_improves(facts: ManagerFacts) -> bool:
    level = facts.proposed_stop
    if level is None or not facts.structural_protection:
        return False
    if facts.direction is Direction.LONG:
        return facts.confirmed_stop < level < facts.current_price
    return facts.current_price < level < facts.confirmed_stop


def eligible_actions(facts: ManagerFacts) -> tuple[str, ...]:
    actions = ["HOLD"]
    if stop_improves(facts):
        actions.append("PROTECT")
    if facts.tp1_confirmed and facts.remaining_quantity > 0:
        actions.append("PARTIAL_EXIT")
    if facts.continuation_confirmed:
        actions.append("LET_RUN")
    if facts.invalidation or facts.reversal_confirmed or facts.emergency:
        actions.append("CLOSE")
    return tuple(actions)


def enforce_action(action: str, facts: ManagerFacts) -> str:
    normalized = str(action or "").upper()
    return normalized if normalized in eligible_actions(facts) else "HOLD"


__all__ = ["MANAGER_ACTIONS", "ManagerFacts", "eligible_actions", "enforce_action", "stop_improves"]
