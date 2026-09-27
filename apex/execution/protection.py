"""Execution protection boundary.

Order protection remains implemented by the canonical manager state machine
for this migration step; this module is the stable execution-facing import.
It intentionally exports only protection primitives and owns no strategy or
Groq decisions.
"""
from apex.manager.state_machine import (
    ProtectionState,
    ProtectionTransition,
    transition_protection,
)

__all__ = ["ProtectionState", "ProtectionTransition", "transition_protection"]
