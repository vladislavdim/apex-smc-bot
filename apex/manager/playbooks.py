"""Manager action vocabulary.

Playbooks may propose only management actions; opening a new position is not a
Manager capability.
"""
ALLOWED_ACTIONS = ("HOLD", "PROTECT", "PARTIAL_TP", "LET_RUN", "CLOSE")

__all__ = ["ALLOWED_ACTIONS"]
