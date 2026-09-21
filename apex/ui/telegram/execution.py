"""Execution Telegram presentation boundary.

The compatibility command implementation remains available while handlers are
migrated here incrementally; trading decisions never belong in this UI layer.
"""
from apex.ui.telegram import commands as compatibility_commands

__all__ = ["compatibility_commands"]
