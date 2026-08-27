"""Best-effort external market context used only after an APEX candidate exists."""

from .aggregator import collect_external_context, format_external_context

__all__ = ("collect_external_context", "format_external_context")
