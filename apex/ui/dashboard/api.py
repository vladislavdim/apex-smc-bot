"""Dashboard read-only API boundary.

The HTTP server remains in :mod:`apex.ui.dashboard.server`; this module marks
the canonical location for extracted API projections.
"""
from apex.ui.dashboard import page as compatibility_page

__all__ = ["compatibility_page"]
