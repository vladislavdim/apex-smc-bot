"""Manager dashboard projection boundary.

Presentation is read-only. The existing dashboard implementation is preserved
during the physical V3 package migration and may be extracted into this module
without changing runtime behavior.
"""
from apex.ui.dashboard import page as compatibility_page

__all__ = ["compatibility_page"]
