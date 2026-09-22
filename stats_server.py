"""APEX Dashboard launcher.

All HTTP, persistence and projection logic lives in
``apex.ui.dashboard.server``.  Keeping this file transport-only makes the
Render start command stable while preventing hidden service initialization at
import time.
"""

from apex.ui.dashboard.server import main


if __name__ == "__main__":
    main()
