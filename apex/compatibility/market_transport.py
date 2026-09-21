"""Transport and configuration exports from the legacy market runtime."""

from core.pair_universe import DEFAULT_UNIVERSE_SIZE
from apex.compatibility.market_constants import (
    FAST_PAIRS, SYMBOL_ALIASES, TF_CATEGORIES, TF_LABELS,
)
from apex.app.health_server import run_server
from apex.db.compatibility_runtime import start_db_writer
from apex.compatibility.legacy_market_runtime import (
    ADMIN_ID, ADMIN_IDS, FAST_DEAL_THREAD_ID, SIGNAL_CHANNEL_MAIN,
    SIGNAL_CHANNEL_SWING, SWING_THREAD_ID, bot, dp, init_db,
)

__all__ = [name for name in globals() if not name.startswith("__")]
