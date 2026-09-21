"""Read-only market-data and indicator compatibility exports."""

from apex.market.runtime_cache import (
    get_confirmed_candles, update_global_candles,
)
from apex.ui.price_format import smart_price_fmt
from apex.market.time_estimate import get_estimated_time
from apex.market.gate_tickers import get_all_market_pairs, get_live_prices, get_top_pairs
from apex.market.gate_orderbook import get_orderbook
from apex.market.context_quotes import get_fear_greed, get_funding_rate
from apex.market.macro_context import get_dxy_signal, get_upcoming_events
from apex.market.structure_bridge import find_swings, get_bos_choch_event
from apex.market.legacy_zones import find_fvg, find_ob
from apex.market.indicators import ema_value
from apex.market.engine_bridge import calculate_vwap, get_liquidity_heatmap

from apex.compatibility.legacy_market_runtime import (
    fetch_candles_batch,
    get_adaptive_params, get_candles,
    get_market_regime,
    get_precomputed_indicators,
    multi_tf_analysis, smc_on_tf,
)

__all__ = [name for name in globals() if not name.startswith("__")]
