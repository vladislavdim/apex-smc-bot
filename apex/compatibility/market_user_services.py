"""User-facing compatibility services without execution authority."""

from apex.ui.market_format import format_accumulation, format_news
from apex.ui.user_memory import get_user_memory, save_chat_log, update_user_memory
from apex.ui.context_store import save_news
from apex.market.news_provider import get_crypto_news, get_market_impact_news
from apex.market.accumulation_analysis import AccumulationAnalysis
from apex.market.gate_orderbook import get_orderbook
from apex.ui.risk_calculator import calc_risk
from apex.ui.live_position import live_position_analysis
from apex.ui.profile_extraction import extract_and_save_profile
from apex.strategies.legacy_scan_registry import analyze_trade_type
from apex.ui.groq_runtime import (
    _GROQ_DAILY_LIMIT, _tokens_available, groq_tokens_used,
)

from market import (
    ask_ai, ask_groq, get_candles,
)

_ACCUMULATION_ANALYSIS = AccumulationAnalysis(
    get_candles,
    get_orderbook,
    lambda prompt, **kwargs: ask_groq(prompt, **kwargs),
)
detect_accumulation = _ACCUMULATION_ANALYSIS.detect

__all__ = [name for name in globals() if not name.startswith("__")]
__all__ += ["_GROQ_DAILY_LIMIT", "_tokens_available"]
