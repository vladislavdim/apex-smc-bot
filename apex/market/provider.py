"""Canonical one-call MarketSnapshot provider for the five V3 strategies."""

from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from apex.domain.enums import Strategy
from apex.strategies.specifications import specification_for

from .gate_client import GateMarketClient, normalize_gate_candles
from .candles import confirmed_candles
from .live_context import fetch_live_context
from .orderflow import proxy_cvd
from .regime import classify_regime
from .snapshots import SnapshotBuild, build_snapshot
from .structure import analyze_market_structure
from .volume import volume_features, vwap
from .volume_profile import volume_profile
from .universe import UniverseContext, market_cap_omission


class _CollectedClient:
    """Bounded parallel I/O; all facts are collected before the decision clock."""
    def __init__(self, client, requests):
        self.values = {}
        def fetch(request):
            method, args, kwargs = request
            try:
                return getattr(client, method)(*args, **kwargs)
            except Exception as exc:
                return exc
        # One symbol is evaluated at a time. Four connections bound both
        # venue load and memory, and every submitted operation is joined.
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="gate-snapshot") as pool:
            for request, value in zip(requests, pool.map(fetch, requests)):
                method, args, kwargs = request
                self.values[(method, args, tuple(sorted(kwargs.items())))] = value

    def __getattr__(self, method):
        def read(*args, **kwargs):
            value = self.values[(method, args, tuple(sorted(kwargs.items())))]
            if isinstance(value, Exception):
                raise value
            return value
        return read


class GateSnapshotProvider:
    def __init__(
        self, client: GateMarketClient, *, candle_limit: int = 500,
        universe_context: UniverseContext | None = None,
        ownership_reader=None,
    ) -> None:
        self.client = client
        self.candle_limit = min(2000, max(100, int(candle_limit)))
        self.universe_context = universe_context
        self.ownership_reader = ownership_reader

    def build(
        self,
        strategy: Strategy | str,
        symbol: str,
        *,
        as_of: datetime | None = None,
    ) -> SnapshotBuild:
        key = strategy if isinstance(strategy, Strategy) else Strategy(str(strategy).upper())
        specification = specification_for(key)
        boundary = as_of or datetime.now(timezone.utc)
        if boundary.tzinfo is None:
            boundary = boundary.replace(tzinfo=timezone.utc)
        boundary = boundary.astimezone(timezone.utc)
        required = tuple(dict.fromkeys((specification.working_timeframe, *specification.context_timeframes)))
        started = time.monotonic()
        requests = [("candles", (symbol, tf), {"limit": self.candle_limit}) for tf in required]
        if symbol.upper() != "BTCUSDT":
            requests += [("candles", ("BTCUSDT", tf), {"limit": 150}) for tf in ("1h", "4h")]
        requests += [(method, (symbol,), {}) for method in (
            "contract_stats", "funding_history", "contract_multiplier", "public_trades", "order_book")]
        client = _CollectedClient(self.client, requests)
        if as_of is None:
            # The observation time is after collection, never before it.
            boundary = datetime.now(timezone.utc)
        candles = {}
        for timeframe in required:
            try:
                response = client.candles(symbol, timeframe, limit=self.candle_limit)
                # Every downstream engine consumes exactly the same confirmed
                # point-in-time rows.  Filtering only inside build_snapshot is
                # too late because structure/regime/volume are derived below.
                candles[timeframe] = confirmed_candles(
                    normalize_gate_candles(response.payload),
                    timeframe,
                    as_of=min(boundary.timestamp(), response.received_at), strict=True,
                )
            except Exception:
                candles[timeframe] = []

        # Context is optional and cannot change StrategyResult. Late-arriving
        # observations are excluded by PointInTimeContext.
        live = fetch_live_context(client, symbol, as_of=int(boundary.timestamp()))
        working_rows = candles.get(specification.working_timeframe, [])
        context_rows = next((candles[tf] for tf in reversed(required) if candles.get(tf)), working_rows)
        volume = volume_features(working_rows)
        volume.update({
            "vwap": vwap(working_rows),
            "volume_profile": volume_profile(working_rows),
            "cvd_proxy": proxy_cvd(working_rows),
        })
        structure = {
            timeframe: analyze_market_structure(rows, swing_lookback=5, max_break_age=1)
            for timeframe, rows in candles.items()
            if rows
        }
        market_context = (
            self.universe_context.for_symbol(symbol, required, as_of=boundary)
            if self.universe_context is not None
            else {
                "authority": "LIVE_CONTEXT", "can_change_strategy_gate": False,
                "status": "UNAVAILABLE", "reason_code": "UNIVERSE_CONTEXT_NOT_LOADED",
                "market_cap": market_cap_omission(),
            }
        )
        # Freeze BTC's directional context with the candidate; native detectors
        # must never read a different symbol from a live/global cache.
        references = {}
        reference_wait = []
        if symbol.upper() != "BTCUSDT":
            for timeframe in ("1h", "4h"):
                try:
                    response = client.candles("BTCUSDT", timeframe, limit=150)
                    references[timeframe] = confirmed_candles(
                        normalize_gate_candles(response.payload), timeframe, as_of=min(boundary.timestamp(), response.received_at), strict=True,
                    )
                except Exception:
                    references[timeframe] = []
        if symbol.upper() != "BTCUSDT":
            from .freshness import assess_candles
            for timeframe, rows in references.items():
                if assess_candles(timeframe, rows, as_of=boundary).status != "FRESH":
                    reference_wait.append("BTC_REFERENCE_UNAVAILABLE:" + timeframe)
        market_context = dict(market_context)
        ownership = self.ownership_reader(symbol) if self.ownership_reader is not None else False
        market_context["pending_signal"] = ownership
        market_context["reference_candles"] = {"BTCUSDT": references} if references else {}
        book = live.microstructure.get("visible_orderbook", {})
        microstructure = dict(live.microstructure)
        if book.get("available"):
            bids, asks = book["bid_depth_usd"], book["ask_depth_usd"]
            microstructure["strategy_orderbook"] = {"bids": bids, "asks": asks, "bias": "BUY" if bids > asks else "SELL"}
        build = build_snapshot(
            symbol=symbol,
            as_of=boundary,
            raw_candles=candles,
            required_timeframes=required,
            structure=structure,
            regime=classify_regime(context_rows),
            volume=volume,
            derivatives_context=live.derivatives,
            microstructure_context=microstructure,
            market_context=market_context,
        )
        logger = logging.getLogger("apex.scans")
        logger.setLevel(logging.INFO)
        logger.info("[APEX Snapshot] strategy=%s symbol=%s duration=%.3fs requests=%s wait=%s",
                    key.value, symbol, time.monotonic()-started, len(requests), build.wait_reason_codes)
        if ownership is None:
            reference_wait.append("PAIR_OWNERSHIP_UNAVAILABLE")
        if reference_wait:
            from dataclasses import replace
            return replace(build, wait_reason_codes=build.wait_reason_codes + tuple(reference_wait))
        return build


__all__ = ["GateSnapshotProvider"]
