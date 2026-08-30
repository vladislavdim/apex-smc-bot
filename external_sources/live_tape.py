"""Bounded public WebSocket tape for Gate, Binance and Bybit USDT perps."""
from __future__ import annotations
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
import json
import logging
import os
import sqlite3
import time
from typing import Any
from core.data_policy import configured_market_data_providers
from .models import number
from .pair_registry import get_pair

try:
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None

SOURCE = "live_market_tape"
_DB_PATH = os.environ.get("APEX_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"))

@dataclass
class TapeEvent:
    timestamp: float
    value_usd: float
    side: str

_trades: dict[tuple[str, str], deque[TapeEvent]] = defaultdict(lambda: deque(maxlen=1500))
_liquidations: dict[tuple[str, str], deque[TapeEvent]] = defaultdict(lambda: deque(maxlen=1200))
_latest: dict[tuple[str, str], dict[str, Any]] = {}
_tasks: list[asyncio.Task] = []
_stop_event: asyncio.Event | None = None
_last_persist: dict[str, float] = {}
_configured_symbols: list[str] = []
_provider_to_apex: dict[tuple[str, str], str] = {}

def _apex_from_provider(provider: str, provider_symbol: str) -> str | None:
    normalized = provider_symbol.upper().replace("-", "_")
    resolved = _provider_to_apex.get((provider, normalized))
    if resolved:
        return resolved
    for symbol in _configured_symbols:  # fallback supports isolated parser tests
        expected = get_pair(symbol).get(f"{provider}_symbol")
        if expected and str(expected).upper() == normalized:
            return symbol
    return None

def _prune(events: deque[TapeEvent], now: float, window: int = 600) -> None:
    while events and now - events[0].timestamp > window:
        events.popleft()

def _add(target, provider, symbol, timestamp, usd, side):
    if usd <= 0:
        return
    events = target[(provider, symbol)]
    if events and int(events[-1].timestamp) == int(timestamp) and events[-1].side == side:
        events[-1].value_usd += usd
    else:
        events.append(TapeEvent(timestamp, usd, side))
    _latest[(provider, symbol)] = {**_latest.get((provider, symbol), {}), "updated_at": timestamp}

def _trade(provider, symbol, timestamp, usd, side):
    if side in {"buy", "sell"}: _add(_trades, provider, symbol, timestamp, usd, side)

def _liquidation(provider, symbol, timestamp, usd, side):
    if side in {"long", "short"}: _add(_liquidations, provider, symbol, timestamp, usd, side)

def ingest_gate(message: dict[str, Any]) -> None:
    if message.get("event") != "update": return
    channel, result = message.get("channel"), message.get("result")
    rows, now = (result if isinstance(result, list) else [result]), time.time()
    for row in rows:
        if not isinstance(row, dict): continue
        symbol = _apex_from_provider("gate", str(row.get("contract") or row.get("s") or ""))
        if not symbol: continue
        multiplier = number(get_pair(symbol).get("gate_multiplier")) or 1.0
        timestamp = (number(row.get("create_time_ms")) or now * 1000) / 1000
        if channel == "futures.trades" and not row.get("is_internal"):
            size, price = number(row.get("size")), number(row.get("price"))
            if size is not None and price is not None:
                _trade("gate", symbol, timestamp, abs(size) * multiplier * price, "buy" if size > 0 else "sell")
        elif channel == "futures.book_ticker":
            _latest[("gate", symbol)] = {**_latest.get(("gate", symbol), {}), "bid": number(row.get("b")), "ask": number(row.get("a")), "updated_at": now}
        elif channel == "futures.tickers":
            mark, units = number(row.get("mark_price")), number(row.get("total_size"))
            _latest[("gate", symbol)] = {**_latest.get(("gate", symbol), {}), "funding": number(row.get("funding_rate")),
                "oi": abs(units) * multiplier * mark if units is not None and mark is not None else None, "updated_at": now}

def ingest_binance(message: dict[str, Any]) -> None:
    row = message.get("data", message)
    if not isinstance(row, dict): return
    event = str(row.get("e", "")); order = row.get("o", {}) if isinstance(row.get("o"), dict) else {}
    provider_symbol = str(order.get("s", "")) if event == "forceOrder" else str(row.get("s", ""))
    symbol = _apex_from_provider("binance", provider_symbol)
    if not symbol: return
    timestamp = (number(row.get("E")) or time.time() * 1000) / 1000
    if event == "aggTrade":
        price, quantity = number(row.get("p")), number(row.get("q"))
        if price is not None and quantity is not None:
            _trade("binance", symbol, timestamp, price * quantity, "sell" if row.get("m") else "buy")
    elif event == "forceOrder":
        price, quantity = number(order.get("ap", order.get("p"))), number(order.get("z", order.get("q")))
        if price is not None and quantity is not None:
            _liquidation("binance", symbol, timestamp, price * quantity, "long" if order.get("S") == "SELL" else "short")
    elif event == "bookTicker":
        _latest[("binance", symbol)] = {**_latest.get(("binance", symbol), {}), "bid": number(row.get("b")), "ask": number(row.get("a")), "updated_at": timestamp}

def ingest_bybit(message: dict[str, Any]) -> None:
    topic = str(message.get("topic", "")); symbol = _apex_from_provider("bybit", topic.rsplit(".", 1)[-1])
    if not symbol: return
    data = message.get("data"); rows = data if isinstance(data, list) else [data]
    now = (number(message.get("ts")) or time.time() * 1000) / 1000
    for row in rows:
        if not isinstance(row, dict): continue
        timestamp = (number(row.get("T")) or now * 1000) / 1000
        if topic.startswith("publicTrade."):
            price, quantity = number(row.get("p")), number(row.get("v"))
            if price is not None and quantity is not None: _trade("bybit", symbol, timestamp, price * quantity, "buy" if row.get("S") == "Buy" else "sell")
        elif topic.startswith("allLiquidation."):
            price, quantity = number(row.get("p")), number(row.get("v"))
            if price is not None and quantity is not None: _liquidation("bybit", symbol, timestamp, price * quantity, "long" if row.get("S") == "Buy" else "short")
        elif topic.startswith("tickers."):
            mark, units = number(row.get("markPrice")), number(row.get("openInterest"))
            _latest[("bybit", symbol)] = {**_latest.get(("bybit", symbol), {}), "funding": number(row.get("fundingRate")),
                "oi": units * mark if units is not None and mark is not None else None,
                "bid": number(row.get("bid1Price")), "ask": number(row.get("ask1Price")), "updated_at": now}

def snapshot(symbol: str, now: float | None = None) -> dict[str, Any]:
    now = now or time.time(); sources = {}
    for provider in ("gate", "binance", "bybit"):
        trades, liqs = _trades[(provider, symbol)], _liquidations[(provider, symbol)]
        _prune(trades, now); _prune(liqs, now)
        recent_trades = [row for row in trades if now - row.timestamp <= 60]
        recent_liq = [row for row in liqs if now - row.timestamp <= 300]
        latest = dict(_latest.get((provider, symbol), {})); updated = number(latest.get("updated_at"))
        if recent_trades or recent_liq or (updated and now - updated <= 120):
            sources[provider] = {"buy_usd_60s": round(sum(r.value_usd for r in recent_trades if r.side == "buy"), 2),
                "sell_usd_60s": round(sum(r.value_usd for r in recent_trades if r.side == "sell"), 2),
                "long_liq_usd_300s": round(sum(r.value_usd for r in recent_liq if r.side == "long"), 2),
                "short_liq_usd_300s": round(sum(r.value_usd for r in recent_liq if r.side == "short"), 2),
                "age_seconds": int(max(0, now - (updated or max([r.timestamp for r in recent_trades + recent_liq] or [now])))),
                **{key: latest.get(key) for key in ("bid", "ask", "oi", "funding")}}
    buy, sell = sum(v["buy_usd_60s"] for v in sources.values()), sum(v["sell_usd_60s"] for v in sources.values())
    return {"symbol": symbol, "sources": sources, "buy_usd_60s": round(buy, 2), "sell_usd_60s": round(sell, 2),
        "long_liq_usd_300s": round(sum(v["long_liq_usd_300s"] for v in sources.values()), 2),
        "short_liq_usd_300s": round(sum(v["short_liq_usd_300s"] for v in sources.values()), 2),
        "bias": "bullish" if buy > sell * 1.25 else "bearish" if sell > buy * 1.25 else "neutral" if buy + sell else "unknown",
        "age_seconds": max((v["age_seconds"] for v in sources.values()), default=None)}

def _persist_snapshot(data: dict[str, Any], db_path: str = _DB_PATH) -> None:
    try:
        conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False); conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE IF NOT EXISTS live_market_tape_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,observed_at INTEGER NOT NULL,payload_json TEXT NOT NULL)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_live_tape_symbol_time ON live_market_tape_snapshots(symbol,observed_at)")
        conn.execute("INSERT INTO live_market_tape_snapshots(symbol,observed_at,payload_json) VALUES (?,?,?)", (data["symbol"], int(time.time()), json.dumps(data, separators=(",", ":"))))
        conn.execute("DELETE FROM live_market_tape_snapshots WHERE observed_at<?", (int(time.time()) - 7 * 86400,)); conn.commit(); conn.close()
    except Exception as exc: logging.debug("[LiveTape] snapshot persistence skipped: %s", exc)

async def collect(symbol: str) -> dict:
    data = snapshot(symbol)
    if not data["sources"]: return {"source": SOURCE, "status": "warming_up", "symbol": symbol}
    if time.time() - _last_persist.get(symbol, 0) >= 60:
        _last_persist[symbol] = time.time(); await asyncio.to_thread(_persist_snapshot, data)
    return {"source": SOURCE, "status": "fresh", "age_seconds": data["age_seconds"], "normalized": data}

async def _consume(provider: str, url: str, subscriptions: list[dict[str, Any]], parser) -> None:
    if aiohttp is None: return
    backoff = 1
    while _stop_event and not _stop_event.is_set():
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=8, sock_read=70)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(url, heartbeat=25, autoping=True) as ws:
                    for subscription in subscriptions: await ws.send_json(subscription)
                    backoff = 1
                    async for message in ws:
                        if _stop_event and _stop_event.is_set(): break
                        if message.type == aiohttp.WSMsgType.TEXT:
                            try: parser(json.loads(message.data))
                            except Exception as exc: logging.debug("[LiveTape] parse: %s", exc)
                        elif message.type in {aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED}: break
        except asyncio.CancelledError: raise
        except Exception as exc:
            logging.warning("[LiveTape] %s websocket retry url=%s error=%s", provider, url.split("?", 1)[0], type(exc).__name__)
        if _stop_event and not _stop_event.is_set(): await asyncio.sleep(min(backoff, 30)); backoff = min(backoff * 2, 30)

async def start(symbols: list[str]) -> dict[str, Any]:
    global _configured_symbols, _stop_event, _tasks, _provider_to_apex
    normalized_symbols = [s.upper().replace("/", "") for s in symbols]
    _tasks = [task for task in _tasks if not task.done()]
    if _tasks and normalized_symbols == _configured_symbols:
        return {"status": "already_running", "symbols": len(_configured_symbols)}
    if _tasks:
        await stop()
    _configured_symbols = normalized_symbols; _provider_to_apex = {}
    for symbol in _configured_symbols:
        pair = get_pair(symbol)
        for provider in ("gate", "binance", "bybit"):
            provider_symbol = pair.get(f"{provider}_symbol")
            if provider_symbol: _provider_to_apex[(provider, str(provider_symbol).upper())] = symbol
    _stop_event = asyncio.Event()
    enabled = set(configured_market_data_providers())
    gate = [get_pair(s)["gate_symbol"] for s in _configured_symbols if get_pair(s).get("gate_supported")]
    binance = [get_pair(s)["binance_symbol"] for s in _configured_symbols if get_pair(s).get("binance_supported")]
    bybit = [get_pair(s)["bybit_symbol"] for s in _configured_symbols if get_pair(s).get("bybit_supported")]
    if gate and "gate" in enabled:
        subs = [{"time": int(time.time()), "channel": channel, "event": "subscribe", "payload": gate} for channel in ("futures.trades", "futures.book_ticker", "futures.tickers")]
        _tasks.append(asyncio.create_task(_consume("gate", "wss://fx-ws.gateio.ws/v4/ws/usdt", subs, ingest_gate)))
    if binance and "binance" in enabled:
        public_streams = [f"{symbol.lower()}@bookTicker" for symbol in binance]
        market_streams = [stream for symbol in binance for stream in (f"{symbol.lower()}@aggTrade", f"{symbol.lower()}@forceOrder")]
        for route, streams in (("public", public_streams), ("market", market_streams)):
            for offset in range(0, len(streams), 80):
                chunk = streams[offset:offset + 80]
                url = f"wss://fstream.binance.com/{route}/stream?streams=" + "/".join(chunk)
                _tasks.append(asyncio.create_task(_consume(f"binance-{route}", url, [], ingest_binance)))
    if bybit and "bybit" in enabled:
        args = [topic for symbol in bybit for topic in (f"publicTrade.{symbol}", f"allLiquidation.{symbol}", f"tickers.{symbol}")]
        subs = [{"op": "subscribe", "args": args[i:i + 50]} for i in range(0, len(args), 50)]
        _tasks.append(asyncio.create_task(_consume("bybit", "wss://stream.bybit.com/v5/public/linear", subs, ingest_bybit)))
    return {"status": "started" if _tasks else "unavailable", "symbols": len(_configured_symbols), "connections": len(_tasks), "providers": sorted(enabled)}

async def stop() -> None:
    global _tasks, _stop_event
    if _stop_event: _stop_event.set()
    for task in _tasks: task.cancel()
    if _tasks: await asyncio.gather(*_tasks, return_exceptions=True)
    _tasks, _stop_event = [], None
