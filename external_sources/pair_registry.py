"""Provider symbol registry and coverage audit for the unchanged APEX universe."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import Any

from .http_client import http_client
from .models import number

SOURCE = "pair_registry"
_DB_PATH = os.environ.get("APEX_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"))
_SCALED = {name: f"1000{name}" for name in ("PEPEUSDT", "SHIBUSDT", "BONKUSDT", "FLOKIUSDT", "SATSUSDT")}
_snapshot: dict[str, dict[str, Any]] = {}
_checked_at = 0.0
_refresh_ttl = 3600
_lock = asyncio.Lock()


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or _DB_PATH, timeout=20, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS external_pair_registry (
        apex_symbol TEXT PRIMARY KEY,
        gate_symbol TEXT, binance_symbol TEXT, bybit_symbol TEXT, hyperliquid_symbol TEXT,
        gate_supported INTEGER DEFAULT 0, binance_supported INTEGER DEFAULT 0,
        bybit_supported INTEGER DEFAULT 0, hyperliquid_supported INTEGER DEFAULT 0,
        gate_multiplier REAL, chain TEXT DEFAULT 'unknown', contract_address TEXT,
        gate_status TEXT DEFAULT 'unverified', binance_status TEXT DEFAULT 'unverified',
        bybit_status TEXT DEFAULT 'unverified', hyperliquid_status TEXT DEFAULT 'unverified',
        checked_at INTEGER NOT NULL
    )""")
    columns = {row[1] for row in conn.execute("PRAGMA table_info(external_pair_registry)")}
    for name in ("gate_status", "binance_status", "bybit_status", "hyperliquid_status"):
        if name not in columns:
            conn.execute(f"ALTER TABLE external_pair_registry ADD COLUMN {name} TEXT DEFAULT 'unverified'")
    return conn


def _verified_contracts() -> dict[str, dict[str, str]]:
    try:
        value = json.loads(os.environ.get("ASSET_CONTRACT_MAP_JSON", "{}"))
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _base(symbol: str) -> str:
    symbol = symbol.upper().replace("/", "")
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def get_pair(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper().replace("/", "")
    if symbol in _snapshot:
        return dict(_snapshot[symbol])
    try:
        conn = _connect()
        row = conn.execute("SELECT * FROM external_pair_registry WHERE apex_symbol=?", (symbol,)).fetchone()
        names = [item[1] for item in conn.execute("PRAGMA table_info(external_pair_registry)")]
        conn.close()
        if row:
            return dict(zip(names, row))
    except Exception:
        pass
    base = _base(symbol)
    return {
        "apex_symbol": symbol, "gate_symbol": f"{base}_USDT",
        "binance_symbol": _SCALED.get(symbol, symbol), "bybit_symbol": _SCALED.get(symbol, symbol),
        "hyperliquid_symbol": base, "gate_supported": 0, "binance_supported": 0,
        "bybit_supported": 0, "hyperliquid_supported": 0,
        "gate_status": "unverified", "binance_status": "unverified",
        "bybit_status": "unverified", "hyperliquid_status": "unverified",
        "chain": "unknown", "contract_address": None,
    }


async def refresh_pair_registry(symbols: list[str], force: bool = False) -> dict[str, dict[str, Any]]:
    """Verify APEX pairs against current provider instrument listings."""
    global _snapshot, _checked_at, _refresh_ttl
    normalized = [symbol.upper().replace("/", "") for symbol in symbols]
    if _snapshot and all(symbol in _snapshot for symbol in normalized) and not force and time.time() - _checked_at < _refresh_ttl:
        return {symbol: dict(_snapshot[symbol]) for symbol in normalized}
    async with _lock:
        if _snapshot and all(symbol in _snapshot for symbol in normalized) and not force and time.time() - _checked_at < _refresh_ttl:
            return {symbol: dict(_snapshot[symbol]) for symbol in normalized}

        previous_rows = {symbol: get_pair(symbol) for symbol in normalized}

        async def safe(awaitable: Any, fallback: Any) -> tuple[bool, Any]:
            try:
                return True, await awaitable
            except Exception:
                return False, fallback

        gate_result, binance_result, bybit_result, hyper_result = await asyncio.gather(
            safe(http_client.get_json("https://api.gateio.ws/api/v4/futures/usdt/contracts"), []),
            safe(http_client.get_json("https://fapi.binance.com/fapi/v1/exchangeInfo"), {}),
            safe(http_client.get_json("https://api.bybit.com/v5/market/instruments-info", {"category": "linear", "limit": 1000}), {}),
            safe(http_client.post_json("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"}), []),
        )
        gate_ok, gate = gate_result
        binance_ok, binance = binance_result
        bybit_ok, bybit = bybit_result
        hyper_ok, hyper = hyper_result
        gate_map = {
            str(row.get("name")): row for row in gate
            if isinstance(row, dict) and row.get("name") and not row.get("in_delisting")
        }
        binance_set = {
            str(row.get("symbol")) for row in (binance.get("symbols", []) if isinstance(binance, dict) else [])
            if isinstance(row, dict) and row.get("contractType") == "PERPETUAL" and row.get("status") in {None, "TRADING"}
        }
        bybit_set = {
            str(row.get("symbol")) for row in (bybit.get("result", {}).get("list", []) if isinstance(bybit, dict) else [])
            if isinstance(row, dict) and row.get("contractType") == "LinearPerpetual" and row.get("status") in {None, "Trading"}
        }
        universe = hyper[0].get("universe", []) if isinstance(hyper, list) and hyper and isinstance(hyper[0], dict) else []
        hyper_set = {str(row.get("name")) for row in universe if isinstance(row, dict)}
        contracts, now = _verified_contracts(), int(time.time())
        snapshot = dict(_snapshot)
        for symbol in normalized:
            base = _base(symbol)
            previous = previous_rows[symbol]
            gate_candidates = [f"{base}_USDT"]
            if symbol in _SCALED:
                gate_candidates.insert(0, f"1000{base}_USDT")
            gate_symbol = next(
                (name for name in gate_candidates if name in gate_map),
                str(previous.get("gate_symbol") or gate_candidates[-1]) if not gate_ok else gate_candidates[-1],
            )
            derivative_candidates = list(dict.fromkeys((_SCALED.get(symbol, symbol), symbol)))
            binance_symbol = next(
                (name for name in derivative_candidates if name in binance_set),
                str(previous.get("binance_symbol") or derivative_candidates[0]) if not binance_ok else derivative_candidates[0],
            )
            bybit_symbol = next(
                (name for name in derivative_candidates if name in bybit_set),
                str(previous.get("bybit_symbol") or derivative_candidates[0]) if not bybit_ok else derivative_candidates[0],
            )
            verified = contracts.get(symbol, {}) if isinstance(contracts.get(symbol, {}), dict) else {}
            gate_row = gate_map.get(gate_symbol, {})
            snapshot[symbol] = {
                "apex_symbol": symbol, "gate_symbol": gate_symbol,
                "binance_symbol": binance_symbol, "bybit_symbol": bybit_symbol,
                "hyperliquid_symbol": str(previous.get("hyperliquid_symbol") or base) if not hyper_ok else base,
                "gate_supported": int(gate_symbol in gate_map) if gate_ok else int(bool(previous.get("gate_supported"))),
                "binance_supported": int(binance_symbol in binance_set) if binance_ok else int(bool(previous.get("binance_supported"))),
                "bybit_supported": int(bybit_symbol in bybit_set) if bybit_ok else int(bool(previous.get("bybit_supported"))),
                "hyperliquid_supported": int(base in hyper_set) if hyper_ok else int(bool(previous.get("hyperliquid_supported"))),
                "gate_status": "supported" if gate_symbol in gate_map else "unsupported" if gate_ok else "unavailable",
                "binance_status": "supported" if binance_symbol in binance_set else "unsupported" if binance_ok else "unavailable",
                "bybit_status": "supported" if bybit_symbol in bybit_set else "unsupported" if bybit_ok else "unavailable",
                "hyperliquid_status": "supported" if base in hyper_set else "unsupported" if hyper_ok else "unavailable",
                "gate_multiplier": number(gate_row.get("quanto_multiplier")) if gate_ok else number(previous.get("gate_multiplier")),
                "chain": str(verified.get("chain") or previous.get("chain") or "unknown"),
                "contract_address": verified.get("address") or previous.get("contract_address"), "checked_at": now,
            }
        try:
            conn = _connect()
            conn.executemany("""INSERT INTO external_pair_registry
                (apex_symbol,gate_symbol,binance_symbol,bybit_symbol,hyperliquid_symbol,
                 gate_supported,binance_supported,bybit_supported,hyperliquid_supported,
                 gate_multiplier,chain,contract_address,gate_status,binance_status,
                 bybit_status,hyperliquid_status,checked_at)
                VALUES (:apex_symbol,:gate_symbol,:binance_symbol,:bybit_symbol,:hyperliquid_symbol,
                 :gate_supported,:binance_supported,:bybit_supported,:hyperliquid_supported,
                 :gate_multiplier,:chain,:contract_address,:gate_status,:binance_status,
                 :bybit_status,:hyperliquid_status,:checked_at)
                ON CONFLICT(apex_symbol) DO UPDATE SET
                 gate_symbol=excluded.gate_symbol,binance_symbol=excluded.binance_symbol,
                 bybit_symbol=excluded.bybit_symbol,hyperliquid_symbol=excluded.hyperliquid_symbol,
                 gate_supported=excluded.gate_supported,binance_supported=excluded.binance_supported,
                 bybit_supported=excluded.bybit_supported,hyperliquid_supported=excluded.hyperliquid_supported,
                 gate_multiplier=excluded.gate_multiplier,chain=excluded.chain,
                 contract_address=excluded.contract_address,gate_status=excluded.gate_status,
                 binance_status=excluded.binance_status,bybit_status=excluded.bybit_status,
                 hyperliquid_status=excluded.hyperliquid_status,checked_at=excluded.checked_at""", snapshot.values())
            conn.commit(); conn.close()
        except Exception:
            pass
        _snapshot, _checked_at = snapshot, time.time()
        _refresh_ttl = 3600 if all((gate_ok, binance_ok, bybit_ok, hyper_ok)) else 300
        return {symbol: dict(snapshot[symbol]) for symbol in normalized}


def coverage_summary(symbols: list[str]) -> dict[str, Any]:
    rows = [get_pair(symbol) for symbol in symbols]
    result: dict[str, Any] = {"total": len(rows)}
    result.update({provider: sum(bool(row.get(f"{provider}_supported")) for row in rows) for provider in ("gate", "binance", "bybit", "hyperliquid")})
    result["unavailable"] = {provider: sum(row.get(f"{provider}_status") == "unavailable" for row in rows) for provider in ("gate", "binance", "bybit", "hyperliquid")}
    return result
