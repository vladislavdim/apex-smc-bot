"""Optional Binance USD-M execution for approved APEX candidates.

The module is deliberately isolated from strategy calculations.  It consumes
an immutable, already reviewed candidate and either records a paper order or
submits an exchange-compatible limit entry.  Live execution is disabled by
default and requires two independent environment switches.
"""

from __future__ import annotations

import hashlib
import hmac
import calendar
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Mapping
from urllib.parse import urlencode

try:
    import requests
except ImportError:  # pure sizing/paper tests do not need an HTTP package
    requests = None

_RequestException = getattr(requests, "RequestException", OSError) if requests is not None else OSError

try:
    from .signal_integrity import validate_candidate
except ImportError:  # direct core/ import compatibility
    from signal_integrity import validate_candidate


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")
LIVE_CONFIRMATION = "ENABLE_LIVE_BINANCE_FUTURES"
_TRUTHY = {"1", "true", "yes", "on"}
_ACCOUNT_STATUS_TTL_SECONDS = 30.0
_account_status_lock = threading.Lock()
_account_status_cache: tuple[float, dict[str, Any]] | None = None
_reconcile_process_lock = threading.Lock()


def _env_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in _TRUTHY


def _bounded_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(high, parsed))


def _bounded_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(high, parsed))


@dataclass(frozen=True)
class ExecutionConfig:
    enabled: bool = False
    mode: str = "paper"
    leverage: int = 5
    risk_pct: float = 0.5
    paper_balance_usdt: float = 1000.0
    fee_bps: float = 10.0
    tp1_fraction: float = 0.5
    api_key: str = ""
    api_secret: str = ""
    base_url: str = "https://fapi.binance.com"
    live_confirmation: str = ""
    timeout_seconds: float = 8.0
    retries: int = 3
    kill_switch: bool = False
    max_open_positions: int = 3
    max_daily_loss_pct: float = 2.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ExecutionConfig":
        source = os.environ if env is None else env
        mode = str(source.get("AUTO_TRADING_MODE", "paper")).strip().lower()
        if mode not in {"paper", "live"}:
            mode = "paper"
        try:
            leverage = int(source.get("AUTO_TRADING_LEVERAGE", "5"))
        except (TypeError, ValueError):
            leverage = 5
        try:
            retries = int(source.get("AUTO_TRADING_RETRIES", "3"))
        except (TypeError, ValueError):
            retries = 3
        return cls(
            enabled=_env_bool(source.get("AUTO_TRADING_ENABLED"), False),
            mode=mode,
            leverage=max(1, min(5, leverage)),
            risk_pct=_bounded_float(source.get("AUTO_TRADING_RISK_PCT"), 0.5, 0.05, 1.0),
            paper_balance_usdt=_bounded_float(
                source.get("AUTO_TRADING_PAPER_BALANCE_USDT"), 1000.0, 0.0, 1_000_000_000.0,
            ),
            fee_bps=_bounded_float(source.get("AUTO_TRADING_FEE_BPS"), 10.0, 0.0, 100.0),
            tp1_fraction=_bounded_float(source.get("AUTO_TRADING_TP1_FRACTION"), 0.5, 0.1, 0.9),
            api_key=str(source.get("BINANCE_API_KEY", "")).strip(),
            api_secret=str(source.get("BINANCE_API_SECRET", "")).strip(),
            base_url=str(source.get("BINANCE_FUTURES_API_URL", "https://fapi.binance.com")).rstrip("/"),
            live_confirmation=str(source.get("AUTO_TRADING_LIVE_CONFIRM", "")).strip(),
            timeout_seconds=_bounded_float(source.get("AUTO_TRADING_TIMEOUT_SECONDS"), 8.0, 3.0, 10.0),
            retries=max(1, min(4, retries)),
            kill_switch=_env_bool(source.get("AUTO_TRADING_KILL_SWITCH"), False),
            max_open_positions=_bounded_int(source.get("AUTO_TRADING_MAX_OPEN_POSITIONS"), 3, 1, 10),
            max_daily_loss_pct=_bounded_float(
                source.get("AUTO_TRADING_MAX_DAILY_LOSS_PCT"), 2.0, 0.25, 10.0,
            ),
        )

    @property
    def live_armed(self) -> bool:
        return (
            self.enabled
            and self.mode == "live"
            and self.live_confirmation == LIVE_CONFIRMATION
            and bool(self.api_key and self.api_secret)
        )


@dataclass(frozen=True)
class SymbolRules:
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc


def _floor_step(value: Any, step: Decimal) -> Decimal:
    number = _decimal(value)
    if step <= 0:
        return number
    units = (number / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def _nearest_step(value: Any, step: Decimal) -> Decimal:
    number = _decimal(value)
    if step <= 0:
        return number
    units = (number / step).to_integral_value(rounding=ROUND_HALF_UP)
    return units * step


def _ceil_step(value: Any, step: Decimal) -> Decimal:
    number = _decimal(value)
    if step <= 0:
        return number
    units = (number / step).to_integral_value(rounding=ROUND_CEILING)
    return units * step


def _plain_decimal(value: Decimal) -> str:
    rendered = format(value.normalize(), "f")
    return rendered if "." not in rendered else rendered.rstrip("0").rstrip(".")


def build_order_plan(
    candidate: dict[str, Any],
    available_balance: float,
    config: ExecutionConfig,
    rules: SymbolRules,
) -> dict[str, Any]:
    """Size a position by stop risk, never by an AI-supplied quantity."""
    integrity = validate_candidate(candidate)
    if not integrity.get("valid"):
        return {"ok": False, "status": "INVALID_CANDIDATE", "errors": integrity.get("errors", [])}
    if available_balance <= 0:
        return {"ok": False, "status": "SKIPPED_NO_BALANCE", "errors": []}

    direction = str(candidate["direction"]).upper()
    entry = _nearest_step(candidate["entry"], rules.tick_size)
    if direction == "BULLISH":
        sl = _floor_step(candidate["sl"], rules.tick_size)
        tp1 = _floor_step(candidate.get("tp1", candidate.get("tp")), rules.tick_size)
        tp2 = _floor_step(candidate.get("tp2") or tp1, rules.tick_size)
        levels_valid = sl < entry < tp1 <= tp2
    else:
        sl = _ceil_step(candidate["sl"], rules.tick_size)
        tp1 = _ceil_step(candidate.get("tp1", candidate.get("tp")), rules.tick_size)
        tp2 = _ceil_step(candidate.get("tp2") or tp1, rules.tick_size)
        levels_valid = sl > entry > tp1 >= tp2
    if not levels_valid:
        return {"ok": False, "status": "INVALID_EXCHANGE_PRECISION", "errors": []}

    stop_distance = float(abs(entry - sl))
    fee_distance = float(entry) * config.fee_bps / 10_000.0
    risk_per_unit = stop_distance + fee_distance
    if risk_per_unit <= 0:
        return {"ok": False, "status": "INVALID_STOP_DISTANCE", "errors": []}

    risk_budget = available_balance * config.risk_pct / 100.0
    risk_qty = risk_budget / risk_per_unit
    margin_qty = available_balance * config.leverage / float(entry)
    quantity = _floor_step(min(risk_qty, margin_qty), rules.step_size)
    notional = quantity * entry
    if quantity <= 0 or quantity < rules.min_qty or notional < rules.min_notional:
        return {
            "ok": False,
            "status": "SKIPPED_BELOW_MIN_NOTIONAL",
            "errors": [],
            "risk_budget": risk_budget,
        }

    return {
        "ok": True,
        "status": "READY",
        "symbol": str(candidate["symbol"]).upper(),
        "direction": str(candidate["direction"]).upper(),
        "entry": _plain_decimal(entry),
        "sl": _plain_decimal(sl),
        "tp1": _plain_decimal(tp1),
        "tp2": _plain_decimal(tp2),
        "quantity": _plain_decimal(quantity),
        "risk_budget": round(risk_budget, 8),
        "available_balance": round(available_balance, 8),
        "leverage": config.leverage,
        "step_size": _plain_decimal(rules.step_size),
        "min_qty": _plain_decimal(rules.min_qty),
    }


class BinanceAPIError(RuntimeError):
    """Structured Binance error without ever including credentials/signatures."""

    def __init__(self, status_code: int, code: int | None, message: str):
        self.status_code = int(status_code)
        self.code = code
        self.message = str(message or "unknown Binance error")[:500]
        super().__init__(f"Binance HTTP {self.status_code} code={self.code}: {self.message}")

    @property
    def retryable(self) -> bool:
        return self.status_code == 429 or self.status_code >= 500


class BinanceFuturesClient:
    """Signed USD-M client with bounded retries and idempotent order recovery."""

    def __init__(self, config: ExecutionConfig, session: Any = None):
        self.config = config
        if session is None and requests is None:
            raise RuntimeError("requests is required for live Binance execution")
        self.session = session or requests.Session()
        self._rules_cache: dict[str, tuple[float, SymbolRules]] = {}
        self._time_offset_ms = 0

    def _sync_server_time(self) -> None:
        response = self.session.request(
            "GET", f"{self.config.base_url}/fapi/v1/time",
            headers={"User-Agent": "APEX-SMC-Bot/1.0"},
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        self._time_offset_ms = int(data["serverTime"]) - int(time.time() * 1000)

    def _request(
        self, method: str, path: str, params: dict[str, Any] | None = None,
        *, signed: bool = False, attempts: int | None = None,
    ):
        base_payload = dict(params or {})
        headers = {"User-Agent": "APEX-SMC-Bot/1.0"}
        if signed:
            headers["X-MBX-APIKEY"] = self.config.api_key

        max_attempts = max(1, attempts if attempts is not None else self.config.retries)
        last_error: Exception | None = None
        for attempt in range(max_attempts):
            try:
                payload = dict(base_payload)
                if signed:
                    payload["timestamp"] = int(time.time() * 1000) + self._time_offset_ms
                    payload["recvWindow"] = 5000
                    query = urlencode(payload)
                    payload["signature"] = hmac.new(
                        self.config.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256,
                    ).hexdigest()
                response = self.session.request(
                    method, f"{self.config.base_url}{path}", params=payload,
                    headers=headers, timeout=self.config.timeout_seconds,
                )
                if response.status_code >= 400:
                    try:
                        details = response.json()
                    except (TypeError, ValueError):
                        details = {}
                    raw_code = details.get("code") if isinstance(details, dict) else None
                    try:
                        error_code = int(raw_code) if raw_code is not None else None
                    except (TypeError, ValueError):
                        error_code = None
                    message = details.get("msg") if isinstance(details, dict) else None
                    error = BinanceAPIError(
                        response.status_code, error_code,
                        message or getattr(response, "text", "request rejected"),
                    )
                    last_error = error
                    if error_code == -1021 and signed and attempt < max_attempts - 1:
                        self._sync_server_time()
                        continue
                    if error.retryable and attempt < max_attempts - 1:
                        retry_after = min(float(response.headers.get("Retry-After", "1") or 1), 5.0)
                        time.sleep(max(0.25, retry_after))
                        continue
                    raise error
                return response.json()
            except BinanceAPIError:
                raise
            except (_RequestException, ValueError, KeyError) as exc:
                last_error = exc
                if attempt < max_attempts - 1:
                    time.sleep(min(0.5 * (2 ** attempt), 2.0))
        raise RuntimeError(f"Binance Futures request failed safely: {last_error}")

    def _submit_standard_order(self, params: dict[str, Any]):
        """Recover an ambiguous POST by its unique client order id."""
        symbol = str(params["symbol"])
        client_id = str(params["newClientOrderId"])
        last_error: Exception | None = None
        for attempt in range(self.config.retries):
            try:
                return self._request(
                    "POST", "/fapi/v1/order", params, signed=True, attempts=1,
                )
            except Exception as exc:
                last_error = exc
                try:
                    recovered = self.query_order_by_client_id(symbol, client_id)
                    if recovered and recovered.get("orderId"):
                        return recovered
                except Exception:
                    pass
                if isinstance(exc, BinanceAPIError) and not exc.retryable:
                    raise
                if attempt < self.config.retries - 1:
                    time.sleep(min(0.5 * (2 ** attempt), 2.0))
        raise RuntimeError(f"ambiguous Binance order submission: {last_error}")

    def _submit_algo_order(self, params: dict[str, Any]):
        """Submit current Binance conditional order API idempotently."""
        client_id = str(params["clientAlgoId"])
        last_error: Exception | None = None
        for attempt in range(self.config.retries):
            try:
                return self._request(
                    "POST", "/fapi/v1/algoOrder", params, signed=True, attempts=1,
                )
            except Exception as exc:
                last_error = exc
                try:
                    recovered = self.query_algo_order(client_algo_id=client_id)
                    if recovered and recovered.get("algoId"):
                        return recovered
                except Exception:
                    pass
                if isinstance(exc, BinanceAPIError) and not exc.retryable:
                    raise
                if attempt < self.config.retries - 1:
                    time.sleep(min(0.5 * (2 ** attempt), 2.0))
        raise RuntimeError(f"ambiguous Binance algo submission: {last_error}")

    def symbol_rules(self, symbol: str) -> SymbolRules:
        cached = self._rules_cache.get(symbol)
        if cached and time.time() - cached[0] < 3600:
            return cached[1]
        data = self._request("GET", "/fapi/v1/exchangeInfo")
        item = next((row for row in data.get("symbols", []) if row.get("symbol") == symbol), None)
        if not item or item.get("status") != "TRADING":
            raise ValueError(f"symbol is not tradable on Binance Futures: {symbol}")
        filters = {row.get("filterType"): row for row in item.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE") or filters.get("MARKET_LOT_SIZE", {})
        notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL", {})
        rules = SymbolRules(
            tick_size=_decimal(price_filter.get("tickSize", "0")),
            step_size=_decimal(lot_filter.get("stepSize", "0")),
            min_qty=_decimal(lot_filter.get("minQty", "0")),
            min_notional=_decimal(notional_filter.get("notional", notional_filter.get("minNotional", "0"))),
        )
        self._rules_cache[symbol] = (time.time(), rules)
        return rules

    def usdt_balance_details(self) -> dict[str, float]:
        """Return actual USD-M wallet and available balances from Binance."""
        balances = self._request("GET", "/fapi/v3/balance", signed=True)
        usdt = next((row for row in balances if row.get("asset") == "USDT"), None)
        if not usdt:
            return {
                "wallet_balance": 0.0,
                "available_balance": 0.0,
                "cross_unrealized_pnl": 0.0,
            }
        return {
            "wallet_balance": float(usdt.get("balance", 0) or 0),
            "available_balance": float(usdt.get("availableBalance", 0) or 0),
            "cross_unrealized_pnl": float(usdt.get("crossUnPnl", 0) or 0),
        }

    def available_usdt(self) -> float:
        return self.usdt_balance_details()["available_balance"]

    def income_history(self, limit: int = 100, start_time: int | None = None) -> list[dict[str, Any]]:
        """Return recent USD-M income records (Binance defaults to seven days)."""
        params = {"limit": max(1, min(int(limit), 1000))}
        if start_time is not None:
            params["startTime"] = int(start_time)
        result = self._request("GET", "/fapi/v1/income", params, signed=True)
        return result if isinstance(result, list) else []

    def open_positions(self) -> list[dict[str, Any]]:
        rows = self._request("GET", "/fapi/v3/positionRisk", signed=True)
        if isinstance(rows, dict):
            rows = [rows]
        return [row for row in rows if abs(float(row.get("positionAmt", 0) or 0)) > 0]

    def realized_pnl_since(self, start_time_ms: int) -> float:
        rows = self.income_history(limit=1000, start_time=start_time_ms)
        return sum(
            float(row.get("income", 0) or 0)
            for row in rows
            if row.get("incomeType") == "REALIZED_PNL" and row.get("asset", "USDT") == "USDT"
        )

    def mark_price(self, symbol: str) -> float:
        result = self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        price = float(result.get("markPrice", 0) or 0)
        if price <= 0:
            raise RuntimeError(f"Binance returned no mark price for {symbol}")
        return price

    def is_one_way_mode(self) -> bool:
        result = self._request("GET", "/fapi/v1/positionSide/dual", signed=True)
        return not bool(result.get("dualSidePosition"))

    def has_open_position(self, symbol: str) -> bool:
        rows = self._request("GET", "/fapi/v3/positionRisk", {"symbol": symbol}, signed=True)
        if isinstance(rows, dict):
            rows = [rows]
        return any(abs(float(row.get("positionAmt", 0) or 0)) > 0 for row in rows)

    def has_open_orders(self, symbol: str) -> bool:
        regular = self._request("GET", "/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
        algo = self._request(
            "GET", "/fapi/v1/openAlgoOrders",
            {"symbol": symbol, "algoType": "CONDITIONAL"}, signed=True,
        )
        return bool(regular or algo)

    def set_isolated_margin(self, symbol: str):
        try:
            return self._request(
                "POST", "/fapi/v1/marginType",
                {"symbol": symbol, "marginType": "ISOLATED"}, signed=True,
            )
        except BinanceAPIError as exc:
            # -4046 means the symbol is already in the requested margin mode.
            if exc.code == -4046:
                return {"code": -4046, "msg": "already isolated"}
            raise

    def set_leverage(self, symbol: str, leverage: int):
        return self._request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage}, signed=True)

    def place_limit_entry(self, plan: dict[str, Any], client_id: str):
        side = "BUY" if plan["direction"] == "BULLISH" else "SELL"
        return self._submit_standard_order({
            "symbol": plan["symbol"], "side": side, "positionSide": "BOTH",
            "type": "LIMIT", "timeInForce": "GTC", "quantity": plan["quantity"],
            "price": plan["entry"], "newClientOrderId": client_id,
        })

    def query_order(self, symbol: str, order_id: str):
        return self._request("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)

    def query_order_by_client_id(self, symbol: str, client_id: str):
        return self._request(
            "GET", "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_id}, signed=True,
        )

    def cancel_order(self, symbol: str, order_id: str):
        return self._request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)

    def place_close_all_trigger(self, symbol: str, side: str, order_type: str, stop_price: str, client_id: str):
        return self._submit_algo_order({
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side,
            "positionSide": "BOTH", "type": order_type,
            "triggerPrice": stop_price, "closePosition": "true", "workingType": "MARK_PRICE",
            "priceProtect": "false", "clientAlgoId": client_id,
        })

    def place_reduce_trigger(
        self, symbol: str, side: str, quantity: str, order_type: str,
        stop_price: str, client_id: str,
    ):
        return self._submit_algo_order({
            "algoType": "CONDITIONAL", "symbol": symbol, "side": side,
            "positionSide": "BOTH", "type": order_type,
            "quantity": quantity, "triggerPrice": stop_price, "reduceOnly": "true",
            "workingType": "MARK_PRICE", "priceProtect": "false", "clientAlgoId": client_id,
        })

    def query_algo_order(self, algo_id: str = "", client_algo_id: str = ""):
        params = {"algoId": algo_id} if algo_id else {"clientAlgoId": client_algo_id}
        return self._request("GET", "/fapi/v1/algoOrder", params, signed=True)

    def cancel_algo_order(self, algo_id: str):
        return self._request(
            "DELETE", "/fapi/v1/algoOrder", {"algoId": algo_id}, signed=True,
        )

    def emergency_close(self, symbol: str, direction: str, quantity: str, client_id: str):
        side = "SELL" if direction == "BULLISH" else "BUY"
        return self._submit_standard_order({
            "symbol": symbol, "side": side, "positionSide": "BOTH", "type": "MARKET",
            "quantity": quantity, "reduceOnly": "true", "newClientOrderId": client_id,
        })


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_execution_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS trade_executions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id INTEGER UNIQUE,
        mode TEXT NOT NULL,
        exchange TEXT NOT NULL DEFAULT 'binance_futures',
        symbol TEXT NOT NULL,
        direction TEXT NOT NULL,
        status TEXT NOT NULL,
        entry REAL,
        sl REAL,
        tp1 REAL,
        tp2 REAL,
        quantity REAL,
        risk_usdt REAL,
        balance_usdt REAL,
        leverage INTEGER,
        entry_order_id TEXT,
        stop_order_id TEXT,
        tp1_order_id TEXT,
        tp2_order_id TEXT,
        last_error TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()


def _store_execution(
    db_path: str, signal_id: int, config: ExecutionConfig, candidate: dict[str, Any],
    status: str, plan: dict[str, Any] | None = None, error: str = "", entry_order_id: str = "",
) -> dict[str, Any]:
    ensure_execution_schema(db_path)
    plan = plan or {}
    conn = _connect(db_path)
    conn.execute(
        """INSERT INTO trade_executions
           (signal_id, mode, symbol, direction, status, entry, sl, tp1, tp2,
            quantity, risk_usdt, balance_usdt, leverage, entry_order_id, last_error)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(signal_id) DO UPDATE SET
             status=excluded.status, entry_order_id=COALESCE(NULLIF(excluded.entry_order_id,''), trade_executions.entry_order_id),
             last_error=excluded.last_error, updated_at=CURRENT_TIMESTAMP""",
        (
            signal_id, config.mode, str(candidate.get("symbol", "")), str(candidate.get("direction", "")), status,
            float(plan.get("entry", candidate.get("entry", 0)) or 0),
            float(plan.get("sl", candidate.get("sl", 0)) or 0),
            float(plan.get("tp1", candidate.get("tp1", candidate.get("tp", 0))) or 0),
            float(plan.get("tp2", candidate.get("tp2") or candidate.get("tp1", candidate.get("tp", 0))) or 0),
            float(plan.get("quantity", 0) or 0), float(plan.get("risk_budget", 0) or 0),
            float(plan.get("available_balance", 0) or 0), int(plan.get("leverage", config.leverage) or config.leverage),
            str(entry_order_id or ""), str(error or "")[:1000],
        ),
    )
    conn.commit()
    conn.close()
    return {"status": status, "signal_id": signal_id, "error": error, "plan": plan}


def execute_approved_candidate(
    candidate: dict[str, Any], signal_id: int, *, db_path: str = DB_PATH,
    config: ExecutionConfig | None = None, client: BinanceFuturesClient | None = None,
) -> dict[str, Any]:
    """Record paper execution or submit one live limit entry; never raise."""
    config = config or ExecutionConfig.from_env()
    if not config.enabled:
        return {"status": "DISABLED", "signal_id": signal_id}

    try:
        if config.mode == "paper":
            rules = SymbolRules(Decimal("0.00000001"), Decimal("0.00000001"), Decimal("0"), Decimal("0"))
            plan = build_order_plan(candidate, config.paper_balance_usdt, config, rules)
            status = "PAPER_PENDING_ENTRY" if plan.get("ok") else plan.get("status", "PAPER_SKIPPED")
            return _store_execution(db_path, signal_id, config, candidate, status, plan)

        if not config.live_armed:
            return _store_execution(
                db_path, signal_id, config, candidate, "LIVE_NOT_ARMED", error=(
                    "live mode requires Binance trade credentials and exact AUTO_TRADING_LIVE_CONFIRM"
                ),
            )

        if config.kill_switch:
            return _store_execution(
                db_path, signal_id, config, candidate, "BLOCKED_KILL_SWITCH",
                error="AUTO_TRADING_KILL_SWITCH is enabled",
            )

        # Telegram delivery stays fail-open, but real money is fail-closed.
        review = candidate.get("_external_quality_review")
        try:
            min_groq = float(os.environ.get("AUTO_TRADING_MIN_GROQ_CONFIDENCE", os.environ.get("GROQ_MIN_APPROVAL_CONFIDENCE", "0.70")))
        except ValueError:
            min_groq = 0.70
        min_groq = max(0.0, min(1.0, min_groq)); groq_error = ""
        if not candidate.get("_external_quality_reviewed") or not isinstance(review, dict): groq_error = "missing Groq quality review"
        elif bool(review.get("degraded")): groq_error = "degraded Groq quality review"
        elif str(review.get("decision", "")).upper() != "APPROVE": groq_error = f"Groq decision is {review.get('decision', 'missing')}"
        else:
            try: confidence = float(review.get("confidence", 0))
            except (TypeError, ValueError): confidence = 0
            if confidence < min_groq: groq_error = f"Groq confidence {confidence:.2f} below {min_groq:.2f}"
        if groq_error:
            return _store_execution(db_path, signal_id, config, candidate, "BLOCKED_GROQ_GUARD", error=groq_error)

        client = client or BinanceFuturesClient(config)
        if not client.is_one_way_mode():
            return _store_execution(
                db_path, signal_id, config, candidate, "SKIPPED_HEDGE_MODE",
                error="safe live execution currently requires Binance One-way Mode",
            )
        # Exchange state is authoritative. Any failure here is caught by the
        # outer fail-closed guard and no order is submitted.
        open_positions = client.open_positions()
        if len(open_positions) >= config.max_open_positions:
            return _store_execution(
                db_path, signal_id, config, candidate, "BLOCKED_MAX_POSITIONS",
                error=f"open positions {len(open_positions)} >= limit {config.max_open_positions}",
            )
        balance_details = client.usdt_balance_details()
        wallet_balance = float(balance_details.get("wallet_balance", 0) or 0)
        now = time.gmtime()
        utc_midnight_ms = int(calendar.timegm((now.tm_year, now.tm_mon, now.tm_mday, 0, 0, 0, 0, 0, 0)) * 1000)
        daily_realized_pnl = client.realized_pnl_since(utc_midnight_ms)
        loss_limit = wallet_balance * config.max_daily_loss_pct / 100.0
        if wallet_balance > 0 and daily_realized_pnl <= -loss_limit:
            return _store_execution(
                db_path, signal_id, config, candidate, "BLOCKED_DAILY_LOSS",
                error=f"daily realized PnL {daily_realized_pnl:.2f} <= -{loss_limit:.2f} USDT",
            )
        symbol = str(candidate.get("symbol", "")).upper()
        # Groq/news review and Telegram delivery take time.  Revalidate the
        # immutable strategy levels against the exchange mark price immediately
        # before sizing/submission so a setup that already hit SL/TP cannot be
        # turned into a late live entry.
        current_price = client.mark_price(symbol)
        current_integrity = validate_candidate(candidate, current_price)
        if not current_integrity.get("valid"):
            return _store_execution(
                db_path, signal_id, config, candidate, "SKIPPED_STALE_MARKET",
                error="; ".join(current_integrity.get("errors", [])),
            )
        if client.has_open_position(symbol):
            return _store_execution(db_path, signal_id, config, candidate, "SKIPPED_EXISTING_POSITION")
        if client.has_open_orders(symbol):
            return _store_execution(db_path, signal_id, config, candidate, "SKIPPED_EXISTING_ORDERS")
        balance = client.available_usdt()
        if balance <= 0:
            return _store_execution(db_path, signal_id, config, candidate, "SKIPPED_NO_BALANCE")
        rules = client.symbol_rules(symbol)
        plan = build_order_plan(candidate, balance, config, rules)
        if not plan.get("ok"):
            return _store_execution(db_path, signal_id, config, candidate, plan.get("status", "SKIPPED"), plan)

        # Isolated margin bounds a symbol failure to its allocated margin.
        client.set_isolated_margin(symbol)
        client.set_leverage(symbol, config.leverage)
        response = client.place_limit_entry(plan, f"apex_e_{signal_id}")
        order_id = str(response.get("orderId", ""))
        if not order_id:
            raise RuntimeError("Binance did not return an entry order id")
        stored = _store_execution(
            db_path, signal_id, config, candidate, "ENTRY_PENDING", plan,
            entry_order_id=order_id,
        )
        # Protect an immediately/partially filled limit without waiting for
        # the periodic scheduler. A transient query error leaves ENTRY_PENDING
        # for the next bounded reconciliation pass.
        try:
            outcomes = reconcile_live_executions(db_path=db_path, config=config, client=client)
            current = next(
                (item for item in outcomes if int(item.get("signal_id", -1)) == int(signal_id)),
                None,
            )
            return current or stored
        except Exception as reconcile_error:
            logging.warning("[AutoTrading] immediate reconcile signal %s: %s", signal_id, reconcile_error)
            return stored
    except Exception as exc:
        logging.error("[AutoTrading] %s failed safely: %s", candidate.get("symbol"), exc)
        return _store_execution(db_path, signal_id, config, candidate, "ERROR", error=str(exc))


def _update_execution(db_path: str, execution_id: int, status: str, **fields: Any) -> None:
    allowed = {"stop_order_id", "tp1_order_id", "tp2_order_id", "last_error", "quantity"}
    updates = ["status=?", "updated_at=CURRENT_TIMESTAMP"]
    values: list[Any] = [status]
    for key, value in fields.items():
        if key in allowed:
            updates.append(f"{key}=?")
            values.append(value)
    values.append(execution_id)
    conn = _connect(db_path)
    conn.execute(f"UPDATE trade_executions SET {', '.join(updates)} WHERE id=?", values)
    conn.commit()
    conn.close()


def _remote_order_id(response: Mapping[str, Any]) -> str:
    """Return a standard orderId or the current conditional algoId."""
    return str(response.get("algoId") or response.get("orderId") or "")


def _required_remote_order_id(response: Mapping[str, Any], label: str) -> str:
    order_id = _remote_order_id(response)
    if not order_id:
        raise RuntimeError(f"Binance did not return {label} order id")
    return order_id


def _install_brackets(
    row: sqlite3.Row, client: BinanceFuturesClient, config: ExecutionConfig,
    db_path: str, filled_quantity: Decimal,
) -> dict[str, Any]:
    execution_id = int(row["id"])
    signal_id = int(row["signal_id"])
    symbol = str(row["symbol"])
    direction = str(row["direction"])
    close_side = "SELL" if direction == "BULLISH" else "BUY"
    rules = client.symbol_rules(symbol)
    quantity = _floor_step(filled_quantity, rules.step_size)
    if quantity < rules.min_qty:
        _update_execution(db_path, execution_id, "ERROR", last_error="filled quantity below exchange minimum")
        return {"status": "ERROR", "signal_id": signal_id}

    try:
        stop = client.place_close_all_trigger(
            symbol, close_side, "STOP_MARKET",
            _plain_decimal(_nearest_step(row["sl"], rules.tick_size)), f"apex_s_{signal_id}",
        )
        stop_id = _required_remote_order_id(stop, "protective stop")
    except Exception as stop_error:
        try:
            client.emergency_close(symbol, direction, _plain_decimal(quantity), f"apex_x_{signal_id}")
            status = "EMERGENCY_CLOSED"
        except Exception as close_error:
            status = "UNPROTECTED_POSITION"
            stop_error = RuntimeError(f"stop failed: {stop_error}; emergency close failed: {close_error}")
        _update_execution(db_path, execution_id, status, last_error=str(stop_error), quantity=float(quantity))
        return {"status": status, "signal_id": signal_id}

    tp1_id = tp2_id = ""
    errors = []
    tp1 = _nearest_step(row["tp1"], rules.tick_size)
    tp2 = _nearest_step(row["tp2"], rules.tick_size)
    distinct_tp2 = abs(tp2 - tp1) >= rules.tick_size
    try:
        if distinct_tp2:
            tp1_qty = _floor_step(quantity * _decimal(config.tp1_fraction), rules.step_size)
            if tp1_qty >= rules.min_qty and tp1_qty < quantity:
                first = client.place_reduce_trigger(
                    symbol, close_side, _plain_decimal(tp1_qty), "TAKE_PROFIT_MARKET",
                    _plain_decimal(tp1), f"apex_t1_{signal_id}",
                )
                tp1_id = _required_remote_order_id(first, "TP1")
                second = client.place_close_all_trigger(
                    symbol, close_side, "TAKE_PROFIT_MARKET",
                    _plain_decimal(tp2), f"apex_t2_{signal_id}",
                )
                tp2_id = _required_remote_order_id(second, "TP2")
            else:
                only = client.place_close_all_trigger(
                    symbol, close_side, "TAKE_PROFIT_MARKET",
                    _plain_decimal(tp1), f"apex_t1_{signal_id}",
                )
                tp1_id = _required_remote_order_id(only, "TP1")
        else:
            only = client.place_close_all_trigger(
                symbol, close_side, "TAKE_PROFIT_MARKET",
                _plain_decimal(tp1), f"apex_t1_{signal_id}",
            )
            tp1_id = _required_remote_order_id(only, "TP1")
    except Exception as tp_error:
        errors.append(str(tp_error))

    status = "PROTECTED" if not errors else "PROTECTED_NO_TP"
    _update_execution(
        db_path, execution_id, status, stop_order_id=stop_id, tp1_order_id=tp1_id,
        tp2_order_id=tp2_id, last_error="; ".join(errors), quantity=float(quantity),
    )
    return {"status": status, "signal_id": signal_id}


def _retry_missing_take_profits(
    row: sqlite3.Row, client: BinanceFuturesClient, config: ExecutionConfig, db_path: str,
) -> dict[str, Any]:
    """Retry only absent take orders; never duplicate the protective stop."""
    execution_id = int(row["id"])
    signal_id = int(row["signal_id"])
    if not row["stop_order_id"]:
        _update_execution(
            db_path, execution_id, "UNPROTECTED_POSITION",
            last_error="cannot retry take orders because protective stop id is missing",
        )
        return {"status": "UNPROTECTED_POSITION", "signal_id": signal_id}

    symbol = str(row["symbol"])
    direction = str(row["direction"])
    close_side = "SELL" if direction == "BULLISH" else "BUY"
    rules = client.symbol_rules(symbol)
    quantity = _floor_step(row["quantity"], rules.step_size)
    tp1 = _nearest_step(row["tp1"], rules.tick_size)
    tp2 = _nearest_step(row["tp2"], rules.tick_size)
    distinct_tp2 = abs(tp2 - tp1) >= rules.tick_size
    tp1_id = str(row["tp1_order_id"] or "")
    tp2_id = str(row["tp2_order_id"] or "")
    errors = []

    try:
        if distinct_tp2:
            tp1_qty = _floor_step(quantity * _decimal(config.tp1_fraction), rules.step_size)
            if not tp1_id and tp1_qty >= rules.min_qty and tp1_qty < quantity:
                first = client.place_reduce_trigger(
                    symbol, close_side, _plain_decimal(tp1_qty), "TAKE_PROFIT_MARKET",
                    _plain_decimal(tp1), f"apex_t1_{signal_id}",
                )
                tp1_id = _required_remote_order_id(first, "TP1")
            if not tp2_id:
                second = client.place_close_all_trigger(
                    symbol, close_side, "TAKE_PROFIT_MARKET",
                    _plain_decimal(tp2), f"apex_t2_{signal_id}",
                )
                tp2_id = _required_remote_order_id(second, "TP2")
        elif not tp1_id:
            only = client.place_close_all_trigger(
                symbol, close_side, "TAKE_PROFIT_MARKET",
                _plain_decimal(tp1), f"apex_t1_{signal_id}",
            )
            tp1_id = _required_remote_order_id(only, "TP1")
    except Exception as exc:
        errors.append(str(exc))

    complete = bool(tp2_id) if distinct_tp2 else bool(tp1_id)
    status = "PROTECTED" if complete and not errors else "PROTECTED_NO_TP"
    _update_execution(
        db_path, execution_id, status, tp1_order_id=tp1_id, tp2_order_id=tp2_id,
        last_error="; ".join(errors),
    )
    return {"status": status, "signal_id": signal_id}


def _cleanup_protective_orders(
    row: sqlite3.Row, client: BinanceFuturesClient, db_path: str,
) -> dict[str, Any]:
    """Cancel only this signal's surviving algo orders after it is closed."""
    execution_id = int(row["id"])
    signal_id = int(row["signal_id"])
    errors = []
    for field in ("stop_order_id", "tp1_order_id", "tp2_order_id"):
        algo_id = str(row[field] or "")
        if not algo_id:
            continue
        try:
            order = client.query_algo_order(algo_id=algo_id)
            status = str(order.get("algoStatus", "")).upper()
            if status in {"NEW", "PENDING"}:
                client.cancel_algo_order(algo_id)
        except BinanceAPIError as exc:
            # Already-finished/cancelled historical orders may no longer be
            # queryable; that is not an orphan-order safety failure.
            if exc.code not in {-2011, -2013}:
                errors.append(f"{field}: {exc}")
        except Exception as exc:
            errors.append(f"{field}: {exc}")

    result = "".join(
        char if char.isalnum() or char == "_" else "_"
        for char in str(row["signal_result"] or "closed").upper()
    )[:30]
    status = "CLEANUP_PENDING" if errors else f"CLOSED_{result}"
    _update_execution(
        db_path, execution_id, status, last_error="; ".join(errors),
    )
    return {"status": status, "signal_id": signal_id}


def reconcile_live_executions(
    *, db_path: str = DB_PATH, config: ExecutionConfig | None = None,
    client: BinanceFuturesClient | None = None,
) -> list[dict[str, Any]]:
    """Run at most one exchange reconciliation per process.

    Entry submission performs an immediate reconciliation while the scheduler
    also polls. A non-blocking process lock prevents both paths from creating
    duplicate protective orders. A skipped call is retried on the next tick.
    """
    if not _reconcile_process_lock.acquire(blocking=False):
        logging.debug("[AutoTrading] reconciliation already in progress; tick skipped")
        return []
    try:
        return _reconcile_live_executions_unlocked(
            db_path=db_path, config=config, client=client,
        )
    finally:
        _reconcile_process_lock.release()


def _reconcile_live_executions_unlocked(
    *, db_path: str = DB_PATH, config: ExecutionConfig | None = None,
    client: BinanceFuturesClient | None = None,
) -> list[dict[str, Any]]:
    """Protect filled live entries and cancel entries whose signal expired."""
    config = config or ExecutionConfig.from_env()
    if not config.live_armed:
        return []
    ensure_execution_schema(db_path)
    client = client or BinanceFuturesClient(config)
    conn = _connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT te.*, COALESCE(s.result, 'cancelled') AS signal_result
           FROM trade_executions te LEFT JOIN signals s ON s.id=te.signal_id
           WHERE te.mode='live' AND te.status IN
                 ('ENTRY_PENDING','PROTECTED','PROTECTED_NO_TP','CLEANUP_PENDING')"""
    ).fetchall()
    conn.close()
    outcomes = []
    for row in rows:
        try:
            signal_pending = str(row["signal_result"]) == "pending"
            if row["status"] == "CLEANUP_PENDING" or (
                row["status"] in {"PROTECTED", "PROTECTED_NO_TP"} and not signal_pending
            ):
                outcomes.append(_cleanup_protective_orders(row, client, db_path))
                continue
            if row["status"] == "PROTECTED":
                continue
            if row["status"] == "PROTECTED_NO_TP":
                outcomes.append(_retry_missing_take_profits(row, client, config, db_path))
                continue
            order = client.query_order(str(row["symbol"]), str(row["entry_order_id"]))
            order_status = str(order.get("status", "")).upper()
            executed = _decimal(order.get("executedQty", "0"))
            if str(row["signal_result"]) != "pending":
                if order_status in {"NEW", "PARTIALLY_FILLED"}:
                    client.cancel_order(str(row["symbol"]), str(row["entry_order_id"]))
                if executed > 0 or order_status == "FILLED":
                    # The signal can expire between the exchange fill and this
                    # reconciliation pass. Do not turn that race into a fresh
                    # discretionary position: reduce exactly the filled size.
                    rules = client.symbol_rules(str(row["symbol"]))
                    filled = executed or _decimal(row["quantity"])
                    close_quantity = _floor_step(filled, rules.step_size)
                    if close_quantity <= 0:
                        raise RuntimeError("expired entry filled below executable quantity")
                    client.emergency_close(
                        str(row["symbol"]), str(row["direction"]),
                        _plain_decimal(close_quantity), f"apex_x_{row['signal_id']}",
                    )
                    _update_execution(
                        db_path, int(row["id"]), "EMERGENCY_CLOSED",
                        quantity=float(close_quantity), last_error="signal expired at entry fill",
                    )
                    outcomes.append({"status": "EMERGENCY_CLOSED", "signal_id": row["signal_id"]})
                else:
                    _update_execution(db_path, int(row["id"]), "ENTRY_CANCELLED")
                    outcomes.append({"status": "ENTRY_CANCELLED", "signal_id": row["signal_id"]})
                continue
            if order_status == "PARTIALLY_FILLED" and executed > 0:
                client.cancel_order(str(row["symbol"]), str(row["entry_order_id"]))
                outcomes.append(_install_brackets(row, client, config, db_path, executed))
            elif order_status == "FILLED":
                filled = executed or _decimal(row["quantity"])
                outcomes.append(_install_brackets(row, client, config, db_path, filled))
            elif order_status in {"CANCELED", "REJECTED", "EXPIRED"}:
                _update_execution(db_path, int(row["id"]), "ENTRY_CANCELLED", last_error=order_status)
                outcomes.append({"status": "ENTRY_CANCELLED", "signal_id": row["signal_id"]})
        except Exception as exc:
            logging.error("[AutoTrading] reconcile signal %s: %s", row["signal_id"], exc)
            retry_status = (
                str(row["status"])
                if row["status"] in {"PROTECTED", "PROTECTED_NO_TP", "CLEANUP_PENDING"}
                else "ENTRY_PENDING"
            )
            _update_execution(db_path, int(row["id"]), retry_status, last_error=str(exc))
            outcomes.append({"status": "RECONCILE_ERROR", "signal_id": row["signal_id"]})
    return outcomes


def _income_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize trading P&L without treating wallet transfers as profit."""
    realized: list[dict[str, Any]] = []
    commission = 0.0
    funding = 0.0
    for row in rows:
        try:
            amount = float(row.get("income", 0) or 0)
        except (TypeError, ValueError):
            continue
        income_type = str(row.get("incomeType", "")).upper()
        asset = str(row.get("asset", "USDT")).upper()
        if asset != "USDT":
            continue
        if income_type == "REALIZED_PNL":
            realized.append({
                "symbol": str(row.get("symbol", "") or "FUTURES"),
                "amount": amount,
                "time": int(row.get("time", 0) or 0),
                "transaction_id": str(row.get("tranId", "") or ""),
            })
        elif income_type == "COMMISSION":
            commission += amount
        elif income_type == "FUNDING_FEE":
            funding += amount

    realized.sort(key=lambda item: item["time"], reverse=True)
    gross_profit = sum(item["amount"] for item in realized if item["amount"] > 0)
    gross_loss = sum(item["amount"] for item in realized if item["amount"] < 0)
    realized_total = gross_profit + gross_loss
    return {
        "available": True,
        "period": "7d",
        "gross_profit": round(gross_profit, 8),
        "gross_loss": round(gross_loss, 8),
        "realized_pnl": round(realized_total, 8),
        "commission": round(commission, 8),
        "funding": round(funding, 8),
        "net_trading_pnl": round(realized_total + commission + funding, 8),
        "positive_count": sum(1 for item in realized if item["amount"] > 0),
        "negative_count": sum(1 for item in realized if item["amount"] < 0),
        "recent": realized[:5],
    }


def _live_account_status(
    config: ExecutionConfig,
    client: BinanceFuturesClient | None = None,
) -> dict[str, Any]:
    """Fetch a bounded, cached live account summary for Telegram."""
    global _account_status_cache

    def fetch(active_client: BinanceFuturesClient) -> dict[str, Any]:
        balance = active_client.usdt_balance_details()
        try:
            pnl = _income_summary(active_client.income_history(limit=1000))
        except Exception as exc:
            logging.warning("[AutoTrading] P&L history unavailable: %s", exc)
            pnl = {"available": False, "error": str(exc)[:300], "period": "7d"}
        return {
            "available": True,
            **balance,
            "pnl": pnl,
        }

    # Explicit clients are used by tests and diagnostics and must not share a
    # process-global cache with the real configured Binance account.
    if client is not None:
        return fetch(client)

    now = time.monotonic()
    with _account_status_lock:
        if _account_status_cache and now - _account_status_cache[0] < _ACCOUNT_STATUS_TTL_SECONDS:
            return dict(_account_status_cache[1])
        try:
            value = fetch(BinanceFuturesClient(config))
        except Exception as exc:
            logging.warning("[AutoTrading] account status unavailable: %s", exc)
            value = {"available": False, "error": str(exc)[:300]}
        _account_status_cache = (time.monotonic(), value)
        return dict(value)


def execution_status(
    db_path: str = DB_PATH,
    config: ExecutionConfig | None = None,
    client: BinanceFuturesClient | None = None,
) -> dict[str, Any]:
    """Safe status summary for Telegram; never exposes credentials."""
    config = config or ExecutionConfig.from_env()
    summary = {
        "enabled": config.enabled,
        "mode": config.mode,
        "live_armed": config.live_armed,
        "leverage": config.leverage,
        "risk_pct": config.risk_pct,
        "counts": {},
        "account": {"available": False},
    }
    try:
        ensure_execution_schema(db_path)
        conn = _connect(db_path)
        summary["counts"] = dict(conn.execute(
            "SELECT status, COUNT(*) FROM trade_executions GROUP BY status"
        ).fetchall())
        conn.close()
    except Exception as exc:
        summary["error"] = str(exc)
    if config.live_armed:
        summary["account"] = _live_account_status(config, client=client)
    return summary
