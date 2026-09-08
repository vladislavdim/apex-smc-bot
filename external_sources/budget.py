"""Persistent, shared external-context admission control (never execution).

Limits below are APEX allocations, not claims about provider subscriptions.
Every HTTP attempt including retries spends units before dispatch. A rolling
hour allocation prevents scans from exhausting the whole day early.
"""
from __future__ import annotations

import math
import os
import sqlite3
import time
from dataclasses import dataclass, asdict
from contextlib import closing
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Policy:
    minute: int
    hour: int
    day: int
    mode: str = "context"


POLICIES = {
    "gate": Policy(480, 12000, 288000),
    "coinalyze": Policy(24, 1200, 28800, "shadow"),
    "hyperliquid": Policy(60, 1200, 28800, "shadow"),
    "coinmetrics": Policy(20, 400, 9600),
    "defillama": Policy(12, 60, 1440),
    "deribit": Policy(12, 120, 2880),
    "mempool": Policy(6, 60, 1440),
    "dexscreener": Policy(30, 1000, 24000),
    "deepbluealpha": Policy(6, 20, 480),
    "news": Policy(12, 120, 2880),
    "bls": Policy(2, 12, 288),
    "custom": Policy(6, 120, 2880),
}
HOSTS = {
    "api.gateio.ws": "gate", "fx-api.gateio.ws": "gate", "fx-ws.gateio.ws": "gate",
    "stream.gateio.ws": "gate",
    "api.coinalyze.net": "coinalyze", "api.hyperliquid.xyz": "hyperliquid",
    "community-api.coinmetrics.io": "coinmetrics", "api.coinmetrics.io": "coinmetrics",
    "api.llama.fi": "defillama", "stablecoins.llama.fi": "defillama",
    "www.deribit.com": "deribit", "deribit.com": "deribit",
    "mempool.space": "mempool", "api.dexscreener.com": "dexscreener",
    "deepbluealpha.io": "deepbluealpha",
    "cointelegraph.com": "news", "www.cointelegraph.com": "news",
    "coindesk.com": "news", "www.coindesk.com": "news",
    "decrypt.co": "news", "www.decrypt.co": "news",
    "nfs.faireconomy.media": "news", "api.bls.gov": "bls",
}


class BudgetDenied(RuntimeError):
    pass


def request_scope(url, params=None, payload=None):
    host = (urlsplit(url).hostname or "").lower()
    if host == "binance.com" or host.endswith(".binance.com"):
        raise BudgetDenied("binance_execution_only")
    source = HOSTS.get(host, "custom")
    units = 1
    if source == "coinalyze":
        # A batch is ONE HTTP request but EACH symbol spends one provider unit.
        units = max(1, len([s for s in str((params or {}).get("symbols", "")).split(",") if s]))
    elif source == "hyperliquid":
        units = 20  # conservative documented default info endpoint weight
    return source, units


class SourceBudget:
    def __init__(self, db_path=None, clock=time.time, policies=None):
        self.db_path = db_path
        self.clock = clock
        self.policies = policies or POLICIES

    def _connect(self):
        path = self.db_path or os.getenv("APEX_DB_PATH") or os.getenv("APEX_BRAIN_DB_PATH") or os.path.join(os.path.dirname(os.path.dirname(__file__)), "brain.db")
        conn = sqlite3.connect(path, timeout=2)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE IF NOT EXISTS external_api_usage (source TEXT, slot INTEGER, units INTEGER NOT NULL, requests INTEGER NOT NULL, PRIMARY KEY(source,slot))")
        conn.execute("CREATE TABLE IF NOT EXISTS external_api_health (source TEXT PRIMARY KEY, failures INTEGER NOT NULL DEFAULT 0, blocked_until REAL NOT NULL DEFAULT 0, rate_limits INTEGER NOT NULL DEFAULT 0, denied INTEGER NOT NULL DEFAULT 0)")
        conn.commit()
        return conn

    def reserve(self, source, units=1):
        if source not in self.policies or not isinstance(units, int) or units < 1:
            raise BudgetDenied("invalid_budget_scope")
        now = self.clock()
        slot = int(now)
        policy = self.policies[source]
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT OR IGNORE INTO external_api_health(source) VALUES(?)", (source,))
            health = conn.execute("SELECT * FROM external_api_health WHERE source=?", (source,)).fetchone()
            reason = "circuit_open" if health["blocked_until"] > now else None
            for seconds, cap, label in ((60, policy.minute, "minute"), (3600, policy.hour, "hour"), (86400, policy.day, "day")):
                used = conn.execute("SELECT COALESCE(SUM(units),0) FROM external_api_usage WHERE source=? AND slot>?", (source, slot-seconds)).fetchone()[0]
                if used + units > cap:
                    reason = reason or f"{label}_budget"
            if reason:
                conn.execute("UPDATE external_api_health SET denied=denied+1 WHERE source=?", (source,))
            else:
                conn.execute("DELETE FROM external_api_usage WHERE slot<=?", (slot-86400,))
                conn.execute("INSERT INTO external_api_usage VALUES(?,?,?,1) ON CONFLICT(source,slot) DO UPDATE SET units=units+excluded.units,requests=requests+1", (source, slot, units))
        if reason:
            raise BudgetDenied(f"{source}:{reason}")

    def outcome(self, source, *, failed=False, rate_limited=False, retry_after=0):
        now = self.clock()
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT OR IGNORE INTO external_api_health(source) VALUES(?)", (source,))
            row = conn.execute("SELECT * FROM external_api_health WHERE source=?", (source,)).fetchone()
            failures = row["failures"] + 1 if failed else 0
            delay = max(60, retry_after) if rate_limited else (min(3600, 30 * 2 ** min(failures-3, 7)) if failures >= 3 else 0)
            # A concurrent successful request must not clear a provider ban.
            blocked = max(row["blocked_until"], now + delay if delay else 0)
            conn.execute("UPDATE external_api_health SET failures=?,blocked_until=?,rate_limits=rate_limits+? WHERE source=?", (failures, blocked, int(rate_limited), source))

    def snapshot(self):
        now = int(self.clock())
        with closing(self._connect()) as conn, conn:
            rows = []
            for source, policy in self.policies.items():
                health = conn.execute("SELECT * FROM external_api_health WHERE source=?", (source,)).fetchone()
                usage = {}
                for label, seconds in (("minute", 60), ("hour", 3600), ("day", 86400)):
                    usage[label] = conn.execute("SELECT COALESCE(SUM(units),0) FROM external_api_usage WHERE source=? AND slot>?", (source, now-seconds)).fetchone()[0]
                rows.append({"source": source, "allocation": asdict(policy), "used": usage, "remaining_day": max(0, policy.day-usage["day"]), "health": dict(health) if health else {}, "limits_origin": "APEX allocation; verify provider/account/shared-IP limits"})
            return rows


def projected_load(symbols, endpoints, interval_seconds, units_per_symbol=1, retry_reserve=0.2):
    if min(symbols, endpoints, interval_seconds, units_per_symbol) <= 0 or not 0 <= retry_reserve <= 1:
        raise ValueError("invalid load inputs")
    cycles = math.ceil(86400 / interval_seconds)
    base = symbols * endpoints * units_per_symbol * cycles
    return {"base_units_day": base, "with_retry_reserve": math.ceil(base*(1+retry_reserve)), "cycles_day": cycles}


def plan_daily_load(
    plans, *, policies=None,
):
    """Estimate a whole-day request plan before enabling an adapter.

    ``plans`` maps a budget key to ``symbols``, ``endpoints``,
    ``interval_seconds`` and optional ``units_per_symbol``/``retry_reserve``.
    The result is diagnostic only; it never raises and never reserves traffic.
    This lets a release prove that optional shadow/context polling fits the
    local minute/hour/day envelopes and leaves retries visible.
    """
    active = policies or POLICIES
    output = {}
    for source, raw in (plans or {}).items():
        key = str(source).lower()
        policy = active.get(key)
        if policy is None:
            output[key] = {"status": "UNKNOWN_POLICY"}
            continue
        try:
            load = projected_load(
                int(raw.get("symbols", 0)), int(raw.get("endpoints", 0)),
                int(raw.get("interval_seconds", 0)), int(raw.get("units_per_symbol", 1)),
                float(raw.get("retry_reserve", 0.2)),
            )
        except (AttributeError, TypeError, ValueError):
            output[key] = {"status": "INVALID_PLAN"}
            continue
        output[key] = {
            **load, "allocation_day": policy.day, "allocation_hour": policy.hour,
            "allocation_minute": policy.minute,
            "within_day": load["with_retry_reserve"] <= policy.day,
            "notes": "local allocation; provider/shared-IP limits still apply",
        }
        batch = int(raw["symbols"]) * int(raw["endpoints"]) * int(raw.get("units_per_symbol", 1))
        interval = int(raw["interval_seconds"])
        reserve = 1 + float(raw.get("retry_reserve", 0.2))
        for window, seconds, cap in (("minute", 60, policy.minute), ("hour", 3600, policy.hour)):
            peak = math.ceil(batch * math.ceil(seconds / interval) * reserve)
            output[key]["peak_units_" + window] = peak
            output[key]["within_" + window] = peak <= cap
        output[key]["status"] = "WITHIN_LOCAL_LIMITS" if all(output[key]["within_" + w] for w in ("minute", "hour", "day")) else "EXCEEDS_LOCAL_LIMITS"
    return output


budget = SourceBudget()
