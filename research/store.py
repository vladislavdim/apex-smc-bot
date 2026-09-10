"""Durable Market History and Research store.

Production uses ``APEX_MARKET_DATABASE_URL`` (or ``DATABASE_URL``) and keeps
these tables logically separate from live telemetry by their ``research_`` / 
``market_`` names. SQLite is supported for tests and local development.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Mapping, Sequence


SCHEMA_VERSION = 4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def stable_id(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(x) for x in parts).encode()).hexdigest()


DDL = [
    """CREATE TABLE IF NOT EXISTS research_meta (
        key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS market_symbols (
        source TEXT NOT NULL, symbol TEXT NOT NULL, provider_symbol TEXT NOT NULL,
        listed_at INTEGER, delisted_at INTEGER, historically_available INTEGER NOT NULL DEFAULT 1,
        metadata_json TEXT NOT NULL DEFAULT '{}', updated_at TEXT NOT NULL,
        PRIMARY KEY(source,symbol))""",
    """CREATE TABLE IF NOT EXISTS market_candles (
        source TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL,
        open_time INTEGER NOT NULL, close_time INTEGER NOT NULL,
        open DOUBLE PRECISION NOT NULL, high DOUBLE PRECISION NOT NULL,
        low DOUBLE PRECISION NOT NULL, close DOUBLE PRECISION NOT NULL,
        volume DOUBLE PRECISION NOT NULL, quote_volume DOUBLE PRECISION,
        is_closed INTEGER NOT NULL, received_at TEXT NOT NULL,
        data_quality TEXT NOT NULL DEFAULT 'VALID', payload_json TEXT NOT NULL DEFAULT '{}',
        PRIMARY KEY(source,symbol,timeframe,open_time))""",
    "CREATE INDEX IF NOT EXISTS idx_market_candles_asof ON market_candles(symbol,timeframe,open_time DESC)",
    """CREATE TABLE IF NOT EXISTS market_context_observations (
        observation_id TEXT PRIMARY KEY, source TEXT NOT NULL, symbol TEXT NOT NULL,
        feature TEXT NOT NULL, event_time INTEGER NOT NULL, received_at TEXT NOT NULL,
        quality TEXT NOT NULL, availability TEXT NOT NULL,
        point_in_time INTEGER NOT NULL DEFAULT 1, value_json TEXT NOT NULL,
        UNIQUE(source,symbol,feature,event_time))""",
    "CREATE INDEX IF NOT EXISTS idx_market_context_asof ON market_context_observations(symbol,feature,event_time DESC)",
    """CREATE TABLE IF NOT EXISTS market_quality_issues (
        issue_id TEXT PRIMARY KEY, source TEXT NOT NULL, symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL, open_time INTEGER, issue_type TEXT NOT NULL,
        severity TEXT NOT NULL, detail_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'OPEN', detected_at TEXT NOT NULL,
        resolved_at TEXT)""",
    "CREATE INDEX IF NOT EXISTS idx_market_quality_open ON market_quality_issues(status,symbol,timeframe)",
    """CREATE TABLE IF NOT EXISTS market_feature_snapshots (
        snapshot_id TEXT PRIMARY KEY, source TEXT NOT NULL, symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL, as_of INTEGER NOT NULL, feature_version TEXT NOT NULL,
        dataset_version TEXT NOT NULL, features_json TEXT NOT NULL,
        data_quality TEXT NOT NULL, created_at TEXT NOT NULL,
        UNIQUE(source,symbol,timeframe,as_of,feature_version,dataset_version))""",
    "CREATE INDEX IF NOT EXISTS idx_feature_asof ON market_feature_snapshots(symbol,timeframe,as_of DESC)",
    """CREATE TABLE IF NOT EXISTS market_levels (
        level_id TEXT PRIMARY KEY, source TEXT NOT NULL, symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL, level_type TEXT NOT NULL, direction TEXT NOT NULL DEFAULT '',
        low DOUBLE PRECISION NOT NULL, high DOUBLE PRECISION NOT NULL,
        created_at_ts INTEGER NOT NULL, status TEXT NOT NULL,
        touched_at INTEGER, reacted_at INTEGER, swept_at INTEGER, broken_at INTEGER,
        flipped_at INTEGER, expired_at INTEGER, attributes_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_levels_active ON market_levels(symbol,timeframe,status,created_at_ts DESC)",
    """CREATE TABLE IF NOT EXISTS market_feature_coverage (
        source TEXT NOT NULL, feature TEXT NOT NULL, symbol TEXT NOT NULL DEFAULT '*',
        timeframe TEXT NOT NULL DEFAULT '*', coverage_start INTEGER, coverage_end INTEGER,
        quality TEXT NOT NULL, availability TEXT NOT NULL,
        samples INTEGER NOT NULL DEFAULT 0, metadata_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL, PRIMARY KEY(source,feature,symbol,timeframe))""",
    """CREATE TABLE IF NOT EXISTS research_jobs (
        job_id TEXT PRIMARY KEY, job_type TEXT NOT NULL, strategy_version TEXT NOT NULL DEFAULT '',
        symbol TEXT NOT NULL DEFAULT '', timeframe TEXT NOT NULL DEFAULT '',
        range_start INTEGER, range_end INTEGER, last_timestamp INTEGER,
        completed_units INTEGER NOT NULL DEFAULT 0, total_units INTEGER NOT NULL DEFAULT 0,
        progress DOUBLE PRECISION NOT NULL DEFAULT 0, status TEXT NOT NULL,
        error TEXT, lease_owner TEXT, lease_until INTEGER,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_research_jobs_status ON research_jobs(status,updated_at)",
    """CREATE TABLE IF NOT EXISTS research_profiles (
        profile_id TEXT PRIMARY KEY, parent_strategy TEXT NOT NULL,
        profile_version TEXT NOT NULL, status TEXT NOT NULL,
        parameters_json TEXT NOT NULL, locked_rules_json TEXT NOT NULL,
        code_sha TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        UNIQUE(parent_strategy,profile_version))""",
    """CREATE TABLE IF NOT EXISTS research_runs (
        research_run_id TEXT PRIMARY KEY, run_type TEXT NOT NULL,
        dataset_version TEXT NOT NULL, strategy_version TEXT NOT NULL,
        feature_version TEXT NOT NULL, code_sha TEXT NOT NULL,
        range_start INTEGER, range_end INTEGER, universe_json TEXT NOT NULL,
        config_json TEXT NOT NULL, status TEXT NOT NULL, progress DOUBLE PRECISION NOT NULL DEFAULT 0,
        started_at TEXT NOT NULL, finished_at TEXT, error TEXT)""",
    """CREATE TABLE IF NOT EXISTS research_attempts (
        attempt_id TEXT PRIMARY KEY, research_run_id TEXT NOT NULL,
        profile_id TEXT NOT NULL, parent_strategy TEXT NOT NULL,
        symbol TEXT NOT NULL, direction TEXT NOT NULL DEFAULT '',
        decision_time INTEGER NOT NULL, stage TEXT NOT NULL, outcome TEXT NOT NULL,
        stop_code TEXT NOT NULL DEFAULT '', entry DOUBLE PRECISION, sl DOUBLE PRECISION,
        tp1 DOUBLE PRECISION, tp2 DOUBLE PRECISION, terminal_tp DOUBLE PRECISION,
        rr DOUBLE PRECISION, snapshot_json TEXT NOT NULL,
        fidelity TEXT NOT NULL DEFAULT 'UNKNOWN', setup_id TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        UNIQUE(research_run_id,profile_id,symbol,decision_time))""",
    "CREATE INDEX IF NOT EXISTS idx_research_attempts_funnel ON research_attempts(profile_id,outcome,decision_time)",
    """CREATE TABLE IF NOT EXISTS research_setups (
        setup_id TEXT PRIMARY KEY, research_run_id TEXT NOT NULL,
        parent_strategy TEXT NOT NULL, symbol TEXT NOT NULL,
        direction TEXT NOT NULL DEFAULT '', timeframe TEXT NOT NULL DEFAULT '',
        first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL,
        state TEXT NOT NULL, checks_count INTEGER NOT NULL DEFAULT 0,
        terminal_reason TEXT, snapshot_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        UNIQUE(research_run_id,parent_strategy,symbol,setup_id))""",
    "CREATE INDEX IF NOT EXISTS idx_research_setups_state ON research_setups(parent_strategy,state,last_seen DESC)",
    """CREATE TABLE IF NOT EXISTS research_trades (
        trade_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, track TEXT NOT NULL,
        status TEXT NOT NULL, entry_state TEXT NOT NULL, side TEXT NOT NULL,
        entry DOUBLE PRECISION NOT NULL, entry_time INTEGER,
        initial_sl DOUBLE PRECISION NOT NULL, current_sl DOUBLE PRECISION NOT NULL,
        tp1 DOUBLE PRECISION NOT NULL, tp2 DOUBLE PRECISION,
        terminal_tp DOUBLE PRECISION NOT NULL, exit_price DOUBLE PRECISION,
        exit_time INTEGER, exit_reason TEXT, quantity DOUBLE PRECISION NOT NULL DEFAULT 1,
        gross_r DOUBLE PRECISION, net_r DOUBLE PRECISION, pnl_pct DOUBLE PRECISION,
        mfe_r DOUBLE PRECISION, mae_r DOUBLE PRECISION, fees_r DOUBLE PRECISION,
        slippage_r DOUBLE PRECISION, giveback_r DOUBLE PRECISION,
        targets_reached_json TEXT NOT NULL DEFAULT '[]', ambiguity TEXT,
        state_json TEXT NOT NULL DEFAULT '{}', duration_seconds DOUBLE PRECISION,
        duration_bars INTEGER, cost_completeness TEXT NOT NULL DEFAULT 'UNKNOWN',
        updated_at TEXT NOT NULL,
        UNIQUE(attempt_id,track))""",
    "CREATE INDEX IF NOT EXISTS idx_research_trades_result ON research_trades(track,status,exit_time)",
    """CREATE TABLE IF NOT EXISTS research_feature_evaluations (
        evaluation_id TEXT PRIMARY KEY, research_run_id TEXT NOT NULL,
        profile_id TEXT NOT NULL, feature TEXT NOT NULL, segment_json TEXT NOT NULL,
        sample_size INTEGER NOT NULL, coverage DOUBLE PRECISION,
        win_rate DOUBLE PRECISION, expectancy DOUBLE PRECISION,
        profit_factor DOUBLE PRECISION, max_drawdown DOUBLE PRECISION,
        uplift DOUBLE PRECISION, oos_uplift DOUBLE PRECISION,
        confidence_low DOUBLE PRECISION, confidence_high DOUBLE PRECISION,
        status TEXT NOT NULL, metrics_json TEXT NOT NULL, comparison_kind TEXT NOT NULL DEFAULT 'DESCRIPTIVE',
        created_at TEXT NOT NULL,
        UNIQUE(research_run_id,profile_id,feature,segment_json))""",
    """CREATE TABLE IF NOT EXISTS research_hypotheses (
        hypothesis_id TEXT PRIMARY KEY, parent_strategy TEXT NOT NULL,
        profile_id TEXT NOT NULL, feature TEXT NOT NULL, status TEXT NOT NULL,
        old_definition_json TEXT NOT NULL, proposed_definition_json TEXT NOT NULL,
        evidence_json TEXT NOT NULL, code_sha TEXT NOT NULL,
        proposed_at TEXT NOT NULL, approved_at TEXT, activated_at TEXT,
        rolled_back_at TEXT, updated_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS research_audit (
        event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, actor TEXT NOT NULL,
        subject_type TEXT NOT NULL, subject_id TEXT NOT NULL,
        before_json TEXT NOT NULL DEFAULT '{}', after_json TEXT NOT NULL DEFAULT '{}',
        reason TEXT NOT NULL DEFAULT '', code_sha TEXT NOT NULL DEFAULT '',
        occurred_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS research_api_usage (
        source TEXT NOT NULL, bucket_kind TEXT NOT NULL, bucket_start TEXT NOT NULL,
        used INTEGER NOT NULL DEFAULT 0, denied INTEGER NOT NULL DEFAULT 0,
        rate_limited INTEGER NOT NULL DEFAULT 0, errors INTEGER NOT NULL DEFAULT 0,
        limit_value INTEGER NOT NULL, updated_at TEXT NOT NULL,
        PRIMARY KEY(source,bucket_kind,bucket_start))""",
    "CREATE INDEX IF NOT EXISTS idx_research_api_usage_recent ON research_api_usage(source,bucket_start DESC)",
    """CREATE TABLE IF NOT EXISTS research_source_registry (
        source TEXT PRIMARY KEY, kind TEXT NOT NULL, owner TEXT NOT NULL DEFAULT '',
        authority TEXT NOT NULL DEFAULT 'CONTEXT_ONLY', coverage_json TEXT NOT NULL DEFAULT '{}',
        freshness_sla_seconds INTEGER, rate_limits_json TEXT NOT NULL DEFAULT '{}',
        license_json TEXT NOT NULL DEFAULT '{}', fallback_source TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'UNCONFIGURED', updated_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS research_attempt_checks (
        check_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, check_order INTEGER NOT NULL,
        check_code TEXT NOT NULL, label TEXT NOT NULL, role TEXT NOT NULL,
        domain TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, measured_json TEXT NOT NULL DEFAULT '{}',
        threshold_json TEXT NOT NULL DEFAULT '{}', source_timeframe TEXT NOT NULL DEFAULT '',
        source_as_of INTEGER, evidence_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL,
        UNIQUE(attempt_id,check_code))""",
    "CREATE INDEX IF NOT EXISTS idx_attempt_checks_funnel ON research_attempt_checks(attempt_id,status,check_order)",
]


class ResearchStore:
    def __init__(self, database_url: str | None = None):
        self.database_url = (database_url or os.environ.get("APEX_MARKET_DATABASE_URL")
                             or os.environ.get("DATABASE_URL") or "market_history.db")
        self.postgres = self.database_url.startswith(("postgres://", "postgresql://"))

    def connect(self):
        if self.postgres:
            import psycopg2
            return psycopg2.connect(self.database_url, connect_timeout=10)
        conn = sqlite3.connect(self.database_url, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _sql(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.postgres else sql

    @contextmanager
    def transaction(self):
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_schema(self) -> None:
        with self.transaction() as conn:
            cur = conn.cursor()
            for statement in DDL:
                cur.execute(statement)
            migrations = {
                "research_attempts": [("fidelity", "TEXT NOT NULL DEFAULT 'UNKNOWN'"),
                    ("setup_id", "TEXT NOT NULL DEFAULT ''")],
                "research_trades": [("duration_seconds", "DOUBLE PRECISION"),
                    ("duration_bars", "INTEGER"), ("cost_completeness", "TEXT NOT NULL DEFAULT 'UNKNOWN'")],
                "research_feature_evaluations": [("comparison_kind", "TEXT NOT NULL DEFAULT 'DESCRIPTIVE'")],
            }
            for table, columns in migrations.items():
                if self.postgres:
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s", (table,))
                    present={str(row[0]) for row in cur.fetchall()}
                else:
                    cur.execute(f"PRAGMA table_info({table})")
                    present={str(row[1]) for row in cur.fetchall()}
                for column, definition in columns:
                    if column not in present:
                        cur.execute(self._sql(f"ALTER TABLE {table} ADD COLUMN {column} {definition}"))
            # This index is created after the additive setup_id migration so
            # an existing v1/v2 research database can upgrade safely.
            cur.execute("CREATE INDEX IF NOT EXISTS idx_research_attempts_setup ON research_attempts(setup_id,decision_time)")
            now = utc_now()
            cur.execute(self._sql("""INSERT INTO research_meta(key,value_json,updated_at) VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at"""),
                ("schema_version", canonical(SCHEMA_VERSION), now))

    def set_meta(self, key: str, value: Any) -> None:
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO research_meta(key,value_json,updated_at)
                VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,updated_at=excluded.updated_at"""),
                (key, canonical(value), utc_now()))

    def upsert_source_contract(self, source: str, *, kind: str, owner: str = "",
                               authority: str = "CONTEXT_ONLY", coverage: Mapping[str, Any] | None = None,
                               freshness_sla_seconds: int | None = None,
                               rate_limits: Mapping[str, Any] | None = None,
                               license_info: Mapping[str, Any] | None = None,
                               fallback_source: str = "", status: str = "UNCONFIGURED") -> None:
        """Register provenance and limits without granting execution authority."""
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO research_source_registry
                (source,kind,owner,authority,coverage_json,freshness_sla_seconds,rate_limits_json,
                 license_json,fallback_source,status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source) DO UPDATE SET kind=excluded.kind,owner=excluded.owner,
                 authority=excluded.authority,coverage_json=excluded.coverage_json,
                 freshness_sla_seconds=excluded.freshness_sla_seconds,rate_limits_json=excluded.rate_limits_json,
                 license_json=excluded.license_json,fallback_source=excluded.fallback_source,
                 status=excluded.status,updated_at=excluded.updated_at"""),
                (str(source).upper(), kind, owner, authority, canonical(coverage or {}),
                 freshness_sla_seconds, canonical(rate_limits or {}), canonical(license_info or {}),
                 fallback_source, status, utc_now()))

    def save_attempt_checks(self, attempt_id: str, checks: Iterable[Mapping[str, Any]]) -> int:
        return self.save_attempt_check_rows(
            [{**dict(check), "attempt_id": attempt_id} for check in checks])

    def save_attempt_check_rows(self, checks: Iterable[Mapping[str, Any]]) -> int:
        rows=[]; now=utc_now()
        for index, check in enumerate(checks):
            attempt_id=str(check.get("attempt_id") or "")
            if not attempt_id:
                continue
            code=str(check.get("check_code") or check.get("code") or f"CHECK_{index}")
            rows.append((stable_id("attempt-check",attempt_id,code),attempt_id,index,code,
                str(check.get("label") or code),str(check.get("role") or "HARD_GATE"),
                str(check.get("domain") or ""),str(check.get("status") or "UNAVAILABLE"),
                canonical(check.get("measured") or {}),canonical(check.get("threshold") or {}),
                str(check.get("source_timeframe") or ""),check.get("source_as_of"),
                canonical(check.get("evidence") or {}),now))
        if not rows: return 0
        with self.transaction() as conn:
            conn.cursor().executemany(self._sql("""INSERT INTO research_attempt_checks
                (check_id,attempt_id,check_order,check_code,label,role,domain,status,measured_json,
                 threshold_json,source_timeframe,source_as_of,evidence_json,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(check_id) DO UPDATE SET
                 check_order=excluded.check_order,label=excluded.label,role=excluded.role,domain=excluded.domain,
                 status=excluded.status,measured_json=excluded.measured_json,threshold_json=excluded.threshold_json,
                 source_timeframe=excluded.source_timeframe,source_as_of=excluded.source_as_of,
                 evidence_json=excluded.evidence_json"""),rows)
        return len(rows)

    def admit_api_request(self, source: str, *, daily_limit: int, minute_limit: int,
                          units: int = 1, now: datetime | None = None) -> bool:
        """Atomically reserve optional API capacity in durable UTC buckets.

        The research worker is intentionally a single consumer. Row locks keep
        the same contract safe if a replacement process overlaps during deploy.
        """
        moment = now or datetime.now(timezone.utc)
        buckets = (
            ("DAY", moment.strftime("%Y-%m-%d"), max(1, int(daily_limit))),
            ("MINUTE", moment.strftime("%Y-%m-%dT%H:%MZ"), max(1, int(minute_limit))),
        )
        source = str(source).upper(); units = max(1, int(units)); updated = utc_now()
        with self.transaction() as conn:
            cur = conn.cursor()
            for kind, start, limit in buckets:
                cur.execute(self._sql("""INSERT INTO research_api_usage
                    (source,bucket_kind,bucket_start,used,denied,rate_limited,errors,limit_value,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(source,bucket_kind,bucket_start) DO NOTHING"""),
                    (source, kind, start, 0, 0, 0, 0, limit, updated))
            allowed = True
            for kind, start, limit in buckets:
                lock = " FOR UPDATE" if self.postgres else ""
                cur.execute(self._sql("""SELECT used FROM research_api_usage
                    WHERE source=? AND bucket_kind=? AND bucket_start=?""" + lock),
                    (source, kind, start))
                row = cur.fetchone(); used = int(row[0]) if row else 0
                if used + units > limit:
                    allowed = False
            if not allowed:
                for kind, start, _limit in buckets:
                    cur.execute(self._sql("""UPDATE research_api_usage SET denied=denied+1,updated_at=?
                        WHERE source=? AND bucket_kind=? AND bucket_start=?"""),
                        (updated, source, kind, start))
                return False
            for kind, start, limit in buckets:
                cur.execute(self._sql("""UPDATE research_api_usage
                    SET used=used+?,limit_value=?,updated_at=?
                    WHERE source=? AND bucket_kind=? AND bucket_start=?"""),
                    (units, limit, updated, source, kind, start))
        return True

    def record_api_result(self, source: str, *, rate_limited: bool = False,
                          error: bool = False, now: datetime | None = None) -> None:
        moment = now or datetime.now(timezone.utc); source = str(source).upper()
        buckets = (("DAY", moment.strftime("%Y-%m-%d")),
                   ("MINUTE", moment.strftime("%Y-%m-%dT%H:%MZ")))
        with self.transaction() as conn:
            cur = conn.cursor()
            for kind, start in buckets:
                cur.execute(self._sql("""UPDATE research_api_usage SET
                    rate_limited=rate_limited+?,errors=errors+?,updated_at=?
                    WHERE source=? AND bucket_kind=? AND bucket_start=?"""),
                    (int(rate_limited), int(error), utc_now(), source, kind, start))

    def api_usage(self, source: str, *, now: datetime | None = None) -> dict[str, Any]:
        moment = now or datetime.now(timezone.utc); source = str(source).upper()
        starts = {"DAY": moment.strftime("%Y-%m-%d"),
                  "MINUTE": moment.strftime("%Y-%m-%dT%H:%MZ")}
        result: dict[str, Any] = {}
        with self.transaction() as conn:
            cur = conn.cursor()
            for kind, start in starts.items():
                cur.execute(self._sql("""SELECT used,denied,rate_limited,errors,limit_value
                    FROM research_api_usage WHERE source=? AND bucket_kind=? AND bucket_start=?"""),
                    (source, kind, start))
                row = cur.fetchone()
                result[kind.lower()] = self._row_dict(cur, row) if row else {
                    "used": 0, "denied": 0, "rate_limited": 0, "errors": 0,
                    "limit_value": 0,
                }
        return result

    def dataset_manifest(self) -> dict[str, Any]:
        with self.transaction() as conn:
            cur=conn.cursor()
            cur.execute("""SELECT source,symbol,timeframe,COUNT(*) AS count,
                MIN(open_time) AS first_open,MAX(open_time) AS last_open
                FROM market_candles GROUP BY source,symbol,timeframe
                ORDER BY source,symbol,timeframe""")
            streams=[self._row_dict(cur,row) for row in cur.fetchall()]
            cur.execute("SELECT status,COUNT(*) AS count FROM research_jobs GROUP BY status ORDER BY status")
            jobs=[self._row_dict(cur,row) for row in cur.fetchall()]
        content={"schema_version":SCHEMA_VERSION,"streams":streams,"jobs":jobs}
        return {**content,"manifest_hash":stable_id(canonical(content)),"created_at":utc_now()}

    @staticmethod
    def _row_dict(cursor, row) -> dict[str, Any]:
        if row is None:
            return {}
        if hasattr(row, "keys"):
            return dict(row)
        return dict(zip([col[0] for col in cursor.description], row))

    def upsert_symbol(self, symbol: str, provider_symbol: str, *, source: str = "GATE",
                      listed_at: int | None = None, delisted_at: int | None = None,
                      metadata: Mapping[str, Any] | None = None) -> None:
        now = utc_now()
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO market_symbols
                (source,symbol,provider_symbol,listed_at,delisted_at,historically_available,metadata_json,updated_at)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(source,symbol) DO UPDATE SET
                provider_symbol=excluded.provider_symbol,listed_at=COALESCE(market_symbols.listed_at,excluded.listed_at),
                delisted_at=excluded.delisted_at,historically_available=excluded.historically_available,
                metadata_json=excluded.metadata_json,updated_at=excluded.updated_at"""),
                (source, symbol, provider_symbol, listed_at, delisted_at, int(delisted_at is None),
                 canonical(dict(metadata or {})), now))

    def upsert_candles(self, candles: Iterable[Mapping[str, Any]], *, source: str = "GATE") -> int:
        rows = []
        received = utc_now()
        for candle in candles:
            rows.append((source, str(candle["symbol"]).upper(), str(candle["timeframe"]),
                         int(candle["open_time"]), int(candle["close_time"]), float(candle["open"]),
                         float(candle["high"]), float(candle["low"]), float(candle["close"]),
                         float(candle.get("volume") or 0),
                         float(candle["quote_volume"]) if candle.get("quote_volume") is not None else None,
                         int(bool(candle.get("is_closed", True))), str(candle.get("received_at") or received),
                         str(candle.get("data_quality") or "VALID"), canonical(candle.get("payload") or {})))
        if not rows:
            return 0
        sql = self._sql("""INSERT INTO market_candles
            (source,symbol,timeframe,open_time,close_time,open,high,low,close,volume,quote_volume,
             is_closed,received_at,data_quality,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source,symbol,timeframe,open_time) DO UPDATE SET
             close_time=excluded.close_time,open=excluded.open,high=excluded.high,low=excluded.low,
             close=excluded.close,volume=excluded.volume,quote_volume=excluded.quote_volume,
             is_closed=excluded.is_closed,received_at=excluded.received_at,
             data_quality=excluded.data_quality,payload_json=excluded.payload_json""")
        with self.transaction() as conn:
            conn.cursor().executemany(sql, rows)
        return len(rows)

    def upsert_context_observations(self, observations: Iterable[Mapping[str, Any]]) -> int:
        """Persist point-in-time derivatives/microstructure context.

        Context observations are deliberately isolated from candles and never
        receive execution authority. Missing values are represented by
        coverage metadata, not synthetic zeroes.
        """
        rows=[]; received=utc_now()
        for item in observations:
            source=str(item.get("source") or "").upper()
            symbol=str(item.get("symbol") or "").upper()
            feature=str(item.get("feature") or "").upper()
            event_time=item.get("event_time")
            if not source or not symbol or not feature or event_time is None:
                continue
            event_time=int(event_time)
            rows.append((stable_id("context",source,symbol,feature,event_time),source,symbol,feature,
                event_time,str(item.get("received_at") or received),str(item.get("quality") or "UNKNOWN"),
                str(item.get("availability") or "OBSERVED"),int(bool(item.get("point_in_time",True))),
                canonical(item.get("value") or {})))
        if not rows: return 0
        with self.transaction() as conn:
            conn.cursor().executemany(self._sql("""INSERT INTO market_context_observations
                (observation_id,source,symbol,feature,event_time,received_at,quality,availability,
                 point_in_time,value_json) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source,symbol,feature,event_time) DO UPDATE SET
                 received_at=excluded.received_at,quality=excluded.quality,
                 availability=excluded.availability,point_in_time=excluded.point_in_time,
                 value_json=excluded.value_json"""),rows)
        return len(rows)

    def context_rows(self, symbol: str, *, start: int | None = None,
                     end: int | None = None) -> list[dict[str, Any]]:
        where=["symbol=?"]; params: list[Any]=[str(symbol).upper()]
        if start is not None:
            where.append("event_time>=?"); params.append(int(start))
        if end is not None:
            where.append("event_time<=?"); params.append(int(end))
        with self.transaction() as conn:
            cur=conn.cursor(); cur.execute(self._sql("""SELECT source,symbol,feature,event_time,
                received_at,quality,availability,point_in_time,value_json
                FROM market_context_observations WHERE """+" AND ".join(where)+
                " ORDER BY feature,event_time"),params)
            rows=[self._row_dict(cur,row) for row in cur.fetchall()]
        for row in rows:
            try: row["value"]=json.loads(row.pop("value_json"))
            except (TypeError,ValueError,json.JSONDecodeError): row["value"]={}
        return rows

    def candles(self, symbol: str, timeframe: str, *, as_of: int | None = None,
                limit: int = 500, source: str = "GATE") -> list[dict[str, Any]]:
        where = "source=? AND symbol=? AND timeframe=? AND is_closed=1"
        params: list[Any] = [source, symbol.upper(), timeframe]
        if as_of is not None:
            where += " AND close_time<=?"
            params.append(int(as_of))
        params.append(max(1, int(limit)))
        with self.transaction() as conn:
            cur = conn.cursor()
            cur.execute(self._sql(f"SELECT * FROM market_candles WHERE {where} ORDER BY open_time DESC LIMIT ?"), params)
            rows = [self._row_dict(cur, row) for row in cur.fetchall()]
        rows.reverse()
        for row in rows:
            row["timestamp"] = row["open_time"]
        return rows

    def candles_between(self, symbol: str, timeframe: str, start: int, end: int,
                        *, source: str = "GATE") -> list[dict[str, Any]]:
        """Return the immutable closed-candle stream for an explicit event-time range."""
        with self.transaction() as conn:
            cur = conn.cursor()
            cur.execute(self._sql("""SELECT * FROM market_candles
                WHERE source=? AND symbol=? AND timeframe=? AND is_closed=1
                  AND close_time>? AND close_time<=? ORDER BY open_time"""),
                (source, symbol.upper(), timeframe, int(start), int(end)))
            rows = [self._row_dict(cur, row) for row in cur.fetchall()]
        for row in rows:
            row["timestamp"] = row["open_time"]
        return rows

    def max_open_time(self, symbol: str, timeframe: str, source: str = "GATE") -> int | None:
        with self.transaction() as conn:
            cur = conn.cursor(); cur.execute(self._sql(
                "SELECT MAX(open_time) FROM market_candles WHERE source=? AND symbol=? AND timeframe=?"),
                (source, symbol.upper(), timeframe))
            row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def candle_count(self, symbol: str, timeframe: str, source: str = "GATE") -> int:
        with self.transaction() as conn:
            cur = conn.cursor(); cur.execute(self._sql(
                "SELECT COUNT(*) FROM market_candles WHERE source=? AND symbol=? AND timeframe=?"),
                (source, symbol.upper(), timeframe)); row = cur.fetchone()
        return int(row[0]) if row else 0

    def save_quality_issue(self, symbol: str, timeframe: str, issue_type: str, *,
                           open_time: int | None = None, severity: str = "WARNING",
                           detail: Mapping[str, Any] | None = None) -> str:
        issue_id = stable_id("quality", symbol, timeframe, issue_type, open_time)
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO market_quality_issues
                (issue_id,source,symbol,timeframe,open_time,issue_type,severity,detail_json,status,detected_at)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(issue_id) DO UPDATE SET
                severity=excluded.severity,detail_json=excluded.detail_json,status='OPEN',resolved_at=NULL"""),
                (issue_id, "GATE", symbol.upper(), timeframe, open_time, issue_type, severity,
                 canonical(dict(detail or {})), "OPEN", utc_now()))
        return issue_id

    def earliest_open_quality_issue(self, symbol: str, timeframe: str, *,
                                    issue_types: Sequence[str] | None = None,
                                    start: int | None = None, end: int | None = None) -> int | None:
        """Return the earliest repairable open issue in a requested range.

        The backfill cursor uses this value to revisit a previously incomplete
        page after a restart.  Issues outside the current rolling window are
        intentionally ignored; their diagnostics remain in the database.
        """
        where = ["source=?", "symbol=?", "timeframe=?", "status='OPEN'", "open_time IS NOT NULL"]
        params: list[Any] = ["GATE", str(symbol).upper(), str(timeframe)]
        if issue_types:
            values = [str(value) for value in issue_types if str(value)]
            if values:
                where.append("issue_type IN (" + ",".join("?" for _ in values) + ")")
                params.extend(values)
        if start is not None:
            where.append("open_time>=?"); params.append(int(start))
        if end is not None:
            where.append("open_time<=?"); params.append(int(end))
        with self.transaction() as conn:
            cur = conn.cursor()
            cur.execute(self._sql("SELECT MIN(open_time) FROM market_quality_issues WHERE " + " AND ".join(where)), params)
            row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def resolve_quality_issue(self, symbol: str, timeframe: str, issue_type: str, *,
                              open_time: int | None = None) -> int:
        """Mark one repaired quality issue resolved, preserving its audit row."""
        where = ["source=?", "symbol=?", "timeframe=?", "issue_type=?", "status='OPEN'"]
        params: list[Any] = ["GATE", str(symbol).upper(), str(timeframe), str(issue_type)]
        if open_time is not None:
            where.append("open_time=?"); params.append(int(open_time))
        with self.transaction() as conn:
            cur = conn.cursor()
            cur.execute(self._sql("UPDATE market_quality_issues SET status='RESOLVED',resolved_at=? WHERE "
                                  + " AND ".join(where)), [utc_now(), *params])
            return int(cur.rowcount or 0)

    def quality_issues(self, symbol: str, timeframe: str, *, status: str = "OPEN",
                       issue_types: Sequence[str] | None = None,
                       start: int | None = None, end: int | None = None) -> list[dict[str, Any]]:
        """Read quality diagnostics for repair/observability without mutating them."""
        where = ["source=?", "symbol=?", "timeframe=?", "status=?"]
        params: list[Any] = ["GATE", str(symbol).upper(), str(timeframe), str(status)]
        if issue_types:
            values = [str(value) for value in issue_types if str(value)]
            if values:
                where.append("issue_type IN (" + ",".join("?" for _ in values) + ")")
                params.extend(values)
        if start is not None:
            where.append("(open_time IS NULL OR open_time>=?)"); params.append(int(start))
        if end is not None:
            where.append("(open_time IS NULL OR open_time<=?)"); params.append(int(end))
        with self.transaction() as conn:
            cur = conn.cursor()
            cur.execute(self._sql("SELECT * FROM market_quality_issues WHERE " + " AND ".join(where)
                                  + " ORDER BY open_time,detected_at"), params)
            return [self._row_dict(cur, row) for row in cur.fetchall()]

    def save_feature_snapshot(self, symbol: str, timeframe: str, as_of: int,
                              features: Mapping[str, Any], *, feature_version: str,
                              dataset_version: str, quality: str = "VALID") -> str:
        snapshot_id = stable_id("features", symbol, timeframe, as_of, feature_version, dataset_version)
        self.save_feature_snapshots([{"symbol":symbol,"timeframe":timeframe,"as_of":as_of,
            "features":features,"feature_version":feature_version,"dataset_version":dataset_version,
            "quality":quality,"snapshot_id":snapshot_id}])
        return snapshot_id

    def save_feature_snapshots(self, snapshots: Iterable[Mapping[str, Any]]) -> int:
        now=utc_now(); rows=[]
        for item in snapshots:
            symbol=str(item["symbol"]).upper(); timeframe=str(item["timeframe"]); as_of=int(item["as_of"])
            feature_version=str(item["feature_version"]); dataset_version=str(item["dataset_version"])
            rows.append((str(item.get("snapshot_id") or stable_id("features",symbol,timeframe,as_of,feature_version,dataset_version)),
                "GATE",symbol,timeframe,as_of,feature_version,dataset_version,canonical(item["features"]),
                str(item.get("quality") or "VALID"),now))
        if not rows: return 0
        with self.transaction() as conn:
            conn.cursor().executemany(self._sql("""INSERT INTO market_feature_snapshots
                (snapshot_id,source,symbol,timeframe,as_of,feature_version,dataset_version,
                 features_json,data_quality,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(snapshot_id) DO UPDATE SET features_json=excluded.features_json,
                 data_quality=excluded.data_quality"""),rows)
        return len(rows)

    def feature_snapshot(self, symbol: str, timeframe: str, *, as_of: int | None = None,
                         feature_version: str | None = None) -> dict[str, Any]:
        where="symbol=? AND timeframe=?"; params: list[Any]=[symbol.upper(),timeframe]
        if as_of is not None:
            where+=" AND as_of<=?"; params.append(int(as_of))
        if feature_version:
            where+=" AND feature_version=?"; params.append(feature_version)
        with self.transaction() as conn:
            cur=conn.cursor(); cur.execute(self._sql(
                f"SELECT features_json FROM market_feature_snapshots WHERE {where} ORDER BY as_of DESC LIMIT 1"),params)
            row=cur.fetchone()
        if not row: return {}
        try: return json.loads(row[0])
        except (TypeError,ValueError,json.JSONDecodeError): return {}

    def feature_timestamps(self, symbol: str, timeframe: str, start: int, end: int,
                           feature_version: str | None = None) -> list[int]:
        where="symbol=? AND timeframe=? AND as_of>=? AND as_of<=?"; params: list[Any]=[symbol.upper(),timeframe,int(start),int(end)]
        if feature_version:
            where+=" AND feature_version=?"; params.append(feature_version)
        with self.transaction() as conn:
            cur=conn.cursor(); cur.execute(self._sql(
                f"SELECT as_of FROM market_feature_snapshots WHERE {where} ORDER BY as_of"),params)
            return [int(row[0]) for row in cur.fetchall()]

    def feature_series(self, symbol: str, timeframe: str, start: int, end: int,
                       feature_version: str) -> list[tuple[int,dict[str,Any]]]:
        """Return one prior snapshot plus a bounded point-in-time batch."""
        with self.transaction() as conn:
            cur=conn.cursor()
            cur.execute(self._sql("""SELECT as_of,features_json FROM market_feature_snapshots
                WHERE symbol=? AND timeframe=? AND feature_version=? AND as_of<=?
                ORDER BY as_of DESC LIMIT 1"""),(symbol.upper(),timeframe,feature_version,int(start)))
            prior=cur.fetchone()
            cur.execute(self._sql("""SELECT as_of,features_json FROM market_feature_snapshots
                WHERE symbol=? AND timeframe=? AND feature_version=? AND as_of>? AND as_of<=?
                ORDER BY as_of"""),(symbol.upper(),timeframe,feature_version,int(start),int(end)))
            rows=([prior] if prior else [])+list(cur.fetchall())
        result=[]
        for row in rows:
            try: result.append((int(row[0]),json.loads(row[1])))
            except (TypeError,ValueError,json.JSONDecodeError): continue
        return result

    def upsert_level(self, level: Mapping[str, Any]) -> str:
        level_id = str(level.get("level_id") or stable_id("level", level.get("symbol"),
                       level.get("timeframe"), level.get("level_type"), level.get("created_at_ts"),
                       level.get("low"), level.get("high")))
        self.upsert_levels([{**dict(level),"level_id":level_id}])
        return level_id

    def upsert_levels(self, levels: Iterable[Mapping[str, Any]]) -> int:
        rows=[]; now=utc_now()
        for level in levels:
            level_id = str(level.get("level_id") or stable_id("level", level.get("symbol"),
                           level.get("timeframe"), level.get("level_type"), level.get("created_at_ts"),
                           level.get("low"), level.get("high")))
            rows.append((level_id, str(level.get("source") or "GATE"), str(level["symbol"]).upper(),
                  str(level["timeframe"]), str(level["level_type"]), str(level.get("direction") or ""),
                  float(level["low"]), float(level["high"]), int(level["created_at_ts"]),
                  str(level.get("status") or "ACTIVE"), level.get("touched_at"), level.get("reacted_at"),
                  level.get("swept_at"), level.get("broken_at"), level.get("flipped_at"),
                  level.get("expired_at"), canonical(level.get("attributes") or {}), now))
        if not rows: return 0
        with self.transaction() as conn:
            conn.cursor().executemany(self._sql("""INSERT INTO market_levels
                (level_id,source,symbol,timeframe,level_type,direction,low,high,created_at_ts,status,
                 touched_at,reacted_at,swept_at,broken_at,flipped_at,expired_at,attributes_json,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(level_id) DO UPDATE SET
                 status=excluded.status,touched_at=excluded.touched_at,reacted_at=excluded.reacted_at,
                 swept_at=excluded.swept_at,broken_at=excluded.broken_at,flipped_at=excluded.flipped_at,
                 expired_at=excluded.expired_at,attributes_json=excluded.attributes_json,
                 updated_at=excluded.updated_at"""), rows)
        return len(rows)

    def update_coverage(self, feature: str, *, source: str, symbol: str = "*", timeframe: str = "*",
                        start: int | None = None, end: int | None = None, quality: str = "UNKNOWN",
                        availability: str = "HISTORICAL", samples: int = 0,
                        metadata: Mapping[str, Any] | None = None) -> None:
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO market_feature_coverage
                (source,feature,symbol,timeframe,coverage_start,coverage_end,quality,availability,
                 samples,metadata_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source,feature,symbol,timeframe) DO UPDATE SET
                 coverage_start=excluded.coverage_start,coverage_end=excluded.coverage_end,
                 quality=excluded.quality,availability=excluded.availability,samples=excluded.samples,
                 metadata_json=excluded.metadata_json,updated_at=excluded.updated_at"""),
                (source, feature, symbol, timeframe, start, end, quality, availability, int(samples),
                 canonical(dict(metadata or {})), utc_now()))

    def checkpoint(self, job_id: str, *, job_type: str, symbol: str = "", timeframe: str = "",
                   strategy_version: str = "", range_start: int | None = None,
                   range_end: int | None = None, last_timestamp: int | None = None,
                   completed_units: int = 0, total_units: int = 0, status: str = "RUNNING",
                   error: str | None = None) -> None:
        progress = (completed_units / total_units * 100.0) if total_units else 0.0
        now = utc_now()
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO research_jobs
                (job_id,job_type,strategy_version,symbol,timeframe,range_start,range_end,last_timestamp,
                 completed_units,total_units,progress,status,error,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET
                 last_timestamp=excluded.last_timestamp,completed_units=excluded.completed_units,
                 total_units=excluded.total_units,progress=excluded.progress,status=excluded.status,
                 error=excluded.error,updated_at=excluded.updated_at"""),
                (job_id, job_type, strategy_version, symbol, timeframe, range_start, range_end,
                 last_timestamp, int(completed_units), int(total_units), progress, status,
                 str(error)[:1000] if error else None, now, now))

    def job(self, job_id: str) -> dict[str, Any]:
        with self.transaction() as conn:
            cur = conn.cursor(); cur.execute(self._sql("SELECT * FROM research_jobs WHERE job_id=?"), (job_id,))
            return self._row_dict(cur, cur.fetchone())

    def upsert_profile(self, parent: str, version: str, status: str, parameters: Mapping[str, Any],
                       locked_rules: Sequence[str], code_sha: str) -> str:
        profile_id = stable_id("profile", parent.upper(), version)
        now = utc_now()
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO research_profiles
                (profile_id,parent_strategy,profile_version,status,parameters_json,locked_rules_json,
                 code_sha,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(profile_id) DO UPDATE SET status=excluded.status,
                 parameters_json=excluded.parameters_json,locked_rules_json=excluded.locked_rules_json,
                 code_sha=excluded.code_sha,updated_at=excluded.updated_at"""),
                (profile_id, parent.upper(), version, status, canonical(parameters),
                 canonical(list(locked_rules)), code_sha, now, now))
        return profile_id

    def save_run(self, run: Mapping[str, Any]) -> str:
        run_id=str(run.get("research_run_id") or stable_id("run",run.get("run_type"),run.get("dataset_version"),
                   run.get("strategy_version"),run.get("range_start"),run.get("range_end"),run.get("universe")))
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO research_runs
                (research_run_id,run_type,dataset_version,strategy_version,feature_version,code_sha,
                 range_start,range_end,universe_json,config_json,status,progress,started_at,finished_at,error)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(research_run_id) DO UPDATE SET
                 run_type=excluded.run_type,dataset_version=excluded.dataset_version,
                 strategy_version=excluded.strategy_version,feature_version=excluded.feature_version,
                 code_sha=excluded.code_sha,range_start=excluded.range_start,range_end=excluded.range_end,
                 universe_json=excluded.universe_json,config_json=excluded.config_json,
                 status=excluded.status,progress=excluded.progress,started_at=excluded.started_at,
                 finished_at=excluded.finished_at,error=excluded.error"""),
                (run_id,str(run.get("run_type") or "REPLAY"),str(run["dataset_version"]),str(run["strategy_version"]),
                 str(run["feature_version"]),str(run.get("code_sha") or "unknown"),run.get("range_start"),run.get("range_end"),
                 canonical(run.get("universe") or []),canonical(run.get("config") or {}),str(run.get("status") or "RUNNING"),
                 float(run.get("progress") or 0),str(run.get("started_at") or utc_now()),run.get("finished_at"),run.get("error")))
        return run_id

    def save_attempt(self, attempt: Mapping[str, Any]) -> str:
        return self.save_attempts([attempt])[0]

    @staticmethod
    def _setup_id_for_attempt(attempt: Mapping[str, Any]) -> str:
        """Derive a conservative, restart-stable setup identity."""
        explicit = str(attempt.get("setup_id") or attempt.get("setup_key") or "").strip()
        if explicit:
            return explicit
        snapshot = attempt.get("snapshot") if isinstance(attempt.get("snapshot"), Mapping) else {}
        candidate = snapshot.get("candidate") if isinstance(snapshot.get("candidate"), Mapping) else {}
        evidence = candidate.get("technical_evidence") if isinstance(candidate.get("technical_evidence"), Mapping) else {}
        direction = str(attempt.get("direction") or candidate.get("direction") or "").upper()
        timeframe = str(candidate.get("timeframe") or attempt.get("timeframe") or "")
        # Do not include current price/RR: those legitimately evolve on each
        # scan.  If no structural key exists, isolate this observation.
        identity = {
            "location_id": evidence.get("location_id") or evidence.get("zone_id") or evidence.get("ob_id") or evidence.get("fvg_id"),
            "ob": evidence.get("ob"), "fvg": evidence.get("fvg"),
            "zone": evidence.get("zone"), "zone_type": evidence.get("zone_type"),
            "structure_event": evidence.get("structure_event") or evidence.get("event"),
            "phases": evidence.get("phases"),
        }
        if not any(value not in (None, "", False, {}) for value in identity.values()):
            identity = {"decision_time": int(attempt.get("decision_time") or 0)}
        return stable_id("setup", attempt.get("research_run_id"),
                         str(attempt.get("parent_strategy") or "").upper(),
                         str(attempt.get("symbol") or "").upper(), direction,
                         timeframe, canonical(identity))

    def save_attempts(self, attempts: Iterable[Mapping[str, Any]]) -> list[str]:
        rows=[]; ids=[]; setup_rows=[]; now=utc_now()
        for attempt in attempts:
            attempt_id = str(attempt.get("attempt_id") or stable_id(attempt.get("research_run_id"),
                             attempt.get("profile_id"), attempt.get("symbol"), attempt.get("decision_time")))
            setup_id = self._setup_id_for_attempt(attempt)
            ids.append(attempt_id)
            rows.append((attempt_id, attempt["research_run_id"], attempt["profile_id"],
                  str(attempt["parent_strategy"]).upper(), str(attempt["symbol"]).upper(),
                  str(attempt.get("direction") or ""), int(attempt["decision_time"]),
                  str(attempt.get("stage") or "SCAN"), str(attempt.get("outcome") or "FILTERED"),
                  str(attempt.get("stop_code") or ""), attempt.get("entry"), attempt.get("sl"),
                  attempt.get("tp1"), attempt.get("tp2"), attempt.get("terminal_tp"), attempt.get("rr"),
                  canonical(attempt.get("snapshot") or {}), str(attempt.get("fidelity") or "UNKNOWN"), setup_id, now))
            snapshot = attempt.get("snapshot") if isinstance(attempt.get("snapshot"), Mapping) else {}
            candidate = snapshot.get("candidate") if isinstance(snapshot.get("candidate"), Mapping) else {}
            setup_rows.append((setup_id, attempt, str(attempt["parent_strategy"]).upper(),
                               str(attempt["symbol"]).upper(), str(attempt.get("direction") or "").upper(),
                               str(candidate.get("timeframe") or attempt.get("timeframe") or ""),
                               candidate))
        if not rows: return []
        with self.transaction() as conn:
            conn.cursor().executemany(self._sql("""INSERT INTO research_attempts
                (attempt_id,research_run_id,profile_id,parent_strategy,symbol,direction,decision_time,
                 stage,outcome,stop_code,entry,sl,tp1,tp2,terminal_tp,rr,snapshot_json,fidelity,setup_id,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(attempt_id) DO UPDATE SET
                 stage=excluded.stage,outcome=excluded.outcome,stop_code=excluded.stop_code,
                 snapshot_json=excluded.snapshot_json,fidelity=excluded.fidelity,setup_id=excluded.setup_id"""),rows)
            for setup_id, attempt, strategy, symbol, direction, timeframe, candidate in setup_rows:
                conn.cursor().execute(self._sql("""INSERT INTO research_setups
                    (setup_id,research_run_id,parent_strategy,symbol,direction,timeframe,
                     first_seen,last_seen,state,checks_count,terminal_reason,snapshot_json,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(setup_id) DO UPDATE SET
                     first_seen=MIN(research_setups.first_seen,excluded.first_seen),
                     last_seen=MAX(research_setups.last_seen,excluded.last_seen),
                     state=excluded.state,checks_count=excluded.checks_count,
                     terminal_reason=excluded.terminal_reason,snapshot_json=excluded.snapshot_json,
                     updated_at=excluded.updated_at"""),
                    (setup_id, attempt["research_run_id"], strategy, symbol, direction, timeframe,
                     int(attempt["decision_time"]), int(attempt["decision_time"]),
                     str(attempt.get("outcome") or "FILTERED"), 1,
                     str(attempt.get("stop_code") or "") or None,
                     canonical(candidate if isinstance(candidate, Mapping) else {}), now, now))
                count_cur = conn.cursor()
                count_cur.execute(self._sql("SELECT COUNT(*) FROM research_attempts WHERE setup_id=?"), (setup_id,))
                count = int(count_cur.fetchone()[0])
                conn.cursor().execute(self._sql("UPDATE research_setups SET checks_count=?,updated_at=? WHERE setup_id=?"),
                                     (count, now, setup_id))
        return ids

    def save_trade(self, trade: Mapping[str, Any]) -> str:
        return self.save_trades([trade])[0]

    def save_trades(self, trades: Iterable[Mapping[str, Any]]) -> list[str]:
        rows=[]; ids=[]; now=utc_now()
        for trade in trades:
            trade_id = str(trade.get("trade_id") or stable_id("trade", trade["attempt_id"], trade["track"]))
            ids.append(trade_id)
            rows.append((trade_id, trade["attempt_id"], trade["track"], trade["status"], trade["entry_state"],
                  trade["side"], trade["entry"], trade.get("entry_time"), trade["initial_sl"],
                  trade.get("current_sl", trade["initial_sl"]), trade["tp1"], trade.get("tp2"),
                  trade.get("terminal_tp", trade.get("tp2") or trade["tp1"]), trade.get("exit_price"),
                  trade.get("exit_time"), trade.get("exit_reason"), trade.get("quantity", 1.0),
                  trade.get("gross_r"), trade.get("net_r"), trade.get("pnl_pct"), trade.get("mfe_r"),
                  trade.get("mae_r"), trade.get("fees_r"), trade.get("slippage_r"), trade.get("giveback_r"),
                  canonical(trade.get("targets_reached") or []), trade.get("ambiguity"),
                  canonical(trade.get("state") or {}), trade.get("duration_seconds"), trade.get("duration_bars"),
                  str(trade.get("cost_completeness") or ((trade.get("state") or {}).get("cost_completeness") if isinstance(trade.get("state"), Mapping) else None) or "UNKNOWN"), now))
        if not rows: return []
        with self.transaction() as conn:
            conn.cursor().executemany(self._sql("""INSERT INTO research_trades
                (trade_id,attempt_id,track,status,entry_state,side,entry,entry_time,initial_sl,current_sl,
                 tp1,tp2,terminal_tp,exit_price,exit_time,exit_reason,quantity,gross_r,net_r,pnl_pct,
                 mfe_r,mae_r,fees_r,slippage_r,giveback_r,targets_reached_json,ambiguity,state_json,
                 duration_seconds,duration_bars,cost_completeness,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(trade_id) DO UPDATE SET status=excluded.status,entry_state=excluded.entry_state,
                 entry_time=excluded.entry_time,current_sl=excluded.current_sl,exit_price=excluded.exit_price,
                 exit_time=excluded.exit_time,exit_reason=excluded.exit_reason,gross_r=excluded.gross_r,
                 net_r=excluded.net_r,pnl_pct=excluded.pnl_pct,mfe_r=excluded.mfe_r,mae_r=excluded.mae_r,
                 fees_r=excluded.fees_r,slippage_r=excluded.slippage_r,giveback_r=excluded.giveback_r,
                 targets_reached_json=excluded.targets_reached_json,ambiguity=excluded.ambiguity,
                 state_json=excluded.state_json,duration_seconds=excluded.duration_seconds,
                 duration_bars=excluded.duration_bars,cost_completeness=excluded.cost_completeness,
                 updated_at=excluded.updated_at"""),rows)
        return ids

    def save_feature_evaluation(self, evaluation: Mapping[str, Any]) -> str:
        segment = canonical(evaluation.get("segment") or {})
        evaluation_id = str(evaluation.get("evaluation_id") or stable_id(
            "evaluation", evaluation["research_run_id"], evaluation["profile_id"],
            evaluation["feature"], segment))
        metrics = dict(evaluation.get("metrics") or {})
        values = (evaluation_id, evaluation["research_run_id"], evaluation["profile_id"],
                  str(evaluation["feature"]), segment, int(evaluation.get("sample_size") or 0),
                  evaluation.get("coverage"), evaluation.get("win_rate"), evaluation.get("expectancy"),
                  evaluation.get("profit_factor"), evaluation.get("max_drawdown"),
                  evaluation.get("uplift"), evaluation.get("oos_uplift"),
                  evaluation.get("confidence_low"), evaluation.get("confidence_high"),
                  str(evaluation.get("status") or "INSUFFICIENT_DATA"), canonical(metrics),
                  str(evaluation.get("comparison_kind") or "DESCRIPTIVE"), utc_now())
        with self.transaction() as conn:
            conn.cursor().execute(self._sql("""INSERT INTO research_feature_evaluations
                (evaluation_id,research_run_id,profile_id,feature,segment_json,sample_size,coverage,
                 win_rate,expectancy,profit_factor,max_drawdown,uplift,oos_uplift,confidence_low,
                 confidence_high,status,metrics_json,comparison_kind,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(evaluation_id) DO UPDATE SET sample_size=excluded.sample_size,
                 coverage=excluded.coverage,win_rate=excluded.win_rate,expectancy=excluded.expectancy,
                 profit_factor=excluded.profit_factor,max_drawdown=excluded.max_drawdown,
                 uplift=excluded.uplift,oos_uplift=excluded.oos_uplift,
                 confidence_low=excluded.confidence_low,confidence_high=excluded.confidence_high,
                 status=excluded.status,metrics_json=excluded.metrics_json,comparison_kind=excluded.comparison_kind,
                 created_at=excluded.created_at"""), values)
        return evaluation_id

    def completed_trade_rows(self, research_run_id: str, profile_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            cur = conn.cursor()
            cur.execute(self._sql("""SELECT t.*,a.parent_strategy,a.symbol,a.decision_time,a.stop_code,
                a.snapshot_json FROM research_trades t JOIN research_attempts a ON a.attempt_id=t.attempt_id
                WHERE a.research_run_id=? AND a.profile_id=? AND t.status='CLOSED'
                ORDER BY a.decision_time"""), (research_run_id, profile_id))
            return [self._row_dict(cur, row) for row in cur.fetchall()]

    def open_trade_rows(self, research_run_id: str) -> list[dict[str, Any]]:
        with self.transaction() as conn:
            cur = conn.cursor()
            cur.execute(self._sql("""SELECT t.*,a.parent_strategy,a.symbol,a.decision_time,
                a.snapshot_json FROM research_trades t JOIN research_attempts a ON a.attempt_id=t.attempt_id
                WHERE a.research_run_id=? AND t.status='OPEN' ORDER BY a.decision_time"""),
                (research_run_id,))
            return [self._row_dict(cur, row) for row in cur.fetchall()]

    def dashboard(self) -> dict[str, Any]:
        def query(sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
            with self.transaction() as conn:
                cur = conn.cursor(); cur.execute(self._sql(sql), params)
                return [self._row_dict(cur, row) for row in cur.fetchall()]
        counts = query("""SELECT timeframe,COUNT(*) AS candles,MIN(open_time) AS coverage_start,
            MAX(open_time) AS coverage_end,COUNT(DISTINCT symbol) AS symbols
            FROM market_candles GROUP BY timeframe ORDER BY timeframe""")
        jobs = query("SELECT * FROM research_jobs ORDER BY updated_at DESC LIMIT 100")
        profiles = query("SELECT * FROM research_profiles ORDER BY updated_at DESC LIMIT 50")
        runs = query("SELECT * FROM research_runs ORDER BY started_at DESC LIMIT 20")
        funnels = query("""SELECT parent_strategy,outcome,COUNT(*) AS count
            FROM research_attempts GROUP BY parent_strategy,outcome ORDER BY parent_strategy,outcome""")
        unique_funnels = query("""SELECT parent_strategy,state,COUNT(*) AS count,
            COUNT(DISTINCT symbol) AS symbols
            FROM research_setups GROUP BY parent_strategy,state
            ORDER BY parent_strategy,state""")
        setups = query("""SELECT setup_id,research_run_id,parent_strategy,symbol,direction,timeframe,
            first_seen,last_seen,state,checks_count,terminal_reason
            FROM research_setups ORDER BY last_seen DESC LIMIT 500""")
        attempts = query("""SELECT attempt_id,setup_id,research_run_id,profile_id,parent_strategy,
            symbol,direction,decision_time,stage,outcome,stop_code,entry,sl,tp1,tp2,terminal_tp,
            rr,fidelity,snapshot_json
            FROM research_attempts ORDER BY decision_time DESC LIMIT 200""")
        attempt_trades = query("""SELECT t.attempt_id,t.track,t.status,t.entry_state,t.entry_time,
            t.exit_time,t.exit_price,t.exit_reason,t.quantity,t.gross_r,t.net_r,t.pnl_pct,
            t.mfe_r,t.mae_r,t.fees_r,t.slippage_r,t.giveback_r,t.targets_reached_json,
            t.ambiguity,t.duration_seconds,t.duration_bars,t.cost_completeness
            FROM research_trades t JOIN research_attempts a ON a.attempt_id=t.attempt_id
            ORDER BY a.decision_time DESC,t.track LIMIT 600""")
        trades = query("""SELECT a.parent_strategy,t.track,t.status,COUNT(*) AS count,
            COUNT(t.net_r) AS resolved_count,AVG(t.net_r) AS expectancy,
            CASE WHEN COUNT(t.net_r)=0 THEN NULL
                 ELSE AVG(CASE WHEN t.net_r>0 THEN 1.0 ELSE 0.0 END)*100 END AS win_rate
            FROM research_trades t JOIN research_attempts a ON a.attempt_id=t.attempt_id
            GROUP BY a.parent_strategy,t.track,t.status ORDER BY a.parent_strategy,t.track,t.status""")
        quality = query("""SELECT severity,issue_type,COUNT(*) AS count FROM market_quality_issues
            WHERE status='OPEN' GROUP BY severity,issue_type ORDER BY count DESC""")
        levels = query("""SELECT timeframe,level_type,status,COUNT(*) AS count,
            COUNT(DISTINCT symbol) AS symbols
            FROM market_levels GROUP BY timeframe,level_type,status
            ORDER BY timeframe,level_type,status""")
        coverage = query("SELECT * FROM market_feature_coverage ORDER BY feature,source LIMIT 300")
        evaluations = query("""SELECT * FROM research_feature_evaluations
            ORDER BY created_at DESC LIMIT 200""")
        checks = query("""SELECT parent_strategy,check_code,label,role,domain,status,COUNT(*) AS count,
            MIN(check_order) AS first_order
            FROM research_attempt_checks c JOIN research_attempts a ON a.attempt_id=c.attempt_id
            GROUP BY parent_strategy,check_code,label,role,domain,status
            ORDER BY parent_strategy,first_order,status""")
        attempt_check_rows = query("""SELECT c.attempt_id,c.check_order,c.check_code,c.label,c.role,
            c.domain,c.status,c.measured_json,c.threshold_json,c.source_timeframe,c.source_as_of,
            c.evidence_json FROM research_attempt_checks c
            JOIN research_attempts a ON a.attempt_id=c.attempt_id
            ORDER BY a.decision_time DESC,c.check_order LIMIT 2400""")
        decision_path_rows = query("""SELECT a.attempt_id,a.parent_strategy,a.symbol,a.direction,
            a.decision_time,a.entry,a.sl,a.tp1,a.tp2,a.terminal_tp,a.rr,
            c.check_order,c.check_code,c.label,c.domain,c.status,c.measured_json,c.threshold_json,
            c.source_timeframe,c.source_as_of
            FROM research_attempts a JOIN research_attempt_checks c ON c.attempt_id=a.attempt_id
            WHERE a.outcome='CANDIDATE' AND c.role='HARD_GATE'
              AND a.research_run_id=(SELECT research_run_id FROM research_runs
                  ORDER BY started_at DESC LIMIT 1)
            ORDER BY a.decision_time DESC,a.attempt_id,c.check_order LIMIT 5000""")
        decision_paths_by_attempt: dict[str, dict[str, Any]] = {}
        for row in decision_path_rows:
            attempt_id = str(row.get("attempt_id") or "")
            path = decision_paths_by_attempt.setdefault(attempt_id, {
                "attempt_id": attempt_id,
                "parent_strategy": row.get("parent_strategy"), "symbol": row.get("symbol"),
                "direction": row.get("direction"), "decision_time": row.get("decision_time"),
                "entry": row.get("entry"), "sl": row.get("sl"), "tp1": row.get("tp1"),
                "tp2": row.get("tp2"), "terminal_tp": row.get("terminal_tp"), "rr": row.get("rr"),
                "steps": [],
            })
            def decoded(field: str) -> Any:
                try:
                    value = json.loads(row.get(field) or "{}")
                    return value.get("value") if isinstance(value, Mapping) and "value" in value else value
                except (TypeError, ValueError, json.JSONDecodeError):
                    return row.get(field)
            path["steps"].append({
                "order": row.get("check_order"), "code": row.get("check_code"),
                "label": row.get("label"), "domain": row.get("domain"), "status": row.get("status"),
                "measured": decoded("measured_json"), "threshold": decoded("threshold_json"),
                "source_timeframe": row.get("source_timeframe"), "source_as_of": row.get("source_as_of"),
            })
        decision_paths = []
        for path in decision_paths_by_attempt.values():
            steps = path["steps"]
            strategy_steps = [x for x in steps if x.get("domain") not in {"DATA", "GEOMETRY"}]
            trigger_steps = [x for x in steps if x.get("domain") in {"TRIGGER", "PARTICIPATION"}]
            path["first_basis"] = (strategy_steps or steps or [None])[0]
            path["final_trigger"] = (trigger_steps or strategy_steps or steps or [None])[-1]
            path["final_validation"] = (steps or [None])[-1]
            decision_paths.append(path)
        parity_rows = query("""SELECT a.parent_strategy,c.measured_json
            FROM research_attempt_checks c JOIN research_attempts a ON a.attempt_id=c.attempt_id
            WHERE c.check_code='SHADOW_REGIME_PARITY' AND a.outcome='CANDIDATE'""")
        parity: dict[str, dict[str, Any]] = {}
        for row in parity_rows:
            strategy = str(row.get("parent_strategy") or "UNKNOWN")
            bucket = parity.setdefault(strategy, {"parent_strategy":strategy,"candidates":0,
                "live_v1_exact":0,"live_v2_family":0,"all_agree":0})
            try:
                measured = json.loads(row.get("measured_json") or "{}")
                value = measured.get("value") or {}
            except (TypeError, ValueError, json.JSONDecodeError):
                value = {}
            bucket["candidates"] += 1
            for field in ("live_v1_exact", "live_v2_family", "all_agree"):
                bucket[field] += int(value.get(field) is True)
        context_parity = []
        for strategy in sorted(parity):
            row = parity[strategy]
            total = max(1, int(row["candidates"]))
            for field in ("live_v1_exact", "live_v2_family", "all_agree"):
                row[field + "_pct"] = round(float(row[field]) / total * 100, 2)
            context_parity.append(row)
        sources = query("SELECT * FROM research_source_registry ORDER BY source")
        context = query("""SELECT source,feature,quality,availability,COUNT(*) AS samples,
            COUNT(DISTINCT symbol) AS symbols,MIN(event_time) AS coverage_start,
            MAX(event_time) AS coverage_end
            FROM market_context_observations
            GROUP BY source,feature,quality,availability
            ORDER BY feature,source""")
        context_recent = query("""SELECT source,symbol,feature,event_time,quality,availability,value_json
            FROM market_context_observations ORDER BY event_time DESC LIMIT 200""")
        active_shadow = query("""SELECT a.parent_strategy,a.symbol,a.direction,a.decision_time,
            t.track,t.status,t.entry,t.initial_sl,t.tp1,t.tp2,t.mfe_r,t.mae_r
            FROM research_trades t JOIN research_attempts a ON a.attempt_id=t.attempt_id
            WHERE t.status='OPEN' ORDER BY a.decision_time DESC LIMIT 100""")
        edge_rows = query("""SELECT a.attempt_id,a.parent_strategy,a.symbol,a.decision_time,
            MAX(CASE WHEN t.track='ACTUAL' THEN t.net_r END) AS actual_r,
            MAX(CASE WHEN t.track='NO_MANAGER' THEN t.net_r END) AS no_manager_r,
            MAX(CASE WHEN t.track='PLAYBOOK_ONLY' THEN t.net_r END) AS playbook_only_r,
            MAX(CASE WHEN t.track='ACTUAL' THEN t.status END) AS actual_status
            FROM research_attempts a JOIN research_trades t ON t.attempt_id=a.attempt_id
            GROUP BY a.attempt_id,a.parent_strategy,a.symbol,a.decision_time
            ORDER BY a.decision_time DESC LIMIT 500""")
        for row in edge_rows:
            actual, no_manager, playbook = row.get("actual_r"), row.get("no_manager_r"), row.get("playbook_only_r")
            row["groq_edge_r"] = actual - no_manager if actual is not None and no_manager is not None else None
            row["groq_vs_rules_edge_r"] = actual - playbook if actual is not None and playbook is not None else None
            row["playbook_edge_r"] = playbook - no_manager if playbook is not None and no_manager is not None else None
        meta = query("SELECT key,value_json,updated_at FROM research_meta")
        api_usage = self.api_usage("GATE_RESEARCH")
        return {"schema_version": SCHEMA_VERSION, "generated_at": utc_now(), "candles": counts,
                "jobs": jobs, "profiles": profiles, "runs": runs, "funnels": funnels,
                "unique_funnels": unique_funnels, "setups": setups,
                "attempts": attempts, "attempt_trades": attempt_trades,
                "trades": trades,
                "quality": quality, "levels": levels, "coverage": coverage, "evaluations": evaluations,
                "active_shadow": active_shadow, "checks": checks,
                "attempt_check_rows": attempt_check_rows, "sources": sources,
                "decision_paths": decision_paths,
                "market_context": context, "market_context_recent": context_recent,
                "context_parity": context_parity,
                "track_edges": edge_rows,
                "api_usage": api_usage,
                "meta": {row["key"]: row["value_json"] for row in meta}}


__all__ = ["ResearchStore", "SCHEMA_VERSION", "canonical", "stable_id", "utc_now"]
