"""Persistent operational control loop for APEX scanners and risk overlays.

This module observes strategy output and closed outcomes.  It never calculates
or edits entry, stop, targets, direction, or strategy confirmation rules.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any


DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.environ.get(
        "APEX_BRAIN_DB_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
    ),
)

_STRATEGIES = ("MTF", "SWING", "ZONE", "FAST", "WYCKOFF")


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_control_schema(db_path: str = DB_PATH) -> None:
    """Install one idempotent schema shared by polling and webhook modes."""
    conn = _connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            scanner TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'RUNNING',
            universe_size INTEGER DEFAULT 0,
            batch_size INTEGER DEFAULT 0,
            pairs_attempted INTEGER DEFAULT 0,
            data_ok INTEGER DEFAULT 0,
            data_failed INTEGER DEFAULT 0,
            filtered INTEGER DEFAULT 0,
            candidates INTEGER DEFAULT 0,
            groq_approve INTEGER DEFAULT 0,
            groq_wait INTEGER DEFAULT 0,
            groq_reject INTEGER DEFAULT 0,
            delivered INTEGER DEFAULT 0,
            active_symbol TEXT,
            error TEXT,
            started_at TEXT DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT,
            heartbeat_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_scan_runs_strategy_started
            ON scan_runs(strategy, started_at DESC);

        CREATE TABLE IF NOT EXISTS scan_pair_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            stage TEXT NOT NULL,
            outcome TEXT NOT NULL,
            reason_code TEXT,
            detail_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_scan_pair_events_run
            ON scan_pair_events(run_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_scan_pair_events_recent
            ON scan_pair_events(strategy, created_at DESC);

        CREATE TABLE IF NOT EXISTS scan_batch_cursors (
            scanner TEXT PRIMARY KEY,
            cursor INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS scan_coverage_rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            universe_size INTEGER NOT NULL DEFAULT 0,
            covered_size INTEGER NOT NULL DEFAULT 0,
            retry_size INTEGER NOT NULL DEFAULT 0,
            started_at TEXT DEFAULT CURRENT_TIMESTAMP,
            completed_at TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_scan_coverage_rounds_strategy
            ON scan_coverage_rounds(strategy,status,id DESC);

        CREATE TABLE IF NOT EXISTS scan_coverage_pairs (
            round_id INTEGER NOT NULL,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'PENDING',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_run_id INTEGER,
            last_outcome TEXT,
            last_reason TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(round_id,symbol)
        );
        CREATE INDEX IF NOT EXISTS idx_scan_coverage_pairs_due
            ON scan_coverage_pairs(round_id,status,attempts,position);

        CREATE TABLE IF NOT EXISTS ltf_watchlist (
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT,
            required_timeframe TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'WAITING',
            reason TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            misses INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_checked_at TEXT,
            expires_at TEXT NOT NULL,
            resolved_at TEXT,
            PRIMARY KEY(strategy,symbol)
        );
        CREATE INDEX IF NOT EXISTS idx_ltf_watchlist_due
            ON ltf_watchlist(state,last_checked_at,expires_at);

        CREATE TABLE IF NOT EXISTS strategy_risk_state (
            strategy TEXT PRIMARY KEY,
            consecutive_losses INTEGER NOT NULL DEFAULT 0,
            consecutive_wins INTEGER NOT NULL DEFAULT 0,
            mode TEXT NOT NULL DEFAULT 'NORMAL',
            groq_min_confidence REAL NOT NULL DEFAULT 0.65,
            live_risk_multiplier REAL NOT NULL DEFAULT 1.0,
            live_paused_until TEXT,
            last_signal_id INTEGER,
            last_result TEXT,
            reason TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS rule_hypotheses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hypothesis_key TEXT UNIQUE NOT NULL,
            strategy TEXT,
            symbol TEXT,
            direction TEXT,
            rule_type TEXT,
            rule_text TEXT NOT NULL,
            source TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'HYPOTHESIS',
            samples INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            confidence REAL NOT NULL DEFAULT 0.0,
            evidence_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT
        );
        """
    )
    for strategy in _STRATEGIES:
        conn.execute(
            "INSERT OR IGNORE INTO strategy_risk_state(strategy) VALUES (?)",
            (strategy,),
        )
    scan_run_columns = {row[1] for row in conn.execute("PRAGMA table_info(scan_runs)")}
    if "round_id" not in scan_run_columns:
        conn.execute("ALTER TABLE scan_runs ADD COLUMN round_id INTEGER")
    coverage_columns = {row[1] for row in conn.execute("PRAGMA table_info(scan_coverage_pairs)")}
    if "last_run_id" not in coverage_columns:
        conn.execute("ALTER TABLE scan_coverage_pairs ADD COLUMN last_run_id INTEGER")
    # A fresh database historically received two incompatible error_patterns
    # definitions.  Keep both operational and trade-pattern columns together.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS error_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            error_type TEXT,
            pattern TEXT,
            symbol TEXT,
            timeframe TEXT,
            conditions TEXT,
            sl_count INTEGER DEFAULT 1,
            count INTEGER DEFAULT 1,
            rule_added TEXT,
            last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
            active INTEGER DEFAULT 1
        )"""
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(error_patterns)")}
    additions = {
        "id": "INTEGER", "error_type": "TEXT", "pattern": "TEXT",
        "symbol": "TEXT", "timeframe": "TEXT", "conditions": "TEXT",
        "sl_count": "INTEGER DEFAULT 1", "count": "INTEGER DEFAULT 1",
        "rule_added": "TEXT", "last_seen": "TEXT", "active": "INTEGER DEFAULT 1",
    }
    for name, definition in additions.items():
        if name not in columns and name != "id":
            conn.execute(f"ALTER TABLE error_patterns ADD COLUMN {name} {definition}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_error_patterns_trade ON error_patterns(symbol, pattern)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_error_patterns_error ON error_patterns(error_type)")
    conn.commit()
    conn.close()


def begin_scan(strategy: str, scanner: str, universe_size: int, batch_size: int, db_path: str = DB_PATH) -> int:
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    cursor = conn.execute(
        """INSERT INTO scan_runs(strategy,scanner,universe_size,batch_size)
           VALUES (?,?,?,?)""",
        (str(strategy).upper(), scanner, int(universe_size), int(batch_size)),
    )
    run_id = int(cursor.lastrowid)
    conn.commit(); conn.close()
    return run_id


def scan_heartbeat(run_id: int, symbol: str = "", db_path: str = DB_PATH) -> None:
    try:
        conn = _connect(db_path)
        conn.execute(
            """UPDATE scan_runs SET active_symbol=?,heartbeat_at=CURRENT_TIMESTAMP,
                      pairs_attempted=pairs_attempted+1 WHERE id=?""",
            (symbol, int(run_id)),
        )
        run = conn.execute("SELECT round_id FROM scan_runs WHERE id=?", (int(run_id),)).fetchone()
        if run and run[0] and symbol:
            conn.execute(
                """UPDATE scan_coverage_pairs
                   SET status='IN_PROGRESS',attempts=attempts+1,last_run_id=?,updated_at=CURRENT_TIMESTAMP
                   WHERE round_id=? AND symbol=?""",
                (int(run_id), int(run[0]), symbol),
            )
        conn.commit(); conn.close()
    except sqlite3.Error:
        return


def set_scan_scope(run_id: int, universe_size: int, batch_size: int, db_path: str = DB_PATH) -> None:
    try:
        conn = _connect(db_path)
        conn.execute(
            "UPDATE scan_runs SET universe_size=?,batch_size=? WHERE id=?",
            (int(universe_size), int(batch_size), int(run_id)),
        )
        conn.commit(); conn.close()
    except sqlite3.Error:
        return


def set_scan_round(run_id: int, round_id: int | None, db_path: str = DB_PATH) -> None:
    try:
        conn = _connect(db_path)
        conn.execute("UPDATE scan_runs SET round_id=? WHERE id=?", (round_id, int(run_id)))
        conn.commit(); conn.close()
    except sqlite3.Error:
        return


def record_scan_event(
    run_id: int, strategy: str, symbol: str, stage: str, outcome: str,
    reason_code: str = "", detail: dict[str, Any] | None = None,
    db_path: str = DB_PATH,
) -> None:
    try:
        conn = _connect(db_path)
        conn.execute(
            """INSERT INTO scan_pair_events
               (run_id,strategy,symbol,stage,outcome,reason_code,detail_json)
               VALUES (?,?,?,?,?,?,?)""",
            (int(run_id), str(strategy).upper(), symbol, stage, outcome, reason_code,
             json.dumps(detail or {}, ensure_ascii=False, default=str)[:4000]),
        )
        field = {
            "DATA_OK": "data_ok", "DATA_FAILED": "data_failed",
            "FILTERED": "filtered", "CANDIDATE": "candidates",
            "GROQ_APPROVE": "groq_approve", "GROQ_WAIT": "groq_wait",
            "GROQ_REJECT": "groq_reject", "DELIVERED": "delivered",
        }.get(str(outcome).upper())
        if field:
            conn.execute(f"UPDATE scan_runs SET {field}={field}+1 WHERE id=?", (int(run_id),))
        run = conn.execute("SELECT round_id FROM scan_runs WHERE id=?", (int(run_id),)).fetchone()
        round_id = int(run[0]) if run and run[0] else None
        normalized_outcome = str(outcome).upper()
        if round_id and symbol:
            if normalized_outcome in {"FILTERED", "CANDIDATE"}:
                conn.execute(
                    """UPDATE scan_coverage_pairs
                       SET status='DONE',last_outcome=?,last_reason=?,
                           updated_at=CURRENT_TIMESTAMP
                       WHERE round_id=? AND symbol=?""",
                    (normalized_outcome, reason_code, round_id, symbol),
                )
            elif normalized_outcome == "DATA_FAILED":
                conn.execute(
                    """UPDATE scan_coverage_pairs
                       SET status='RETRY',last_outcome=?,last_reason=?,
                           updated_at=CURRENT_TIMESTAMP
                       WHERE round_id=? AND symbol=?""",
                    (normalized_outcome, reason_code, round_id, symbol),
                )
            covered, retry, target = conn.execute(
                """SELECT SUM(status='DONE'),SUM(status='RETRY'),COUNT(*)
                   FROM scan_coverage_pairs WHERE round_id=?""",
                (round_id,),
            ).fetchone()
            covered, retry, target = int(covered or 0), int(retry or 0), int(target or 0)
            complete = bool(target and covered == target)
            conn.execute(
                """UPDATE scan_coverage_rounds
                   SET covered_size=?,retry_size=?,status=?,updated_at=CURRENT_TIMESTAMP,
                       completed_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE completed_at END
                   WHERE id=?""",
                (covered, retry, "COMPLETED" if complete else "ACTIVE", int(complete), round_id),
            )
        conn.commit(); conn.close()
    except sqlite3.Error:
        return


def finish_scan(run_id: int, status: str = "COMPLETED", error: str = "", db_path: str = DB_PATH) -> None:
    try:
        conn = _connect(db_path)
        requested = str(status).upper()
        row = conn.execute(
            "SELECT pairs_attempted,batch_size FROM scan_runs WHERE id=?", (int(run_id),)
        ).fetchone()
        if requested == "COMPLETED" and row and int(row[1] or 0) > int(row[0] or 0):
            requested = "PARTIAL"
            if not error:
                error = f"processed {int(row[0] or 0)} of {int(row[1] or 0)} pairs"
        round_row = conn.execute("SELECT round_id FROM scan_runs WHERE id=?", (int(run_id),)).fetchone()
        round_id = int(round_row[0]) if round_row and round_row[0] else None
        if round_id:
            if requested == "COMPLETED":
                conn.execute(
                    """UPDATE scan_coverage_pairs
                       SET status='DONE',last_outcome=COALESCE(last_outcome,'CHECKED'),
                           updated_at=CURRENT_TIMESTAMP
                       WHERE round_id=? AND last_run_id=? AND status='IN_PROGRESS'""",
                    (round_id, int(run_id)),
                )
            else:
                conn.execute(
                    """UPDATE scan_coverage_pairs SET status='PENDING',updated_at=CURRENT_TIMESTAMP
                       WHERE round_id=? AND last_run_id=? AND status='IN_PROGRESS'""",
                    (round_id, int(run_id)),
                )
            covered, retry, target = conn.execute(
                """SELECT SUM(status='DONE'),SUM(status='RETRY'),COUNT(*)
                   FROM scan_coverage_pairs WHERE round_id=?""",
                (round_id,),
            ).fetchone()
            covered, retry, target = int(covered or 0), int(retry or 0), int(target or 0)
            complete = bool(target and covered == target)
            conn.execute(
                """UPDATE scan_coverage_rounds SET covered_size=?,retry_size=?,status=?,
                          updated_at=CURRENT_TIMESTAMP,
                          completed_at=CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE completed_at END
                   WHERE id=?""",
                (covered, retry, "COMPLETED" if complete else "ACTIVE", int(complete), round_id),
            )
        conn.execute(
            """UPDATE scan_runs SET status=?,error=?,active_symbol=NULL,
                      heartbeat_at=CURRENT_TIMESTAMP,completed_at=CURRENT_TIMESTAMP WHERE id=?""",
            (requested, str(error)[:1000], int(run_id)),
        )
        conn.execute("DELETE FROM scan_pair_events WHERE created_at < datetime('now','-14 days')")
        conn.execute("DELETE FROM scan_runs WHERE started_at < datetime('now','-90 days')")
        conn.execute("DELETE FROM scan_coverage_pairs WHERE round_id IN (SELECT id FROM scan_coverage_rounds WHERE started_at < datetime('now','-30 days'))")
        conn.execute("DELETE FROM scan_coverage_rounds WHERE started_at < datetime('now','-30 days')")
        conn.execute("DELETE FROM ltf_watchlist WHERE state IN ('RESOLVED','EXPIRED','INVALIDATED') AND resolved_at < datetime('now','-14 days')")
        conn.commit(); conn.close()
    except sqlite3.Error:
        return


def mark_scan_skipped(scanner: str, active_scanner: str, elapsed: float, db_path: str = DB_PATH) -> None:
    strategy = scanner.replace("auto_scan_", "").replace("auto_", "").replace("_scan", "").upper()
    run_id = begin_scan(strategy, scanner, 0, 0, db_path)
    finish_scan(run_id, "SKIPPED", f"{active_scanner} running for {elapsed:.1f}s", db_path)


def take_persistent_batch(scanner: str, items: list[Any], size: int, db_path: str = DB_PATH) -> list[Any]:
    """Rotating batch whose cursor survives deploys and process restarts."""
    values = list(items)
    if not values:
        return []
    ensure_control_schema(db_path)
    count = min(len(values), max(1, int(size)))
    conn = _connect(db_path)
    row = conn.execute("SELECT cursor FROM scan_batch_cursors WHERE scanner=?", (scanner,)).fetchone()
    start = int(row[0] if row else 0) % len(values)
    result = [values[(start + offset) % len(values)] for offset in range(count)]
    conn.execute(
        """INSERT INTO scan_batch_cursors(scanner,cursor,updated_at) VALUES (?,?,CURRENT_TIMESTAMP)
           ON CONFLICT(scanner) DO UPDATE SET cursor=excluded.cursor,updated_at=CURRENT_TIMESTAMP""",
        (scanner, (start + count) % len(values)),
    )
    conn.commit(); conn.close()
    return result


def take_strategy_round_batch(
    strategy: str, items: list[Any], size: int, db_path: str = DB_PATH,
) -> dict[str, Any]:
    """Take a restart-safe batch from one complete strategy coverage round."""
    values = list(dict.fromkeys(items))
    normalized = _strategy_name(strategy)
    if not values:
        return {"round_id": None, "pairs": [], "covered": 0, "target": 0, "retry": 0}
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    conn.execute("BEGIN IMMEDIATE")
    active = conn.execute(
        "SELECT id FROM scan_coverage_rounds WHERE strategy=? AND status='ACTIVE' ORDER BY id DESC LIMIT 1",
        (normalized,),
    ).fetchone()
    if active:
        round_id = int(active[0])
    else:
        cursor = conn.execute(
            "INSERT INTO scan_coverage_rounds(strategy,universe_size) VALUES (?,?)",
            (normalized, len(values)),
        )
        round_id = int(cursor.lastrowid)
        conn.executemany(
            """INSERT INTO scan_coverage_pairs(round_id,strategy,symbol,position)
               VALUES (?,?,?,?)""",
            [(round_id, normalized, str(symbol), position) for position, symbol in enumerate(values)],
        )
    # A process restart can leave the last pair IN_PROGRESS.  No strategy can
    # own two market-lock runs, so it is safe to return such rows to the queue.
    conn.execute(
        "UPDATE scan_coverage_pairs SET status='PENDING' WHERE round_id=? AND status='IN_PROGRESS'",
        (round_id,),
    )
    count = min(len(values), max(1, int(size)))
    rows = conn.execute(
        """SELECT symbol FROM scan_coverage_pairs
           WHERE round_id=? AND status IN ('PENDING','RETRY')
           ORDER BY attempts ASC,position ASC LIMIT ?""",
        (round_id, count),
    ).fetchall()
    stats = conn.execute(
        """SELECT SUM(status='DONE'),SUM(status='RETRY'),COUNT(*)
           FROM scan_coverage_pairs WHERE round_id=?""",
        (round_id,),
    ).fetchone()
    conn.commit(); conn.close()
    return {
        "round_id": round_id,
        "pairs": [row[0] for row in rows],
        "covered": int(stats[0] or 0),
        "retry": int(stats[1] or 0),
        "target": int(stats[2] or 0),
    }


def upsert_ltf_watch(
    strategy: str, symbol: str, direction: str, required_timeframe: str,
    reason: str, ttl_hours: int, db_path: str = DB_PATH,
) -> None:
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    conn.execute(
        """INSERT INTO ltf_watchlist
           (strategy,symbol,direction,required_timeframe,state,reason,expires_at)
           VALUES (?,?,?,?, 'WAITING', ?, datetime('now', ?))
           ON CONFLICT(strategy,symbol) DO UPDATE SET
             direction=excluded.direction,required_timeframe=excluded.required_timeframe,
             state='WAITING',reason=excluded.reason,expires_at=excluded.expires_at,
             resolved_at=NULL""",
        (_strategy_name(strategy), symbol, direction, required_timeframe, reason, f"+{int(ttl_hours)} hours"),
    )
    conn.commit(); conn.close()


def due_ltf_watches(limit: int = 12, db_path: str = DB_PATH) -> list[dict[str, Any]]:
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    conn.execute(
        """UPDATE ltf_watchlist SET state='EXPIRED',resolved_at=CURRENT_TIMESTAMP
           WHERE state='WAITING' AND expires_at <= CURRENT_TIMESTAMP"""
    )
    rows = conn.execute(
        """SELECT * FROM ltf_watchlist WHERE state='WAITING'
           ORDER BY CASE WHEN last_checked_at IS NULL THEN 0 ELSE 1 END,
                    last_checked_at ASC,created_at ASC LIMIT ?""",
        (max(1, int(limit)),),
    ).fetchall()
    conn.commit(); conn.close()
    return [dict(row) for row in rows]


def touch_ltf_watch(
    strategy: str, symbol: str, result: str, resolved: bool = False,
    db_path: str = DB_PATH,
) -> None:
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    if resolved:
        conn.execute(
            """UPDATE ltf_watchlist SET state='RESOLVED',attempts=attempts+1,
                      reason=?,last_checked_at=CURRENT_TIMESTAMP,resolved_at=CURRENT_TIMESTAMP
               WHERE strategy=? AND symbol=?""",
            (result, _strategy_name(strategy), symbol),
        )
    else:
        conn.execute(
            """UPDATE ltf_watchlist SET attempts=attempts+1,misses=misses+1,
                      reason=?,last_checked_at=CURRENT_TIMESTAMP
               WHERE strategy=? AND symbol=?""",
            (result, _strategy_name(strategy), symbol),
        )
    conn.commit(); conn.close()


def _strategy_name(value: Any) -> str:
    raw = str(value or "UNKNOWN").upper()
    if raw == "FAST_DEAL":
        return "FAST"
    return next((name for name in _STRATEGIES if name in raw), raw)


def rebuild_strategy_risk_states(db_path: str = DB_PATH) -> dict[str, dict[str, Any]]:
    """Rebuild streaks from activated, objectively resolved trades."""
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT s.id,UPPER(COALESCE(NULLIF(s.grade,''),NULLIF(s.signal_type,''),'UNKNOWN')) strategy,
                      lower(s.result) result,COALESCE(s.closed_at,s.created_at) resolved_at
               FROM signals s LEFT JOIN signal_execution_state x ON x.signal_id=s.id
               WHERE lower(s.result) IN ('tp1','tp2','tp3','sl')
                 AND (x.signal_id IS NULL OR x.status IN ('active','closed'))
               ORDER BY strategy,resolved_at DESC,s.id DESC"""
        ).fetchall()
    except sqlite3.Error:
        rows = []
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(_strategy_name(row["strategy"]), []).append(row)
    result: dict[str, dict[str, Any]] = {}
    now = time.time()
    for strategy in _STRATEGIES:
        history = grouped.get(strategy, [])
        losses = wins = 0
        if history:
            first_win = str(history[0]["result"]).startswith("tp")
            for row in history:
                is_win = str(row["result"]).startswith("tp")
                if is_win != first_win:
                    break
                if is_win: wins += 1
                else: losses += 1
        last_ts = 0.0
        if history:
            try:
                last_ts = datetime.fromisoformat(
                    str(history[0]["resolved_at"]).replace("Z", "+00:00")
                ).replace(tzinfo=timezone.utc).timestamp()
            except (TypeError, ValueError):
                last_ts = now
        recent_sequence = bool(last_ts and now - last_ts < 86400)
        mode = (
            "PAUSED" if losses >= 5 and recent_sequence
            else "CAUTION" if losses >= 3 and recent_sequence
            else "NORMAL"
        )
        confidence = 0.75 if mode in {"CAUTION", "PAUSED"} else 0.65
        multiplier = 0.0 if mode == "PAUSED" else 0.5 if mode == "CAUTION" else 1.0
        paused_until = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(last_ts + 86400)) if mode == "PAUSED" else None
        last = history[0] if history else None
        reason = f"{losses} consecutive activated SL" if losses else "normal objective sequence"
        conn.execute(
            """INSERT INTO strategy_risk_state
               (strategy,consecutive_losses,consecutive_wins,mode,groq_min_confidence,
                live_risk_multiplier,live_paused_until,last_signal_id,last_result,reason,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
               ON CONFLICT(strategy) DO UPDATE SET
                consecutive_losses=excluded.consecutive_losses,
                consecutive_wins=excluded.consecutive_wins,mode=excluded.mode,
                groq_min_confidence=excluded.groq_min_confidence,
                live_risk_multiplier=excluded.live_risk_multiplier,
                live_paused_until=excluded.live_paused_until,
                last_signal_id=excluded.last_signal_id,last_result=excluded.last_result,
                reason=excluded.reason,updated_at=CURRENT_TIMESTAMP""",
            (strategy, losses, wins, mode, confidence, multiplier, paused_until,
             int(last["id"]) if last else None, str(last["result"]) if last else None, reason),
        )
        result[strategy] = {
            "strategy": strategy, "consecutive_losses": losses, "consecutive_wins": wins,
            "mode": mode, "groq_min_confidence": confidence,
            "live_risk_multiplier": multiplier, "live_paused_until": paused_until,
            "last_signal_id": int(last["id"]) if last else None,
            "last_result": str(last["result"]) if last else None, "reason": reason,
        }
    conn.commit(); conn.close()
    return result


def strategy_risk_state(strategy: str, db_path: str = DB_PATH, rebuild: bool = True) -> dict[str, Any]:
    normalized = _strategy_name(strategy)
    if rebuild:
        states = rebuild_strategy_risk_states(db_path)
        return states.get(normalized, {"strategy": normalized, "mode": "NORMAL", "groq_min_confidence": 0.65, "live_risk_multiplier": 1.0})
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    row = conn.execute("SELECT * FROM strategy_risk_state WHERE strategy=?", (normalized,)).fetchone()
    conn.close()
    return dict(row) if row else {"strategy": normalized, "mode": "NORMAL", "groq_min_confidence": 0.65, "live_risk_multiplier": 1.0}


def scanner_dashboard(db_path: str = DB_PATH) -> dict[str, Any]:
    ensure_control_schema(db_path)
    conn = _connect(db_path)
    runs = []
    for strategy in _STRATEGIES:
        row = conn.execute(
            "SELECT * FROM scan_runs WHERE strategy=? ORDER BY id DESC LIMIT 1", (strategy,)
        ).fetchone()
        run = dict(row) if row else {"strategy": strategy, "status": "NEVER"}
        coverage = conn.execute(
            """SELECT id,status,universe_size,covered_size,retry_size,started_at,completed_at
               FROM scan_coverage_rounds WHERE strategy=? ORDER BY id DESC LIMIT 1""",
            (strategy,),
        ).fetchone()
        if coverage:
            run.update({f"round_{key}": value for key, value in dict(coverage).items()})
        runs.append(run)
    reasons = conn.execute(
        """SELECT strategy,COALESCE(NULLIF(reason_code,''),'UNSPECIFIED') reason_code,COUNT(*) count
           FROM scan_pair_events WHERE created_at >= datetime('now','-24 hours')
             AND outcome IN ('FILTERED','DATA_FAILED')
           GROUP BY strategy,COALESCE(NULLIF(reason_code,''),'UNSPECIFIED')
           ORDER BY count DESC LIMIT 12"""
    ).fetchall()
    risk = conn.execute("SELECT * FROM strategy_risk_state ORDER BY strategy").fetchall()
    watches = conn.execute(
        """SELECT strategy,symbol,direction,required_timeframe,state,reason,attempts,
                  created_at,last_checked_at,expires_at
           FROM ltf_watchlist WHERE state='WAITING'
             AND expires_at > CURRENT_TIMESTAMP
           ORDER BY created_at ASC LIMIT 20"""
    ).fetchall()
    watch_counts = conn.execute(
        """SELECT strategy,required_timeframe,COUNT(*) count
           FROM ltf_watchlist WHERE state='WAITING' AND expires_at > CURRENT_TIMESTAMP
           GROUP BY strategy,required_timeframe ORDER BY strategy,required_timeframe"""
    ).fetchall()
    conn.close()
    return {
        "runs": runs,
        "reasons": [dict(row) for row in reasons],
        "risk": [dict(row) for row in risk],
        "watches": [dict(row) for row in watches],
        "watch_counts": [dict(row) for row in watch_counts],
    }


def finish_latest_running(scanner: str, status: str, error: str = "", db_path: str = DB_PATH) -> None:
    """Close an orphan RUNNING row after a wrapper timeout or cancellation."""
    try:
        conn = _connect(db_path)
        row = conn.execute(
            "SELECT id FROM scan_runs WHERE scanner=? AND status='RUNNING' ORDER BY id DESC LIMIT 1",
            (scanner,),
        ).fetchone()
        conn.close()
        if row:
            finish_scan(int(row[0]), status, error, db_path)
    except sqlite3.Error:
        return
