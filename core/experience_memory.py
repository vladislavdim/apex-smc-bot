"""Persistent counterfactual memory and shadow execution for APEX.

The module is deliberately downstream of strategy calculations.  It records
immutable candidate levels and observes what the market did afterwards.  It
never creates a signal or modifies direction, entry, stop, targets, or RR.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable


DB_PATH = os.environ.get(
    "APEX_DB_PATH",
    os.environ.get(
        "APEX_BRAIN_DB_PATH",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db"),
    ),
)

MIN_DISCOVERY_SAMPLES = 30
MIN_PROBATION_SAMPLES = 20
MIN_UPLIFT = 0.10
RULE_TTL_DAYS = 90
_TERMINAL = {"TP1", "SL", "EXPIRED", "INVALID"}


def _connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def ensure_experience_schema(db_path: str = DB_PATH) -> None:
    conn = _connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS experience_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT UNIQUE NOT NULL,
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            direction TEXT NOT NULL,
            regime TEXT NOT NULL DEFAULT 'UNKNOWN',
            entry REAL NOT NULL,
            sl REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL,
            tp3 REAL,
            rr REAL,
            decision TEXT NOT NULL DEFAULT 'PENDING',
            decision_reason TEXT,
            groq_confidence REAL,
            status TEXT NOT NULL DEFAULT 'WAITING_ENTRY',
            snapshot_json TEXT NOT NULL DEFAULT '{}',
            external_json TEXT NOT NULL DEFAULT '{}',
            detected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT,
            entry_at TEXT,
            closed_at TEXT,
            outcome TEXT,
            mfe_price REAL NOT NULL DEFAULT 0,
            mae_price REAL NOT NULL DEFAULT 0,
            mfe_r REAL NOT NULL DEFAULT 0,
            mae_r REAL NOT NULL DEFAULT 0,
            last_candle_ts TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_experience_open
          ON experience_candidates(status, detected_at);
        CREATE INDEX IF NOT EXISTS idx_experience_stats
          ON experience_candidates(strategy, regime, direction, outcome);

        CREATE TABLE IF NOT EXISTS experience_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            detail_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_experience_events_candidate
          ON experience_events(candidate_id, created_at);

        CREATE TABLE IF NOT EXISTS experience_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_key TEXT UNIQUE NOT NULL,
            strategy TEXT NOT NULL,
            regime TEXT NOT NULL,
            direction TEXT NOT NULL,
            rule_kind TEXT NOT NULL DEFAULT 'UNDECIDED',
            rule_text TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'HYPOTHESIS',
            samples INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            baseline_rate REAL NOT NULL DEFAULT 0,
            observed_rate REAL NOT NULL DEFAULT 0,
            wilson_lower REAL NOT NULL DEFAULT 0,
            probation_start_candidate_id INTEGER,
            probation_samples INTEGER NOT NULL DEFAULT 0,
            probation_wins INTEGER NOT NULL DEFAULT 0,
            probation_losses INTEGER NOT NULL DEFAULT 0,
            version INTEGER NOT NULL DEFAULT 1,
            reason TEXT,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT,
            activated_at TEXT,
            rolled_back_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_experience_rules_state
          ON experience_rules(state, strategy);

        CREATE TABLE IF NOT EXISTS experience_rule_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            old_state TEXT,
            new_state TEXT NOT NULL,
            reason TEXT NOT NULL,
            evidence_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit(); conn.close()


def _strategy(candidate: dict[str, Any]) -> str:
    scan = str(candidate.get("scan_type") or "").upper()
    grade = str(candidate.get("grade") or candidate.get("signal_type") or "MTF").upper()
    aliases = {"FAST_DEAL": "FAST", "FAST": "FAST", "SWING": "SWING", "ZONE": "ZONE",
               "WYCKOFF": "WYCKOFF", "MTF": "MTF"}
    return aliases.get(scan, aliases.get(grade, "MTF"))


def _json(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, default=str, separators=(",", ":"))[:30000]


def _parse_time(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        raw = float(value)
        return raw / 1000 if raw > 10_000_000_000 else raw
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(
            tzinfo=timezone.utc
        ).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _iso(ts: float | None = None) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() if ts is None else ts))


def capture_candidate(candidate: dict[str, Any], db_path: str = DB_PATH) -> int | None:
    """Persist one immutable technical candidate, idempotently per in-memory candidate."""
    ensure_experience_schema(db_path)
    if candidate.get("_experience_id"):
        return int(candidate["_experience_id"])
    try:
        entry, sl = float(candidate.get("entry") or 0), float(candidate.get("sl") or 0)
        tp1 = float(candidate.get("tp1", candidate.get("tp")) or 0)
        if min(entry, sl, tp1) <= 0 or entry == sl:
            return None
        strategy = _strategy(candidate)
        detected = str(candidate.get("detected_at") or _iso())
        material = "|".join(map(str, (
            strategy, candidate.get("symbol"), candidate.get("direction"), candidate.get("timeframe"),
            round(entry, 12), round(sl, 12), round(tp1, 12), detected,
        )))
        fingerprint = hashlib.sha256(material.encode()).hexdigest()
        hours = int(candidate.get("estimated_hours") or {
            "FAST": 4, "MTF": 72, "SWING": 120, "ZONE": 72, "WYCKOFF": 504,
        }.get(strategy, 72))
        expires = _iso(_parse_time(detected) + max(1, hours) * 3600)
        snapshot = {
            key: value for key, value in candidate.items()
            if not key.startswith("_") and key not in {"text"}
        }
        conn = _connect(db_path)
        cursor = conn.execute(
            """INSERT OR IGNORE INTO experience_candidates
               (fingerprint,symbol,strategy,timeframe,direction,regime,entry,sl,tp1,tp2,tp3,rr,
                snapshot_json,detected_at,expires_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (fingerprint, str(candidate.get("symbol") or "").upper(), strategy,
             str(candidate.get("timeframe") or "1h"), str(candidate.get("direction") or "").upper(),
             str(candidate.get("regime") or "UNKNOWN").upper(), entry, sl, tp1,
             float(candidate.get("tp2") or tp1), float(candidate.get("tp3") or candidate.get("tp2") or tp1),
             float(candidate.get("rr") or 0), _json(snapshot), detected, expires),
        )
        row = conn.execute("SELECT id FROM experience_candidates WHERE fingerprint=?", (fingerprint,)).fetchone()
        experience_id = int(row[0]) if row else int(cursor.lastrowid or 0)
        if cursor.rowcount:
            conn.execute(
                "INSERT INTO experience_events(candidate_id,event_type,detail_json) VALUES (?,?,?)",
                (experience_id, "CANDIDATE_CAPTURED", _json({"strategy": strategy})),
            )
        conn.commit(); conn.close()
        candidate["_experience_id"] = experience_id
        return experience_id or None
    except (sqlite3.Error, TypeError, ValueError):
        return None


def record_decision(
    candidate: dict[str, Any], decision: str, reason: str = "",
    review: dict[str, Any] | None = None, db_path: str = DB_PATH,
) -> None:
    experience_id = capture_candidate(candidate, db_path)
    if not experience_id:
        return
    try:
        review = review or {}
        external = review.get("context") if isinstance(review.get("context"), dict) else {}
        confidence = review.get("confidence")
        conn = _connect(db_path)
        conn.execute(
            """UPDATE experience_candidates SET decision=?,decision_reason=?,groq_confidence=?,
                      external_json=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (str(decision).upper(), str(reason)[:2000],
             float(confidence) if confidence not in (None, "") else None,
             _json(external), experience_id),
        )
        conn.execute(
            "INSERT INTO experience_events(candidate_id,event_type,detail_json) VALUES (?,?,?)",
            (experience_id, "DECISION", _json({"decision": decision, "reason": reason,
                                                "confidence": confidence})),
        )
        conn.commit(); conn.close()
    except (sqlite3.Error, TypeError, ValueError):
        return


def _candle(candle: Any) -> dict[str, float]:
    if isinstance(candle, dict):
        ts_value = candle.get("timestamp", candle.get("time", candle.get("open_time", 0)))
        open_value, high_value = candle.get("open", 0), candle.get("high", 0)
        low_value, close_value = candle.get("low", 0), candle.get("close", 0)
    else:
        values = list(candle)
        ts_value, open_value, high_value, low_value, close_value = values[:5]
    return {"ts": _parse_time(ts_value), "open": float(open_value), "high": float(high_value),
            "low": float(low_value), "close": float(close_value)}


def apply_candles_to_candidate(row: dict[str, Any], candles: Iterable[Any]) -> dict[str, Any]:
    """Pure conservative shadow fill/outcome reducer used by runtime and tests."""
    state = dict(row)
    status = str(state.get("status") or "WAITING_ENTRY")
    entry, sl, tp1 = float(state["entry"]), float(state["sl"]), float(state["tp1"])
    bullish = str(state.get("direction")).upper() == "BULLISH"
    risk = abs(entry - sl)
    last_seen = _parse_time(state.get("last_candle_ts"))
    detected = _parse_time(state.get("detected_at"))
    for raw in candles:
        bar = _candle(raw)
        # Counterfactual ordering is invalid without a candle timestamp.
        if not bar["ts"]:
            continue
        if bar["ts"] <= last_seen or bar["ts"] < detected:
            continue
        state["last_candle_ts"] = _iso(bar["ts"])
        if status == "WAITING_ENTRY":
            if not (bar["low"] <= entry <= bar["high"]):
                continue
            status = "ACTIVE"
            state["status"], state["entry_at"] = status, _iso(bar["ts"] or time.time())
        stop_hit = bar["low"] <= sl if bullish else bar["high"] >= sl
        take_hit = bar["high"] >= tp1 if bullish else bar["low"] <= tp1
        favorable = max(0.0, bar["high"] - entry) if bullish else max(0.0, entry - bar["low"])
        adverse = max(0.0, entry - bar["low"]) if bullish else max(0.0, bar["high"] - entry)
        if stop_hit:
            # Intrabar path is unknown.  A stopped bar receives no favourable
            # excursion credit from prices that may have printed afterwards.
            favorable = 0.0
            adverse = max(adverse, risk)
        state["mfe_price"] = max(float(state.get("mfe_price") or 0), favorable)
        state["mae_price"] = max(float(state.get("mae_price") or 0), adverse)
        state["mfe_r"] = state["mfe_price"] / risk if risk else 0.0
        state["mae_r"] = state["mae_price"] / risk if risk else 0.0
        # A bar containing both levels is unknowable without lower-timeframe
        # ordering.  Resolve against the strategy, never in its favour.
        if stop_hit:
            status, state["outcome"] = "CLOSED", "SL"
        elif take_hit:
            status, state["outcome"] = "CLOSED", "TP1"
        if status == "CLOSED":
            state["status"] = status
            state["closed_at"] = _iso(bar["ts"] or time.time())
            break
    state["status"] = status
    return state


def refresh_shadow_positions(
    get_candles: Callable[[str, str, int], list[Any]], db_path: str = DB_PATH,
    now_ts: float | None = None,
) -> dict[str, int]:
    ensure_experience_schema(db_path)
    now_ts = time.time() if now_ts is None else now_ts
    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT * FROM experience_candidates WHERE status IN ('WAITING_ENTRY','ACTIVE') ORDER BY id"
    ).fetchall()
    conn.close()
    result = {"checked": 0, "activated": 0, "closed": 0, "expired": 0, "errors": 0}
    for raw_row in rows:
        row = dict(raw_row)
        try:
            if row["status"] == "WAITING_ENTRY" and _parse_time(row.get("expires_at")) < now_ts:
                updated = {**row, "status": "EXPIRED", "outcome": "EXPIRED", "closed_at": _iso(now_ts)}
                result["expired"] += 1
            elif row["status"] == "ACTIVE" and now_ts > (
                _parse_time(row.get("expires_at"))
                + 2 * max(3600, _parse_time(row.get("expires_at")) - _parse_time(row.get("detected_at")))
            ):
                updated = {**row, "status": "EXPIRED", "outcome": "EXPIRED", "closed_at": _iso(now_ts)}
                result["expired"] += 1
            else:
                candles = get_candles(row["symbol"], row["timeframe"], 300) or []
                updated = apply_candles_to_candidate(row, candles)
            result["checked"] += 1
            if row["status"] != "ACTIVE" and updated["status"] == "ACTIVE":
                result["activated"] += 1
            if row["status"] != "CLOSED" and updated["status"] == "CLOSED":
                result["closed"] += 1
            conn = _connect(db_path)
            conn.execute(
                """UPDATE experience_candidates SET status=?,entry_at=?,closed_at=?,outcome=?,
                   mfe_price=?,mae_price=?,mfe_r=?,mae_r=?,last_candle_ts=?,updated_at=CURRENT_TIMESTAMP
                   WHERE id=?""",
                (updated.get("status"), updated.get("entry_at"), updated.get("closed_at"),
                 updated.get("outcome"), updated.get("mfe_price", 0), updated.get("mae_price", 0),
                 updated.get("mfe_r", 0), updated.get("mae_r", 0), updated.get("last_candle_ts"), row["id"]),
            )
            if row["status"] != updated.get("status"):
                conn.execute(
                    "INSERT INTO experience_events(candidate_id,event_type,detail_json) VALUES (?,?,?)",
                    (row["id"], str(updated.get("status")), _json({"outcome": updated.get("outcome")})),
                )
            conn.commit(); conn.close()
        except Exception:
            result["errors"] += 1
    evaluate_rule_lifecycle(db_path, now_ts)
    _cleanup(db_path)
    return result


def wilson_lower(successes: int, samples: int, z: float = 1.0) -> float:
    if samples <= 0:
        return 0.0
    p = successes / samples
    denominator = 1 + z * z / samples
    centre = p + z * z / (2 * samples)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * samples)) / samples)
    return max(0.0, (centre - margin) / denominator)


def _transition(conn: sqlite3.Connection, row: sqlite3.Row, state: str, reason: str,
                evidence: dict[str, Any]) -> None:
    if row["state"] == state:
        return
    conn.execute(
        """UPDATE experience_rules SET state=?,reason=?,evidence_json=?,version=version+1,
           updated_at=CURRENT_TIMESTAMP,
           activated_at=CASE WHEN ?='ACTIVE' THEN CURRENT_TIMESTAMP ELSE activated_at END,
           rolled_back_at=CASE WHEN ?='ROLLED_BACK' THEN CURRENT_TIMESTAMP ELSE rolled_back_at END
           WHERE id=?""",
        (state, reason, _json(evidence), state, state, row["id"]),
    )
    conn.execute(
        """INSERT INTO experience_rule_audit(rule_id,old_state,new_state,reason,evidence_json)
           VALUES (?,?,?,?,?)""",
        (row["id"], row["state"], state, reason, _json(evidence)),
    )


def evaluate_rule_lifecycle(db_path: str = DB_PATH, now_ts: float | None = None) -> None:
    """Build broad regime hypotheses and promote only out-of-sample survivors."""
    ensure_experience_schema(db_path)
    now_ts = time.time() if now_ts is None else now_ts
    conn = _connect(db_path)
    groups = conn.execute(
        """SELECT strategy,regime,direction,COUNT(*) samples,
                  SUM(CASE WHEN outcome='TP1' THEN 1 ELSE 0 END) wins,
                  SUM(CASE WHEN outcome='SL' THEN 1 ELSE 0 END) losses,
                  MAX(id) max_id
           FROM experience_candidates WHERE status='CLOSED' AND outcome IN ('TP1','SL')
           GROUP BY strategy,regime,direction"""
    ).fetchall()
    baselines = {
        row["strategy"]: (int(row["wins"] or 0), int(row["samples"] or 0))
        for row in conn.execute(
            """SELECT strategy,COUNT(*) samples,SUM(CASE WHEN outcome='TP1' THEN 1 ELSE 0 END) wins
               FROM experience_candidates WHERE status='CLOSED' AND outcome IN ('TP1','SL') GROUP BY strategy"""
        )
    }
    for group in groups:
        strategy, regime, direction = group["strategy"], group["regime"], group["direction"]
        samples, wins, losses = int(group["samples"]), int(group["wins"]), int(group["losses"])
        base_wins, base_n = baselines.get(strategy, (0, 0))
        base_wr = base_wins / base_n if base_n else 0.5
        key = hashlib.sha256(f"{strategy}|{regime}|{direction}".encode()).hexdigest()
        conn.execute(
            """INSERT OR IGNORE INTO experience_rules
               (rule_key,strategy,regime,direction,rule_text,state,expires_at)
               VALUES (?,?,?,?,?,'HYPOTHESIS',?)""",
            (key, strategy, regime, direction,
             f"{strategy} {direction} in {regime} regime",
             _iso(now_ts + RULE_TTL_DAYS * 86400)),
        )
        row = conn.execute("SELECT * FROM experience_rules WHERE rule_key=?", (key,)).fetchone()
        if not row:
            continue
        if row["state"] in {"REJECTED", "ROLLED_BACK", "EXPIRED"}:
            continue
        evidence = {"samples": samples, "wins": wins, "losses": losses,
                    "baseline_win_rate": base_wr, "win_rate": wins / samples,
                    "loss_rate": losses / samples}
        conn.execute(
            """UPDATE experience_rules SET samples=?,wins=?,losses=?,baseline_rate=?,
               observed_rate=?,wilson_lower=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (samples, wins, losses, base_wr, wins / samples,
             wilson_lower(wins, samples), row["id"]),
        )
        if row["state"] == "HYPOTHESIS":
            _transition(conn, row, "OBSERVING", "first objective activated outcome", evidence)
            row = conn.execute("SELECT * FROM experience_rules WHERE id=?", (row["id"],)).fetchone()
        if row["state"] == "OBSERVING" and samples >= MIN_DISCOVERY_SAMPLES:
            win_rate, loss_rate = wins / samples, losses / samples
            positive = wins >= 20 and win_rate >= base_wr + MIN_UPLIFT and wilson_lower(wins, samples) >= base_wr
            base_loss = 1 - base_wr
            protective = losses >= 18 and loss_rate >= base_loss + MIN_UPLIFT and wilson_lower(losses, samples) >= base_loss
            conn.execute(
                """UPDATE experience_rules SET samples=?,wins=?,losses=?,baseline_rate=?,
                   observed_rate=?,wilson_lower=?,updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                (samples, wins, losses, base_wr if positive else base_loss,
                 win_rate if positive else loss_rate,
                 wilson_lower(wins if positive else losses, samples), row["id"]),
            )
            if positive or protective:
                # Only one concurrent experiment per strategy keeps attribution possible.
                busy = conn.execute(
                    "SELECT 1 FROM experience_rules WHERE strategy=? AND state='PROBATION' AND id<>?",
                    (strategy, row["id"]),
                ).fetchone()
                if not busy:
                    conn.execute(
                        "UPDATE experience_rules SET rule_kind=?,probation_start_candidate_id=? WHERE id=?",
                        ("CONFIRM" if positive else "AVOID", int(group["max_id"]), row["id"]),
                    )
                    _transition(conn, row, "PROBATION", "30-sample discovery threshold passed", evidence)
        row = conn.execute("SELECT * FROM experience_rules WHERE id=?", (row["id"],)).fetchone()
        if row["state"] in {"PROBATION", "ACTIVE"}:
            start_id = int(row["probation_start_candidate_id"] or 0)
            probation = conn.execute(
                """SELECT COUNT(*) samples,SUM(CASE WHEN outcome='TP1' THEN 1 ELSE 0 END) wins,
                          SUM(CASE WHEN outcome='SL' THEN 1 ELSE 0 END) losses
                   FROM experience_candidates WHERE id>? AND strategy=? AND regime=? AND direction=?
                     AND status='CLOSED' AND outcome IN ('TP1','SL')""",
                (start_id, strategy, regime, direction),
            ).fetchone()
            pn, pw, pl = int(probation["samples"] or 0), int(probation["wins"] or 0), int(probation["losses"] or 0)
            conn.execute(
                "UPDATE experience_rules SET probation_samples=?,probation_wins=?,probation_losses=? WHERE id=?",
                (pn, pw, pl, row["id"]),
            )
            target_success = pw if row["rule_kind"] == "CONFIRM" else pl
            opposing = pl if row["rule_kind"] == "CONFIRM" else pw
            retained = pn and target_success / pn >= float(row["baseline_rate"] or 0) + MIN_UPLIFT
            if row["state"] == "PROBATION" and pn >= MIN_PROBATION_SAMPLES:
                _transition(conn, row, "ACTIVE" if retained else "ROLLED_BACK",
                            "out-of-sample probation completed", {"samples": pn, "target": target_success})
            elif row["state"] == "ACTIVE" and (opposing >= 5 and not retained):
                _transition(conn, row, "ROLLED_BACK", "objective contradiction sequence", {"samples": pn})
        if _parse_time(row["expires_at"]) < now_ts and row["state"] in {"HYPOTHESIS", "OBSERVING"}:
            _transition(conn, row, "EXPIRED", "insufficient fresh evidence", evidence)
    conn.commit(); conn.close()


def active_rule_evidence(strategy: str, regime: str = "", db_path: str = DB_PATH) -> str:
    ensure_experience_schema(db_path)
    conn = _connect(db_path)
    rows = conn.execute(
        """SELECT rule_kind,rule_text,observed_rate,baseline_rate,probation_samples,state
           FROM experience_rules WHERE strategy=? AND state IN ('PROBATION','ACTIVE')
             AND (?='' OR regime=?) ORDER BY state='ACTIVE' DESC,updated_at DESC LIMIT 5""",
        (str(strategy).upper(), str(regime).upper(), str(regime).upper()),
    ).fetchall()
    conn.close()
    if not rows:
        return "no_validated_experience_rules"
    return "\n".join(
        f"{row['state']} {row['rule_kind']}: {row['rule_text']} "
        f"observed={float(row['observed_rate']):.1%} baseline={float(row['baseline_rate']):.1%}"
        for row in rows
    )


def experience_dashboard(db_path: str = DB_PATH) -> dict[str, Any]:
    ensure_experience_schema(db_path)
    conn = _connect(db_path)
    funnel = [dict(row) for row in conn.execute(
        """SELECT strategy,COUNT(*) candidates,
           SUM(CASE WHEN decision='APPROVE' THEN 1 ELSE 0 END) approve,
           SUM(CASE WHEN decision='WAIT' THEN 1 ELSE 0 END) wait,
           SUM(CASE WHEN decision='REJECT' THEN 1 ELSE 0 END) reject,
           SUM(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END) active,
           SUM(CASE WHEN outcome='TP1' THEN 1 ELSE 0 END) wins,
           SUM(CASE WHEN outcome='SL' THEN 1 ELSE 0 END) losses
           FROM experience_candidates GROUP BY strategy ORDER BY strategy"""
    )]
    active = [dict(row) for row in conn.execute(
        """SELECT symbol,strategy,direction,timeframe,entry,sl,tp1,mfe_r,mae_r,decision,entry_at
           FROM experience_candidates WHERE status='ACTIVE' ORDER BY entry_at DESC LIMIT 12"""
    )]
    rules = [dict(row) for row in conn.execute(
        """SELECT strategy,regime,direction,rule_kind,state,samples,wins,losses,
                  probation_samples,probation_wins,probation_losses,reason
           FROM experience_rules ORDER BY state='ACTIVE' DESC,updated_at DESC LIMIT 15"""
    )]
    conn.close()
    return {"funnel": funnel, "active": active, "rules": rules}


def _cleanup(db_path: str) -> None:
    try:
        conn = _connect(db_path)
        conn.execute("DELETE FROM experience_events WHERE created_at < datetime('now','-180 days')")
        conn.execute(
            """DELETE FROM experience_candidates WHERE status IN ('EXPIRED','CLOSED')
               AND closed_at < datetime('now','-730 days')"""
        )
        conn.commit(); conn.close()
    except sqlite3.Error:
        return
