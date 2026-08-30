"""Read-only Telegram dashboard views over APEX operational data."""

from __future__ import annotations

import html
import json
import sqlite3
from collections import defaultdict
from typing import Any


def _price(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "—"
    if number >= 100:
        return f"{number:.2f}"
    if number >= 1:
        return f"{number:.4f}"
    return f"{number:.8f}".rstrip("0").rstrip(".")


def fetch_watchlist(db_path: str, limit: int = 20) -> list[dict[str, Any]]:
    """Return only persisted candidates; never manufacture future trades."""
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    items: list[dict[str, Any]] = []
    try:
        queued = conn.execute(
            """SELECT symbol,direction,timeframe,entry,sl,tp1,tp2,grade,timing_score,
                      created_at,expires_at
               FROM timing_queue
               WHERE status='waiting' AND (expires_at IS NULL OR expires_at >= CURRENT_TIMESTAMP)
               ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        items.extend({**dict(row), "state": "timing"} for row in queued)
    except sqlite3.Error:
        pass
    try:
        waiting_entry = conn.execute(
            """SELECT s.symbol,s.direction,s.timeframe,s.entry,s.sl,s.tp1,s.tp2,s.grade,
                      NULL timing_score,s.created_at,NULL expires_at
               FROM signals s JOIN signal_execution_state x ON x.signal_id=s.id
               WHERE x.status='waiting_entry' AND lower(COALESCE(s.result,'pending'))='pending'
               ORDER BY s.created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        existing = {(item["symbol"], item["direction"], item["timeframe"]) for item in items}
        for row in waiting_entry:
            item = {**dict(row), "state": "entry"}
            key = (item["symbol"], item["direction"], item["timeframe"])
            if key not in existing:
                items.append(item)
                existing.add(key)
    except sqlite3.Error:
        pass
    try:
        groq_waits = conn.execute(
            """SELECT symbol,direction,timeframe,strategy,evidence_json,created_at
               FROM strategy_decisions
               WHERE outcome='WAIT' AND stage='groq_quality_gate'
                 AND created_at >= datetime('now','-12 hours')
               ORDER BY created_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        existing = {(item["symbol"], item["direction"], item["timeframe"]) for item in items}
        for row in groq_waits:
            try:
                evidence = json.loads(row["evidence_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                evidence = {}
            levels = evidence.get("candidate") if isinstance(evidence.get("candidate"), dict) else {}
            item = {
                "symbol": row["symbol"], "direction": row["direction"],
                "timeframe": row["timeframe"], "grade": row["strategy"],
                "entry": levels.get("entry"), "sl": levels.get("sl"),
                "tp1": levels.get("tp1"), "tp2": levels.get("tp2"),
                "timing_score": None, "created_at": row["created_at"],
                "expires_at": None, "state": "groq_wait",
            }
            key = (item["symbol"], item["direction"], item["timeframe"])
            if key not in existing:
                items.append(item)
                existing.add(key)
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return items[:limit]


def format_watchlist(items: list[dict[str, Any]]) -> str:
    if not items:
        return (
            "👀 <b>Наблюдаемые</b>\n\n"
            "Сейчас подтверждённых кандидатов в ожидании нет.\n\n"
            "<i>Сканер продолжает работать автоматически. Здесь появятся только "
            "реальные сетапы, которым ещё нужен тайминг или касание entry.</i>"
        )
    lines = ["👀 <b>Наблюдаемые кандидаты</b>", ""]
    for item in items:
        direction = str(item.get("direction", "")).upper()
        icon = "🟢" if direction == "BULLISH" else "🔴"
        if item.get("state") == "entry":
            label = "ожидает касания entry"
        elif item.get("state") == "groq_wait":
            label = "технический кандидат · Groq WAIT"
        else:
            label = "ожидает подтверждения тайминга"
        score = item.get("timing_score")
        score_text = f" · тайминг {score}/3" if score is not None else ""
        lines.extend([
            f"{icon} <b>{html.escape(str(item.get('symbol', '—')))}</b> · "
            f"{html.escape(str(item.get('grade') or '—'))} · {html.escape(str(item.get('timeframe') or '—'))}",
            f"{html.escape(label)}{score_text}",
            f"Entry <code>{_price(item.get('entry'))}</code> · SL <code>{_price(item.get('sl'))}</code> · "
            f"TP1 <code>{_price(item.get('tp1'))}</code>",
            "",
        ])
    lines.append("<i>Наблюдение не является открытой сделкой и не запускает автоторговлю.</i>")
    return "\n".join(lines)[:4000]


def fetch_strategy_stats(db_path: str) -> list[dict[str, Any]]:
    """Count only objectively resolved, activated/legacy-active signals."""
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """SELECT UPPER(COALESCE(NULLIF(s.grade,''),NULLIF(s.signal_type,''),'UNKNOWN')) strategy,
                      UPPER(COALESCE(s.direction,'UNKNOWN')) direction,
                      lower(s.result) result
               FROM signals s
               LEFT JOIN signal_execution_state x ON x.signal_id=s.id
               WHERE lower(s.result) IN ('tp1','tp2','tp3','sl')
                 AND (x.signal_id IS NULL OR x.status IN ('active','closed'))"""
        ).fetchall()
    except sqlite3.Error:
        rows = []
    finally:
        conn.close()
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"tp1": 0, "tp2": 0, "tp3": 0, "sl": 0, "long": 0, "short": 0}
    )
    for row in rows:
        stats = grouped[str(row["strategy"])]
        stats[str(row["result"])] += 1
        if row["direction"] == "BULLISH":
            stats["long"] += 1
        elif row["direction"] == "BEARISH":
            stats["short"] += 1
    result = []
    preferred = ["MTF", "SWING", "ZONE", "FAST", "WYCKOFF"]
    for strategy in preferred + sorted(name for name in grouped if name not in preferred):
        stats = grouped.get(strategy, {"tp1": 0, "tp2": 0, "tp3": 0, "sl": 0, "long": 0, "short": 0})
        closed = stats["tp1"] + stats["tp2"] + stats["tp3"] + stats["sl"]
        wins = stats["tp1"] + stats["tp2"] + stats["tp3"]
        result.append({"strategy": strategy, **stats, "closed": closed,
                       "win_rate": wins / closed * 100 if closed else None})
    return result


def format_strategy_stats(rows: list[dict[str, Any]], min_samples: int = 30) -> str:
    lines = ["📊 <b>Результаты по стратегиям</b>", "",
             "Учитываются только сделки, которые достигли entry и закрылись по TP/SL.", ""]
    for row in rows:
        samples = int(row["closed"])
        wr = f"{row['win_rate']:.1f}%" if row["win_rate"] is not None else "—"
        status = "достаточно для первичной оценки" if samples >= min_samples else f"нужно ещё {min_samples - samples}"
        lines.extend([
            f"<b>{html.escape(str(row['strategy']))}</b> · закрыто {samples} · WR <b>{wr}</b>",
            f"✅ TP1 {row['tp1']} · TP2 {row['tp2']} · TP3 {row['tp3']} · 🛑 SL {row['sl']}",
            f"LONG {row['long']} · SHORT {row['short']} · <i>{status}</i>",
            "",
        ])
    lines.append("<i>Win rate не является гарантией будущей доходности.</i>")
    return "\n".join(lines)[:4000]


def fetch_system_health(db_path: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=20)
    result = {"gate_total": 0, "gate_candles": 0, "open_errors": 0,
              "groq_24h": 0, "groq_last": None}
    try:
        result["gate_total"] = conn.execute(
            "SELECT COUNT(*) FROM external_pair_registry WHERE gate_status='supported'"
        ).fetchone()[0]
        result["gate_candles"] = conn.execute(
            "SELECT COUNT(*) FROM external_pair_registry WHERE gate_candles_status='available'"
        ).fetchone()[0]
    except sqlite3.Error:
        pass
    try:
        result["open_errors"] = conn.execute("SELECT COUNT(*) FROM bot_errors WHERE fixed=0").fetchone()[0]
    except sqlite3.Error:
        pass
    try:
        row = conn.execute(
            """SELECT COUNT(*),MAX(created_at) FROM strategy_decisions
               WHERE stage='groq_quality_gate' AND created_at >= datetime('now','-24 hours')"""
        ).fetchone()
        result["groq_24h"], result["groq_last"] = row[0], row[1]
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return result
