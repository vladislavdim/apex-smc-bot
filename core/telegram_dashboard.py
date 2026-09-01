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


def format_scanner_dashboard(data: dict[str, Any]) -> str:
    """Compact proof that every strategy is scanning and where candidates stop."""
    lines = ["📡 <b>Радар · Сканеры и контроль</b>", ""]
    labels = {"COMPLETED": "✅", "RUNNING": "🔄", "PARTIAL": "◐", "SKIPPED": "⏭", "TIMEOUT": "⏱",
              "ERROR": "⚠️", "CANCELLED": "⏹", "NEVER": "▫️"}
    for run in data.get("runs", []):
        status = str(run.get("status") or "NEVER").upper()
        strategy = html.escape(str(run.get("strategy") or "—"))
        lines.append(
            f"{labels.get(status, '•')} <b>{strategy}</b> · {html.escape(status)} · "
            f"партия {int(run.get('pairs_attempted') or 0)}/{int(run.get('batch_size') or 0)}"
        )
        round_target = int(run.get("round_universe_size") or 0)
        if round_target:
            lines.append(
                f"   круг {int(run.get('round_covered_size') or 0)}/{round_target}"
                + (f" · повтор данных {int(run.get('round_retry_size') or 0)}" if int(run.get("round_retry_size") or 0) else "")
            )
        if status == "RUNNING" and run.get("active_symbol"):
            lines.append(f"   сейчас: <code>{html.escape(str(run['active_symbol']))}</code>")
        lines.append(
            f"   кандидаты {int(run.get('candidates') or 0)} · Groq "
            f"✅{int(run.get('groq_approve') or 0)} "
            f"⏳{int(run.get('groq_wait') or 0)} "
            f"🚫{int(run.get('groq_reject') or 0)} · отправлено {int(run.get('delivered') or 0)}"
        )
    watches = data.get("watches", [])
    if watches:
        lines.extend(["", "<b>Младшие ТФ · наблюдение</b>"])
        for item in watches[:10]:
            direction = str(item.get("direction") or "").upper()
            icon = "🟢" if direction == "BULLISH" else "🔴" if direction == "BEARISH" else "👁"
            lines.append(
                f"{icon} <code>{html.escape(str(item.get('symbol') or '—'))}</code> · "
                f"{html.escape(str(item.get('strategy') or '—'))} → "
                f"{html.escape(str(item.get('required_timeframe') or '—'))} · "
                f"проверок {int(item.get('attempts') or 0)}"
            )
    else:
        lines.extend(["", "<b>Младшие ТФ · наблюдение</b>", "Сейчас пар в ожидании подтверждения нет."])
    lines.extend(["", "<b>Риск по стратегиям</b>"])
    for state in data.get("risk", []):
        mode = str(state.get("mode") or "NORMAL")
        icon = "🟢" if mode == "NORMAL" else "🟡" if mode == "CAUTION" else "🔴"
        lines.append(
            f"{icon} {html.escape(str(state.get('strategy') or '—'))}: {mode} · "
            f"SL подряд {int(state.get('consecutive_losses') or 0)} · "
            f"риск ×{float(state.get('live_risk_multiplier') or 0):.1f}"
        )
    reasons = data.get("reasons", [])
    if reasons:
        lines.extend(["", "<b>Главные причины за 24ч</b>"])
        for row in reasons[:8]:
            lines.append(
                f"• {html.escape(str(row.get('strategy') or '—'))}: "
                f"{html.escape(str(row.get('reason_code') or 'UNSPECIFIED'))} ×{int(row.get('count') or 0)}"
            )
    lines.extend(["", "<i>Панель показывает фактические проходы, а не расписание.</i>"])
    return "\n".join(lines)[:4000]


def format_experience_dashboard(data: dict[str, Any]) -> str:
    lines = ["🧠 <b>Experience / Shadow</b>", ""]
    funnel = data.get("funnel", [])
    if not funnel:
        lines.extend(["Кандидатов пока нет.",
                      "<i>Память начнёт наблюдение с первого технического сетапа.</i>"])
    else:
        lines.append("<b>Воронка по стратегиям</b>")
        for row in funnel:
            lines.append(
                f"• <b>{html.escape(str(row.get('strategy') or '—'))}</b>: "
                f"кандидаты {int(row.get('candidates') or 0)} · "
                f"✅{int(row.get('approve') or 0)} ⏳{int(row.get('wait') or 0)} "
                f"🚫{int(row.get('reject') or 0)} · shadow активны {int(row.get('active') or 0)} · "
                f"TP/SL {int(row.get('wins') or 0)}/{int(row.get('losses') or 0)}"
            )
    active = data.get("active", [])
    if active:
        lines.extend(["", "<b>Виртуально активные</b>"])
        for row in active[:8]:
            icon = "🟢" if str(row.get("direction")).upper() == "BULLISH" else "🔴"
            lines.append(
                f"{icon} {html.escape(str(row.get('symbol') or '—'))} · "
                f"{html.escape(str(row.get('strategy') or '—'))} · "
                f"MFE {float(row.get('mfe_r') or 0):.2f}R / MAE {float(row.get('mae_r') or 0):.2f}R"
            )
    rules = data.get("rules", [])
    if rules:
        lines.extend(["", "<b>Автоматические гипотезы</b>"])
        icons = {"OBSERVING": "👁", "PROBATION": "🧪", "ACTIVE": "✅",
                 "ROLLED_BACK": "↩️", "EXPIRED": "⌛", "HYPOTHESIS": "💭"}
        for row in rules[:10]:
            state = str(row.get("state") or "HYPOTHESIS")
            if state == "PROBATION":
                progress = f"{int(row.get('probation_samples') or 0)}/20"
            else:
                progress = f"{int(row.get('samples') or 0)}/30"
            lines.append(
                f"{icons.get(state, '•')} {html.escape(str(row.get('strategy') or '—'))} "
                f"{html.escape(str(row.get('regime') or 'UNKNOWN'))} · {state} · {progress}"
            )
    lines.extend(["", "<i>Shadow-наблюдение не открывает ордера и не меняет уровни сделок.</i>"])
    return "\n".join(lines)[:4000]


def format_setup_evidence_dashboard(data: dict[str, Any]) -> str:
    """Explain causal setup classes; intentionally contains no control buttons."""
    lines = ["🧭 <b>Качество сетапов</b>", ""]
    summary = data.get("summary", [])
    if not summary:
        lines.extend([
            "Завершённых причинных оценок пока нет.",
            "<i>Записи появятся после первого технического кандидата.</i>",
        ])
    else:
        grouped: dict[str, dict[str, int]] = defaultdict(dict)
        for row in summary:
            grouped[str(row.get("strategy") or "—")][str(row.get("state") or "—")] = int(row.get("count") or 0)
        lines.append(f"<b>За {int(data.get('hours') or 24)}ч</b>")
        icons = {"INVALID": "⛔", "DEVELOPING": "🟡", "VALID": "🔵", "STRONG": "🟢", "EXCEPTIONAL": "💎"}
        for strategy, states in grouped.items():
            values = " ".join(f"{icons.get(state, '•')}{state} {count}" for state, count in states.items())
            lines.append(f"• <b>{html.escape(strategy)}</b>: {html.escape(values)}")

    recent = data.get("recent", [])
    if recent:
        lines.extend(["", "<b>Последние кандидаты</b>"])
        icons = {"INVALID": "⛔", "DEVELOPING": "🟡", "VALID": "🔵", "STRONG": "🟢", "EXCEPTIONAL": "💎"}
        for row in recent[:10]:
            state = str(row.get("state") or "—")
            direction = str(row.get("direction") or "").upper()
            arrow = "LONG" if direction == "BULLISH" else "SHORT" if direction == "BEARISH" else direction
            lines.append(
                f"{icons.get(state, '•')} <code>{html.escape(str(row.get('symbol') or '—'))}</code> · "
                f"{html.escape(str(row.get('strategy') or '—'))} {html.escape(arrow)} · <b>{html.escape(state)}</b>"
            )
            try:
                assessment = json.loads(row.get("assessment_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                assessment = {}
            dimensions = assessment.get("dimensions") if isinstance(assessment.get("dimensions"), dict) else {}
            if dimensions:
                lines.append(
                    "   Основа {context_quality} · Триггер {trigger_quality} · "
                    "Подтв. {confirmation_quality} · Вход {entry_quality} · Риск {conflict_risk}".format(
                        **{key: html.escape(str(dimensions.get(key) or "—")) for key in (
                            "context_quality", "trigger_quality", "confirmation_quality", "entry_quality", "conflict_risk"
                        )}
                    )
                )
            missing = assessment.get("missing") if isinstance(assessment.get("missing"), list) else []
            if missing:
                lines.append(f"   ждёт: {html.escape('; '.join(str(x) for x in missing[:2]))}")
    lines.extend(["", "<i>Класс задаёт целостность цепочки, а не число индикаторов. Уровни сделки здесь не изменяются.</i>"])
    return "\n".join(lines)[:4000]


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


def fetch_groq_rejections(db_path: str, hours: int = 24, limit: int = 30) -> dict[str, Any]:
    """Read recent final Groq REJECT decisions without affecting trading state."""
    conn = sqlite3.connect(db_path, timeout=20)
    conn.row_factory = sqlite3.Row
    result: dict[str, Any] = {"hours": hours, "total": 0, "by_strategy": [], "recent": []}
    try:
        period = f"-{int(hours)} hours"
        rows = conn.execute(
            """SELECT symbol,strategy,timeframe,direction,reason,groq_confidence,created_at
               FROM strategy_decisions
               WHERE outcome='REJECT' AND stage='groq_quality_gate'
                 AND created_at >= datetime('now', ?)
               ORDER BY created_at DESC LIMIT ?""",
            (period, int(limit)),
        ).fetchall()
        result["recent"] = [dict(row) for row in rows]
        total = conn.execute(
            """SELECT COUNT(*) FROM strategy_decisions
               WHERE outcome='REJECT' AND stage='groq_quality_gate'
                 AND created_at >= datetime('now', ?)""",
            (period,),
        ).fetchone()[0]
        result["total"] = int(total or 0)
        grouped = conn.execute(
            """SELECT UPPER(COALESCE(NULLIF(strategy,''),'UNKNOWN')) strategy,
                      COALESCE(NULLIF(reason,''),'без причины') reason,
                      COUNT(*) count
               FROM strategy_decisions
               WHERE outcome='REJECT' AND stage='groq_quality_gate'
                 AND created_at >= datetime('now', ?)
               GROUP BY UPPER(COALESCE(NULLIF(strategy,''),'UNKNOWN')),
                        COALESCE(NULLIF(reason,''),'без причины')
               ORDER BY strategy, count DESC""",
            (period,),
        ).fetchall()
        result["by_strategy"] = [dict(row) for row in grouped]
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return result


def format_groq_rejections(data: dict[str, Any]) -> str:
    hours = int(data.get("hours", 24) or 24)
    total = int(data.get("total", 0) or 0)
    if total <= 0:
        return (
            f"🚫 <b>Отказы Groq · {hours}ч</b>\n\n"
            "За этот период финальных REJECT нет.\n\n"
            "<i>Здесь отображаются только кандидаты, которые дошли до финального Groq quality gate.</i>"
        )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data.get("by_strategy", []):
        grouped[str(row.get("strategy") or "UNKNOWN")].append(row)

    lines = [f"🚫 <b>Отказы Groq · {hours}ч</b>", f"Всего: <b>{total}</b>", ""]
    preferred = ["MTF", "SWING", "ZONE", "FAST", "WYCKOFF"]
    for strategy in preferred + sorted(name for name in grouped if name not in preferred):
        rows = grouped.get(strategy)
        if not rows:
            continue
        strategy_total = sum(int(row.get("count", 0) or 0) for row in rows)
        lines.append(f"<b>{html.escape(strategy)}</b> · {strategy_total} шт")
        for row in rows[:5]:
            reason = html.escape(str(row.get("reason") or "без причины"))
            if len(reason) > 170:
                reason = reason[:167] + "…"
            lines.append(f"• {int(row.get('count', 0) or 0)}× {reason}")
        lines.append("")

    recent = data.get("recent", [])
    if recent:
        lines.append("<b>Последние:</b>")
        for row in recent[:10]:
            icon = "🟢" if str(row.get("direction", "")).upper() == "BULLISH" else "🔴"
            reason = html.escape(str(row.get("reason") or "без причины"))
            if len(reason) > 120:
                reason = reason[:117] + "…"
            confidence = row.get("groq_confidence")
            conf = f" · {float(confidence):.0%}" if confidence not in (None, "") else ""
            lines.append(
                f"{icon} <b>{html.escape(str(row.get('symbol') or '—'))}</b> · "
                f"{html.escape(str(row.get('strategy') or '—'))}{conf}\n{reason}"
            )
    lines.extend(["", "<i>Это журнал AI-отказов, а не открытые сделки.</i>"])
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
        # Every final quality-gate call is persisted here, including APPROVE.
        # strategy_decisions only contains the WAIT/REJECT branches.
        row = conn.execute(
            """SELECT COUNT(*),MAX(created_at) FROM ai_signal_reviews
               WHERE created_at >= datetime('now','-24 hours')"""
        ).fetchone()
        result["groq_24h"], result["groq_last"] = row[0], row[1]
    except sqlite3.Error:
        # Compatibility with databases created before ai_signal_reviews existed.
        try:
            row = conn.execute(
                """SELECT COUNT(*),MAX(created_at) FROM strategy_decisions
                   WHERE stage='groq_quality_gate'
                     AND created_at >= datetime('now','-24 hours')"""
            ).fetchone()
            result["groq_24h"], result["groq_last"] = row[0], row[1]
        except sqlite3.Error:
            pass
    finally:
        conn.close()
    return result
