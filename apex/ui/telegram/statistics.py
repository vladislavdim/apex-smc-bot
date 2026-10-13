"""General Telegram statistics over backed-up local databases, no web service."""
from __future__ import annotations

import html
import sqlite3
from datetime import datetime, timedelta, timezone

from apex.db.connection import connect_compatibility
from apex.telemetry.scan_statistics import FIELDS

STRATEGIES = ("FAST", "MTF", "SWING", "ZONE", "WYCKOFF")
PERIODS = {"today": "Сегодня", "7d": "7 дней", "30d": "30 дней", "all": "Всё время"}


def general_statistics(config, period="all", strategy="ALL", *, now=None, runtime=None):
    if period not in PERIODS or strategy not in ("ALL", *STRATEGIES):
        raise ValueError("invalid_statistics_filter")
    current = now or datetime.now(timezone.utc)
    days = {"today": 0, "7d": 6, "30d": 29}
    start = (current.astimezone(timezone.utc).date() - timedelta(days=days[period])).isoformat() if period != "all" else None
    scans, trades, issues = [], [], []
    since = None
    conn = None
    try:
        conn = connect_compatibility(config.database.compatibility_db_path, read_only=True)
        where, params = ["1=1"], []
        if start:
            where.append("day>=?"); params.append(start)
        if strategy != "ALL":
            where.append("strategy=?"); params.append(strategy)
        since = conn.execute("SELECT MIN(day) FROM scan_statistics_daily").fetchone()[0]
        columns = ",".join(f"SUM({name}) AS {name}" for name in FIELDS)
        scans = [dict(row) for row in conn.execute(
            f"SELECT strategy,{columns} FROM scan_statistics_daily WHERE " +
            " AND ".join(where) + " GROUP BY strategy ORDER BY strategy", params)]
    except sqlite3.Error:
        issues.append("История проверок временно недоступна")
    finally:
        if conn is not None:
            conn.close()
    conn = None
    try:
        conn = connect_compatibility(config.database.memory_db_path, read_only=True)
        where, params = ["1=1"], []
        if start:
            where.append("julianday(closed_at)>=julianday(?)"); params.append(start)
        if strategy != "ALL":
            where.append("strategy=?"); params.append(strategy)
        trades = [dict(row) for row in conn.execute("SELECT strategy,COUNT(*) AS closed,"
            "SUM(net_r>0) AS wins,SUM(net_r<0) AS losses,SUM(net_r) AS net_r,"
            "SUM(fees) AS fees,SUM(funding) AS funding FROM live_trade_outcomes WHERE " +
            " AND ".join(where) + " GROUP BY strategy ORDER BY strategy", params)]
    except sqlite3.Error:
        issues.append("Подтверждённые результаты временно недоступны")
    finally:
        if conn is not None:
            conn.close()
    return {"period":period,"strategy":strategy,"scans":scans,"trades":trades,
            "since":since,"issues":issues,"runtime":runtime or {}}


def format_general_statistics(data):
    period = PERIODS[data["period"]]
    lines = ["📊 <b>Общая статистика APEX</b>",
             f"<b>{period}</b> · {html.escape('Все стратегии' if data['strategy']=='ALL' else data['strategy'])} · UTC", ""]
    if data.get("since"):
        lines.append(f"Сохранившаяся история с {html.escape(str(data['since']))}")
    for issue in data.get("issues", []):
        lines.append(f"⚠️ {html.escape(issue)}")
    totals = {name: sum(int(row.get(name) or 0) for row in data['scans']) for name in FIELDS}
    reviews = sum(totals[name] for name in ('groq_approve','groq_wait','groq_reject'))
    if not any('История' in issue for issue in data.get('issues', [])):
        lines.extend(["", f"Проверок пар: <b>{totals['pairs_attempted']}</b>",
            f"Найдено кандидатов: <b>{totals['candidates']}</b>",
            f"Проверок Groq: <b>{reviews}</b>",
            f"✅ Одобрено {totals['groq_approve']} · ⏳ Ожидают {totals['groq_wait']} · 🚫 Отказано {totals['groq_reject']}",
            f"Отправлено сигналов: <b>{totals['delivered']}</b>",
            f"Данные: успешно {totals['data_ok']} · ошибки {totals['data_failed']} · отсеяно {totals['filtered']}"])
        lines.extend(["", "<b>По стратегиям</b>"])
        by_strategy = {row['strategy']:row for row in data['scans']}
        for strategy in STRATEGIES if data['strategy']=='ALL' else (data['strategy'],):
            row = by_strategy.get(strategy, {})
            lines.extend([f"<b>{strategy}</b>: проверок {int(row.get('pairs_attempted') or 0)} · кандидатов {int(row.get('candidates') or 0)}",
                f"  Groq ✅{int(row.get('groq_approve') or 0)} ⏳{int(row.get('groq_wait') or 0)} 🚫{int(row.get('groq_reject') or 0)} · сигналов {int(row.get('delivered') or 0)}"])
    lines.extend(["", "<b>Подтверждённые live-сделки</b>"])
    count = sum(row['closed'] for row in data['trades'])
    if data['trades']:
        wins, losses = (sum(int(row[key] or 0) for row in data['trades']) for key in ('wins','losses'))
        net = sum(float(row['net_r'] or 0) for row in data['trades'])
        lines.append(f"Закрыто {count} · прибыльных {wins} · убыточных {losses}")
        lines.append(f"Итог net: <b>{net:+.2f}R</b> · средний {net/count:+.2f}R")
        for row in data['trades']:
            lines.append(f"{html.escape(row['strategy'])}: N={row['closed']} · net {float(row['net_r'] or 0):+.2f}R")
    elif not any('результаты' in issue for issue in data.get('issues', [])):
        lines.append("Подтверждённых закрытых сделок за период пока нет.")
    lines.extend(["", "<i>Повторные проходы пар и проверки Groq считаются отдельно. Результаты сделок включают только подтверждённые fills, комиссии и funding.</i>"])
    return '\n'.join(lines)


def statistics_keyboard(button, markup, period='all', strategy='ALL'):
    return markup(inline_keyboard=[
        [button(text=label,callback_data=f'stats:{key}:{strategy}') for key,label in PERIODS.items()],
        [button(text='Все' if label=='ALL' else label,callback_data=f'stats:{period}:{label}') for label in ('ALL','FAST','MTF')],
        [button(text=label,callback_data=f'stats:{period}:{label}') for label in ('SWING','ZONE','WYCKOFF')],
        [button(text='🔄 Обновить',callback_data=f'stats:{period}:{strategy}'),
         button(text='📡 Радар',callback_data='menu_scanners')],
        [button(text='🚫 Причины отказов',callback_data='menu_groq_rejections'),
         button(text='📚 Обучение',callback_data='menu_live_learning')],
        [button(text='🔙 Меню',callback_data='menu_back')],
    ])
