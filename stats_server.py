"""Protected read-only APEX strategy statistics service.

This process is deployed separately from the Telegram polling worker but lives in
the same repository/project. It never imports or executes trading code. Passive
telemetry is accepted at /ingest and persisted in Postgres.
"""
from __future__ import annotations

import hmac
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg2
import psycopg2.extras

from core.strategy_catalog import STRATEGY_CATALOG

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "").strip()
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "").strip()
PORT = int(os.environ.get("PORT", "10000"))


def _connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(DATABASE_URL, connect_timeout=8)


def ensure_schema() -> None:
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS apex_stats_events (
                event_key TEXT PRIMARY KEY, kind TEXT NOT NULL, strategy TEXT, symbol TEXT,
                occurred_at TIMESTAMPTZ NOT NULL, payload JSONB NOT NULL,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_apex_stats_recent ON apex_stats_events(occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_apex_stats_lookup ON apex_stats_events(strategy,symbol,occurred_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_apex_stats_kind ON apex_stats_events(kind,occurred_at DESC)")
    finally:
        conn.close()


def _safe_event(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    key, kind = str(raw.get("event_key") or "")[:100], str(raw.get("kind") or "")[:40]
    if not key or not kind:
        return None
    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    return {"event_key": key, "kind": kind, "strategy": str(raw.get("strategy") or "")[:40].upper(),
            "symbol": str(raw.get("symbol") or "")[:40].upper(),
            "occurred_at": str(raw.get("occurred_at") or datetime.now(timezone.utc).isoformat())[:64],
            "payload": payload}


def ingest(raw: Any) -> int:
    items = raw if isinstance(raw, list) else [raw]
    events = [x for x in (_safe_event(v) for v in items[:500]) if x]
    if not events:
        return 0
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            for e in events:
                cur.execute("""INSERT INTO apex_stats_events(event_key,kind,strategy,symbol,occurred_at,payload)
                    VALUES (%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT(event_key) DO UPDATE SET kind=EXCLUDED.kind,strategy=EXCLUDED.strategy,
                    symbol=EXCLUDED.symbol,occurred_at=EXCLUDED.occurred_at,payload=EXCLUDED.payload,received_at=NOW()""",
                    (e["event_key"], e["kind"], e["strategy"], e["symbol"], e["occurred_at"],
                     json.dumps(e["payload"], ensure_ascii=False, default=str)))
            cur.execute("DELETE FROM apex_stats_events WHERE occurred_at < NOW() - INTERVAL '95 days'")
    finally:
        conn.close()
    return len(events)


def _num(value: Any) -> float | None:
    try: return float(value)
    except (TypeError, ValueError): return None


def _reason(text: str) -> str:
    text = " ".join(str(text or "").split())
    text = re.sub(r"\b\d+(?:\.\d+)?%", "#%", text)
    return text[:260] or "без причины"


def _fetch(days: int, strategy: str, symbol: str, from_date: str = "", to_date: str = "") -> list[dict[str, Any]]:
    days = max(1, min(int(days), 30)); where: list[str] = []; params: list[Any] = []
    if from_date and re.fullmatch(r"\d{4}-\d{2}-\d{2}", from_date): where.append("occurred_at >= %s::date"); params.append(from_date)
    else: where.append("occurred_at >= NOW() - (%s * INTERVAL '1 day')"); params.append(days)
    if to_date and re.fullmatch(r"\d{4}-\d{2}-\d{2}", to_date): where.append("occurred_at < (%s::date + INTERVAL '1 day')"); params.append(to_date)
    if strategy: where.append("strategy=%s"); params.append(strategy.upper())
    if symbol: where.append("symbol=%s"); params.append(symbol.upper())
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT event_key,kind,strategy,symbol,occurred_at,payload FROM apex_stats_events WHERE " + " AND ".join(where) + " ORDER BY occurred_at DESC LIMIT 50000", params)
            rows = cur.fetchall()
    finally: conn.close()
    return [{"event_key": r["event_key"], "kind": r["kind"], "strategy": r["strategy"], "symbol": r["symbol"],
             "occurred_at": r["occurred_at"].isoformat(), "payload": r["payload"] if isinstance(r["payload"], dict) else {}} for r in rows]


def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
                    min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",
                    page: int = 1, page_size: int = 100) -> dict[str, Any]:
    events = _fetch(days, strategy, symbol, from_date, to_date); attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]; trade_events=[]
    for e in events:
        p=e["payload"]; key=str(p.get("attempt_key") or "")
        if e["kind"]=="attempt":
            row=dict(p); row.setdefault("attempt_key",e["event_key"]); row.setdefault("strategy",e["strategy"]); row.setdefault("symbol",e["symbol"]); row["occurred_at"]=e["occurred_at"]; attempts.append(row)
        elif e["kind"]=="groq_review" and key: reviews[key]={**p,"occurred_at":e["occurred_at"]}
        elif e["kind"]=="decision" and key: decisions[key].append({**p,"occurred_at":e["occurred_at"]})
        elif e["kind"]=="scan_event": scan_events.append({**p,"occurred_at":e["occurred_at"]})
        elif e["kind"]=="trade_event": trade_events.append({**p,"strategy":e["strategy"],"symbol":e["symbol"],"occurred_at":e["occurred_at"]})
    joined=[]
    for a in attempts:
        key=str(a.get("attempt_key") or ""); c=a.get("candidate") if isinstance(a.get("candidate"),dict) else {}; stop=a.get("stop") if isinstance(a.get("stop"),dict) else {}; snap=stop.get("snapshot") if isinstance(stop.get("snapshot"),dict) else {}
        rr=_num(c.get("rr")); rr=rr if rr is not None else _num(snap.get("rr") or snap.get("rr_check") or snap.get("_wy_rr") or snap.get("_wyd_rr"))
        row={**a,"groq_review":reviews.get(key),"decisions":sorted(decisions.get(key,[]),key=lambda x:x.get("occurred_at","")),"rr_value":rr}
        row["near_setup"]=bool(a.get("outcome")=="FILTERED" and snap.get("entry") is not None and snap.get("sl") is not None)
        joined.append(row)
    if outcome: joined=[r for r in joined if str(r.get("outcome") or "").upper()==outcome.upper()]
    if groq: joined=[r for r in joined if str((r.get("groq_review") or {}).get("decision") or "").upper()==groq.upper()]
    if min_rr is not None: joined=[r for r in joined if r.get("rr_value") is not None and float(r["rr_value"])>=min_rr]
    if max_rr is not None: joined=[r for r in joined if r.get("rr_value") is not None and float(r["rr_value"])<=max_rr]
    failures=Counter(); by_strategy=defaultdict(Counter); checks=defaultdict(Counter); groq_reasons=Counter(); groq_risks=Counter(); groq_counts=Counter(); conf=defaultdict(list)
    for r in joined:
        name=str(r.get("strategy") or "UNKNOWN"); stop=r.get("stop") if isinstance(r.get("stop"),dict) else {}
        if r.get("outcome")=="FILTERED": label=str(stop.get("label") or stop.get("code") or "UNLABELED"); failures[label]+=1; by_strategy[name][label]+=1
        for ch in r.get("checks",[]) if isinstance(r.get("checks"),list) else []:
            if isinstance(ch,dict):
                label=str(ch.get("label") or ch.get("condition") or ch.get("code") or "check")[:300]
                checks[(name,label)][str(ch.get("state") or "UNKNOWN").upper()]+=1
        g=r.get("groq_review") or {}
        if g:
            d=str(g.get("decision") or "UNKNOWN").upper(); groq_counts[d]+=1
            v=_num(g.get("confidence"));
            if v is not None: conf[d].append(v)
            if d in {"WAIT","REJECT"}:
                for x in g.get("reasons",[]) if isinstance(g.get("reasons"),list) else []: groq_reasons[_reason(str(x))]+=1
                for x in g.get("risks",[]) if isinstance(g.get("risks"),list) else []: groq_risks[_reason(str(x))]+=1
    opened=[t for t in trade_events if str(t.get("action") or "").upper()=="OPEN"]
    closed=[t for t in trade_events if str(t.get("action") or "").upper()=="CLOSE"]
    wins=[t for t in closed if str(t.get("result") or "").lower() in {"tp1","tp2","tp3"}]
    losses=[t for t in closed if str(t.get("result") or "").lower()=="sl"]
    pnl_vals=[float(t["pnl_pct"]) for t in closed if _num(t.get("pnl_pct")) is not None]
    r_vals=[float(t["realized_r"]) for t in closed if _num(t.get("realized_r")) is not None]
    trade_by_strategy=defaultdict(lambda:{"opened":0,"closed":0,"wins":0,"losses":0,"pnl_pct":0.0,"r_sum":0.0,"r_n":0})
    for t in opened:
        trade_by_strategy[str(t.get("strategy") or "UNKNOWN")]["opened"]+=1
    for t in closed:
        st=str(t.get("strategy") or "UNKNOWN"); d=trade_by_strategy[st]; d["closed"]+=1
        res=str(t.get("result") or "").lower()
        if res in {"tp1","tp2","tp3"}: d["wins"]+=1
        elif res=="sl": d["losses"]+=1
        pv=_num(t.get("pnl_pct")); rv=_num(t.get("realized_r"))
        if pv is not None: d["pnl_pct"]+=pv
        if rv is not None: d["r_sum"]+=rv; d["r_n"]+=1
    trade_rows=[]
    for st,d in sorted(trade_by_strategy.items()):
        decided=d["wins"]+d["losses"]
        trade_rows.append({"strategy":st,"opened":d["opened"],"closed":d["closed"],"wins":d["wins"],"losses":d["losses"],
            "win_rate":round(d["wins"]/decided*100,1) if decided else None,"pnl_pct":round(d["pnl_pct"],3),
            "avg_r":round(d["r_sum"]/d["r_n"],3) if d["r_n"] else None})
    trade_stats={"opened":len(opened),"closed":len(closed),"wins":len(wins),"losses":len(losses),
        "win_rate":round(len(wins)/(len(wins)+len(losses))*100,1) if (wins or losses) else None,
        "pnl_pct":round(sum(pnl_vals),3) if pnl_vals else 0.0,"avg_pnl_pct":round(sum(pnl_vals)/len(pnl_vals),3) if pnl_vals else None,
        "avg_r":round(sum(r_vals)/len(r_vals),3) if r_vals else None,"by_strategy":trade_rows,
        "recent":sorted(closed,key=lambda x:x.get("occurred_at",""),reverse=True)[:50]}

    total=len(joined); page_size=max(20,min(int(page_size),200)); page=max(1,int(page)); start=(page-1)*page_size
    reviews_n=sum(groq_counts.values()); delivered=sum(1 for r in joined if any(str(d.get("stage") or "").lower()=="delivered" or str(d.get("outcome") or "").upper()=="ACCEPT" for d in r.get("decisions",[])))
    return {"period_days":days,"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
      "summary":{"attempts":total,"candidates":sum(r.get("outcome")=="CANDIDATE" for r in joined),"near_setups":sum(bool(r.get("near_setup")) for r in joined),"groq_total":reviews_n,"groq_approve":groq_counts.get("APPROVE",0),"groq_wait":groq_counts.get("WAIT",0),"groq_reject":groq_counts.get("REJECT",0),"delivered":delivered,"scan_events":len(scan_events)},
      "strategy_counts":dict(Counter(str(r.get("strategy") or "UNKNOWN") for r in joined)),
      "failures":[{"label":k,"count":v} for k,v in failures.most_common(30)],
      "failures_by_strategy":{k:[{"label":a,"count":b} for a,b in v.most_common(30)] for k,v in by_strategy.items()},
      "criterion_stats":[{"strategy":k[0],"label":k[1],"pass":v.get("PASS",0),"fail":v.get("FAIL",0),"total":v.get("PASS",0)+v.get("FAIL",0)} for k,v in sorted(checks.items(),key=lambda x:sum(x[1].values()),reverse=True)[:300]],
      "groq":{"decisions":dict(groq_counts),"reasons":[{"label":k,"count":v} for k,v in groq_reasons.most_common(30)],"risks":[{"label":k,"count":v} for k,v in groq_risks.most_common(30)],"avg_confidence":{k:(sum(v)/len(v) if v else None) for k,v in conf.items()}},
      "trade_stats":trade_stats,"catalog":STRATEGY_CATALOG,"rows":joined[start:start+page_size],"pagination":{"page":page,"page_size":page_size,"total":total,"pages":max(1,(total+page_size-1)//page_size)}}


HTML=r'''<!doctype html><html lang=ru><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><meta name=robots content="noindex,nofollow"><meta name=referrer content=no-referrer><title>APEX Strategy Lab</title><style>
:root{color-scheme:dark;--bg:#0c0e12;--card:#151921;--muted:#8e98a8;--line:#29303c;--text:#eef2f7;--good:#4fd18b;--bad:#ff6b78;--warn:#f0c760;--accent:#77a7ff}*{box-sizing:border-box}body{margin:0;font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:var(--bg);color:var(--text)}main{max-width:1500px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}.title{font-size:24px;font-weight:800}.muted{color:var(--muted)}.controls,.tabs{display:flex;gap:8px;flex-wrap:wrap;margin:14px 0}.controls input,.controls select,.btn{background:#10141b;border:1px solid var(--line);color:var(--text);border-radius:9px;padding:9px 11px}.btn{cursor:pointer}.btn.active{border-color:var(--accent);background:#17243a}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:13px}.num{font-size:24px;font-weight:800}.section{margin-top:16px}.section h2{font-size:16px;margin:0 0 10px}.cols{display:grid;grid-template-columns:1fr 1fr;gap:12px}.barrow{display:grid;grid-template-columns:minmax(170px,1.8fr) 4fr 55px;gap:8px;align-items:center;margin:7px 0}.bar{height:8px;background:#232a35;border-radius:6px;overflow:hidden}.bar i{display:block;height:100%;background:var(--accent)}.tablewrap{overflow:auto;border:1px solid var(--line);border-radius:12px}table{width:100%;border-collapse:collapse;min-width:1180px;background:var(--card)}th,td{padding:9px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}th{position:sticky;top:0;background:#171c25;z-index:2}.badge{display:inline-block;padding:2px 7px;border-radius:999px;border:1px solid var(--line);font-size:12px}.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}.details{display:none}.details.open{display:table-row}.detailbox{white-space:pre-wrap;max-height:620px;overflow:auto;background:#0e1218;padding:12px;border-radius:8px}.criteria{display:grid;grid-template-columns:repeat(2,minmax(280px,1fr));gap:8px}.crit{padding:9px;border:1px solid var(--line);border-radius:9px}.footer{margin:18px 0;color:var(--muted)}@media(max-width:1000px){.grid{grid-template-columns:repeat(2,1fr)}.cols{grid-template-columns:1fr}.criteria{grid-template-columns:1fr}}
</style></head><body><main><div class=top><div><div class=title>📊 APEX · Strategy Lab</div><div class=muted>Полный read-only журнал критериев, кандидатов и Groq</div></div><div id=updated class=muted></div></div>
<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button><button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button></div><div class=tabs id=strategies><button class="btn active" data-strategy="">Все</button><button class=btn data-strategy=FAST>FAST</button><button class=btn data-strategy=MTF>MTF</button><button class=btn data-strategy=SWING>SWING</button><button class=btn data-strategy=ZONE>ZONE</button><button class=btn data-strategy=WYCKOFF>WYCKOFF</button></div>
<div class=controls><input id=symbol placeholder="Пара, напр. BTCUSDT"><input id=fromdate type=date title="Дата от"><input id=todate type=date title="Дата до"><select id=outcome><option value="">Все исходы</option><option>FILTERED</option><option>CANDIDATE</option><option>PENDING_LTF</option><option>ERROR</option></select><select id=groq><option value="">Любой Groq</option><option>APPROVE</option><option>WAIT</option><option>REJECT</option></select><input id=minrr type=number step=.1 placeholder="RR от"><input id=maxrr type=number step=.1 placeholder="RR до"><button class=btn id=apply>Применить</button><button class=btn id=refresh>↻</button></div><div class=grid id=summary></div>
<div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div></div><div class=card><h2>Groq: причины WAIT/REJECT</h2><div id=groqReasons></div></div></div><div class="section card"><h2>Проходимость критериев</h2><div id=criteriaStats></div></div><div class="section card"><h2>Статистика сделок</h2><div id=tradeStats></div></div><div class=section><h2>Все проверки / потенциальные сделки</h2><div class=tablewrap><table><thead><tr><th>Дата</th><th>Стратегия</th><th>Пара</th><th>Напр.</th><th>Статус</th><th>Где остановилась</th><th>Entry</th><th>SL</th><th>TP1</th><th>TP2</th><th>RR</th><th>Groq</th><th></th></tr></thead><tbody id=rows></tbody></table></div><div class=controls><button class=btn id=prev>←</button><span id=pageinfo class=muted></span><button class=btn id=next>→</button></div></div><div class=footer>Dashboard только читает статистику. Он не меняет Entry / SL / TP / RR, стратегии, Groq или Binance.</div></main><script>
const TOKEN=new URLSearchParams(location.search).get('key')||'';let DAYS=1,STRATEGY='',PAGE=1,LAST=null;const esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));const num=v=>v===null||v===undefined||v===''?'—':Number(v).toLocaleString('ru-RU',{maximumFractionDigits:8});function params(){const p=new URLSearchParams({key:TOKEN,days:DAYS,strategy:STRATEGY,page:PAGE,page_size:100});for(const id of ['symbol','outcome','groq','fromdate','todate']){const v=document.getElementById(id).value.trim();if(v)p.set(id,v)}const a=minrr.value,b=maxrr.value;if(a)p.set('min_rr',a);if(b)p.set('max_rr',b);return p}async function load(){const r=await fetch('/api/dashboard?'+params());if(!r.ok){document.body.innerHTML='<main><h2>Статистика недоступна</h2><p>'+r.status+'</p></main>';return}LAST=await r.json();render()}function cards(s){const t=LAST.trade_stats||{};const wr=t.win_rate===null||t.win_rate===undefined?'—':t.win_rate+'%';const pnl=(Number(t.pnl_pct||0)>=0?'+':'')+Number(t.pnl_pct||0).toFixed(2)+'%';const ar=t.avg_r===null||t.avg_r===undefined?'—':Number(t.avg_r).toFixed(2)+'R';return [['Проверок',s.attempts],['Кандидатов',s.candidates],['Почти сделок',s.near_setups],['До Groq',s.groq_total],['Groq APPROVE',s.groq_approve],['WAIT / REJECT',s.groq_wait+' / '+s.groq_reject],['Отправлено',s.delivered],['Открыто',t.opened||0],['Закрыто',t.closed||0],['Win rate',wr],['P&L',pnl],['Средний R',ar]].map(x=>`<div class=card><div class=muted>${x[0]}</div><div class=num>${x[1]}</div></div>`).join('')}function bars(items,id){const el=document.getElementById(id),max=Math.max(1,...items.map(x=>x.count));el.innerHTML=items.slice(0,15).map(x=>`<div class=barrow><div title="${esc(x.label)}">${esc(x.label).slice(0,90)}</div><div class=bar><i style="width:${100*x.count/max}%"></i></div><b>${x.count}</b></div>`).join('')||'<span class=muted>Нет данных</span>'}function renderChecks(){const a=LAST.criterion_stats.filter(x=>!STRATEGY||x.strategy===STRATEGY);criteriaStats.innerHTML=a.slice(0,80).map(x=>{const p=x.total?Math.round(x.pass/x.total*100):0;return `<div class=barrow><div><b>${esc(x.strategy)}</b> · ${esc(x.label).slice(0,90)}<div class=muted>✅ ${x.pass} · ❌ ${x.fail} · всего ${x.total}</div></div><div class=bar><i style="width:${p}%"></i></div><b>${p}%</b></div>`}).join('')||'<span class=muted>Нет данных</span>'}function renderTradeStats(){const t=LAST.trade_stats||{},rows=t.by_strategy||[];const head=`<div class=muted style="margin-bottom:10px">Открыто: <b>${t.opened||0}</b> · Закрыто: <b>${t.closed||0}</b> · TP: <b class=good>${t.wins||0}</b> · SL: <b class=bad>${t.losses||0}</b> · Win rate: <b>${t.win_rate==null?'—':t.win_rate+'%'}</b> · P&L: <b class="${Number(t.pnl_pct||0)>=0?'good':'bad'}">${Number(t.pnl_pct||0)>=0?'+':''}${Number(t.pnl_pct||0).toFixed(3)}%</b> · Avg R: <b>${t.avg_r==null?'—':Number(t.avg_r).toFixed(3)+'R'}</b></div>`;const body=rows.length?`<div class=tablewrap><table style="min-width:760px"><thead><tr><th>Стратегия</th><th>Открыто</th><th>Закрыто</th><th>TP</th><th>SL</th><th>Win rate</th><th>P&L %</th><th>Avg R</th></tr></thead><tbody>${rows.map(r=>`<tr><td><b>${esc(r.strategy)}</b></td><td>${r.opened}</td><td>${r.closed}</td><td class=good>${r.wins}</td><td class=bad>${r.losses}</td><td>${r.win_rate==null?'—':r.win_rate+'%'}</td><td class="${Number(r.pnl_pct||0)>=0?'good':'bad'}">${Number(r.pnl_pct||0)>=0?'+':''}${Number(r.pnl_pct||0).toFixed(3)}%</td><td>${r.avg_r==null?'—':Number(r.avg_r).toFixed(3)+'R'}</td></tr>`).join('')}</tbody></table></div>`:'<span class=muted>Закрытых сделок пока нет</span>';tradeStats.innerHTML=head+body}function renderCatalog(){const names=STRATEGY?[STRATEGY]:['FAST','MTF','SWING','ZONE','WYCKOFF'];catalogTitle.textContent=STRATEGY?'Критерии '+STRATEGY:'Полный ромб критериев всех стратегий';catalog.innerHTML=names.map(n=>{const c=LAST.catalog[n];return `<div class=crit style="grid-column:1/-1"><b>${n}</b> · ${esc(c.timeframes)} · RR: ${esc(c.rr)}</div>`+c.criteria.map(q=>`<div class=crit><b>${q.required?'●':'○'} ${esc(q.label)}</b><div class=muted>${esc(q.category)}${q.required?' · обязательный':' · контекст/бонус'}</div>${q.detail?`<div>${esc(q.detail)}</div>`:''}</div>`).join('')}).join('')}function levels(r){const c=r.candidate||{},s=(r.stop||{}).snapshot||{};return {entry:c.entry??s.entry,sl:c.sl??s.sl,tp1:c.tp1??c.tp??s.tp1??s.tp,tp2:c.tp2??s.tp2,tp3:c.tp3??s.tp3,rr:r.rr_value}}
function normText(v){return String(v||'').toLowerCase().replace(/[_/.-]+/g,' ').replace(/[^a-zа-я0-9 ]/gi,' ').replace(/\s+/g,' ').trim()}
function words(v){return new Set(normText(v).split(' ').filter(x=>x.length>2&&!['the','and','for','with','not','или','для','при','что','это'].includes(x)))}
function matchCriterion(q,checks,stop){const qc=normText(q.code),ql=normText(q.label),qw=words(q.code+' '+q.label);let best=null,bestScore=0;for(const ch of checks){const raw=[ch.code,ch.label,ch.condition].filter(Boolean).join(' '),n=normText(raw);let score=0;if(qc&&n.includes(qc))score+=12;if(ql&&n.includes(ql))score+=10;const cw=words(raw);for(const w of qw)if(cw.has(w))score+=1;if(score>bestScore){bestScore=score;best=ch}}if(bestScore>=3)return {state:String(best.state||'UNKNOWN').toUpperCase(),source:best};const st=normText((stop?.code||'')+' '+(stop?.label||'')+' '+(stop?.condition||''));let ss=0;for(const w of qw)if(st.includes(w))ss++;if((qc&&st.includes(qc))||(ql&&st.includes(ql))||ss>=2)return {state:'FAIL',source:stop};return {state:'NOT_REACHED',source:null}}
function sectionName(cat,required){const c=normText(cat);if(/final deterministic|geometry/.test(c))return 'FINAL / GEOMETRY';if(/trigger|structure|phase/.test(c))return 'TRIGGER / STRUCTURE';if(/confirmation|risk context|market context|learning/.test(c))return required?'CORE CONTEXT':'ADDITIONAL';if(/ai quality/.test(c))return 'GROQ';return required?'CORE':'ADDITIONAL'}
function detailText(r){const l=levels(r),g=r.groq_review||{},stop=r.stop||{},snap=stop.snapshot||{},checks=Array.isArray(r.checks)?r.checks:[],catalog=(LAST.catalog||{})[r.strategy]||{criteria:[]};const dir=String(r.candidate?.direction??snap.direction??'').toUpperCase()||'—';const lines=[`${r.symbol||'—'} · ${r.strategy||'—'} · ${dir}`,(r.finished_at||r.occurred_at||'').replace('T',' ').slice(0,19)+' UTC',''];const groups={};let pass=0,fail=0,nr=0,opt=0;for(const q of (catalog.criteria||[])){let m=matchCriterion(q,checks,stop);if(q.code.endsWith('_groq'))m=g.decision?{state:String(g.decision).toUpperCase()==='APPROVE'?'PASS':'FAIL'}:{state:'NOT_REACHED'};const group=sectionName(q.category,q.required);if(!groups[group])groups[group]=[];const state=m.state,icon=state==='PASS'?'✅':state==='FAIL'?'❌':'⏭';if(state==='PASS')pass++;else if(state==='FAIL')fail++;else nr++;if(!q.required)opt++;groups[group].push(`${icon} ${q.required?'[MAIN]':'[ADD]'} ${q.label}${state==='NOT_REACHED'?' · not reached':''}`)}lines.push(`[SUMMARY]`,`✅ Passed: ${pass} · ❌ Failed: ${fail} · ⏭ Not reached: ${nr} · ADD criteria: ${opt}`,'');for(const k of ['CORE','CORE CONTEXT','TRIGGER / STRUCTURE','ADDITIONAL','FINAL / GEOMETRY','GROQ'])if(groups[k]?.length)lines.push(`[${k}]`,...groups[k],'');const matched=new Set();for(const q of (catalog.criteria||[])){const m=matchCriterion(q,checks,stop);if(m.source)matched.add(m.source)}const extras=checks.filter(ch=>!matched.has(ch)&&!String(ch.code||'').startsWith('_')&&!String(ch.label||'').startsWith('_'));if(extras.length){lines.push('[OTHER RECORDED CHECKS]');for(const ch of extras){const st=String(ch.state||'UNKNOWN').toUpperCase();lines.push(`${st==='PASS'?'✅':st==='FAIL'?'❌':'⚠️'} ${ch.label||ch.condition||ch.code}`)}lines.push('')}lines.push('[LEVELS]',`Entry: ${num(l.entry)}`,`SL:    ${num(l.sl)}`,`TP1:   ${num(l.tp1)}`,`TP2:   ${num(l.tp2)}`,`TP3:   ${num(l.tp3)}`,`RR:    ${num(l.rr)}`,'');if(g.decision){lines.push('[GROQ RESULT]',`Decision: ${g.decision}`,`Confidence: ${g.confidence!==undefined?Math.round(Number(g.confidence)*100)+'%':'—'}`);if(Array.isArray(g.reasons)&&g.reasons.length)lines.push('Reasons:',...g.reasons.map(x=>'❌ '+x));if(Array.isArray(g.risks)&&g.risks.length)lines.push('Risks:',...g.risks.map(x=>'⚠️ '+x));lines.push('')}else lines.push('[GROQ RESULT]','⏭ Not reached','');lines.push('[STOP]',stop.label||stop.code||'—');return lines.join('\n')}
function renderRows(){rows.innerHTML=LAST.rows.map((r,i)=>{const l=levels(r),g=r.groq_review||{},st=r.outcome||'—',stop=r.stop||{},cls=st==='CANDIDATE'?'good':st==='ERROR'?'bad':'warn',dir=String(r.candidate?.direction??stop.snapshot?.direction??'').toUpperCase(),det={stop:r.stop,checks:r.checks,candidate:r.candidate,groq_review:r.groq_review,decisions:r.decisions,subtype:r.subtype,function:r.function,run_id:r.run_id,duration_ms:r.duration_ms};return `<tr><td>${esc((r.finished_at||r.occurred_at||'').replace('T',' ').slice(0,19))}</td><td><b>${esc(r.strategy)}</b>${r.subtype?`<div class=muted>${esc(r.subtype)}</div>`:''}</td><td>${esc(r.symbol)}</td><td>${esc(dir)}</td><td><span class="badge ${cls}">${esc(st)}</span>${r.near_setup?'<div class=warn>почти сделка</div>':''}</td><td>${esc(stop.label||stop.code||'—').slice(0,120)}</td><td>${num(l.entry)}</td><td>${num(l.sl)}</td><td>${num(l.tp1)}</td><td>${num(l.tp2)}</td><td>${num(l.rr)}</td><td>${g.decision?`<b>${esc(g.decision)}</b><div>${g.confidence!==undefined?Math.round(Number(g.confidence)*100)+'%':''}</div>`:'—'}</td><td><button class=btn onclick="toggle(${i})">детали</button></td></tr><tr class=details id=d${i}><td colspan=13><div class=detailbox>${esc(detailText(r))}</div><details><summary class=muted style="cursor:pointer;margin-top:8px">Raw data</summary><div class=detailbox>${esc(JSON.stringify(det,null,2))}</div></details></td></tr>`}).join('')||'<tr><td colspan=13 class=muted>Нет строк</td></tr>';pageinfo.textContent=`Страница ${LAST.pagination.page}/${LAST.pagination.pages} · строк ${LAST.pagination.total}`}window.toggle=i=>document.getElementById('d'+i).classList.toggle('open');function render(){updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC';summary.innerHTML=cards(LAST.summary);bars(LAST.failures,'failures');bars(LAST.groq.reasons,'groqReasons');renderChecks();renderTradeStats();renderRows()}document.querySelectorAll('#periods .btn').forEach(b=>b.onclick=()=>{document.querySelectorAll('#periods .btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');DAYS=Number(b.dataset.days);PAGE=1;load()});document.querySelectorAll('#strategies .btn').forEach(b=>b.onclick=()=>{document.querySelectorAll('#strategies .btn').forEach(x=>x.classList.remove('active'));b.classList.add('active');STRATEGY=b.dataset.strategy;PAGE=1;load()});apply.onclick=()=>{PAGE=1;load()};refresh.onclick=load;prev.onclick=()=>{if(PAGE>1){PAGE--;load()}};next.onclick=()=>{if(LAST&&PAGE<LAST.pagination.pages){PAGE++;load()}};load();setInterval(load,60000);
</script></body></html>'''


class Handler(BaseHTTPRequestHandler):
    server_version="APEXStats/1.0"
    def _json(self,data,status=200):
        body=json.dumps(data,ensure_ascii=False,default=str).encode(); self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Content-Length",str(len(body))); self.send_header("Cache-Control","no-store"); self.send_header("X-Content-Type-Options","nosniff"); self.end_headers(); self.wfile.write(body)
    def _html(self,text,status=200):
        body=text.encode(); self.send_response(status); self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Content-Length",str(len(body))); self.send_header("Cache-Control","no-store"); self.send_header("Referrer-Policy","no-referrer"); self.send_header("X-Frame-Options","DENY"); self.send_header("X-Content-Type-Options","nosniff"); self.send_header("Content-Security-Policy","default-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'"); self.end_headers(); self.wfile.write(body)
    def _auth(self,q):
        supplied=(q.get("key") or [""])[0]; return bool(DASHBOARD_TOKEN and hmac.compare_digest(supplied,DASHBOARD_TOKEN))
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def do_GET(self):
        p=urlparse(self.path); q=parse_qs(p.query)
        if p.path=="/health": self._json({"ok":True,"service":"apex-strategy-stats"}); return
        if not self._auth(q): self._html("<!doctype html><meta charset=utf-8><h2>403 · закрытая статистика APEX</h2>",403); return
        if p.path in {"/","/stats"}: self._html(HTML); return
        if p.path=="/api/dashboard":
            try:
                val=lambda k,d="":(q.get(k) or [d])[0]; data=build_dashboard(int(val("days","1")),val("strategy"),val("symbol"),val("outcome"),val("groq"),float(val("min_rr")) if val("min_rr") else None,float(val("max_rr")) if val("max_rr") else None,val("fromdate"),val("todate"),int(val("page","1")),int(val("page_size","100"))); self._json(data)
            except Exception as exc: self._json({"error":f"{type(exc).__name__}: {exc}"},500)
            return
        self._json({"error":"not found"},404)
    def do_POST(self):
        if urlparse(self.path).path!="/ingest": self._json({"error":"not found"},404); return
        if not INGEST_TOKEN or not hmac.compare_digest(self.headers.get("X-APEX-Ingest-Token",""),INGEST_TOKEN): self._json({"error":"forbidden"},403); return
        try:
            n=min(int(self.headers.get("Content-Length","0") or 0),2_000_000); count=ingest(json.loads(self.rfile.read(n).decode())); self._json({"ok":True,"accepted":count})
        except Exception as exc: self._json({"error":f"{type(exc).__name__}: {exc}"},400)
    def log_message(self,fmt,*args): print(f"[stats] {self.command} {urlparse(self.path).path}")


def main():
    if not DATABASE_URL or not DASHBOARD_TOKEN or not INGEST_TOKEN: raise SystemExit("DATABASE_URL, DASHBOARD_TOKEN and INGEST_TOKEN are required")
    ensure_schema(); print(f"APEX Strategy Stats listening on :{PORT}"); ThreadingHTTPServer(("0.0.0.0",PORT),Handler).serve_forever()


if __name__=="__main__": main()
