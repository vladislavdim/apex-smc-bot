from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected 1 match, got {count}")
    return text.replace(old, new, 1)


# --- market.py: conservative gate tuning + passive trade telemetry ---
p = Path('market.py')
s = p.read_text(encoding='utf-8')

s = replace_once(
    s,
    'from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail\n',
    'from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail, emit_event as _emit_stats_event\n',
    'setup audit import',
)

s = replace_once(
    s,
    '''        # ── Swing highs/lows (lookback=12) ──\n        swing_highs, swing_lows = find_swings(candles, lookback=12)\n        if _audit_test('SWING_DETECT_SWING_SETUP_G7142', (len(swing_highs) < 2 or len(swing_lows) < 2), 'Swing highs/lows (lookback=12)', 'len(swing_highs) < 2 or len(swing_lows) < 2', 7142):\n            return _audit_fail('SWING_DETECT_SWING_SETUP_R7143', 'Swing highs/lows (lookback=12)', locals(), 'len(swing_highs) < 2 or len(swing_lows) < 2', 7143)\n''',
    '''        # ── Swing highs/lows: still structural, but aligned with the later sweep detector. ──\n        # lookback=12 rejected almost the whole universe before the actual sweep/CHoCH logic.\n        # Seven bars remains selective while allowing genuine 4h swing structure to reach the trigger layer.\n        swing_highs, swing_lows = find_swings(candles, lookback=7)\n        if _audit_test('SWING_DETECT_SWING_SETUP_G7142', (len(swing_highs) < 2 or len(swing_lows) < 2), 'Свинг-структура найдена (lookback=7)', 'len(swing_highs) < 2 or len(swing_lows) < 2', 7142):\n            return _audit_fail('SWING_DETECT_SWING_SETUP_R7143', 'Свинг-структура найдена (lookback=7)', locals(), 'len(swing_highs) < 2 or len(swing_lows) < 2', 7143)\n''',
    'SWING lookback',
)

s = replace_once(
    s,
    "        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7796', (not candles or len(candles) < 50), 'not candles or len(candles) < 50', 'not candles or len(candles) < 50', 7796):\n            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7797', 'not candles or len(candles) < 50', locals(), 'not candles or len(candles) < 50', 7797)\n",
    "        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7796', (not candles or len(candles) < 40), 'Достаточно 4H истории (≥40 закрытых свечей)', 'not candles or len(candles) < 40', 7796):\n            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7797', 'Достаточно 4H истории (≥40 закрытых свечей)', locals(), 'not candles or len(candles) < 40', 7797)\n",
    'ZONE history',
)

s = replace_once(
    s,
    '                    if c_range > 0 and c_body / c_range >= 0.5 and c_body > atr * _vf_zone:\n',
    '                    if c_range > 0 and c_body / c_range >= 0.5 and c_body > atr * _vf_zone * 0.8:\n',
    'ZONE displacement strength',
)

s = replace_once(
    s,
    '            _vol_threshold = 2.0\n',
    '            _vol_threshold = 1.6\n',
    'FAST institutional volume',
)

helper_anchor = 'def check_pending_signals():\n'
helper = '''def _emit_trade_stats_event(action, sig_id, symbol, signal_type, direction, entry, sl, tp1, tp2=None, tp3=None, *, result="", exit_price=None, hours=None):\n    """Passive OPEN/CLOSE telemetry for Strategy Lab; never affects trade state."""\n    try:\n        entry_f = float(entry or 0)\n        sl_f = float(sl or 0)\n        exit_f = float(exit_price) if exit_price is not None else None\n        risk = abs(entry_f - sl_f) if entry_f and sl_f else 0.0\n        pnl_pct = None\n        realized_r = None\n        if exit_f is not None and entry_f > 0:\n            signed = (exit_f - entry_f) if str(direction).upper() == "BULLISH" else (entry_f - exit_f)\n            pnl_pct = round(signed / entry_f * 100.0, 4)\n            if risk > 0:\n                realized_r = round(signed / risk, 3)\n        strategy = str(signal_type or "UNKNOWN").upper()\n        payload = {\n            "action": str(action or "").upper(), "signal_id": int(sig_id),\n            "symbol": str(symbol or "").upper(), "strategy": strategy,\n            "direction": str(direction or "").upper(), "entry": entry_f or None,\n            "sl": sl_f or None, "tp1": float(tp1) if tp1 else None,\n            "tp2": float(tp2) if tp2 else None, "tp3": float(tp3) if tp3 else None,\n            "result": str(result or "").lower(), "exit_price": exit_f,\n            "pnl_pct": pnl_pct, "realized_r": realized_r,\n            "planned_rr": round(abs(float(tp1) - entry_f) / risk, 3) if tp1 and risk > 0 else None,\n            "hours": round(float(hours), 2) if hours is not None else None,\n        }\n        suffix = str(result or "open").lower() if str(action).upper() == "CLOSE" else str(action or "event").lower()\n        _emit_stats_event("trade_event", strategy, symbol, payload, event_key=f"trade:{int(sig_id)}:{str(action).lower()}:{suffix}")\n    except Exception as exc:\n        logging.debug("[TradeStats] emit skipped for %s: %s", sig_id, exc)\n\n\n'''
if helper not in s:
    if helper_anchor not in s:
        raise SystemExit('check_pending_signals anchor not found')
    s = s.replace(helper_anchor, helper + helper_anchor, 1)

s = replace_once(
    s,
    '''                logging.info("[SignalLifecycle] %s entry activated at %s", symbol, entry)\n                # Never infer entry→TP/SL ordering from the activation bar.\n                continue\n''',
    '''                logging.info("[SignalLifecycle] %s entry activated at %s", symbol, entry)\n                _emit_trade_stats_event(\n                    "OPEN", sig_id, symbol, _sig_type_check, direction, entry, sl, tp1, tp2, tp3,\n                    hours=hours_elapsed,\n                )\n                # Never infer entry→TP/SL ordering from the activation bar.\n                continue\n''',
    'trade OPEN event',
)

s = replace_once(
    s,
    '''                conn2.commit()\n                conn2.close()\n\n                is_win = result in ("tp1", "tp2", "tp3")\n''',
    '''                conn2.commit()\n                conn2.close()\n\n                if result in ("sl", "tp1", "tp2", "tp3"):\n                    _exit_for_stats = (\n                        _active_sl if result == "sl" else\n                        tp1 if result == "tp1" else\n                        tp2 if result == "tp2" else tp3\n                    )\n                    _emit_trade_stats_event(\n                        "CLOSE", sig_id, symbol, _sig_type_check, direction, entry, sl, tp1, tp2, tp3,\n                        result=result, exit_price=_exit_for_stats, hours=hours_elapsed,\n                    )\n\n                is_win = result in ("tp1", "tp2", "tp3")\n''',
    'trade CLOSE event',
)

p.write_text(s, encoding='utf-8')


# --- stats_server.py: aggregate and render trade performance ---
p = Path('stats_server.py')
s = p.read_text(encoding='utf-8')

s = replace_once(
    s,
    '    events = _fetch(days, strategy, symbol, from_date, to_date); attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]\n',
    '    events = _fetch(days, strategy, symbol, from_date, to_date); attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]; trade_events=[]\n',
    'stats event buckets',
)

s = replace_once(
    s,
    '        elif e["kind"]=="scan_event": scan_events.append({**p,"occurred_at":e["occurred_at"]})\n',
    '        elif e["kind"]=="scan_event": scan_events.append({**p,"occurred_at":e["occurred_at"]})\n        elif e["kind"]=="trade_event": trade_events.append({**p,"strategy":e["strategy"],"symbol":e["symbol"],"occurred_at":e["occurred_at"]})\n',
    'trade event collection',
)

trade_aggregate_anchor = '    total=len(joined); page_size=max(20,min(int(page_size),200)); page=max(1,int(page)); start=(page-1)*page_size\n'
trade_aggregate = '''    opened=[t for t in trade_events if str(t.get("action") or "").upper()=="OPEN"]\n    closed=[t for t in trade_events if str(t.get("action") or "").upper()=="CLOSE"]\n    wins=[t for t in closed if str(t.get("result") or "").lower() in {"tp1","tp2","tp3"}]\n    losses=[t for t in closed if str(t.get("result") or "").lower()=="sl"]\n    pnl_vals=[float(t["pnl_pct"]) for t in closed if _num(t.get("pnl_pct")) is not None]\n    r_vals=[float(t["realized_r"]) for t in closed if _num(t.get("realized_r")) is not None]\n    trade_by_strategy=defaultdict(lambda:{"opened":0,"closed":0,"wins":0,"losses":0,"pnl_pct":0.0,"r_sum":0.0,"r_n":0})\n    for t in opened:\n        trade_by_strategy[str(t.get("strategy") or "UNKNOWN")]["opened"]+=1\n    for t in closed:\n        st=str(t.get("strategy") or "UNKNOWN"); d=trade_by_strategy[st]; d["closed"]+=1\n        res=str(t.get("result") or "").lower()\n        if res in {"tp1","tp2","tp3"}: d["wins"]+=1\n        elif res=="sl": d["losses"]+=1\n        pv=_num(t.get("pnl_pct")); rv=_num(t.get("realized_r"))\n        if pv is not None: d["pnl_pct"]+=pv\n        if rv is not None: d["r_sum"]+=rv; d["r_n"]+=1\n    trade_rows=[]\n    for st,d in sorted(trade_by_strategy.items()):\n        decided=d["wins"]+d["losses"]\n        trade_rows.append({"strategy":st,"opened":d["opened"],"closed":d["closed"],"wins":d["wins"],"losses":d["losses"],\n            "win_rate":round(d["wins"]/decided*100,1) if decided else None,"pnl_pct":round(d["pnl_pct"],3),\n            "avg_r":round(d["r_sum"]/d["r_n"],3) if d["r_n"] else None})\n    trade_stats={"opened":len(opened),"closed":len(closed),"wins":len(wins),"losses":len(losses),\n        "win_rate":round(len(wins)/(len(wins)+len(losses))*100,1) if (wins or losses) else None,\n        "pnl_pct":round(sum(pnl_vals),3) if pnl_vals else 0.0,"avg_pnl_pct":round(sum(pnl_vals)/len(pnl_vals),3) if pnl_vals else None,\n        "avg_r":round(sum(r_vals)/len(r_vals),3) if r_vals else None,"by_strategy":trade_rows,\n        "recent":sorted(closed,key=lambda x:x.get("occurred_at",""),reverse=True)[:50]}\n\n'''
if trade_aggregate not in s:
    if trade_aggregate_anchor not in s:
        raise SystemExit('trade aggregate anchor not found')
    s = s.replace(trade_aggregate_anchor, trade_aggregate + trade_aggregate_anchor, 1)

s = replace_once(
    s,
    '      "catalog":STRATEGY_CATALOG,"rows":joined[start:start+page_size],"pagination":{"page":page,"page_size":page_size,"total":total,"pages":max(1,(total+page_size-1)//page_size)}}\n',
    '      "trade_stats":trade_stats,"catalog":STRATEGY_CATALOG,"rows":joined[start:start+page_size],"pagination":{"page":page,"page_size":page_size,"total":total,"pages":max(1,(total+page_size-1)//page_size)}}\n',
    'trade stats API payload',
)

s = replace_once(
    s,
    '.grid{display:grid;grid-template-columns:repeat(7,minmax(120px,1fr));gap:10px}',
    '.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}',
    'summary grid',
)

s = replace_once(
    s,
    '<div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div></div><div class=card><h2>Groq: причины WAIT/REJECT</h2><div id=groqReasons></div></div></div><div class="section card"><h2>Проходимость критериев</h2><div id=criteriaStats></div></div><div class=section><h2>Все проверки / потенциальные сделки</h2>',
    '<div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div></div><div class=card><h2>Groq: причины WAIT/REJECT</h2><div id=groqReasons></div></div></div><div class="section card"><h2>Проходимость критериев</h2><div id=criteriaStats></div></div><div class="section card"><h2>Статистика сделок</h2><div id=tradeStats></div></div><div class=section><h2>Все проверки / потенциальные сделки</h2>',
    'trade stats HTML section',
)

old_cards = "function cards(s){return [['Проверок',s.attempts],['Кандидатов',s.candidates],['Почти сделок',s.near_setups],['До Groq',s.groq_total],['Groq APPROVE',s.groq_approve],['WAIT / REJECT',s.groq_wait+' / '+s.groq_reject],['Отправлено',s.delivered]].map(x=>`<div class=card><div class=muted>${x[0]}</div><div class=num>${x[1]}</div></div>`).join('')}"
new_cards = "function cards(s){const t=LAST.trade_stats||{};const wr=t.win_rate===null||t.win_rate===undefined?'—':t.win_rate+'%';const pnl=(Number(t.pnl_pct||0)>=0?'+':'')+Number(t.pnl_pct||0).toFixed(2)+'%';const ar=t.avg_r===null||t.avg_r===undefined?'—':Number(t.avg_r).toFixed(2)+'R';return [['Проверок',s.attempts],['Кандидатов',s.candidates],['Почти сделок',s.near_setups],['До Groq',s.groq_total],['Groq APPROVE',s.groq_approve],['WAIT / REJECT',s.groq_wait+' / '+s.groq_reject],['Отправлено',s.delivered],['Открыто',t.opened||0],['Закрыто',t.closed||0],['Win rate',wr],['P&L',pnl],['Средний R',ar]].map(x=>`<div class=card><div class=muted>${x[0]}</div><div class=num>${x[1]}</div></div>`).join('')}"
s = replace_once(s, old_cards, new_cards, 'summary trade cards')

render_checks_anchor = "function renderChecks(){const a=LAST.criterion_stats.filter(x=>!STRATEGY||x.strategy===STRATEGY);criteriaStats.innerHTML=a.slice(0,80).map(x=>{const p=x.total?Math.round(x.pass/x.total*100):0;return `<div class=barrow><div><b>${esc(x.strategy)}</b> · ${esc(x.label).slice(0,90)}<div class=muted>✅ ${x.pass} · ❌ ${x.fail} · всего ${x.total}</div></div><div class=bar><i style=\"width:${p}%\"></i></div><b>${p}%</b></div>`}).join('')||'<span class=muted>Нет данных</span>'}"
render_trade = render_checks_anchor + "function renderTradeStats(){const t=LAST.trade_stats||{},rows=t.by_strategy||[];const head=`<div class=muted style=\"margin-bottom:10px\">Открыто: <b>${t.opened||0}</b> · Закрыто: <b>${t.closed||0}</b> · TP: <b class=good>${t.wins||0}</b> · SL: <b class=bad>${t.losses||0}</b> · Win rate: <b>${t.win_rate==null?'—':t.win_rate+'%'}</b> · P&L: <b class=\"${Number(t.pnl_pct||0)>=0?'good':'bad'}\">${Number(t.pnl_pct||0)>=0?'+':''}${Number(t.pnl_pct||0).toFixed(3)}%</b> · Avg R: <b>${t.avg_r==null?'—':Number(t.avg_r).toFixed(3)+'R'}</b></div>`;const body=rows.length?`<div class=tablewrap><table style=\"min-width:760px\"><thead><tr><th>Стратегия</th><th>Открыто</th><th>Закрыто</th><th>TP</th><th>SL</th><th>Win rate</th><th>P&L %</th><th>Avg R</th></tr></thead><tbody>${rows.map(r=>`<tr><td><b>${esc(r.strategy)}</b></td><td>${r.opened}</td><td>${r.closed}</td><td class=good>${r.wins}</td><td class=bad>${r.losses}</td><td>${r.win_rate==null?'—':r.win_rate+'%'}</td><td class=\"${Number(r.pnl_pct||0)>=0?'good':'bad'}\">${Number(r.pnl_pct||0)>=0?'+':''}${Number(r.pnl_pct||0).toFixed(3)}%</td><td>${r.avg_r==null?'—':Number(r.avg_r).toFixed(3)+'R'}</td></tr>`).join('')}</tbody></table></div>`:'<span class=muted>Закрытых сделок пока нет</span>';tradeStats.innerHTML=head+body}"
s = replace_once(s, render_checks_anchor, render_trade, 'render trade stats function')

s = replace_once(
    s,
    "bars(LAST.groq.reasons,'groqReasons');renderChecks();renderRows()",
    "bars(LAST.groq.reasons,'groqReasons');renderChecks();renderTradeStats();renderRows()",
    'render trade stats invocation',
)

p.write_text(s, encoding='utf-8')


# --- static regression tests for the exact requested behavior ---
t = Path('tests/test_strategy_tuning_trade_stats.py')
t.write_text('''import unittest\nfrom pathlib import Path\n\n\nclass StrategyTuningTradeStatsTests(unittest.TestCase):\n    @classmethod\n    def setUpClass(cls):\n        cls.market = Path("market.py").read_text(encoding="utf-8")\n        cls.stats = Path("stats_server.py").read_text(encoding="utf-8")\n\n    def test_swing_is_relaxed_only_at_initial_structure_gate(self):\n        self.assertIn("find_swings(candles, lookback=7)", self.market)\n        self.assertIn("get_bos_choch_event", self.market)\n        self.assertIn("Variant 2: минимум RR 2.0", self.market)\n\n    def test_fast_retains_quality_stack(self):\n        self.assertIn("_vol_threshold = 1.6", self.market)\n        self.assertIn("curr_body / curr_range < 0.65", self.market)\n        self.assertIn("not _acceptance", self.market)\n        self.assertIn("not _fast_structure_event", self.market)\n        self.assertIn("not 2.0 <= rr <= 4.0", self.market)\n\n    def test_zone_retains_structure_quality_and_rr(self):\n        self.assertIn("len(candles) < 40", self.market)\n        self.assertIn("c_body / c_range >= 0.5", self.market)\n        self.assertIn("q_score < _q_min", self.market)\n        self.assertIn("not _zone_ltf_structure", self.market)\n        self.assertIn("rr < 2.0", self.market)\n\n    def test_trade_telemetry_is_passive_and_dashboard_only(self):\n        self.assertIn("_emit_trade_stats_event", self.market)\n        self.assertIn('"trade_event"', self.market)\n        self.assertIn('"trade_stats":trade_stats', self.stats)\n        self.assertIn("Статистика сделок", self.stats)\n        self.assertIn("realized_r", self.stats)\n        self.assertIn("pnl_pct", self.stats)\n\n\nif __name__ == "__main__":\n    unittest.main()\n''', encoding='utf-8')

print('strategy tuning + trade stats patch applied')
