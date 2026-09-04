from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    s = p.read_text(encoding='utf-8')
    if old not in s:
        raise SystemExit(f'anchor not found in {path}: {old[:120]!r}')
    p.write_text(s.replace(old, new, 1), encoding='utf-8')

# 1) SWING: 4h BOS/CHoCH is context only; fresh 1h structure remains mandatory.
replace_once('market.py', '''        # ── BOS/CHoCH after the sweep, confirmed by candle close ──\n        # The canonical engine distinguishes continuation (BOS) from a real\n        # change of character (CHoCH) using the prior paired swing structure.\n        _swing_structure_event = get_bos_choch_event(\n            candles,\n            direction,\n            lookback=30,\n            max_break_age=max(1, trigger_lookback),\n        )\n        if _audit_test('SWING_DETECT_SWING_SETUP_G7403', (not _swing_structure_event), 'not _swing_structure_event', 'not _swing_structure_event', 7403):\n            logging.info(f"[SWING] {symbol}: нет подтверждённого BOS/CHoCH после триггера")\n            return _audit_fail('SWING_DETECT_SWING_SETUP_R7405', 'not _swing_structure_event', locals(), 'not _swing_structure_event', 7405)\n''', '''        # ── 4h BOS/CHoCH is thesis quality context, not a second hard trigger. ──\n        # Direction already comes from a 4h sweep/EQH-EQL/OB reaction. Execution\n        # still requires a fresh 1h BOS/CHoCH and the 15m entry trigger below.\n        _swing_structure_event = get_bos_choch_event(\n            candles,\n            direction,\n            lookback=30,\n            max_break_age=max(1, trigger_lookback),\n        )\n        _audit_test(\n            'SWING_4H_STRUCTURE_CONTEXT',\n            (not _swing_structure_event),\n            '4h BOS/CHoCH context after trigger (non-blocking)',\n            'not _swing_structure_event',\n            7403,\n        )\n        if not _swing_structure_event:\n            logging.info(f"[SWING] {symbol}: 4h BOS/CHoCH не подтверждён — context weak, ждём обязательный 1h trigger")\n''')

# 2) FAST: preliminary 1.1x impulse volume becomes context; the real 1.6x trigger remains mandatory.
replace_once('market.py', '''        # Volume check на 15m impulse — должен быть выше среднего\n        _avg_vol_15m_imp = sum(c.get("volume", 0) for c in candles_15m_imp[:-1]) / max(len(candles_15m_imp) - 1, 1)\n        if _audit_test('FAST_DETECT_FAST_DEAL_G9249', (_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1), 'Volume check на 15m impulse — должен быть выше среднего', '_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1', 9249):\n            return _audit_fail('FAST_DETECT_FAST_DEAL_R9250', 'Volume check на 15m impulse — должен быть выше среднего', locals(), '_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1', 9250)  # Импульс без объёма — ненадёжный\n''', '''        # Preliminary impulse volume is context only. The executable trigger below\n        # still requires the mandatory 1.6x volume spike on the actual trigger candle.\n        _avg_vol_15m_imp = sum(c.get("volume", 0) for c in candles_15m_imp[:-1]) / max(len(candles_15m_imp) - 1, 1)\n        _fast_impulse_volume_context_weak = bool(\n            _avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1\n        )\n        _audit_test(\n            'FAST_IMPULSE_VOLUME_CONTEXT',\n            _fast_impulse_volume_context_weak,\n            '15m preliminary impulse volume >= 1.1x average (non-blocking)',\n            '_fast_impulse_volume_context_weak',\n            9249,\n        )\n''')

# 3) WYCKOFF Distribution: symmetric range telemetry before the same 25% hard rule.
replace_once('market.py', '''        if dist_range_pct < 15:\n            score += 20\n            signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")\n        elif dist_range_pct < 25:\n            score += 10\n            signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")\n        else:\n            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725', 'dist_range_pct < 25', locals(), 'dist_range_pct < 25', 8725)\n''', '''        _dist_range_too_wide = dist_range_pct >= 25\n        if _audit_test(\n            'WYCKOFF_DIST_RANGE',\n            _dist_range_too_wide,\n            'WYCKOFF Distribution: 30d range < 25%',\n            'dist_range_pct >= 25',\n            8720,\n        ):\n            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725', 'WYCKOFF Distribution: 30d range < 25%', locals(), 'dist_range_pct >= 25', 8725)\n        if dist_range_pct < 15:\n            score += 20\n            signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")\n        else:\n            score += 10\n            signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")\n''')

# Setup evidence must match the two-stage SWING architecture.
replace_once('core/setup_evidence.py', '"SWING": {"core": "HTF thesis and major liquidity/OB/FVG reaction", "trigger": "4h plus fresh closed 1h BOS/CHoCH"},', '"SWING": {"core": "4h liquidity/OB/FVG thesis; 4h BOS/CHoCH is quality context", "trigger": "fresh closed 1h BOS/CHoCH plus 15m execution trigger"},')
replace_once('core/setup_evidence.py', '''        if structure_4h and structure_1h: trigger.append("4h and fresh 1h structure confirm realization")\n        else: missing.append("4h plus fresh 1h BOS/CHoCH trigger")\n''', '''        if structure_1h:\n            trigger.append("fresh 1h structure confirms realization")\n            if structure_4h:\n                trigger.append("4h BOS/CHoCH strengthens the thesis")\n        else:\n            missing.append("fresh 1h BOS/CHoCH trigger")\n''')

# Release SHA on every telemetry event so dashboard cohorts do not mix deployments.
replace_once('core/setup_audit.py', '''DB_PATH = os.environ.get("APEX_SETUP_AUDIT_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "setup_audit.db"))\n_MAX_PAYLOAD_CHARS = 60000\n''', '''DB_PATH = os.environ.get("APEX_SETUP_AUDIT_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "setup_audit.db"))\nRELEASE_SHA = (os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "").strip()\n_MAX_PAYLOAD_CHARS = 60000\n''')
replace_once('core/setup_audit.py', '''def emit_event(kind: str, strategy: str, symbol: str, payload: dict[str, Any], *, event_key: str | None = None) -> str:\n    key = event_key or str(uuid.uuid4())\n    event = {"event_key": key, "kind": str(kind), "strategy": str(strategy or "").upper(),\n             "symbol": str(symbol or "").upper(), "occurred_at": _utc_now(),\n             "payload": payload if isinstance(payload, dict) else {}}\n''', '''def emit_event(kind: str, strategy: str, symbol: str, payload: dict[str, Any], *, event_key: str | None = None) -> str:\n    key = event_key or str(uuid.uuid4())\n    payload_data = dict(payload) if isinstance(payload, dict) else {}\n    if RELEASE_SHA:\n        payload_data.setdefault("release_sha", RELEASE_SHA)\n    event = {"event_key": key, "kind": str(kind), "strategy": str(strategy or "").upper(),\n             "symbol": str(symbol or "").upper(), "occurred_at": _utc_now(),\n             "payload": payload_data}\n''')

# Catalog: describe the live rules rather than old hard blockers / RR ceilings.
p = Path('core/strategy_catalog.py'); s = p.read_text(encoding='utf-8')
s = s.replace('"rr": "Detector 2.0–4.0; Signal Integrity min 2.0",', '"rr": "Minimum 2.0; no upper RR ceiling; structural targets only",', 1)
s = s.replace('_c("fast_15m_impulse_volume", "Trigger", "Latest confirmed 15m impulse volume ≥ 1.1× average"),', '_c("fast_15m_impulse_volume", "Context", "Preliminary 15m impulse volume ≥ 1.1× average", required=False, detail="Telemetry/context only; executable trigger still requires 1.6× volume."),', 1)
s = s.replace('"timeframes": "4H thesis/trigger + mandatory fresh 1H structure; 1D/1W context",', '"timeframes": "4H thesis/context → mandatory fresh 1H structure → 15m execution",', 1)
s = s.replace('"rr": "Detector accepts 2.0–4.0; final Signal Integrity min 2.5",', '"rr": "Minimum 2.0; no upper RR ceiling; structural targets only",', 1)
s = s.replace('_c("swing_4h_structure", "Structure", "Confirmed BOS/CHoCH after trigger"),', '_c("swing_4h_structure", "Context", "4H BOS/CHoCH after trigger strengthens thesis", required=False),', 1)
s = s.replace('_c("swing_setup_evidence", "Final deterministic gate", "SWING causal matrix remains complete"),', '_c("swing_setup_evidence", "Final deterministic gate", "SWING causal matrix requires 4H thesis/location plus fresh 1H structure and 15m execution"),', 1)
p.write_text(s, encoding='utf-8')

# Dashboard: filter by latest observed release, expose observed funnel and WYCKOFF range distribution.
p = Path('stats_server.py'); s = p.read_text(encoding='utf-8')
anchor = '\n\ndef build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",\n'
if anchor not in s: raise SystemExit('stats build_dashboard anchor missing')
helpers = r'''

def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    pos = (len(ordered) - 1) * max(0.0, min(float(fraction), 1.0))
    lo, hi = int(pos), min(int(pos) + 1, len(ordered) - 1)
    if lo == hi:
        return round(ordered[lo], 3)
    weight = pos - lo
    return round(ordered[lo] * (1 - weight) + ordered[hi] * weight, 3)


def _observed_funnels(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("strategy") or "UNKNOWN")].append(row)
    result = []
    for strategy, items in sorted(grouped.items()):
        steps: dict[str, dict[str, Any]] = {}
        for row in items:
            seen = set()
            for idx, check in enumerate(row.get("checks", []) if isinstance(row.get("checks"), list) else []):
                if not isinstance(check, dict):
                    continue
                label = str(check.get("label") or check.get("condition") or check.get("code") or "check")[:180]
                if label in seen:
                    continue
                seen.add(label)
                bucket = steps.setdefault(label, {"label": label, "reached": 0, "passed": 0, "failed": 0, "positions": []})
                bucket["reached"] += 1
                state = str(check.get("state") or "").upper()
                bucket["passed"] += int(state == "PASS")
                bucket["failed"] += int(state == "FAIL")
                bucket["positions"].append(idx)
        min_reached = max(2, int(len(items) * 0.02))
        ordered = []
        for bucket in steps.values():
            if bucket["reached"] < min_reached:
                continue
            avg_pos = sum(bucket["positions"]) / len(bucket["positions"]) if bucket["positions"] else 999
            ordered.append({
                "label": bucket["label"], "reached": bucket["reached"], "passed": bucket["passed"], "failed": bucket["failed"],
                "pass_rate": round(bucket["passed"] / bucket["reached"] * 100, 1) if bucket["reached"] else None,
                "from_attempts": round(bucket["passed"] / len(items) * 100, 1) if items else None,
                "avg_position": round(avg_pos, 2),
            })
        ordered.sort(key=lambda x: (x["avg_position"], -x["reached"]))
        result.append({
            "strategy": strategy, "attempts": len(items), "steps": ordered[:18],
            "candidates": sum(str(x.get("outcome") or "").upper() == "CANDIDATE" for x in items),
            "groq": sum(bool(x.get("groq_review")) for x in items),
            "delivered": sum(any(str(d.get("stage") or "").lower() == "delivered" or str(d.get("outcome") or "").upper() == "ACCEPT" for d in x.get("decisions", [])) for x in items),
        })
    return result
'''
s = s.replace(anchor, helpers + anchor, 1)
s = s.replace('''                    min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",\n                    page: int = 1, page_size: int = 100) -> dict[str, Any]:\n    events = _fetch(days, strategy, symbol, from_date, to_date); attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]; trade_events=[]\n''', '''                    min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",\n                    page: int = 1, page_size: int = 100, release: str = "") -> dict[str, Any]:\n    events = _fetch(days, strategy, symbol, from_date, to_date)\n    available_releases = []\n    for e in events:\n        sha = str((e.get("payload") or {}).get("release_sha") or "").strip()\n        if sha and sha not in available_releases:\n            available_releases.append(sha)\n    active_release = available_releases[0] if release == "latest" and available_releases else str(release or "").strip()\n    if active_release:\n        events = [e for e in events if str((e.get("payload") or {}).get("release_sha") or "").strip() == active_release]\n    attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]; trade_events=[]\n''', 1)
needle = '    total=len(joined); page_size=max(20,min(int(page_size),200)); page=max(1,int(page)); start=(page-1)*page_size\n'
if needle not in s: raise SystemExit('stats total anchor missing')
insert = '''    funnels = _observed_funnels(joined)\n    wy_dist_values = []\n    for r in joined:\n        if str(r.get("strategy") or "").upper() != "WYCKOFF" or str(r.get("subtype") or "").upper() != "DISTRIBUTION":\n            continue\n        c = r.get("candidate") if isinstance(r.get("candidate"), dict) else {}\n        stop = r.get("stop") if isinstance(r.get("stop"), dict) else {}\n        snap = stop.get("snapshot") if isinstance(stop.get("snapshot"), dict) else {}\n        value = _num(c.get("dist_range") if c.get("dist_range") is not None else snap.get("dist_range_pct"))\n        if value is not None:\n            wy_dist_values.append(value)\n    wy_dist_range = {\n        "count": len(wy_dist_values),\n        "min": round(min(wy_dist_values), 3) if wy_dist_values else None,\n        "p25": _percentile(wy_dist_values, 0.25),\n        "median": _percentile(wy_dist_values, 0.50),\n        "p75": _percentile(wy_dist_values, 0.75),\n        "p90": _percentile(wy_dist_values, 0.90),\n        "max": round(max(wy_dist_values), 3) if wy_dist_values else None,\n    }\n\n'''
s = s.replace(needle, insert + needle, 1)
s = s.replace('''    return {"period_days":days,"baseline":"post97","baseline_utc":STATS_BASELINE_UTC.isoformat(),"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),\n''', '''    return {"period_days":days,"baseline":"post97","baseline_utc":STATS_BASELINE_UTC.isoformat(),"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),\n      "release_filter":release,"release_sha":active_release,"available_releases":available_releases[:12],"funnels":funnels,"wyckoff_dist_range":wy_dist_range,\n''', 1)
s = s.replace('''val("fromdate"),val("todate"),int(val("page","1")),int(val("page_size","100"))); self._json(data)''', '''val("fromdate"),val("todate"),int(val("page","1")),int(val("page_size","100")),val("release")); self._json(data)''', 1)
s = s.replace('<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button><button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button></div>', '<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button><button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button><button class=btn id=latestRelease>После последнего deploy</button></div>', 1)
s = s.replace('<div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2>', '<div class="section card"><h2>Сквозная воронка по стратегиям</h2><div id=funnels></div><div id=wyRange class=muted style="margin-top:10px"></div></div><div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2>', 1)
s = s.replace("let DAYS=1,STRATEGY='',PAGE=1,LAST=null;", "let DAYS=1,STRATEGY='',PAGE=1,LAST=null,RELEASE='';", 1)
s = s.replace("const p=new URLSearchParams({key:TOKEN,days:DAYS,strategy:STRATEGY,page:PAGE,page_size:100});", "const p=new URLSearchParams({key:TOKEN,days:DAYS,strategy:STRATEGY,page:PAGE,page_size:100});if(RELEASE)p.set('release',RELEASE);", 1)
render_anchor = "function render(){updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC';summary.innerHTML=cards(LAST.summary);"
if render_anchor not in s: raise SystemExit('render anchor missing')
render_fn = "function renderFunnels(){const data=(LAST.funnels||[]).filter(x=>!STRATEGY||x.strategy===STRATEGY);funnels.innerHTML=data.map(f=>`<div style=\"margin-bottom:14px\"><b>${esc(f.strategy)}</b> · старт ${f.attempts} → кандидаты ${f.candidates} → Groq ${f.groq} → отправлено ${f.delivered}<div class=muted>${f.steps.map(x=>`${esc(x.label)}: ${x.passed}/${x.reached} (${x.pass_rate??0}%)`).join(' → ')}</div></div>`).join('')||'<span class=muted>Нет данных</span>';const w=LAST.wyckoff_dist_range||{};wyRange.textContent=w.count?`WYCKOFF Distribution range: n=${w.count} · P25=${w.p25}% · median=${w.median}% · P75=${w.p75}% · P90=${w.p90}%`:'';}"
s = s.replace(render_anchor, render_fn + render_anchor.replace("summary.innerHTML=cards(LAST.summary);", "summary.innerHTML=cards(LAST.summary);renderFunnels();"), 1)
s = s.replace("refresh.onclick=load;prev.onclick=", "refresh.onclick=load;latestRelease.onclick=()=>{RELEASE=RELEASE?'':'latest';latestRelease.classList.toggle('active',!!RELEASE);PAGE=1;load()};prev.onclick=", 1)
s = s.replace("updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC';", "updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC'+(LAST.release_sha?' · '+LAST.release_sha.slice(0,8):'');", 1)
p.write_text(s, encoding='utf-8')

print('strategy funnel cleanup patch applied')
