from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"anchor not found in {path}: {old[:140]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# SWING: 4h BOS/CHoCH remains evidence, but fresh 1h structure is the mandatory realization trigger.
replace_once(
    "market.py",
    '''        # ── BOS/CHoCH after the sweep, confirmed by candle close ──
        # The canonical engine distinguishes continuation (BOS) from a real
        # change of character (CHoCH) using the prior paired swing structure.
        _swing_structure_event = get_bos_choch_event(
            candles,
            direction,
            lookback=30,
            max_break_age=max(1, trigger_lookback),
        )
        if _audit_test('SWING_DETECT_SWING_SETUP_G7403', (not _swing_structure_event), 'not _swing_structure_event', 'not _swing_structure_event', 7403):
            logging.info(f"[SWING] {symbol}: нет подтверждённого BOS/CHoCH после триггера")
            return _audit_fail('SWING_DETECT_SWING_SETUP_R7405', 'not _swing_structure_event', locals(), 'not _swing_structure_event', 7405)
''',
    '''        # ── 4h BOS/CHoCH is thesis-quality context, not a second hard trigger. ──
        # Direction already comes from a 4h sweep/EQH-EQL/OB reaction. Execution
        # still requires a fresh 1h BOS/CHoCH and the 15m entry trigger below.
        _swing_structure_event = get_bos_choch_event(
            candles,
            direction,
            lookback=30,
            max_break_age=max(1, trigger_lookback),
        )
        _audit_test(
            'SWING_4H_STRUCTURE_CONTEXT',
            (not _swing_structure_event),
            '4h BOS/CHoCH context after trigger (non-blocking)',
            'not _swing_structure_event',
            7403,
        )
        if not _swing_structure_event:
            logging.info(f"[SWING] {symbol}: 4h BOS/CHoCH не подтверждён — context weak; обязательный 1h trigger остаётся")
''',
)

# FAST: the preliminary 1.1x impulse-volume observation is context only.
replace_once(
    "market.py",
    '''        # Volume check на 15m impulse — должен быть выше среднего
        _avg_vol_15m_imp = sum(c.get("volume", 0) for c in candles_15m_imp[:-1]) / max(len(candles_15m_imp) - 1, 1)
        if _audit_test('FAST_DETECT_FAST_DEAL_G9249', (_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1), 'Volume check на 15m impulse — должен быть выше среднего', '_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1', 9249):
            return _audit_fail('FAST_DETECT_FAST_DEAL_R9250', 'Volume check на 15m impulse — должен быть выше среднего', locals(), '_avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1', 9250)  # Импульс без объёма — ненадёжный
''',
    '''        # Preliminary impulse volume is context only. The executable trigger below
        # still requires the mandatory 1.6x volume spike on the actual trigger candle.
        _avg_vol_15m_imp = sum(c.get("volume", 0) for c in candles_15m_imp[:-1]) / max(len(candles_15m_imp) - 1, 1)
        _fast_impulse_volume_context_weak = bool(
            _avg_vol_15m_imp > 0 and last_15m.get("volume", 0) < _avg_vol_15m_imp * 1.1
        )
        _audit_test(
            'FAST_IMPULSE_VOLUME_CONTEXT',
            _fast_impulse_volume_context_weak,
            '15m preliminary impulse volume >= 1.1x average (non-blocking)',
            '_fast_impulse_volume_context_weak',
            9249,
        )
''',
)

# WYCKOFF Distribution: keep 25% unchanged, but record both PASS and FAIL for a clean distribution.
replace_once(
    "market.py",
    '''        if dist_range_pct < 15:
            score += 20
            signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")
        elif dist_range_pct < 25:
            score += 10
            signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")
        else:
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725', 'dist_range_pct < 25', locals(), 'dist_range_pct < 25', 8725)
''',
    '''        _dist_range_too_wide = dist_range_pct >= 25
        if _audit_test(
            'WYCKOFF_DIST_RANGE',
            _dist_range_too_wide,
            'WYCKOFF Distribution: 30d range < 25%',
            'dist_range_pct >= 25',
            8720,
        ):
            return _audit_fail('WYCKOFF_DETECT_WYCKOFF_DISTRIBUTION_R8725', 'WYCKOFF Distribution: 30d range < 25%', locals(), 'dist_range_pct >= 25', 8725)
        if dist_range_pct < 15:
            score += 20
            signals.append(f"✅ Боковик {dist_range_pct:.1f}% у вершины")
        else:
            score += 10
            signals.append(f"⚡️ Диапазон {dist_range_pct:.1f}% у вершины")
''',
)

# Setup-evidence causal matrix: 4h structure supports SWING; fresh 1h structure gates realization.
replace_once(
    "core/setup_evidence.py",
    '    "SWING": {"core": "HTF thesis and major liquidity/OB/FVG reaction", "trigger": "4h plus fresh closed 1h BOS/CHoCH"},',
    '    "SWING": {"core": "4h liquidity/OB/FVG thesis; 4h BOS/CHoCH is quality context", "trigger": "fresh closed 1h BOS/CHoCH plus 15m execution trigger"},',
)
replace_once(
    "core/setup_evidence.py",
    '''        if structure_4h and structure_1h: trigger.append("4h and fresh 1h structure confirm realization")
        else: missing.append("4h plus fresh 1h BOS/CHoCH trigger")
''',
    '''        if structure_1h:
            trigger.append("fresh 1h structure confirms realization")
            if structure_4h:
                trigger.append("4h BOS/CHoCH strengthens the thesis")
        else:
            missing.append("fresh 1h BOS/CHoCH trigger")
''',
)

# Stamp every telemetry payload with the actual Render/Git commit for clean cohorts.
replace_once(
    "core/setup_audit.py",
    'DB_PATH = os.environ.get("APEX_SETUP_AUDIT_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "setup_audit.db"))\n_MAX_PAYLOAD_CHARS = 60000',
    'DB_PATH = os.environ.get("APEX_SETUP_AUDIT_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "setup_audit.db"))\nRELEASE_SHA = (os.environ.get("RENDER_GIT_COMMIT") or os.environ.get("GIT_COMMIT") or "").strip()\n_MAX_PAYLOAD_CHARS = 60000',
)
replace_once(
    "core/setup_audit.py",
    '''def emit_event(kind: str, strategy: str, symbol: str, payload: dict[str, Any], *, event_key: str | None = None) -> str:
    key = event_key or str(uuid.uuid4())
    event = {"event_key": key, "kind": str(kind), "strategy": str(strategy or "").upper(),
             "symbol": str(symbol or "").upper(), "occurred_at": _utc_now(),
             "payload": payload if isinstance(payload, dict) else {}}
''',
    '''def emit_event(kind: str, strategy: str, symbol: str, payload: dict[str, Any], *, event_key: str | None = None) -> str:
    key = event_key or str(uuid.uuid4())
    payload_data = dict(payload) if isinstance(payload, dict) else {}
    if RELEASE_SHA:
        payload_data.setdefault("release_sha", RELEASE_SHA)
    event = {"event_key": key, "kind": str(kind), "strategy": str(strategy or "").upper(),
             "symbol": str(symbol or "").upper(), "occurred_at": _utc_now(),
             "payload": payload_data}
''',
)

# Keep the human-readable catalog aligned with runtime behavior.
p = Path("core/strategy_catalog.py")
s = p.read_text(encoding="utf-8")
replacements = [
    ('"rr": "Detector 2.0–4.0; Signal Integrity min 2.0",', '"rr": "Minimum 2.0; no upper RR ceiling; structural targets only",'),
    ('_c("fast_15m_impulse_volume", "Trigger", "Latest confirmed 15m impulse volume ≥ 1.1× average"),', '_c("fast_15m_impulse_volume", "Context", "Preliminary 15m impulse volume ≥ 1.1× average", required=False, detail="Telemetry/context only; executable trigger still requires 1.6× volume."),'),
    ('"timeframes": "4H thesis/trigger + mandatory fresh 1H structure; 1D/1W context",', '"timeframes": "4H thesis/context → mandatory fresh 1H structure → 15m execution",'),
    ('"rr": "Detector accepts 2.0–4.0; final Signal Integrity min 2.5",', '"rr": "Minimum 2.0; no upper RR ceiling; structural targets only",'),
    ('_c("swing_4h_structure", "Structure", "Confirmed BOS/CHoCH after trigger"),', '_c("swing_4h_structure", "Context", "4H BOS/CHoCH after trigger strengthens thesis", required=False),'),
    ('_c("swing_setup_evidence", "Final deterministic gate", "SWING causal matrix remains complete"),', '_c("swing_setup_evidence", "Final deterministic gate", "SWING causal matrix requires 4H thesis/location plus fresh 1H structure and 15m execution"),'),
]
for old, new in replacements:
    if old not in s:
        raise SystemExit(f"catalog anchor missing: {old}")
    s = s.replace(old, new, 1)
p.write_text(s, encoding="utf-8")

# Dashboard helpers.
p = Path("stats_server.py")
s = p.read_text(encoding="utf-8")
helper_anchor = '''\n\ndef build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",\n'''
if helper_anchor not in s:
    raise SystemExit("stats build_dashboard anchor missing")
helpers = '''\n\ndef _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    pos = (len(ordered) - 1) * max(0.0, min(float(fraction), 1.0))
    lo = int(pos); hi = min(lo + 1, len(ordered) - 1)
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
            raw_checks = row.get("checks", []) if isinstance(row.get("checks"), list) else []
            for idx, check in enumerate(raw_checks):
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
s = s.replace(helper_anchor, helpers + helper_anchor, 1)

old_sig = '''def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
                    min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",
                    page: int = 1, page_size: int = 100) -> dict[str, Any]:
    events = _fetch(days, strategy, symbol, from_date, to_date); attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]; trade_events=[]
'''
new_sig = '''def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",
                    min_rr: float | None = None, max_rr: float | None = None, from_date: str = "", to_date: str = "",
                    page: int = 1, page_size: int = 100, release: str = "") -> dict[str, Any]:
    events = _fetch(days, strategy, symbol, from_date, to_date)
    available_releases = []
    for e in events:
        sha = str((e.get("payload") or {}).get("release_sha") or "").strip()
        if sha and sha not in available_releases:
            available_releases.append(sha)
    active_release = available_releases[0] if release == "latest" and available_releases else str(release or "").strip()
    if active_release:
        events = [e for e in events if str((e.get("payload") or {}).get("release_sha") or "").strip() == active_release]
    attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]; trade_events=[]
'''
if old_sig not in s:
    raise SystemExit("stats dashboard signature anchor missing")
s = s.replace(old_sig, new_sig, 1)

metric_anchor = '''    total=len(joined); page_size=max(20,min(int(page_size),200)); page=max(1,int(page)); start=(page-1)*page_size
'''
metric_block = '''    funnels = _observed_funnels(joined)
    wy_dist_values = []
    for r in joined:
        if str(r.get("strategy") or "").upper() != "WYCKOFF" or str(r.get("subtype") or "").upper() != "DISTRIBUTION":
            continue
        c = r.get("candidate") if isinstance(r.get("candidate"), dict) else {}
        stop = r.get("stop") if isinstance(r.get("stop"), dict) else {}
        snap = stop.get("snapshot") if isinstance(stop.get("snapshot"), dict) else {}
        value = _num(c.get("dist_range") if c.get("dist_range") is not None else snap.get("dist_range_pct"))
        if value is not None:
            wy_dist_values.append(value)
    wy_dist_range = {
        "count": len(wy_dist_values),
        "min": round(min(wy_dist_values), 3) if wy_dist_values else None,
        "p25": _percentile(wy_dist_values, 0.25), "median": _percentile(wy_dist_values, 0.50),
        "p75": _percentile(wy_dist_values, 0.75), "p90": _percentile(wy_dist_values, 0.90),
        "max": round(max(wy_dist_values), 3) if wy_dist_values else None,
    }

'''
if metric_anchor not in s:
    raise SystemExit("stats metric anchor missing")
s = s.replace(metric_anchor, metric_block + metric_anchor, 1)

return_anchor = '''    return {"period_days":days,"baseline":"post97","baseline_utc":STATS_BASELINE_UTC.isoformat(),"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),
'''
return_new = return_anchor + '''      "release_filter":release,"release_sha":active_release,"available_releases":available_releases[:12],"funnels":funnels,"wyckoff_dist_range":wy_dist_range,
'''
if return_anchor not in s:
    raise SystemExit("stats return anchor missing")
s = s.replace(return_anchor, return_new, 1)

api_old = '''val("fromdate"),val("todate"),int(val("page","1")),int(val("page_size","100"))); self._json(data)'''
api_new = '''val("fromdate"),val("todate"),int(val("page","1")),int(val("page_size","100")),val("release")); self._json(data)'''
if api_old not in s:
    raise SystemExit("stats api anchor missing")
s = s.replace(api_old, api_new, 1)

period_old = '''<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button><button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button></div>'''
period_new = '''<div class=tabs id=periods><button class="btn active" data-days=1>24 часа</button><button class=btn data-days=7>7 дней</button><button class=btn data-days=30>30 дней</button><button class=btn id=latestRelease>После последнего deploy</button></div>'''
if period_old not in s:
    raise SystemExit("stats period anchor missing")
s = s.replace(period_old, period_new, 1)

section_old = '''<div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2>'''
section_new = '''<div class="section card"><h2>Сквозная воронка по стратегиям</h2><div id=funnels></div><div id=wyRange class=muted style="margin-top:10px"></div></div><div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2>'''
if section_old not in s:
    raise SystemExit("stats section anchor missing")
s = s.replace(section_old, section_new, 1)

if "let DAYS=1,STRATEGY='',PAGE=1,LAST=null;" not in s:
    raise SystemExit("stats js state anchor missing")
s = s.replace("let DAYS=1,STRATEGY='',PAGE=1,LAST=null;", "let DAYS=1,STRATEGY='',PAGE=1,LAST=null,RELEASE='';", 1)

params_old = '''const p=new URLSearchParams({key:TOKEN,days:DAYS,strategy:STRATEGY,page:PAGE,page_size:100});'''
params_new = '''const p=new URLSearchParams({key:TOKEN,days:DAYS,strategy:STRATEGY,page:PAGE,page_size:100});if(RELEASE)p.set('release',RELEASE);'''
if params_old not in s:
    raise SystemExit("stats params anchor missing")
s = s.replace(params_old, params_new, 1)

render_old = '''function render(){updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC';summary.innerHTML=cards(LAST.summary);'''
render_new = '''function renderFunnels(){const data=(LAST.funnels||[]).filter(x=>!STRATEGY||x.strategy===STRATEGY);funnels.innerHTML=data.map(f=>`<div style="margin-bottom:14px"><b>${esc(f.strategy)}</b> · старт ${f.attempts} → кандидаты ${f.candidates} → Groq ${f.groq} → отправлено ${f.delivered}<div class=muted>${f.steps.map(x=>`${esc(x.label)}: ${x.passed}/${x.reached} (${x.pass_rate??0}%)`).join(' → ')}</div></div>`).join('')||'<span class=muted>Нет данных</span>';const w=LAST.wyckoff_dist_range||{};wyRange.textContent=w.count?`WYCKOFF Distribution range: n=${w.count} · P25=${w.p25}% · median=${w.median}% · P75=${w.p75}% · P90=${w.p90}%`:'';}function render(){updated.textContent='Обновлено '+LAST.generated_at.replace('T',' ').slice(0,19)+' UTC'+(LAST.release_sha?' · '+LAST.release_sha.slice(0,8):'');summary.innerHTML=cards(LAST.summary);renderFunnels();'''
if render_old not in s:
    raise SystemExit("stats render anchor missing")
s = s.replace(render_old, render_new, 1)

controls_old = '''refresh.onclick=load;prev.onclick='''
controls_new = '''refresh.onclick=load;latestRelease.onclick=()=>{RELEASE=RELEASE?'':'latest';latestRelease.classList.toggle('active',!!RELEASE);PAGE=1;load()};prev.onclick='''
if controls_old not in s:
    raise SystemExit("stats controls anchor missing")
s = s.replace(controls_old, controls_new, 1)

p.write_text(s, encoding="utf-8")
print("strategy funnel cleanup v2 applied")
