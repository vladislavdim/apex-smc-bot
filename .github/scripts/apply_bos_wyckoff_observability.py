from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one match, found {count}: {old[:100]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# core/setup_audit.py: keep telemetry inside the existing attempt payload.
replace_once(
    "core/setup_audit.py",
    '            "scanner": runtime.get("scanner"), "started_at": _utc_now(), "started_monotonic": time.monotonic(),\n            "checks": [], "finished": False, "stop": None}',
    '            "scanner": runtime.get("scanner"), "started_at": _utc_now(), "started_monotonic": time.monotonic(),\n            "checks": [], "telemetry": {}, "finished": False, "stop": None}',
)
replace_once(
    "core/setup_audit.py",
    '               "outcome": outcome, "stop": context.get("stop"), "checks": context.get("checks", []),\n               "candidate": _candidate_snapshot(candidate or {}), "error": str(error)[:2000] if error else ""}',
    '               "outcome": outcome, "stop": context.get("stop"), "checks": context.get("checks", []),\n               "telemetry": _safe_value(context.get("telemetry") or {}) or {},\n               "candidate": _candidate_snapshot(candidate or {}), "error": str(error)[:2000] if error else ""}',
)
replace_once(
    "core/setup_audit.py",
    '\n\ndef _compact_label(label: str, condition: str = "") -> str:\n',
    '''\n\ndef audit_observe(key: str, value: Any, *, append: bool = False) -> None:\n    """Attach fail-open, decision-neutral telemetry to the current strategy attempt."""\n    context = _current()\n    if context is None:\n        return\n    try:\n        safe = _safe_value(value)\n        if safe is None:\n            return\n        telemetry = context.setdefault("telemetry", {})\n        name = str(key or "")[:100]\n        if not name:\n            return\n        if append:\n            bucket = telemetry.setdefault(name, [])\n            if isinstance(bucket, list):\n                bucket.append(safe)\n        elif isinstance(telemetry.get(name), dict) and isinstance(safe, dict):\n            telemetry[name].update(safe)\n        else:\n            telemetry[name] = safe\n    except Exception:\n        pass\n\n\ndef _compact_label(label: str, condition: str = "") -> str:\n''',
)

# market.py: import the passive observer only; all existing blockers/thresholds stay untouched.
replace_once(
    "market.py",
    'from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail, emit_event as _emit_stats_event',
    'from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail, audit_observe as _audit_observe, emit_event as _emit_stats_event',
)

# SWING: age of the exact 1h structure event + progression of existing stages.
replace_once(
    "market.py",
    '''        event = get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)\n        if not event:\n            return out\n        out["structure_event"] = event\n        out["structure_ok"] = True\n''',
    '''        event = get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)\n        if not event:\n            return out\n        _swing_bos_age = max(1, len(c1h) - int(event.get("candle_index", len(c1h) - 1)))\n        _audit_observe("bos_events", {\n            "role": "SWING_ENTRY_STRUCTURE", "timeframe": "1h", "age_bars": _swing_bos_age,\n            "event_type": event.get("type"), "direction": event.get("direction"),\n        }, append=True)\n        _audit_observe("bos_progress", {"structure_confirmed": True})\n        out["structure_event"] = event\n        out["structure_ok"] = True\n''',
)
replace_once(
    "market.py",
    '''        if not zones:\n            return out\n        out["zone_ok"] = True\n''',
    '''        if not zones:\n            return out\n        out["zone_ok"] = True\n        _audit_observe("bos_progress", {"zone_reached": True, "zone_confirmed": True})\n''',
)
replace_once(
    "market.py",
    '''        if not touched:\n            return out\n        touched.sort(key=lambda x: x[0])\n        distance, zone_type, bottom, top = touched[0]\n        out["retest_ok"] = True\n''',
    '''        if not touched:\n            return out\n        touched.sort(key=lambda x: x[0])\n        distance, zone_type, bottom, top = touched[0]\n        out["retest_ok"] = True\n        _audit_observe("bos_progress", {"retest_reached": True, "retest_confirmed": True})\n''',
)
replace_once(
    "market.py",
    '''        out["volume_ok"] = bool(avg_vol > 0 and last_vol >= avg_vol * 1.20)\n\n        out["chase_ok"] = bool(distance <= atr1h * 0.75)\n        if not out["displacement_ok"] or not out["volume_ok"] or not out["chase_ok"]:\n''',
    '''        out["volume_ok"] = bool(avg_vol > 0 and last_vol >= avg_vol * 1.20)\n\n        out["chase_ok"] = bool(distance <= atr1h * 0.75)\n        _audit_observe("bos_progress", {\n            "displacement_reached": True, "displacement_confirmed": bool(out["displacement_ok"]),\n            "volume_reached": True, "volume_confirmed": bool(out["volume_ok"]),\n        })\n        if not out["displacement_ok"] or not out["volume_ok"] or not out["chase_ok"]:\n''',
)
replace_once(
    "market.py",
    '''        out["entry"] = smart_round(entry)\n        out["sl"] = smart_round(sl)\n        out["ready"] = True\n        return out\n''',
    '''        out["entry"] = smart_round(entry)\n        out["sl"] = smart_round(sl)\n        out["ready"] = True\n        _audit_observe("bos_progress", {"ltf_ready": True})\n        return out\n''',
)
replace_once(
    "market.py",
    '''        rr_check = reward / risk\n        if _audit_test('SWING_DETECT_SWING_SETUP_G7511', (rr_check < 2.0), 'rr_check < 2.0', 'rr_check < 2.0', 7511):\n''',
    '''        rr_check = reward / risk\n        _audit_observe("bos_progress", {"rr_reached": True, "rr_passed": bool(rr_check >= 2.0)})\n        if _audit_test('SWING_DETECT_SWING_SETUP_G7511', (rr_check < 2.0), 'rr_check < 2.0', 'rr_check < 2.0', 7511):\n''',
)

# FAST: log initial 15m structure age and the later executable structure event separately.
replace_once(
    "market.py",
    '''        direction = "BULLISH" if _fast_bull_event else "BEARISH"\n\n        # Balanced replay winner: at least one pair HTF supports the 15m thesis.\n''',
    '''        direction = "BULLISH" if _fast_bull_event else "BEARISH"\n        _fast_thesis_event = _fast_bull_event or _fast_bear_event\n        _fast_bos_age = max(1, len(_fast_context_15m) - int(_fast_thesis_event.get("candle_index", len(_fast_context_15m) - 1)))\n        _audit_observe("bos_events", {\n            "role": "FAST_THESIS", "timeframe": "15m", "age_bars": _fast_bos_age,\n            "event_type": _fast_thesis_event.get("type"), "direction": _fast_thesis_event.get("direction"),\n        }, append=True)\n        _audit_observe("bos_progress", {"structure_confirmed": True})\n\n        # Balanced replay winner: at least one pair HTF supports the 15m thesis.\n''',
)
replace_once(
    "market.py",
    '''        _fast_ltf_retest = any(\n            float(c["low"]) <= _fast_zone_top + _fast_zone_tol\n            and float(c["high"]) >= _fast_zone_bottom - _fast_zone_tol\n            for c in candles_15m[-8:]\n        )\n        if _audit_test('FAST_LTF_RETEST', (not _fast_ltf_retest), 'FAST: recent 15m OB/FVG retest', 'not _fast_ltf_retest', 9261):\n''',
    '''        _fast_ltf_retest = any(\n            float(c["low"]) <= _fast_zone_top + _fast_zone_tol\n            and float(c["high"]) >= _fast_zone_bottom - _fast_zone_tol\n            for c in candles_15m[-8:]\n        )\n        _audit_observe("bos_progress", {"retest_reached": True, "retest_confirmed": bool(_fast_ltf_retest)})\n        if _audit_test('FAST_LTF_RETEST', (not _fast_ltf_retest), 'FAST: recent 15m OB/FVG retest', 'not _fast_ltf_retest', 9261):\n''',
)
replace_once(
    "market.py",
    '''        engulfing_found = False\n        entry = None\n        sl = None\n\n        for i in range(1, 11):  # смотрим 10 свечей назад\n''',
    '''        engulfing_found = False\n        entry = None\n        sl = None\n        _fast_telem_displacement_seen = False\n        _fast_telem_engulfing_seen = False\n        _fast_telem_volume_confirmed = False\n\n        for i in range(1, 11):  # смотрим 10 свечей назад\n''',
)
replace_once(
    "market.py",
    '''            if curr_range > 0 and curr_body / curr_range < 0.65:\n                continue\n\n            # Engulfing паттерн\n''',
    '''            if curr_range > 0 and curr_body / curr_range < 0.65:\n                continue\n            _fast_telem_displacement_seen = True\n\n            # Engulfing паттерн\n''',
)
replace_once(
    "market.py",
    '''                entry = smart_round(curr["close"])\n                sl = smart_round(curr["high"] + atr_15m * 0.5)\n\n            # Для FAST нужен заметный институциональный объём.\n''',
    '''                entry = smart_round(curr["close"])\n                sl = smart_round(curr["high"] + atr_15m * 0.5)\n\n            _fast_telem_engulfing_seen = True\n            # Для FAST нужен заметный институциональный объём.\n''',
)
replace_once(
    "market.py",
    '''            if avg_vol_15m > 0 and curr["volume"] < avg_vol_15m * _vol_threshold:\n                continue\n\n            engulfing_found = True\n''',
    '''            if avg_vol_15m > 0 and curr["volume"] < avg_vol_15m * _vol_threshold:\n                continue\n\n            _fast_telem_volume_confirmed = True\n            engulfing_found = True\n''',
)
replace_once(
    "market.py",
    '''        if _audit_test('FAST_DETECT_FAST_DEAL_G9303', (not engulfing_found or entry is None), 'not engulfing_found or entry is None', 'not engulfing_found or entry is None', 9303):\n''',
    '''        _audit_observe("bos_progress", {\n            "displacement_reached": True, "displacement_confirmed": bool(_fast_telem_displacement_seen),\n            "volume_reached": bool(_fast_telem_engulfing_seen), "volume_confirmed": bool(_fast_telem_volume_confirmed),\n        })\n        if _audit_test('FAST_DETECT_FAST_DEAL_G9303', (not engulfing_found or entry is None), 'not engulfing_found or entry is None', 'not engulfing_found or entry is None', 9303):\n''',
)
replace_once(
    "market.py",
    '''        if _audit_test('FAST_DETECT_FAST_DEAL_G9332', (not _fast_structure_event), 'not _fast_structure_event', 'not _fast_structure_event', 9332):\n            logging.debug(f"[FAST] {symbol}: нет свежего 15m BOS/CHoCH")\n            return _audit_fail('FAST_DETECT_FAST_DEAL_R9334', 'not _fast_structure_event', locals(), 'not _fast_structure_event', 9334)\n\n        # ── TP = confirmed 15m swing liquidity ──\n''',
    '''        if _audit_test('FAST_DETECT_FAST_DEAL_G9332', (not _fast_structure_event), 'not _fast_structure_event', 'not _fast_structure_event', 9332):\n            logging.debug(f"[FAST] {symbol}: нет свежего 15m BOS/CHoCH")\n            return _audit_fail('FAST_DETECT_FAST_DEAL_R9334', 'not _fast_structure_event', locals(), 'not _fast_structure_event', 9334)\n        _fast_exec_bos_age = max(1, len(candles_15m) - int(_fast_structure_event.get("candle_index", len(candles_15m) - 1)))\n        _audit_observe("bos_events", {\n            "role": "FAST_EXECUTION", "timeframe": "15m", "age_bars": _fast_exec_bos_age,\n            "event_type": _fast_structure_event.get("type"), "direction": _fast_structure_event.get("direction"),\n        }, append=True)\n\n        # ── TP = confirmed 15m swing liquidity ──\n''',
)
replace_once(
    "market.py",
    '''        rr = round(reward / risk, 2)\n        if _audit_test('FAST_DETECT_FAST_DEAL_G9356', (rr < 2.0), 'rr < 2.0', 'rr < 2.0', 9356):\n''',
    '''        rr = round(reward / risk, 2)\n        _audit_observe("bos_progress", {"rr_reached": True, "rr_passed": bool(rr >= 2.0)})\n        if _audit_test('FAST_DETECT_FAST_DEAL_G9356', (rr < 2.0), 'rr < 2.0', 'rr < 2.0', 9356):\n''',
)

# WYCKOFF Distribution: read-only phase extraction before the existing 25% blocker.
replace_once(
    "market.py",
    '''        dist_range_pct = (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0\n\n        _dist_range_too_wide = dist_range_pct >= 25\n''',
    '''        dist_range_pct = (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0\n        _audit_observe("wyckoff_distribution", {"dist_range_pct": round(dist_range_pct, 6)})\n        try:\n            _telemetry_phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)\n            _telemetry_points = []\n            for _telemetry_name in ("BC", "AR", "ST"):\n                _telemetry_phase = _telemetry_phases.get(_telemetry_name) if isinstance(_telemetry_phases, dict) else None\n                if isinstance(_telemetry_phase, dict) and _telemetry_phase.get("price") is not None:\n                    _telemetry_points.append((_telemetry_name, float(_telemetry_phase["price"])))\n            if len(_telemetry_points) >= 2:\n                _telemetry_prices = [p for _, p in _telemetry_points]\n                _telemetry_box_low = min(_telemetry_prices)\n                _telemetry_box_high = max(_telemetry_prices)\n                _telemetry_box_width_pct = (\n                    (_telemetry_box_high - _telemetry_box_low) / _telemetry_box_low * 100\n                    if _telemetry_box_low > 0 else None\n                )\n                _audit_observe("wyckoff_distribution", {\n                    "distribution_box_width_pct": round(_telemetry_box_width_pct, 6) if _telemetry_box_width_pct is not None else None,\n                    "structure_points": {name: price for name, price in _telemetry_points},\n                })\n        except Exception:\n            pass\n\n        _dist_range_too_wide = dist_range_pct >= 25\n''',
)

# stats_server.py backend aggregates BOS ages/progression and old-vs-new WYCKOFF widths.
replace_once(
    "stats_server.py",
    '''    funnels = _observed_funnels(joined)\n    wy_dist_values = []\n''',
    '''    funnels = _observed_funnels(joined)\n    bos_age_stats = {}\n    for bos_strategy in ("SWING", "FAST"):\n        buckets = {label: {"bucket": label, "events": 0, "retest": 0, "displacement": 0, "volume": 0, "rr": 0, "groq": 0, "delivered": 0}\n                   for label in ("1", "2", "3", "4", "5+")}\n        for r in joined:\n            if str(r.get("strategy") or "").upper() != bos_strategy:\n                continue\n            telemetry = r.get("telemetry") if isinstance(r.get("telemetry"), dict) else {}\n            events_for_attempt = telemetry.get("bos_events") if isinstance(telemetry.get("bos_events"), list) else []\n            progress = telemetry.get("bos_progress") if isinstance(telemetry.get("bos_progress"), dict) else {}\n            reached_groq = bool(r.get("groq_review"))\n            reached_delivery = any(\n                str(d.get("stage") or "").lower() == "delivered" or str(d.get("outcome") or "").upper() == "ACCEPT"\n                for d in r.get("decisions", []) if isinstance(d, dict)\n            )\n            for event in events_for_attempt:\n                if not isinstance(event, dict):\n                    continue\n                age = _num(event.get("age_bars"))\n                if age is None or age < 1:\n                    continue\n                age_i = int(age)\n                label = str(age_i) if age_i <= 4 else "5+"\n                b = buckets[label]\n                b["events"] += 1\n                b["retest"] += int(bool(progress.get("retest_confirmed")))\n                b["displacement"] += int(bool(progress.get("displacement_confirmed")))\n                b["volume"] += int(bool(progress.get("volume_confirmed")))\n                b["rr"] += int(bool(progress.get("rr_reached")))\n                b["groq"] += int(reached_groq)\n                b["delivered"] += int(reached_delivery)\n        rows = []\n        for label in ("1", "2", "3", "4", "5+"):\n            b = buckets[label]\n            n = b["events"]\n            rows.append({**b, "groq_pct": round(b["groq"] / n * 100, 1) if n else 0.0,\n                         "delivered_pct": round(b["delivered"] / n * 100, 1) if n else 0.0})\n        bos_age_stats[bos_strategy] = rows\n\n    wy_dist_values = []\n    wy_box_values = []\n''',
)
replace_once(
    "stats_server.py",
    '''        if value is not None:\n            wy_dist_values.append(value)\n''',
    '''        if value is not None:\n            wy_dist_values.append(value)\n        telemetry = r.get("telemetry") if isinstance(r.get("telemetry"), dict) else {}\n        wy_telemetry = telemetry.get("wyckoff_distribution") if isinstance(telemetry.get("wyckoff_distribution"), dict) else {}\n        box_value = _num(wy_telemetry.get("distribution_box_width_pct"))\n        if box_value is not None:\n            wy_box_values.append(box_value)\n''',
)
replace_once(
    "stats_server.py",
    '''    wy_dist_range = {\n        "count": len(wy_dist_values),\n        "min": round(min(wy_dist_values), 3) if wy_dist_values else None,\n        "p25": _percentile(wy_dist_values, 0.25), "median": _percentile(wy_dist_values, 0.50),\n        "p75": _percentile(wy_dist_values, 0.75), "p90": _percentile(wy_dist_values, 0.90),\n        "max": round(max(wy_dist_values), 3) if wy_dist_values else None,\n    }\n\n    total=len(joined);''',
    '''    wy_dist_range = {\n        "count": len(wy_dist_values),\n        "min": round(min(wy_dist_values), 3) if wy_dist_values else None,\n        "p25": _percentile(wy_dist_values, 0.25), "median": _percentile(wy_dist_values, 0.50),\n        "p75": _percentile(wy_dist_values, 0.75), "p90": _percentile(wy_dist_values, 0.90),\n        "max": round(max(wy_dist_values), 3) if wy_dist_values else None,\n    }\n    wy_box_range = {\n        "count": len(wy_box_values),\n        "min": round(min(wy_box_values), 3) if wy_box_values else None,\n        "p25": _percentile(wy_box_values, 0.25), "median": _percentile(wy_box_values, 0.50),\n        "p75": _percentile(wy_box_values, 0.75), "p90": _percentile(wy_box_values, 0.90),\n        "max": round(max(wy_box_values), 3) if wy_box_values else None,\n    }\n\n    total=len(joined);''',
)
replace_once(
    "stats_server.py",
    '''      "release_filter":release,"release_sha":active_release,"available_releases":available_releases[:12],"funnels":funnels,"wyckoff_dist_range":wy_dist_range,\n''',
    '''      "release_filter":release,"release_sha":active_release,"available_releases":available_releases[:12],"funnels":funnels,\n      "bos_choch_age":bos_age_stats,"wyckoff_dist_range":wy_dist_range,"wyckoff_box_width":wy_box_range,\n''',
)

# Strategy Lab UI: passive histogram-style bars only.
replace_once(
    "stats_server.py",
    '''<div class="section card"><h2>Сквозная воронка по стратегиям</h2><div id=funnels></div><div id=wyRange class=muted style="margin-top:10px"></div></div><div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div></div><div class=card><h2>Groq: причины WAIT/REJECT</h2><div id=groqReasons></div></div></div>''',
    '''<div class="section card"><h2>Сквозная воронка по стратегиям</h2><div id=funnels></div><div id=wyRange class=muted style="margin-top:10px"></div></div><div class="cols section"><div class=card><h2>BOS/CHoCH age telemetry</h2><div id=bosAge></div></div><div class=card><h2>WYCKOFF Distribution width telemetry</h2><div id=wyCompare></div></div></div><div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div></div><div class=card><h2>Groq: причины WAIT/REJECT</h2><div id=groqReasons></div></div></div>''',
)
replace_once(
    "stats_server.py",
    '''function renderRows(){rows.innerHTML=LAST.rows.map((r,i)=>{''',
    '''function renderBosAge(){const data=LAST.bos_choch_age||{};const blocks=['SWING','FAST'].map(st=>{const items=data[st]||[],max=Math.max(1,...items.map(x=>x.events||0));const body=items.map(x=>`<div class=barrow><div><b>${st} · age ${esc(x.bucket)}</b><div class=muted>events ${x.events} · retest ${x.retest} · disp ${x.displacement} · vol ${x.volume} · RR ${x.rr} · Groq ${x.groq_pct}% · sent ${x.delivered_pct}%</div></div><div class=bar><i style="width:${100*(x.events||0)/max}%"></i></div><b>${x.events}</b></div>`).join('');return `<div style="margin-bottom:12px">${body||'<span class=muted>Нет данных</span>'}</div>`}).join('');bosAge.innerHTML=blocks||'<span class=muted>Нет данных</span>'}function renderWyCompare(){const old=LAST.wyckoff_dist_range||{},box=LAST.wyckoff_box_width||{};const fmt=(name,x)=>x.count?`<div class=crit><b>${name}</b><div class=muted>n=${x.count}</div><div>P25 ${num(x.p25)}% · P50 ${num(x.median)}% · P75 ${num(x.p75)}% · P90 ${num(x.p90)}%</div></div>`:`<div class=crit><b>${name}</b><div class=muted>Нет данных</div></div>`;wyCompare.innerHTML=`<div class=criteria>${fmt('Старый 30d high-low',old)}${fmt('Новый BC/AR/ST box',box)}</div>`}function renderRows(){rows.innerHTML=LAST.rows.map((r,i)=>{''',
)
replace_once(
    "stats_server.py",
    '''summary.innerHTML=cards(LAST.summary);renderFunnels();bars(LAST.failures,'failures');bars(LAST.groq.reasons,'groqReasons');renderChecks();renderTradeStats();renderRows()''',
    '''summary.innerHTML=cards(LAST.summary);renderFunnels();renderBosAge();renderWyCompare();bars(LAST.failures,'failures');bars(LAST.groq.reasons,'groqReasons');renderChecks();renderTradeStats();renderRows()''',
)

# Regression tests: source invariants + audit observer behavior.
Path("tests/test_observability_telemetry_only.py").write_text(r'''import unittest
from pathlib import Path
from unittest import mock

from core import setup_audit


ROOT = Path(__file__).resolve().parents[1]
MARKET = (ROOT / "market.py").read_text(encoding="utf-8")
STATS = (ROOT / "stats_server.py").read_text(encoding="utf-8")


class TelemetryOnlyInvariantTests(unittest.TestCase):
    def test_existing_trading_thresholds_are_unchanged(self):
        self.assertIn('get_bos_choch_event(c1h, direction, lookback=8, max_break_age=2)', MARKET)
        self.assertIn('get_bos_choch_event(_fast_context_15m, "BULLISH", lookback=15, max_break_age=4)', MARKET)
        self.assertIn('get_bos_choch_event(_fast_context_15m, "BEARISH", lookback=15, max_break_age=4)', MARKET)
        self.assertIn('_vol_threshold = 1.6', MARKET)
        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)
        self.assertIn("(rr_check < 2.0)", MARKET)
        self.assertIn("(rr < 2.0)", MARKET)
        self.assertNotIn('rr > 4', MARKET)
        self.assertNotIn('rr_check > 4', MARKET)

    def test_wyckoff_telemetry_does_not_replace_trading_phases(self):
        telemetry_call = '_telemetry_phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'
        trading_call = 'phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'
        blocker = '_dist_range_too_wide = dist_range_pct >= 25'
        self.assertIn(telemetry_call, MARKET)
        self.assertIn(trading_call, MARKET)
        self.assertLess(MARKET.index(telemetry_call), MARKET.index(blocker))
        self.assertLess(MARKET.index(blocker), MARKET.index(trading_call))
        self.assertNotIn('phases = _telemetry_phases', MARKET)

    def test_dashboard_exposes_only_observability_fields(self):
        self.assertIn('"bos_choch_age":bos_age_stats', STATS)
        self.assertIn('"wyckoff_box_width":wy_box_range', STATS)
        self.assertIn('BOS/CHoCH age telemetry', STATS)
        self.assertIn('WYCKOFF Distribution width telemetry', STATS)

    def test_observer_does_not_change_decorated_return_value(self):
        @setup_audit.audit_strategy("TEST")
        def baseline(value):
            return {"value": value}

        @setup_audit.audit_strategy("TEST")
        def observed(value):
            setup_audit.audit_observe("probe", {"value": value})
            return {"value": value}

        with mock.patch.object(setup_audit, "emit_event", return_value="x"):
            a = baseline(7)
            b = observed(7)
        a.pop("_audit_attempt_key", None)
        b.pop("_audit_attempt_key", None)
        self.assertEqual(a, b)

    def test_observer_merges_and_appends_inside_same_attempt(self):
        captured = []

        def capture(kind, strategy, symbol, payload, event_key=None):
            captured.append((kind, payload, event_key))
            return event_key or "x"

        @setup_audit.audit_strategy("TEST")
        def observed(_value):
            setup_audit.audit_observe("progress", {"retest": True})
            setup_audit.audit_observe("progress", {"volume": False})
            setup_audit.audit_observe("bos_events", {"age_bars": 1}, append=True)
            setup_audit.audit_observe("bos_events", {"age_bars": 2}, append=True)
            return {"ok": True}

        with mock.patch.object(setup_audit, "emit_event", side_effect=capture):
            observed(1)
        payload = captured[-1][1]
        self.assertEqual(payload["telemetry"]["progress"], {"retest": True, "volume": False})
        self.assertEqual([x["age_bars"] for x in payload["telemetry"]["bos_events"]], [1, 2])


if __name__ == "__main__":
    unittest.main()
''', encoding="utf-8")

print("Telemetry-only observability patch applied")
