from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one match, found {count}: {old[:140]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# ---------------------------------------------------------------------------
# bot.py — technical MTF throughput + read-only MTF numeric telemetry.
# ---------------------------------------------------------------------------
replace_once(
    "bot.py",
    "from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail",
    "from core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail, audit_observe as _audit_observe",
)

# A 40-pair sequential MTF batch repeatedly exceeded the existing 210s job timeout.
# Keep the same rotating-batch mechanism and timeout, but complete ~1/3 universe per
# pass so no entire scan is discarded by the outer timeout.
replace_once(
    "bot.py",
    '''    batch = await asyncio.to_thread(\n        _take_strategy_round_batch, "MTF", universe, (len(universe) + 1) // 2, DB_PATH\n    )\n''',
    '''    batch = await asyncio.to_thread(\n        _take_strategy_round_batch, "MTF", universe, (len(universe) + 2) // 3, DB_PATH\n    )\n''',
)

replace_once(
    "bot.py",
    '''                _pd_mid = (_pd_high + _pd_low) / 2\n                _pd_price = _pd_raw[-1]["close"]\n\n                if _audit_test('MTF_FULL_SCAN_RAW_G4672', (direction == "BULLISH" and _pd_price > _pd_mid), 'direction == "BULLISH" and _pd_price > _pd_mid', 'direction == "BULLISH" and _pd_price > _pd_mid', 4672):\n''',
    '''                _pd_mid = (_pd_high + _pd_low) / 2\n                _pd_price = _pd_raw[-1]["close"]\n                _pd_span = _pd_high - _pd_low\n                _audit_observe("mtf_numeric", {\n                    "pd_position_pct": round((_pd_price - _pd_low) / _pd_span * 100, 6) if _pd_span > 0 else None,\n                    "pd_mid_distance_pct": round((_pd_price - _pd_mid) / _pd_span * 100, 6) if _pd_span > 0 else None,\n                    "pd_price": _pd_price, "pd_low": _pd_low, "pd_mid": _pd_mid, "pd_high": _pd_high,\n                })\n\n                if _audit_test('MTF_FULL_SCAN_RAW_G4672', (direction == "BULLISH" and _pd_price > _pd_mid), 'direction == "BULLISH" and _pd_price > _pd_mid', 'direction == "BULLISH" and _pd_price > _pd_mid', 4672):\n''',
)

replace_once(
    "bot.py",
    '''        _positive_confluence = [c for c in confluence if c.lstrip().startswith(("✅", "🎯", "🔥", "🚀"))]\n        min_conf = {"1h": 3, "4h": 4, "1d": 4, "1w": 3}\n        if _audit_test('MTF_FULL_SCAN_RAW_G4767', (len(_positive_confluence) < min_conf.get(timeframe, 4)), 'сетапов нужны реальные положительные confluence.', 'len(_positive_confluence) < min_conf.get(timeframe, 4)', 4767):\n''',
    '''        _positive_confluence = [c for c in confluence if c.lstrip().startswith(("✅", "🎯", "🔥", "🚀"))]\n        min_conf = {"1h": 3, "4h": 4, "1d": 4, "1w": 3}\n        _audit_observe("mtf_numeric", {\n            "positive_confluence_count": len(_positive_confluence),\n            "positive_confluence_required": min_conf.get(timeframe, 4),\n        })\n        if _audit_test('MTF_FULL_SCAN_RAW_G4767', (len(_positive_confluence) < min_conf.get(timeframe, 4)), 'сетапов нужны реальные положительные confluence.', 'len(_positive_confluence) < min_conf.get(timeframe, 4)', 4767):\n''',
)

replace_once(
    "bot.py",
    '''        _tf_match = sum([_dir_15m == direction, _dir_1h == direction, _dir_4h == direction])\n        _core_tf_match = sum([_dir_1h == direction, _dir_4h == direction])\n        # Hierarchical MTF: 4h defines structure, 1h defines the setup and\n''',
    '''        _tf_match = sum([_dir_15m == direction, _dir_1h == direction, _dir_4h == direction])\n        _core_tf_match = sum([_dir_1h == direction, _dir_4h == direction])\n        _audit_observe("mtf_numeric", {"tf_match": _tf_match, "core_tf_match": _core_tf_match})\n        # Hierarchical MTF: 4h defines structure, 1h defines the setup and\n''',
)

# Restore the already-approved universal RR contract. This old bot.py cap was
# missed because the existing regression test only inspected market.py.
replace_once(
    "bot.py",
    '''        # RR — контекст для Groq\n        _rr_val = levels.get("rr", 0)\n        if _audit_test('MTF_FULL_SCAN_RAW_G4817', (not 2.0 <= _rr_val <= 4.0), 'RR — контекст для Groq', 'not 2.0 <= _rr_val <= 4.0', 4817):\n            logging.debug(f"[full_scan_raw] {symbol} {timeframe}: RR {_rr_val:.2f} вне диапазона 2.0–4.0 — пропускаем")\n            return _audit_fail('MTF_FULL_SCAN_RAW_R4819', 'RR — контекст для Groq', locals(), 'not 2.0 <= _rr_val <= 4.0', 4819)\n''',
    '''        # Universal RR contract: floor 2.0, no upper ceiling.\n        _rr_val = levels.get("rr", 0)\n        _audit_observe("mtf_numeric", {"rr_value": _rr_val})\n        if _audit_test('MTF_FULL_SCAN_RAW_G4817', (_rr_val < 2.0), 'RR >= 2.0, no upper ceiling', '_rr_val < 2.0', 4817):\n            logging.debug(f"[full_scan_raw] {symbol} {timeframe}: RR {_rr_val:.2f} < 2.0 — пропускаем")\n            return _audit_fail('MTF_FULL_SCAN_RAW_R4819', 'RR >= 2.0, no upper ceiling', locals(), '_rr_val < 2.0', 4819)\n''',
)


# ---------------------------------------------------------------------------
# market.py — exact numeric telemetry only; no threshold/control-flow changes.
# ---------------------------------------------------------------------------
replace_once(
    "market.py",
    '''        out["chase_ok"] = bool(distance <= atr1h * 0.75)\n        _audit_observe("bos_progress", {\n            "displacement_reached": True, "displacement_confirmed": bool(out["displacement_ok"]),\n            "volume_reached": True, "volume_confirmed": bool(out["volume_ok"]),\n        })\n''',
    '''        out["chase_ok"] = bool(distance <= atr1h * 0.75)\n        _audit_observe("swing_numeric", {\n            "displacement_body_ratio": round(candle_body / candle_range, 6) if candle_range > 0 else None,\n            "direction_ok": bool(direction_ok),\n            "volume_ratio": round(last_vol / avg_vol, 6) if avg_vol > 0 else None,\n            "retest_distance_atr": round(distance / atr1h, 6) if atr1h > 0 else None,\n        })\n        _audit_observe("bos_progress", {\n            "displacement_reached": True, "displacement_confirmed": bool(out["displacement_ok"]),\n            "volume_reached": True, "volume_confirmed": bool(out["volume_ok"]),\n        })\n''',
)

replace_once(
    "market.py",
    '''        in_discount = price <= range_low + range_size * 0.30\n        in_premium = price >= range_high - range_size * 0.30\n\n        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7818', (not in_discount and not in_premium), 'Require a real range extreme and leave the middle 40% neutral.', 'not in_discount and not in_premium', 7818):\n''',
    '''        in_discount = price <= range_low + range_size * 0.30\n        in_premium = price >= range_high - range_size * 0.30\n        _audit_observe("zone_numeric", {\n            "range_position_pct": round((price - range_low) / range_size * 100, 6) if range_size > 0 else None,\n            "range_atr": round(range_size / atr, 6) if atr > 0 else None,\n        })\n\n        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7818', (not in_discount and not in_premium), 'Require a real range extreme and leave the middle 40% neutral.', 'not in_discount and not in_premium', 7818):\n''',
)

replace_once(
    "market.py",
    '''        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7852', (not zone_level), 'not zone_level', 'not zone_level', 7852):\n            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7853', 'not zone_level', locals(), 'not zone_level', 7853)  # Нет зоны интереса рядом с ценой\n\n        # ── 2.5. Проверка свежести зоны (unmitigated + strong move away) ──\n''',
    '''        if _audit_test('ZONE_DETECT_ZONE_SETUP_G7852', (not zone_level), 'not zone_level', 'not zone_level', 7852):\n            return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7853', 'not zone_level', locals(), 'not zone_level', 7853)  # Нет зоны интереса рядом с ценой\n        _audit_observe("zone_numeric", {\n            "zone_distance_atr": round(abs(price - zone_level) / atr, 6) if atr > 0 else None,\n        })\n\n        # ── 2.5. Проверка свежести зоны (unmitigated + strong move away) ──\n''',
)

replace_once(
    "market.py",
    '''                if _audit_test('ZONE_DETECT_ZONE_SETUP_G7866', (_test_count > 2), '_test_count > 2', '_test_count > 2', 7866):\n                    logging.debug(f"[ZONE] {symbol}: зона протестирована {_test_count} раз — mitigated")\n                    return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7868', '_test_count > 2', locals(), '_test_count > 2', 7868)\n\n                # Strong move away: displacement ≥0.5 + body > ATR×1.0\n                _strong_move = False\n                for i in range(max(-len(candles), -35), -3):\n                    c = candles[i]\n                    c_body = abs(c["close"] - c["open"])\n                    c_range = c["high"] - c["low"]\n                    if c_range > 0 and c_body / c_range >= 0.5 and c_body > atr * _vf_zone * 0.8:\n''',
    '''                _audit_observe("zone_numeric", {"test_count": _test_count})\n                if _audit_test('ZONE_DETECT_ZONE_SETUP_G7866', (_test_count > 2), '_test_count > 2', '_test_count > 2', 7866):\n                    logging.debug(f"[ZONE] {symbol}: зона протестирована {_test_count} раз — mitigated")\n                    return _audit_fail('ZONE_DETECT_ZONE_SETUP_R7868', '_test_count > 2', locals(), '_test_count > 2', 7868)\n\n                # Strong move away: displacement ≥0.5 + body > ATR×1.0\n                _strong_move = False\n                _zone_best_displacement = 0.0\n                _zone_best_body_atr = 0.0\n                for i in range(max(-len(candles), -35), -3):\n                    c = candles[i]\n                    c_body = abs(c["close"] - c["open"])\n                    c_range = c["high"] - c["low"]\n                    _zone_directional = (direction == "BULLISH" and c["close"] > c["open"]) or (direction == "BEARISH" and c["close"] < c["open"])\n                    if _zone_directional and c_range > 0:\n                        _zone_best_displacement = max(_zone_best_displacement, c_body / c_range)\n                        if atr > 0:\n                            _zone_best_body_atr = max(_zone_best_body_atr, c_body / atr)\n                    if c_range > 0 and c_body / c_range >= 0.5 and c_body > atr * _vf_zone * 0.8:\n''',
)

replace_once(
    "market.py",
    '''                if _audit_test('ZONE_DETECT_ZONE_SETUP_G7884', (not _strong_move), 'not _strong_move', 'not _strong_move', 7884):\n                    logging.debug(f"[ZONE] {symbol}: нет сильного импульса (displacement < 0.5)")\n''',
    '''                _audit_observe("zone_numeric", {\n                    "best_directional_displacement_ratio": round(_zone_best_displacement, 6),\n                    "best_directional_body_atr": round(_zone_best_body_atr, 6),\n                })\n                if _audit_test('ZONE_DETECT_ZONE_SETUP_G7884', (not _strong_move), 'not _strong_move', 'not _strong_move', 7884):\n                    logging.debug(f"[ZONE] {symbol}: нет сильного импульса (displacement < 0.5)")\n''',
)

replace_once(
    "market.py",
    '''        _zone_vf = _zone_ap.get("volatility_factor", 1.0) if _zone_ap else 1.0\n        _q_min = 3\n        if _audit_test('ZONE_DETECT_ZONE_SETUP_G8050', (q_score < _q_min), 'wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.', 'q_score < _q_min', 8050):\n''',
    '''        _zone_vf = _zone_ap.get("volatility_factor", 1.0) if _zone_ap else 1.0\n        _q_min = 3\n        _audit_observe("zone_numeric", {"quality_score": q_score, "quality_required": _q_min})\n        if _audit_test('ZONE_DETECT_ZONE_SETUP_G8050', (q_score < _q_min), 'wick/RSI/FVG/BTC/funding/rejection-volume, without double-counting.', 'q_score < _q_min', 8050):\n''',
)

replace_once(
    "market.py",
    '''        dist_range_pct = (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0\n        _audit_observe("wyckoff_distribution", {"dist_range_pct": round(dist_range_pct, 6)})\n''',
    '''        dist_range_pct = (dist_high - dist_low) / dist_low * 100 if dist_low > 0 else 0\n        _audit_observe("wyckoff_distribution", {\n            "dist_range_pct": round(dist_range_pct, 6),\n            "old_range_under_25": bool(dist_range_pct < 25),\n        })\n''',
)

replace_once(
    "market.py",
    '''                _audit_observe("wyckoff_distribution", {\n                    "distribution_box_width_pct": round(_telemetry_box_width_pct, 6) if _telemetry_box_width_pct is not None else None,\n                    "structure_points": {name: price for name, price in _telemetry_points},\n                })\n''',
    '''                _audit_observe("wyckoff_distribution", {\n                    "distribution_box_width_pct": round(_telemetry_box_width_pct, 6) if _telemetry_box_width_pct is not None else None,\n                    "structural_box_under_25": bool(_telemetry_box_width_pct < 25) if _telemetry_box_width_pct is not None else None,\n                    "structure_points": {name: price for name, price in _telemetry_points},\n                })\n''',
)


# ---------------------------------------------------------------------------
# stats_server.py — aggregate and display numeric diagnostics + WY shadow delta.
# ---------------------------------------------------------------------------
replace_once(
    "stats_server.py",
    '''def _observed_funnels(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:\n''',
    '''def _metric_summary(values: list[float]) -> dict[str, Any]:\n    clean = [float(v) for v in values if _num(v) is not None]\n    return {\n        "count": len(clean),\n        "min": round(min(clean), 3) if clean else None,\n        "p25": _percentile(clean, 0.25), "median": _percentile(clean, 0.50),\n        "p75": _percentile(clean, 0.75), "p90": _percentile(clean, 0.90),\n        "max": round(max(clean), 3) if clean else None,\n    }\n\n\ndef _observed_funnels(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:\n''',
)

replace_once(
    "stats_server.py",
    '''    wy_box_range = {\n        "count": len(wy_box_values),\n        "min": round(min(wy_box_values), 3) if wy_box_values else None,\n        "p25": _percentile(wy_box_values, 0.25), "median": _percentile(wy_box_values, 0.50),\n        "p75": _percentile(wy_box_values, 0.75), "p90": _percentile(wy_box_values, 0.90),\n        "max": round(max(wy_box_values), 3) if wy_box_values else None,\n    }\n\n    total=len(joined);''',
    '''    wy_box_range = {\n        "count": len(wy_box_values),\n        "min": round(min(wy_box_values), 3) if wy_box_values else None,\n        "p25": _percentile(wy_box_values, 0.25), "median": _percentile(wy_box_values, 0.50),\n        "p75": _percentile(wy_box_values, 0.75), "p90": _percentile(wy_box_values, 0.90),\n        "max": round(max(wy_box_values), 3) if wy_box_values else None,\n    }\n\n    numeric_specs = {\n        "SWING": ("swing_numeric", ("displacement_body_ratio", "volume_ratio", "retest_distance_atr")),\n        "MTF": ("mtf_numeric", ("pd_position_pct", "pd_mid_distance_pct", "positive_confluence_count", "core_tf_match", "rr_value")),\n        "ZONE": ("zone_numeric", ("range_position_pct", "range_atr", "zone_distance_atr", "test_count", "best_directional_displacement_ratio", "best_directional_body_atr", "quality_score")),\n    }\n    numeric_values = {strategy: {metric: [] for metric in metrics} for strategy, (_, metrics) in numeric_specs.items()}\n    wy_shadow = {"observed": 0, "old_pass": 0, "structural_pass": 0, "both_pass": 0, "structural_only": 0, "old_only": 0}\n    for r in joined:\n        strategy_name = str(r.get("strategy") or "").upper()\n        telemetry = r.get("telemetry") if isinstance(r.get("telemetry"), dict) else {}\n        if strategy_name in numeric_specs:\n            telemetry_key, metrics = numeric_specs[strategy_name]\n            payload = telemetry.get(telemetry_key) if isinstance(telemetry.get(telemetry_key), dict) else {}\n            for metric in metrics:\n                value = _num(payload.get(metric))\n                if value is not None:\n                    numeric_values[strategy_name][metric].append(value)\n        if strategy_name == "WYCKOFF" and str(r.get("subtype") or "").upper() == "DISTRIBUTION":\n            payload = telemetry.get("wyckoff_distribution") if isinstance(telemetry.get("wyckoff_distribution"), dict) else {}\n            old_pass = payload.get("old_range_under_25")\n            structural_pass = payload.get("structural_box_under_25")\n            if isinstance(old_pass, bool) and isinstance(structural_pass, bool):\n                wy_shadow["observed"] += 1\n                wy_shadow["old_pass"] += int(old_pass)\n                wy_shadow["structural_pass"] += int(structural_pass)\n                wy_shadow["both_pass"] += int(old_pass and structural_pass)\n                wy_shadow["structural_only"] += int(structural_pass and not old_pass)\n                wy_shadow["old_only"] += int(old_pass and not structural_pass)\n    numeric_telemetry = {\n        strategy_name: {metric: _metric_summary(values) for metric, values in metrics.items()}\n        for strategy_name, metrics in numeric_values.items()\n    }\n\n    total=len(joined);''',
)

replace_once(
    "stats_server.py",
    '''      "bos_choch_age":bos_age_stats,"wyckoff_dist_range":wy_dist_range,"wyckoff_box_width":wy_box_range,\n''',
    '''      "bos_choch_age":bos_age_stats,"wyckoff_dist_range":wy_dist_range,"wyckoff_box_width":wy_box_range,\n      "numeric_telemetry":numeric_telemetry,"wyckoff_shadow":wy_shadow,\n''',
)

replace_once(
    "stats_server.py",
    '''<div class="section card"><h2>Сквозная воронка по стратегиям</h2><div id=funnels></div><div id=wyRange class=muted style="margin-top:10px"></div></div><div class="cols section"><div class=card><h2>BOS/CHoCH age telemetry</h2><div id=bosAge></div></div><div class=card><h2>WYCKOFF Distribution width telemetry</h2><div id=wyCompare></div></div></div><div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div><\/div>'''.replace('<\\/div>', '</div>'),
    '''<div class="section card"><h2>Сквозная воронка по стратегиям</h2><div id=funnels></div><div id=wyRange class=muted style="margin-top:10px"></div></div><div class="cols section"><div class=card><h2>BOS/CHoCH age telemetry</h2><div id=bosAge></div></div><div class=card><h2>WYCKOFF Distribution width telemetry</h2><div id=wyCompare></div></div></div><div class="section card"><h2>Numeric funnel diagnostics</h2><div id=numericDiag></div></div><div class="cols section"><div class=card><h2>Где чаще всего останавливаются</h2><div id=failures></div></div>''',
)

replace_once(
    "stats_server.py",
    '''function renderBosAge(){const data=LAST.bos_choch_age||{};''',
    '''function renderNumericDiag(){const data=LAST.numeric_telemetry||{};const labels={displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg',retest_distance_atr:'SWING retest distance/ATR',pd_position_pct:'MTF Premium/Discount position %',pd_mid_distance_pct:'MTF distance from mid (% range)',positive_confluence_count:'MTF positive confluence',core_tf_match:'MTF 1h/4h match count',rr_value:'MTF RR',range_position_pct:'ZONE range position %',range_atr:'ZONE range/ATR',zone_distance_atr:'ZONE distance/ATR',test_count:'ZONE test count',best_directional_displacement_ratio:'ZONE best displacement',best_directional_body_atr:'ZONE best body/ATR',quality_score:'ZONE quality score'};const blocks=[];for(const st of ['SWING','MTF','ZONE']){for(const [key,x] of Object.entries(data[st]||{})){if(!x||!x.count)continue;blocks.push(`<div class=crit><b>${esc(labels[key]||st+' '+key)}</b><div class=muted>n=${x.count}</div><div>P25 ${num(x.p25)} · P50 ${num(x.median)} · P75 ${num(x.p75)} · P90 ${num(x.p90)}</div></div>`)}}numericDiag.innerHTML=`<div class=criteria>${blocks.join('')||'<span class=muted>Нет данных</span>'}</div>`}function renderBosAge(){const data=LAST.bos_choch_age||{};''',
)

replace_once(
    "stats_server.py",
    '''function renderWyCompare(){const old=LAST.wyckoff_dist_range||{},box=LAST.wyckoff_box_width||{};const fmt=(name,x)=>x.count?`<div class=crit><b>${name}</b><div class=muted>n=${x.count}</div><div>P25 ${num(x.p25)}% · P50 ${num(x.median)}% · P75 ${num(x.p75)}% · P90 ${num(x.p90)}%</div></div>`:`<div class=crit><b>${name}</b><div class=muted>Нет данных</div></div>`;wyCompare.innerHTML=`<div class=criteria>${fmt('Старый 30d high-low',old)}${fmt('Новый BC/AR/ST box',box)}</div>`}''',
    '''function renderWyCompare(){const old=LAST.wyckoff_dist_range||{},box=LAST.wyckoff_box_width||{},sh=LAST.wyckoff_shadow||{};const fmt=(name,x)=>x.count?`<div class=crit><b>${name}</b><div class=muted>n=${x.count}</div><div>P25 ${num(x.p25)}% · P50 ${num(x.median)}% · P75 ${num(x.p75)}% · P90 ${num(x.p90)}%</div></div>`:`<div class=crit><b>${name}</b><div class=muted>Нет данных</div></div>`;const shadow=sh.observed?`<div class=crit><b>Shadow @ 25%</b><div class=muted>n=${sh.observed}</div><div>old pass ${sh.old_pass} · structural pass ${sh.structural_pass} · structural-only ${sh.structural_only} · old-only ${sh.old_only}</div></div>`:'';wyCompare.innerHTML=`<div class=criteria>${fmt('Старый 30d high-low',old)}${fmt('Новый BC/AR/ST box',box)}${shadow}</div>`}''',
)

replace_once(
    "stats_server.py",
    '''summary.innerHTML=cards(LAST.summary);renderFunnels();renderBosAge();renderWyCompare();bars(LAST.failures,'failures');''',
    '''summary.innerHTML=cards(LAST.summary);renderFunnels();renderBosAge();renderWyCompare();renderNumericDiag();bars(LAST.failures,'failures');''',
)


# ---------------------------------------------------------------------------
# Tests — explicitly protect throughput change and numeric-only invariants.
# ---------------------------------------------------------------------------
Path("tests/test_numeric_diagnostics_and_mtf_throughput.py").write_text(r'''import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT = (ROOT / "bot.py").read_text(encoding="utf-8")
MARKET = (ROOT / "market.py").read_text(encoding="utf-8")
STATS = (ROOT / "stats_server.py").read_text(encoding="utf-8")


class NumericDiagnosticsAndMtfThroughputTests(unittest.TestCase):
    def test_mtf_rotating_batch_is_one_third_and_timeout_is_not_relaxed(self):
        self.assertIn('_take_strategy_round_batch, "MTF", universe, (len(universe) + 2) // 3, DB_PATH', BOT)
        self.assertNotIn('_take_strategy_round_batch, "MTF", universe, (len(universe) + 1) // 2, DB_PATH', BOT)
        self.assertIn('_run_market_scan_exclusive("auto_scan_1h", _auto_scan_1h_impl, 210)', BOT)

    def test_mtf_rr_contract_has_floor_and_no_upper_cap_in_bot(self):
        self.assertIn("(_rr_val < 2.0)", BOT)
        self.assertNotIn("not 2.0 <= _rr_val <= 4.0", BOT)
        self.assertNotIn("RR {_rr_val:.2f} вне диапазона 2.0–4.0", BOT)

    def test_numeric_telemetry_does_not_replace_existing_thresholds(self):
        self.assertIn('candle_body / candle_range >= 0.50', MARKET)
        self.assertIn('last_vol >= avg_vol * 1.20', MARKET)
        self.assertIn('distance <= atr1h * 0.75', MARKET)
        self.assertIn('range_low + range_size * 0.30', MARKET)
        self.assertIn('range_high - range_size * 0.30', MARKET)
        self.assertIn('_test_count > 2', MARKET)
        self.assertIn('c_body / c_range >= 0.5', MARKET)
        self.assertIn('_q_min = 3', MARKET)
        self.assertIn('_dist_range_too_wide = dist_range_pct >= 25', MARKET)

    def test_expected_numeric_payloads_are_observability_only(self):
        self.assertIn('_audit_observe("mtf_numeric"', BOT)
        self.assertIn('_audit_observe("swing_numeric"', MARKET)
        self.assertIn('_audit_observe("zone_numeric"', MARKET)
        self.assertIn('"old_range_under_25"', MARKET)
        self.assertIn('"structural_box_under_25"', MARKET)

    def test_strategy_lab_aggregates_numeric_metrics_and_shadow_comparison(self):
        self.assertIn('"numeric_telemetry":numeric_telemetry', STATS)
        self.assertIn('"wyckoff_shadow":wy_shadow', STATS)
        self.assertIn('Numeric funnel diagnostics', STATS)
        self.assertIn('structural-only', STATS)


if __name__ == "__main__":
    unittest.main()
''', encoding="utf-8")

# Strengthen the pre-existing RR regression so it also inspects bot.py.
replace_once(
    "tests/test_rr_floor_fast_balanced.py",
    '''        cls.market = Path("market.py").read_text(encoding="utf-8")\n        cls.evidence = Path("core/setup_evidence.py").read_text(encoding="utf-8")\n''',
    '''        cls.market = Path("market.py").read_text(encoding="utf-8")\n        cls.bot = Path("bot.py").read_text(encoding="utf-8")\n        cls.evidence = Path("core/setup_evidence.py").read_text(encoding="utf-8")\n''',
)
replace_once(
    "tests/test_rr_floor_fast_balanced.py",
    '''        for text in forbidden:\n            self.assertNotIn(text, self.market)\n        self.assertIn("max_rr=None", self.market)\n''',
    '''        for text in forbidden:\n            self.assertNotIn(text, self.market)\n            self.assertNotIn(text, self.bot)\n        self.assertNotIn("not 2.0 <= _rr_val <= 4.0", self.bot)\n        self.assertIn("(_rr_val < 2.0)", self.bot)\n        self.assertIn("max_rr=None", self.market)\n''',
)

print("Applied MTF throughput fix and numeric diagnostics telemetry")
