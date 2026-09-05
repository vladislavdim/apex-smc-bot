from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding='utf-8')
    count = text.count(old)
    if count != 1:
        raise SystemExit(f'{path}: expected one match, found {count}: {old[:120]!r}')
    p.write_text(text.replace(old, new, 1), encoding='utf-8')

# Keep BOS event payloads shallow enough for the existing safe serializer.
replace_once(
    'market.py',
    '''        _audit_observe("bos_events", {\n            "role": "SWING_ENTRY_STRUCTURE", "timeframe": "1h", "age_bars": _swing_bos_age,\n            "event_type": event.get("type"), "direction": event.get("direction"),\n        }, append=True)''',
    '''        _audit_observe("bos_event", {\n            "role": "SWING_ENTRY_STRUCTURE", "timeframe": "1h", "age_bars": _swing_bos_age,\n            "event_type": event.get("type"), "direction": event.get("direction"),\n        })''',
)
replace_once(
    'market.py',
    '''        _audit_observe("bos_events", {\n            "role": "FAST_THESIS", "timeframe": "15m", "age_bars": _fast_bos_age,\n            "event_type": _fast_thesis_event.get("type"), "direction": _fast_thesis_event.get("direction"),\n        }, append=True)''',
    '''        _audit_observe("bos_event", {\n            "role": "FAST_THESIS", "timeframe": "15m", "age_bars": _fast_bos_age,\n            "event_type": _fast_thesis_event.get("type"), "direction": _fast_thesis_event.get("direction"),\n        })''',
)
replace_once(
    'market.py',
    '''        _audit_observe("bos_events", {\n            "role": "FAST_EXECUTION", "timeframe": "15m", "age_bars": _fast_exec_bos_age,\n            "event_type": _fast_structure_event.get("type"), "direction": _fast_structure_event.get("direction"),\n        }, append=True)''',
    '''        _audit_observe("bos_execution_event", {\n            "role": "FAST_EXECUTION", "timeframe": "15m", "age_bars": _fast_exec_bos_age,\n            "event_type": _fast_structure_event.get("type"), "direction": _fast_structure_event.get("direction"),\n        })''',
)

replace_once(
    'stats_server.py',
    '''            events_for_attempt = telemetry.get("bos_events") if isinstance(telemetry.get("bos_events"), list) else []\n            progress = telemetry.get("bos_progress") if isinstance(telemetry.get("bos_progress"), dict) else {}''',
    '''            events_for_attempt = []\n            for telemetry_key in ("bos_event", "bos_execution_event"):\n                telemetry_event = telemetry.get(telemetry_key)\n                if isinstance(telemetry_event, dict):\n                    events_for_attempt.append(telemetry_event)\n            progress = telemetry.get("bos_progress") if isinstance(telemetry.get("bos_progress"), dict) else {}''',
)

# Make the WYCKOFF source-order assertion exact: do not let `_telemetry_phases` satisfy it.
replace_once(
    'tests/test_observability_telemetry_only.py',
    '''        trading_call = 'phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'\n        blocker = '_dist_range_too_wide = dist_range_pct >= 25'\n        self.assertIn(telemetry_call, MARKET)\n        self.assertIn(trading_call, MARKET)\n        self.assertLess(MARKET.index(telemetry_call), MARKET.index(blocker))\n        self.assertLess(MARKET.index(blocker), MARKET.index(trading_call))''',
    '''        trading_call = '\\n        phases = _find_wyckoff_phases_distribution(candles_1d, candles_4h)'\n        blocker = '_dist_range_too_wide = dist_range_pct >= 25'\n        self.assertIn(telemetry_call, MARKET)\n        self.assertIn(trading_call, MARKET)\n        telemetry_pos = MARKET.index(telemetry_call)\n        blocker_pos = MARKET.index(blocker, telemetry_pos)\n        trading_pos = MARKET.index(trading_call, blocker_pos)\n        self.assertLess(telemetry_pos, blocker_pos)\n        self.assertLess(blocker_pos, trading_pos)''',
)

old_test = '''    def test_observer_merges_and_appends_inside_same_attempt(self):\n        captured = []\n\n        def capture(kind, strategy, symbol, payload, event_key=None):\n            captured.append((kind, payload, event_key))\n            return event_key or "x"\n\n        @setup_audit.audit_strategy("TEST")\n        def observed(_value):\n            setup_audit.audit_observe("progress", {"retest": True})\n            setup_audit.audit_observe("progress", {"volume": False})\n            setup_audit.audit_observe("bos_events", {"age_bars": 1}, append=True)\n            setup_audit.audit_observe("bos_events", {"age_bars": 2}, append=True)\n            return {"ok": True}\n\n        with mock.patch.object(setup_audit, "emit_event", side_effect=capture):\n            observed(1)\n        payload = captured[-1][1]\n        self.assertEqual(payload["telemetry"]["progress"], {"retest": True, "volume": False})\n        self.assertEqual([x["age_bars"] for x in payload["telemetry"]["bos_events"]], [1, 2])\n'''
new_test = '''    def test_observer_merges_shallow_fields_inside_same_attempt(self):\n        captured = []\n\n        def capture(kind, strategy, symbol, payload, event_key=None):\n            captured.append((kind, payload, event_key))\n            return event_key or "x"\n\n        @setup_audit.audit_strategy("TEST")\n        def observed(_value):\n            setup_audit.audit_observe("progress", {"retest": True})\n            setup_audit.audit_observe("progress", {"volume": False})\n            setup_audit.audit_observe("bos_event", {"age_bars": 1, "timeframe": "1h"})\n            setup_audit.audit_observe("bos_execution_event", {"age_bars": 2, "timeframe": "15m"})\n            return {"ok": True}\n\n        with mock.patch.object(setup_audit, "emit_event", side_effect=capture):\n            observed(1)\n        payload = captured[-1][1]\n        self.assertEqual(payload["telemetry"]["progress"], {"retest": True, "volume": False})\n        self.assertEqual(payload["telemetry"]["bos_event"]["age_bars"], 1)\n        self.assertEqual(payload["telemetry"]["bos_execution_event"]["age_bars"], 2)\n'''
replace_once('tests/test_observability_telemetry_only.py', old_test, new_test)

print('Telemetry regression fix applied')
