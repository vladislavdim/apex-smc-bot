from pathlib import Path

p = Path('bot.py')
s = p.read_text(encoding='utf-8')

old = '''    if _SIGNAL_QUALITY_GATE_OK and not sd.get("_external_quality_reviewed"):\n        review = await _review_signal_candidate(sd, ask_groq, get_candles)'''
new = '''    if not _SIGNAL_QUALITY_GATE_OK:\n        reason = "quality gate unavailable; final Groq confirmation required"\n        logging.error("[SignalQualityGate] %s blocked: %s", sd.get("symbol"), reason)\n        _record_strategy_decision(sd, "WAIT", "groq_quality_gate", reason, db_path=DB_PATH)\n        await _remember("WAIT", reason)\n        if _run_id:\n            await asyncio.to_thread(\n                _record_scan_event, _run_id, _strategy, sd.get("symbol", ""),\n                "GROQ", "GROQ_WAIT", "QUALITY_GATE_UNAVAILABLE", {"reason": reason}, DB_PATH,\n            )\n        return False\n    if not sd.get("_external_quality_reviewed"):\n        review = await _review_signal_candidate(sd, ask_groq, get_candles)'''
if old not in s:
    raise SystemExit('quality gate call anchor not found')
s = s.replace(old, new, 1)

s = s.replace('''        decision = review.get("decision", "APPROVE")''', '''        decision = str(review.get("decision") or "WAIT").upper()''', 1)

old_else = '''    else:\n        existing_review = sd.get("_external_quality_review")\n        if isinstance(existing_review, dict):\n            existing_decision = str(existing_review.get("decision") or "APPROVE").upper()\n            await _remember(\n                existing_decision,\n                "; ".join(existing_review.get("reasons", [])),\n                existing_review,\n            )\n        else:\n            await _remember("APPROVE", "quality gate unavailable; analytical candidate retained")'''
new_else = '''    else:\n        existing_review = sd.get("_external_quality_review")\n        if not isinstance(existing_review, dict):\n            reason = "quality review missing; final Groq confirmation required"\n            logging.error("[SignalQualityGate] %s blocked: %s", sd.get("symbol"), reason)\n            _record_strategy_decision(sd, "WAIT", "groq_quality_gate", reason, db_path=DB_PATH)\n            await _remember("WAIT", reason)\n            return False\n        existing_decision = str(existing_review.get("decision") or "WAIT").upper()\n        if existing_decision != "APPROVE":\n            reason = "; ".join(existing_review.get("reasons", [])) or f"existing quality review={existing_decision}"\n            _record_strategy_decision(sd, existing_decision if existing_decision in {"WAIT", "REJECT"} else "WAIT", "groq_quality_gate", reason, db_path=DB_PATH)\n            await _remember(existing_decision if existing_decision in {"WAIT", "REJECT"} else "WAIT", reason, existing_review)\n            return False\n        await _remember("APPROVE", "; ".join(existing_review.get("reasons", [])), existing_review)'''
if old_else not in s:
    raise SystemExit('quality gate fallback anchor not found')
s = s.replace(old_else, new_else, 1)

p.write_text(s, encoding='utf-8')
print('quality gate fail-closed patch applied')
