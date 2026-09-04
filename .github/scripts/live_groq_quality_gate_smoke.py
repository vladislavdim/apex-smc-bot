import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from groq import Groq
from core.groq_models import configured_groq_models
from core.signal_quality_gate import _extract_json

key = os.environ.get('GROQ_API_KEY', '').strip()
if not key:
    raise SystemExit('GROQ_API_KEY secret unavailable')

candidate = {
    'symbol': 'KAITOUSDT', 'strategy': 'MTF', 'direction': 'BEARISH',
    'timeframe': '4h', 'entry': 0.303, 'sl': 0.30656, 'tp1': 0.2945, 'rr': 2.39,
    'technical_evidence': {'causal_matrix_ready': True, 'note': 'pre-deploy smoke only'}
}
assessment = {'state': 'VALID', 'class': 'QUALITY', 'fatal': []}
prompt = f'''You are the final quality reviewer for an already calculated crypto trade candidate.
Do NOT recalculate or replace entry, SL, TP or RR.
The deterministic SETUP EVIDENCE assessment is authoritative: INVALID cannot be approved; DEVELOPING cannot be approved.

CANDIDATE:
{json.dumps(candidate)}

SETUP EVIDENCE:
{json.dumps(assessment)}

Return JSON only, with no markdown or commentary:
{{
  "valid": true,
  "decision": "APPROVE|WAIT|REJECT",
  "confidence": 0.0,
  "reasons": ["specific evidence"],
  "risks": ["specific risk"]
}}'''

client = Groq(api_key=key)
last_error = None
for model in configured_groq_models():
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{'role': 'user', 'content': prompt}],
            max_tokens=250,
            timeout=30,
        )
        raw = response.choices[0].message.content
        parsed = _extract_json(raw)
        if not parsed:
            raise RuntimeError(f'model {model} returned non-JSON: {raw[:300]!r}')
        decision = str(parsed.get('decision', '')).upper()
        confidence = float(parsed.get('confidence', -1))
        if decision not in {'APPROVE', 'WAIT', 'REJECT'}:
            raise RuntimeError(f'invalid decision {decision!r}')
        if not 0 <= confidence <= 1:
            raise RuntimeError(f'invalid confidence {confidence!r}')
        print(json.dumps({'model': model, 'decision': decision, 'confidence': confidence, 'valid': parsed.get('valid')}, sort_keys=True))
        raise SystemExit(0)
    except SystemExit:
        raise
    except Exception as exc:
        last_error = exc
        print(f'{model}: {type(exc).__name__}: {exc}')

raise SystemExit(f'No configured Groq model passed live JSON smoke: {last_error}')
