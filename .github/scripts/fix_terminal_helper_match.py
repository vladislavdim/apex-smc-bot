from pathlib import Path

p = Path('.github/scripts/apply_strategy_lab_terminal_truth.py')
text = p.read_text(encoding='utf-8')
needle = "replace_once(\n    \"stats_server.py\",\n    '''displacement_body_ratio:'SWING displacement body/range'"
start = text.find(needle)
if start < 0:
    raise SystemExit('brittle label replacement block start not found')
end_marker = "\n\n# The funnel header is a single JS template literal."
end = text.find(end_marker, start)
if end < 0:
    raise SystemExit('brittle label replacement block end not found')
text = text[:start] + text[end:]
p.write_text(text, encoding='utf-8')
print('brittle label replacement removed from helper')
