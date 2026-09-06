from pathlib import Path

p = Path('.github/scripts/apply_strategy_lab_terminal_truth.py')
text = p.read_text(encoding='utf-8')
marker = "displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg'"
needle = marker + " '''"
replacement = marker + "'''"
if text.count(needle) != 1:
    raise SystemExit(f'helper matcher literal count={text.count(needle)}')
p.write_text(text.replace(needle, replacement, 1), encoding='utf-8')
print('helper matcher corrected')
