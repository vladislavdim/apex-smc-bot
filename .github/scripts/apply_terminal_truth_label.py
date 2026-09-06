from pathlib import Path

p = Path('stats_server.py')
text = p.read_text(encoding='utf-8')
old = "displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg'"
new = "displacement_body_ratio:'SWING body/range (raw; direction ignored)',directional_displacement_ratio:'SWING directional displacement (actual gate ratio)',volume_ratio:'SWING volume/avg'"
if text.count(old) != 1:
    raise SystemExit(f'Strategy Lab SWING label match count={text.count(old)}')
p.write_text(text.replace(old, new, 1), encoding='utf-8')
print('Strategy Lab SWING labels updated safely')
