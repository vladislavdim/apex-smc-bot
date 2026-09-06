from pathlib import Path

p = Path('.github/scripts/apply_strategy_lab_terminal_truth.py')
text = p.read_text(encoding='utf-8')
old = "displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg' "
new = "displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg'"
if text.count(old) != 2:
    # one occurrence is the old matcher and one is embedded in its error-prone replacement block
    # Replace only the literal matcher target while preserving the replacement content.
    marker = "'''displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg' '''"
    replacement = "'''displacement_body_ratio:'SWING displacement body/range',volume_ratio:'SWING volume/avg' '''".replace("/avg' ''", "/avg'''")
    if marker not in text:
        raise SystemExit('helper matcher literal not found')
    text = text.replace(marker, replacement, 1)
else:
    text = text.replace(old, new, 1)
p.write_text(text, encoding='utf-8')
print('helper matcher corrected')
