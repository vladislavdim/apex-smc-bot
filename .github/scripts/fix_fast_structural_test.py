from pathlib import Path

p = Path('tests/test_fast_structural_target_rr.py')
text = p.read_text(encoding='utf-8')
old = "        block = MARKET[MARKET.index('# RR is defined from TP1 by the central integrity/evidence pipeline.'):MARKET.index('sl_pct = round(abs(entry - sl) / entry * 100, 2)')]\n"
new = "        start = MARKET.index('# RR is defined from TP1 by the central integrity/evidence pipeline.')\n        end = MARKET.index('sl_pct = round(abs(entry - sl) / entry * 100, 2)', start)\n        block = MARKET[start:end]\n"
if text.count(old) != 1:
    raise SystemExit('expected test locator not found exactly once')
p.write_text(text.replace(old, new, 1), encoding='utf-8')
print('FAST structural target test locator fixed')
