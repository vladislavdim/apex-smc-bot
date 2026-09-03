from pathlib import Path

p=Path('stats_server.py')
s=p.read_text(encoding='utf-8')

anchor='PORT = int(os.environ.get("PORT", "10000"))\n'
insert=anchor+'STATS_BASELINE_UTC = datetime.fromisoformat("2026-09-03T11:03:20+00:00")  # PR #95 live on Render\n\n'
if 'STATS_BASELINE_UTC' not in s:
    if anchor not in s: raise SystemExit('PORT anchor missing')
    s=s.replace(anchor,insert,1)

old='''    if from_date and re.fullmatch(r"\\d{4}-\\d{2}-\\d{2}", from_date): where.append("occurred_at >= %s::date"); params.append(from_date)\n    else: where.append("occurred_at >= NOW() - (%s * INTERVAL \'1 day\')"); params.append(days)\n'''
new='''    if from_date and re.fullmatch(r"\\d{4}-\\d{2}-\\d{2}", from_date):\n        where.append("occurred_at >= GREATEST(%s::date, %s::timestamptz)"); params.extend([from_date, STATS_BASELINE_UTC])\n    else:\n        where.append("occurred_at >= GREATEST(NOW() - (%s * INTERVAL '1 day'), %s::timestamptz)"); params.extend([days, STATS_BASELINE_UTC])\n'''
if old not in s: raise SystemExit('fetch date block missing')
s=s.replace(old,new,1)

old='''    return {"period_days":days,"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),\n'''
new='''    return {"period_days":days,"baseline":"post95","baseline_utc":STATS_BASELINE_UTC.isoformat(),"generated_at":datetime.now(timezone.utc).isoformat(timespec="seconds"),\n'''
if old not in s: raise SystemExit('return anchor missing')
s=s.replace(old,new,1)

old='<div class=muted>Полный read-only журнал критериев, кандидатов и Groq</div>'
new='<div class=muted>Актуальная статистика после #95 · с 11:03:20 UTC 03.09.2026</div>'
if old not in s: raise SystemExit('subtitle missing')
s=s.replace(old,new,1)

p.write_text(s,encoding='utf-8')

# tests
p=Path('tests/test_strategy_stats_post95.py')
p.write_text('''import unittest\nfrom pathlib import Path\n\nclass Post95StatsTests(unittest.TestCase):\n    def test_dashboard_is_clamped_to_post95_baseline(self):\n        s=Path("stats_server.py").read_text(encoding="utf-8")\n        self.assertIn("STATS_BASELINE_UTC", s)\n        self.assertIn("GREATEST(NOW() - (%s * INTERVAL '1 day'), %s::timestamptz)", s)\n        self.assertIn('"baseline":"post95"', s)\n        self.assertIn("Актуальная статистика после #95", s)\n\nif __name__ == "__main__": unittest.main()\n''',encoding='utf-8')
print('post95 baseline applied')
