from pathlib import Path

p=Path('stats_server.py')
s=p.read_text(encoding='utf-8')
s=s.replace('STATS_BASELINE_UTC = datetime.fromisoformat("2026-09-03T11:03:20+00:00")  # PR #95 live on Render','STATS_BASELINE_UTC = datetime.fromisoformat("2026-09-03T14:54:22+00:00")  # PR #97 live on Render',1)
s=s.replace('"baseline":"post95"','"baseline":"post97"',1)
s=s.replace('Актуальная статистика после #95 · с 11:03:20 UTC 03.09.2026','Актуальная статистика после #97 · с 14:54:22 UTC 03.09.2026',1)
p.write_text(s,encoding='utf-8')

t=Path('tests/test_strategy_stats_post95.py')
t.write_text('''import unittest\nfrom pathlib import Path\n\nclass Post97StatsTests(unittest.TestCase):\n    def test_dashboard_is_clamped_to_post97_baseline(self):\n        s=Path("stats_server.py").read_text(encoding="utf-8")\n        self.assertIn("2026-09-03T14:54:22+00:00", s)\n        self.assertIn("GREATEST(NOW() - (%s * INTERVAL '1 day'), %s::timestamptz)", s)\n        self.assertIn('"baseline":"post97"', s)\n        self.assertIn("Актуальная статистика после #97", s)\n\nif __name__ == "__main__": unittest.main()\n''',encoding='utf-8')
print('post97 baseline applied')
