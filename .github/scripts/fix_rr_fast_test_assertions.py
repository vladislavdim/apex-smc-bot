from pathlib import Path

p=Path('tests/test_rr_floor_fast_balanced.py')
s=p.read_text(encoding='utf-8')
s=s.replace('self.assertIn("max_rr=None", self.market)', 'self.assertIn("max_rr is None or rr <= max_rr", self.market)')
s=s.replace("self.assertIn('curr_body / curr_range < 0.65', self.market)", "self.assertIn('curr_body / curr_range < 0.65', self.market)")
p.write_text(s,encoding='utf-8')
print('regression assertions normalized')
