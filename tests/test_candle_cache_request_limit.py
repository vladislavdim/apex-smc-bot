import unittest
from pathlib import Path

class CandleCacheRequestLimitTests(unittest.TestCase):
    def test_get_candles_requires_cached_sample_to_cover_requested_limit(self):
        s = Path("market.py").read_text(encoding="utf-8")
        self.assertIn("requested_limit = max(1, int(limit or 1))", s)
        self.assertIn("len(cached) >= requested_limit", s)
        self.assertIn("len(_gc) >= requested_limit", s)
        self.assertNotIn("len(cached) >= 20:\n            return cached", s)
        self.assertIn("return rc[-requested_limit:]", s)
        self.assertIn("return candles[-requested_limit:]", s)

if __name__ == "__main__":
    unittest.main()
