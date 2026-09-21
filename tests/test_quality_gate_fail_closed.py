import unittest
from pathlib import Path


class QualityGateFailClosedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = Path('bot.py').read_text(encoding='utf-8')

    def test_unavailable_gate_blocks_candidate(self):
        self.assertIn('if not _SIGNAL_QUALITY_GATE_OK:', self.bot)
        self.assertIn('quality gate unavailable; final Groq confirmation required', self.bot)
        self.assertIn('return False', self.bot)

    def test_missing_review_blocks_candidate(self):
        self.assertIn('quality review missing; final Groq confirmation required', self.bot)
        self.assertNotIn('quality gate unavailable; analytical candidate retained', self.bot)

    def test_review_defaults_to_wait(self):
        self.assertIn('decision = str(review.get("decision") or "WAIT").upper()', self.bot)
        self.assertIn('existing_decision = str(existing_review.get("decision") or "WAIT").upper()', self.bot)


if __name__ == '__main__':
    unittest.main()
