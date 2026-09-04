import unittest
from pathlib import Path


class GroqGptOssOutputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.market = Path("market.py").read_text(encoding="utf-8")

    def test_gpt_oss_requests_final_content_not_reasoning_budget(self):
        self.assertIn('str(model).startswith("openai/gpt-oss-")', self.market)
        self.assertIn('"max_completion_tokens": max_tokens', self.market)
        self.assertIn('"reasoning_effort": "low"', self.market)
        self.assertIn('"include_reasoning": False', self.market)

    def test_empty_final_content_falls_through_to_model_fallback(self):
        self.assertIn('if not content.strip():', self.market)
        self.assertIn('returned empty final content; trying fallback', self.market)

    def test_non_gpt_oss_keeps_existing_max_tokens_contract(self):
        self.assertIn('_request_kwargs["max_tokens"] = max_tokens', self.market)


if __name__ == "__main__":
    unittest.main()
