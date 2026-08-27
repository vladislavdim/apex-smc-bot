import os
import unittest
from unittest.mock import patch

from core.groq_models import configured_groq_models, is_model_unavailable_error


class GroqModelsTests(unittest.TestCase):
    def test_current_models_are_used_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                configured_groq_models(),
                ("openai/gpt-oss-20b", "openai/gpt-oss-120b"),
            )

    def test_configured_model_is_tried_first_without_duplicates(self):
        with patch.dict(os.environ, {"GROQ_MODEL": "custom/model, openai/gpt-oss-20b"}, clear=True):
            self.assertEqual(configured_groq_models()[0], "custom/model")
            self.assertEqual(configured_groq_models().count("openai/gpt-oss-20b"), 1)

    def test_model_404_is_not_a_key_quota_error(self):
        self.assertTrue(is_model_unavailable_error("404 model_not_found: does not exist"))
        self.assertFalse(is_model_unavailable_error("429 rate limit exceeded"))
