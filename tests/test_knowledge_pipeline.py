import os
import sqlite3
import tempfile
import unittest
from unittest import mock


class KnowledgePipelineTests(unittest.TestCase):
    def test_core_seed_has_no_legacy_pseudo_statistics(self):
        text = Path('core/learning.py').read_text()
        block = text[text.index('def _seed_smc_knowledge():'):text.index('\ndef save_signal(', text.index('def _seed_smc_knowledge():'))]
        self.assertIn("source='core_seed_v2'", block)
        self.assertIn("source='smc_seed' AND active=1", block)
        self.assertNotIn('70-80%', block)
        self.assertNotIn('выше на 30%', block)
        self.assertNotIn('манипуляция гарантирована', block)

    def test_web_research_does_not_activate_self_rules(self):
        text = Path('web_learner.py').read_text()
        self.assertIn('CREATE TABLE IF NOT EXISTS knowledge_candidates', text)
        self.assertIn("'candidate'", text)
        research = text[text.index('def groq_research_topic'):text.index('def run_web_learning_cycle')]
        self.assertNotIn('INSERT OR IGNORE INTO self_rules', research)

    def test_polling_mode_schedules_web_learning(self):
        text = Path('bot.py').read_text()
        polling = text[text.index('async def polling_main():'):]
        self.assertIn('scheduler.add_job(_polling_web_learner, "interval", hours=1', polling)
        self.assertIn('scheduler.add_job(run_brain_builder_async, "interval", hours=1', polling)


from pathlib import Path

if __name__ == '__main__':
    unittest.main()
