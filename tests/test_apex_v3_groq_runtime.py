import unittest

from apex.ui.groq_runtime import (
    GroqTokenBudget,
    configure_legacy_strategy_groq,
    legacy_strategy_groq_enabled,
)


class GroqRuntimeTests(unittest.TestCase):
    def tearDown(self):
        configure_legacy_strategy_groq(False)

    def test_budget_tracks_and_resets_after_twenty_four_hours(self):
        now = [100.0]
        budget = GroqTokenBudget(limit=100, clock=lambda: now[0])
        budget.track(60)
        self.assertEqual(budget.used, 60)
        self.assertTrue(budget.available())
        budget.track(40)
        self.assertFalse(budget.available())
        now[0] += 86_401
        budget.track(5)
        self.assertEqual(budget.used, 5)

    def test_legacy_strategy_ai_policy_defaults_fail_closed(self):
        configure_legacy_strategy_groq(False)
        self.assertFalse(legacy_strategy_groq_enabled())
        configure_legacy_strategy_groq(True)
        self.assertTrue(legacy_strategy_groq_enabled())


if __name__ == "__main__":
    unittest.main()
