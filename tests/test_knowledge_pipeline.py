import unittest
from pathlib import Path


class KnowledgePipelineTests(unittest.TestCase):
    def test_legacy_autonomous_modules_are_physically_removed(self):
        for name in (
            "brain_builder.py",
            "brain_router.py",
            "web_learner.py",
            "apex_autopilot.py",
            "core/learning.py",
            "core/trade_baseline_reset.py",
            "groq_extensions.py",
        ):
            self.assertFalse(Path(name).exists(), name)

    def test_startup_never_resets_trade_statistics(self):
        source = Path("apex", "compatibility", "legacy_bot_runtime.py").read_text()
        self.assertNotIn("_apply_trade_learning_baseline_reset", source)
        self.assertNotIn("trade_baseline_reset", source)

    def test_telegram_exposes_live_learning_without_legacy_mutation_buttons(self):
        text = Path('apex/compatibility/legacy_bot_runtime.py').read_text()
        self.assertIn('callback_data="menu_live_learning"', text)
        self.assertIn('_format_live_learning', text)
        for callback in ('menu_brain', 'brain_run_analysis', 'brain_web_learn_now',
                         'brain_strategy_refresh', 'brain_router_strategy_refresh'):
            self.assertNotIn(f'callback_data="{callback}"', text)

    def test_polling_mode_does_not_schedule_rule_mutating_learning(self):
        bootstrap = Path('apex/app/bootstrap.py').read_text()
        registry = Path('apex/app/job_registry.py').read_text()
        runtime = bootstrap + registry
        self.assertIn('async def _run_polling(', bootstrap)
        self.assertNotIn('_polling_web_learner', runtime)
        self.assertNotIn('run_brain_builder_async', runtime)
        self.assertNotIn('autonomous_learning_cycle', runtime)

    def test_strategies_do_not_call_removed_adaptive_learning_hooks(self):
        source = Path("apex/compatibility/legacy_market_runtime.py").read_text()
        for hook in (
            "_LEARNING_OK",
            "_learn_should_skip",
            "_learn_patterns",
            "_learn_save_pattern",
            "_brain_router",
            "_autopilot_on_close",
            "get_relevant_rules(",
            "get_recent_errors(",
            "calc_size_multiplier(",
        ):
            self.assertNotIn(hook, source)
if __name__ == '__main__':
    unittest.main()
