import asyncio
from dataclasses import fields, MISSING
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

from apex.config.settings import ApexConfig
from apex.config.validation import validate_config
from apex.ui.telegram.callbacks import StateCallbackDependencies, StateCallbackHandlers
from apex.ui.telegram.commands import CommandDependencies, TelegramCommandHandlers


class StatisticsRoutingTests(unittest.TestCase):
    def test_telegram_only_uses_polling_even_with_an_old_webhook_url(self):
        from unittest.mock import patch
        from apex.app.bootstrap import run_production
        deps=SimpleNamespace(config=SimpleNamespace(integrations=SimpleNamespace(
            telegram_only=True,webhook_url='https://old.example')))
        with patch('apex.app.bootstrap._acquire_process_lock',return_value=object()), \
             patch('apex.app.bootstrap._run_polling',return_value='poll') as polling, \
             patch('apex.app.bootstrap.asyncio.run') as run, \
             patch('apex.app.bootstrap.build_webhook_application') as webhook:
            run_production(deps)
        polling.assert_called_once_with(deps)
        run.assert_called_once_with('poll')
        webhook.assert_not_called()

    def dependencies(self):
        values={field.name:Mock() for field in fields(StateCallbackDependencies) if field.default is MISSING}
        values.update(edit_message=AsyncMock(),button=lambda **kwargs:kwargs,
                      markup=lambda **kwargs:kwargs,
                      general_statistics=Mock(return_value='📊 Общая статистика'))
        return StateCallbackDependencies(**values)

    def test_stats_button_and_period_strategy_filters_use_local_view(self):
        deps=self.dependencies(); router=StateCallbackHandlers(deps)
        callback=SimpleNamespace(data='stats:7d:FAST',message=object(),answer=AsyncMock())
        self.assertTrue(asyncio.run(router.handle(callback)))
        deps.general_statistics.assert_called_once_with('7d','FAST')
        keyboard=deps.edit_message.call_args.kwargs['reply_markup']['inline_keyboard']
        self.assertIn('stats:today:FAST',[button['callback_data'] for row in keyboard for button in row])
        self.assertFalse(any('url' in button for row in keyboard for button in row))
        deps.live_learning.assert_not_called()

    def test_invalid_callback_is_rejected_without_query(self):
        deps=self.dependencies(); router=StateCallbackHandlers(deps)
        callback=SimpleNamespace(data='stats:all:OTHER',message=object(),answer=AsyncMock())
        self.assertTrue(asyncio.run(router.handle(callback)))
        deps.general_statistics.assert_not_called()
        deps.edit_message.assert_not_called()

    def test_stats_command_uses_same_view_and_keeps_admin_guard(self):
        source=Mock(return_value='📊 Общая статистика')
        deps=CommandDependencies(admin_ids={7},get_user_memory=Mock(),update_user_memory=Mock(),
            main_menu=Mock(),pairs_keyboard=Mock(),live_stats=Mock(),
            general_stats=source,statistics_markup=lambda:{'statistics':True})
        handler=TelegramCommandHandlers(deps)
        message=SimpleNamespace(from_user=SimpleNamespace(id=8),answer=AsyncMock())
        asyncio.run(handler.stats(message)); message.answer.assert_not_called()
        message.from_user.id=7
        asyncio.run(handler.stats(message))
        source.assert_called_once(); deps.live_stats.assert_not_called()
        self.assertEqual(message.answer.call_args.kwargs['reply_markup'],{'statistics':True})

    def test_telegram_mode_requires_real_shared_backup_configuration(self):
        bad=ApexConfig.from_env({'APEX_TELEGRAM_ONLY':'true'})
        self.assertIn('TELEGRAM_ONLY_SHARED_FENCING_REQUIRED',validate_config(bad).errors)
        good=ApexConfig.from_env({'APEX_TELEGRAM_ONLY':'true','APEX_RUNTIME_LEASE_BACKEND':'github',
            'GITHUB_BACKUP_REPO':'owner/repository','GITHUB_TOKEN':'fake'})
        self.assertTrue(validate_config(good).valid)
        self.assertEqual(good.operational.runtime_lease_ttl_seconds,180)
