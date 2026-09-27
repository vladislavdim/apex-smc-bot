"""Exercise shutdown without importing trading or contacting exchanges."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from apex.app.shutdown import ShutdownDependencies, shutdown_production


class ShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_fences_before_cleanup_and_continues_after_lease_failure(self):
        events = []
        async def release():
            events.append('lease')
            raise RuntimeError('offline')
        async def backup(reason):
            events.append('backup')
            self.assertEqual(reason, 'render_sigterm')
        stop = AsyncMock()
        marker = Mock()
        deps = ShutdownDependencies(
            runtime=SimpleNamespace(inhibit_entries=lambda reason: events.append(reason)),
            state_db_path='state.db', instance_id='worker', release_lease=release,
            record_shutdown=marker, backup=backup, stop_market=stop,
        )
        await shutdown_production(deps, 'polling_shutdown')
        self.assertEqual(events, ['GRACEFUL_SHUTDOWN', 'lease', 'backup'])
        marker.assert_called_once_with('state.db', 'polling_shutdown', instance_id='worker')
        stop.assert_awaited_once()

    async def test_marker_and_backup_errors_do_not_skip_market_cleanup(self):
        for error in (RuntimeError('backup unavailable'), TimeoutError()):
            with self.subTest(error=type(error).__name__):
                stop = AsyncMock()
                backup = AsyncMock(side_effect=error)
                deps = ShutdownDependencies(
                    runtime=Mock(), state_db_path='state.db', instance_id='worker',
                    release_lease=AsyncMock(), record_shutdown=Mock(side_effect=OSError('disk')),
                    backup=backup, stop_market=stop,
                )
                await shutdown_production(deps, 'render_sigterm')
                backup.assert_awaited_once()
                stop.assert_awaited_once()
