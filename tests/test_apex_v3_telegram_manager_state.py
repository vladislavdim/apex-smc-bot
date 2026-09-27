"""Production Manager Telegram projections require an explicit State repository."""
import unittest
from unittest.mock import Mock

from apex.ui.telegram.manager import fetch_state_manager_trade, fetch_state_manager_trades


class ManagerStateViewTests(unittest.TestCase):
    def test_list_and_detail_project_confirmed_state(self):
        repository = Mock()
        repository.recent.return_value = [
            {"signal_id": 21, "status": "ACTIVE", "confirmed_protect_level": 96},
            {"signal_id": 22, "status": "CLOSED", "close_result": "tp1"},
        ]
        repository.get.return_value = {"signal_id": 21, "confirmed_protect_level": 96}
        repository.events.return_value = [{"summary": "HOLD", "confirmed_protect_level": 96}]
        rows = fetch_state_manager_trades(repository, 2)
        detail = fetch_state_manager_trade(repository, 21, 3)
        self.assertEqual([row["signal_result"] for row in rows], ["pending", "tp1"])
        self.assertEqual(rows[0]["manager_protect_level"], 96)
        self.assertEqual(detail["state"]["manager_protect_level"], 96)
        self.assertEqual(detail["events"][0]["reason"], "HOLD")
        repository.recent.assert_called_once_with(limit=2)
        repository.events.assert_called_once_with(21, limit=3)

    def test_read_error_does_not_fall_back_to_legacy(self):
        repository = Mock()
        repository.recent.side_effect = RuntimeError("state_unavailable")
        with self.assertRaisesRegex(RuntimeError, "state_unavailable"):
            fetch_state_manager_trades(repository)
