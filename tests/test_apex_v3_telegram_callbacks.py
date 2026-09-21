import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from apex.ui.telegram.callbacks import StateCallbackDependencies, StateCallbackHandlers


class StateCallbackHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.edit = AsyncMock()
        self.fetch_manager = Mock(return_value=[{"signal_id": 7}])
        self.dependencies = StateCallbackDependencies(
            edit_message=self.edit,
            main_menu=lambda: "menu",
            fetch_manager_trades=self.fetch_manager,
            fetch_manager_trade=lambda signal_id, limit: {"signal_id": signal_id},
            format_manager_dashboard=lambda rows: f"manager:{len(rows)}",
            format_manager_trade_detail=lambda row: f"detail:{row['signal_id']}",
            manager_trade_buttons=lambda rows: [("BTC", "manager_trade_7")],
            fetch_trade_rows=lambda category, limit: [category],
            format_trade_view=lambda category, rows: f"{category}:{len(rows)}",
            fetch_watchlist=lambda limit: ["BTC"],
            format_watchlist=lambda rows: f"watch:{len(rows)}",
            rebuild_strategy_risk=Mock(),
            scanner_dashboard=lambda: {"FAST": "READY"},
            format_scanner_dashboard=lambda payload: "scanners",
            setup_evidence_dashboard=lambda hours, limit: [hours, limit],
            format_setup_evidence_dashboard=lambda payload: "evidence",
            fetch_groq_rejections=lambda hours, limit: [hours, limit],
            format_groq_rejections=lambda payload: "rejections",
            get_user_memory=lambda user_id: {"deposit": 1000, "risk": 0.5},
            live_learning=lambda: "learning",
            current_incidents=lambda: [{"code": "TEST"}],
            format_incidents=lambda rows: f"incidents:{len(rows)}",
            fetch_strategy_stats=lambda: ["FAST"],
            format_strategy_stats=lambda rows: f"strategies:{len(rows)}",
            system_dashboard=lambda: "system:READY",
            stats_url="https://stats.example",
            button=lambda **kwargs: SimpleNamespace(**kwargs),
            markup=lambda **kwargs: SimpleNamespace(**kwargs),
        )
        self.handlers = StateCallbackHandlers(self.dependencies)

    def callback(self, data):
        return SimpleNamespace(
            data=data,
            message=SimpleNamespace(),
            from_user=SimpleNamespace(id=7),
        )

    async def test_manager_dashboard_reads_injected_state_projection(self):
        handled = await self.handlers.handle(self.callback("menu_trade_manager"))

        self.assertTrue(handled)
        self.fetch_manager.assert_called_once_with(12)
        self.assertEqual(self.edit.await_args.args[1], "manager:1")

    async def test_manager_detail_parses_only_numeric_signal_id(self):
        handled = await self.handlers.handle(self.callback("manager_trade_7"))

        self.assertTrue(handled)
        self.assertEqual(self.edit.await_args.args[1], "detail:7")

    async def test_unknown_callback_is_left_for_market_controller(self):
        handled = await self.handlers.handle(self.callback("menu_market"))

        self.assertFalse(handled)
        self.edit.assert_not_awaited()

    async def test_stats_reads_injected_live_memory_view(self):
        handled = await self.handlers.handle(self.callback("menu_stats"))

        self.assertTrue(handled)
        self.assertEqual(self.edit.await_args.args[1], "learning")

    async def test_system_reads_canonical_runtime_projection(self):
        handled = await self.handlers.handle(self.callback("menu_system"))

        self.assertTrue(handled)
        self.assertEqual(self.edit.await_args.args[1], "system:READY")


if __name__ == "__main__":
    unittest.main()
