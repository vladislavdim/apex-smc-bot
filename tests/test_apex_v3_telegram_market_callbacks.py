from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from apex.ui.telegram.market_callbacks import (
    MarketNavigationCallbacks,
    MarketNavigationDependencies,
)


def _button(**values):
    return values


def _markup(**values):
    return values


class MarketNavigationCallbackTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.edit = AsyncMock()
        self.edit_markup = AsyncMock()
        self.states = {}
        self.live_calls = []
        self.handler = MarketNavigationCallbacks(MarketNavigationDependencies(
            edit_message=self.edit,
            edit_markup=self.edit_markup,
            pairs_keyboard=lambda action, page: (action, page),
            timeframe_keyboard=lambda: "tf-markup",
            live_timeframe_keyboard=lambda: "live-markup",
            live_position_analysis=self._live,
            get_top_pairs=lambda limit: ["BTCUSDT"],
            full_scan=lambda *args: None,
            scan_diagnostics=lambda symbol: f"no {symbol}",
            get_user_memory=lambda user_id: {"deposit": 0, "risk": 1},
            calculate_risk=lambda *args: None,
            get_crypto_news=lambda: [{"title": "crypto"}],
            get_market_news=lambda: [{"title": "macro"}],
            format_news=lambda items: ",".join(item["title"] for item in items),
            ask_groq=lambda prompt, **kwargs: "review",
            save_news=lambda *args: None,
            detect_accumulation=lambda symbol: None,
            scan_all_deals=lambda limit: [],
            get_fear_greed=lambda: None,
            get_dxy_signal=lambda: None,
            get_market_regime=lambda symbol: None,
            get_upcoming_events=lambda: None,
            get_candles=lambda symbol, timeframe, limit: [],
            universe_size=60,
            user_states=self.states,
            timeframe_labels={"1h": "1 час"},
            button=_button,
            markup=_markup,
        ))

    def _live(self, symbol, timeframe):
        self.live_calls.append((symbol, timeframe))
        return f"{symbol} {timeframe}"

    @staticmethod
    def callback(data):
        return SimpleNamespace(
            data=data,
            from_user=SimpleNamespace(id=7),
            message=SimpleNamespace(answer=AsyncMock()),
        )

    async def test_pair_navigation_is_owned_by_injected_adapter(self):
        callback = self.callback("pairs_scan_2")
        self.assertTrue(await self.handler.handle(callback))
        self.edit_markup.assert_awaited_once_with(
            callback.message, reply_markup=("scan", 2)
        )

    async def test_live_selection_updates_only_injected_user_state(self):
        callback = self.callback("live_1h")
        self.assertTrue(await self.handler.handle(callback))
        self.assertEqual(self.states[7], {"action": "live_analysis", "tf": "1h"})
        self.assertIn("1 час", self.edit.await_args.args[1])

    async def test_live_refresh_uses_injected_analysis_and_markup(self):
        callback = self.callback("live_refresh_BTCUSDT_1h")
        self.assertTrue(await self.handler.handle(callback))
        self.assertEqual(self.live_calls, [("BTCUSDT", "1h")])
        final = self.edit.await_args
        self.assertEqual(final.args[1], "BTCUSDT 1h")
        self.assertEqual(
            final.kwargs["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            "live_refresh_BTCUSDT_1h",
        )

    async def test_market_summary_uses_injected_context(self):
        callback = self.callback("menu_market")
        self.assertTrue(await self.handler.handle(callback))
        self.assertIn("Рынок сейчас", self.edit.await_args.args[1])

    async def test_symbol_scan_uses_injected_detector_and_diagnostics(self):
        callback = self.callback("scan_BTCUSDT")
        self.assertTrue(await self.handler.handle(callback))
        self.assertEqual(self.edit.await_args.args[1], "no BTCUSDT")

    async def test_news_callback_uses_injected_sources(self):
        callback = self.callback("menu_news")
        self.assertTrue(await self.handler.handle(callback))
        text = self.edit.await_args.args[1]
        self.assertIn("crypto", text)
        self.assertIn("macro", text)
        self.assertIn("review", text)

    async def test_empty_pump_scan_is_bounded(self):
        callback = self.callback("menu_pump")
        self.assertTrue(await self.handler.handle(callback))
        self.assertIn("Накоплений не найдено", self.edit.await_args.args[1])

    async def test_empty_deal_scan_uses_injected_scanner(self):
        callback = self.callback("menu_find_deals")
        self.assertTrue(await self.handler.handle(callback))
        self.assertIn("Сделок нет", self.edit.await_args.args[1])

    async def test_missing_open_deal_returns_to_deal_list(self):
        callback = self.callback("deal_open_BTCUSDT")
        self.assertTrue(await self.handler.handle(callback))
        self.assertIn("сигнал пропал", self.edit.await_args.args[1])


if __name__ == "__main__":
    unittest.main()
