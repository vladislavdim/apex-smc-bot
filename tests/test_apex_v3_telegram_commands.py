import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from apex.ui.telegram.commands import (
    CommandDependencies,
    CompatibilityCommandDependencies,
    CompatibilityCommandHandlers,
    MarketCommandDependencies,
    MarketCommandHandlers,
    TelegramCommandHandlers,
)


def _message(*, user_id=7, name="Vlad", text=""):
    return SimpleNamespace(
        from_user=SimpleNamespace(id=user_id, first_name=name),
        text=text,
        answer=AsyncMock(),
    )


class TelegramCommandHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.memory = {"messages": 2, "deposit": 1000.0, "risk": 0.5}
        self.update_memory = Mock()
        self.dependencies = CommandDependencies(
            admin_ids=frozenset({7}),
            get_user_memory=lambda _user_id: dict(self.memory),
            update_user_memory=self.update_memory,
            main_menu=lambda: "main-menu",
            pairs_keyboard=lambda action: f"pairs-{action}",
            live_stats=lambda: "live-stats",
        )
        self.handlers = TelegramCommandHandlers(self.dependencies)

    async def test_unauthorized_user_gets_no_command_response(self):
        message = _message(user_id=99, text="/risk 5000")

        await self.handlers.risk(message)

        message.answer.assert_not_awaited()
        self.update_memory.assert_not_called()

    async def test_start_uses_injected_memory_and_menu(self):
        message = _message()

        await self.handlers.start(message)

        self.update_memory.assert_called_once_with(7, name="Vlad")
        _, kwargs = message.answer.await_args
        self.assertEqual(kwargs["parse_mode"], "HTML")
        self.assertEqual(kwargs["reply_markup"], "main-menu")
        self.assertIn("С возвращением, Vlad", message.answer.await_args.args[0])

    async def test_risk_updates_only_user_preferences(self):
        message = _message(text="/risk 2500")

        await self.handlers.risk(message)

        self.update_memory.assert_called_once_with(7, deposit=2500.0)
        self.assertIn("$2,500.00", message.answer.await_args.args[0])

    async def test_setrisk_rejects_out_of_range_value(self):
        message = _message(text="/setrisk 25")

        await self.handlers.setrisk(message)

        self.update_memory.assert_not_called()
        message.answer.assert_awaited_once_with("Риск должен быть от 0.1% до 10%")

    async def test_stats_uses_injected_live_memory_view(self):
        message = _message(text="/stats")

        await self.handlers.stats(message)

        message.answer.assert_awaited_once_with("live-stats", parse_mode="HTML")


class CompatibilityCommandHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.executescript(
            """
            CREATE TABLE alerts(
                id INTEGER PRIMARY KEY, user_id INTEGER, symbol TEXT,
                price_level REAL, direction TEXT, triggered INTEGER,
                created_at TEXT
            );
            CREATE TABLE journal(
                id INTEGER PRIMARY KEY, user_id INTEGER, symbol TEXT,
                direction TEXT, entry REAL, exit_price REAL, result TEXT,
                note TEXT, pnl_percent REAL, created_at TEXT
            );
            CREATE TABLE knowledge(
                id INTEGER PRIMARY KEY, topic TEXT, content TEXT,
                source TEXT, created_at TEXT
            );
            INSERT INTO knowledge(topic, content, source, created_at)
            VALUES ('live outcome', 'confirmed', 'live', '2026-09-19');
            """
        )
        self.dependencies = CompatibilityCommandDependencies(
            admin_ids=frozenset({7}),
            connect=lambda: _NonClosingConnection(self.connection),
            get_live_prices=lambda: {"BTCUSDT": {"price": 65000.0}},
            ask_groq=lambda *_args, **_kwargs: "review",
        )
        self.handlers = CompatibilityCommandHandlers(self.dependencies)

    def tearDown(self):
        self.connection.close()

    async def test_alert_write_is_owned_by_injected_adapter(self):
        message = _message(text="/alert BTCUSDT 70000")

        await self.handlers.alert(message)

        row = self.connection.execute(
            "SELECT user_id, symbol, price_level, direction FROM alerts"
        ).fetchone()
        self.assertEqual(row, (7, "BTCUSDT", 70000.0, "above"))
        self.assertIn("Алерт установлен", message.answer.await_args.args[0])

    async def test_journal_calculates_short_pnl_without_trading_access(self):
        message = _message(text="/journal BTC SHORT 100 90 win test")

        await self.handlers.journal(message)

        row = self.connection.execute(
            "SELECT direction, pnl_percent, note FROM journal"
        ).fetchone()
        self.assertEqual(row, ("SHORT", 10.0, "test"))

    async def test_brain_is_a_read_only_compatibility_view(self):
        message = _message(text="/brain")

        await self.handlers.brain(message)

        self.assertIn("Всего знаний: <b>1</b>", message.answer.await_args.args[0])


class _NonClosingConnection:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, *args, **kwargs):
        return self.connection.execute(*args, **kwargs)

    def commit(self):
        return self.connection.commit()

    def close(self):
        pass


class MarketCommandHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ask_groq = Mock(return_value="market-review")
        self.save_news = Mock()
        self.detect_accumulation = Mock(
            return_value={"symbol": "BTCUSDT", "score": 75}
        )
        self.analyze_trade_type = Mock(return_value={"text": "existing-analysis"})
        self.handlers = MarketCommandHandlers(
            MarketCommandDependencies(
                get_crypto_news=lambda: [{"title": "crypto"}],
                get_market_impact_news=lambda: [{"title": "macro"}],
                format_news=lambda items: ",".join(item["title"] for item in items),
                ask_groq=self.ask_groq,
                save_news=self.save_news,
                detect_accumulation=self.detect_accumulation,
                format_accumulation=lambda item: f"score={item['score']}",
                get_top_pairs=lambda limit: ["BTCUSDT"][:limit],
                analyze_trade_type=self.analyze_trade_type,
                symbol_aliases={"btc": "BTCUSDT"},
                timeframe_categories={
                    "scalp": ("1m", "5m", "15m"),
                    "swing": ("1h", "4h"),
                    "long": ("1d", "1w", "1M"),
                },
            )
        )

    async def test_news_uses_injected_sources_and_bounded_output(self):
        message = _message(text="/news")

        await self.handlers.news(message)

        self.ask_groq.assert_called_once()
        self.save_news.assert_called_once_with("crypto news", "crypto\nmacro")
        self.assertEqual(message.answer.await_count, 2)
        self.assertIn("market-review", message.answer.await_args.args[0])

    async def test_single_pump_analysis_uses_existing_detector(self):
        message = _message(text="/pump btc")

        await self.handlers.pump(message)

        self.detect_accumulation.assert_called_once_with("BTCUSDT")
        self.assertIn("score=75", message.answer.await_args.args[0])

    async def test_trade_analysis_preserves_existing_strategy_result(self):
        message = _message(text="/trade btc swing")

        await self.handlers.trade(message)

        self.analyze_trade_type.assert_called_once_with("BTCUSDT", "swing")
        self.assertIn("existing-analysis", message.answer.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
