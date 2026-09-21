from pathlib import Path
import unittest

from apex.ui.telegram.router import (
    COMMAND_ROUTES,
    TelegramHandlers,
    register_telegram_handlers,
)


class _Route:
    def __init__(self):
        self.calls = []

    def register(self, *args):
        self.calls.append(args)


class _Dispatcher:
    def __init__(self):
        self.message = _Route()
        self.callback_query = _Route()
        self.chat_member = _Route()


def _handler(name):
    def handler():
        return name

    return handler


class TelegramRouterTests(unittest.TestCase):
    def test_router_registers_explicit_commands_before_catch_all(self):
        dispatcher = _Dispatcher()
        values = {
            field: _handler(field) for field in TelegramHandlers.__dataclass_fields__
        }
        handlers = TelegramHandlers(**values)

        register_telegram_handlers(
            dispatcher, handlers, lambda value: ("command", value)
        )

        self.assertEqual(
            [call[1] for call in dispatcher.message.calls[:-1]],
            [("command", command) for command, _ in COMMAND_ROUTES],
        )
        self.assertEqual(dispatcher.message.calls[-1], (handlers.text,))
        self.assertEqual(dispatcher.callback_query.calls, [(handlers.callback,)])
        self.assertEqual(dispatcher.chat_member.calls, [(handlers.chat_member,)])

    def test_launcher_has_no_import_time_handler_decorators(self):
        source = Path("apex", "compatibility", "legacy_bot_runtime.py").read_text(encoding="utf-8")

        self.assertNotIn("@dp.message", source)
        self.assertNotIn("@dp.callback_query", source)
        self.assertNotIn("@dp.chat_member", source)
        self.assertIn("_v3_register_telegram_handlers(", source)
        self.assertNotIn("async def cmd_start", source)
        self.assertNotIn("async def cmd_risk", source)
        self.assertIn("_v3_command_handlers.start", source)
        alert_body = source.split("async def cmd_alert", 1)[1].split(
            "async def cmd_journal", 1
        )[0]
        self.assertNotIn("INSERT INTO alerts", alert_body)
        self.assertIn("_v3_compatibility_commands.alert", alert_body)
        news_body = source.split("async def cmd_news", 1)[1].split(
            "def scan_diagnostics", 1
        )[0]
        self.assertIn("_v3_market_commands.news", news_body)
        self.assertNotIn("get_crypto_news", news_body)
        text_body = source.split("async def handle_text", 1)[1].split(
            "def _v3_live_analysis_markup", 1
        )[0]
        self.assertIn("_v3_chat_handlers.text", text_body)
        self.assertNotIn("ask_ai", text_body)
        callback_body = source.split("async def handle_callback", 1)[1].split(
            "async def cmd_pump", 1
        )[0]
        self.assertIn("_v3_state_callback_handlers.handle", callback_body)
        self.assertIn("_get_v3_market_navigation_callbacks().handle", callback_body)
        self.assertNotIn("callback.data", callback_body)
        self.assertNotIn('data == "menu_', callback_body)


if __name__ == "__main__":
    unittest.main()
