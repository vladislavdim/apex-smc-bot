import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from apex.ui.telegram.chat import ChatDependencies, TelegramChatHandlers


def _message(text="hello"):
    return SimpleNamespace(
        from_user=SimpleNamespace(id=7, first_name="Vlad"),
        text=text,
        answer=AsyncMock(),
    )


class TelegramChatHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.states = {}
        self.save_log = Mock()
        self.extract_profile = Mock()
        self.send_message = AsyncMock()
        self.dependencies = ChatDependencies(
            user_states=self.states,
            timeframe_labels={"1h": "1 час"},
            live_position_analysis=lambda symbol, timeframe: f"{symbol}:{timeframe}",
            live_markup=lambda symbol, timeframe: (symbol, timeframe),
            save_chat_log=self.save_log,
            ask_ai=lambda user_id, name, text: f"reply:{user_id}:{name}:{text}",
            extract_profile=self.extract_profile,
            ask_groq=lambda *_args, **_kwargs: "Добро пожаловать в APEX!",
            send_message=self.send_message,
        )
        self.handlers = TelegramChatHandlers(self.dependencies)

    async def test_live_analysis_consumes_state_and_uses_injected_view(self):
        self.states[7] = {"action": "live_analysis", "tf": "1h"}
        message = _message("btc")
        thinking = SimpleNamespace(delete=AsyncMock())
        message.answer.side_effect = [thinking, None]

        await self.handlers.text(message)

        self.assertNotIn(7, self.states)
        self.assertEqual(message.answer.await_args.args[0], "BTCUSDT:1h")
        self.assertEqual(message.answer.await_args.kwargs["reply_markup"], ("BTCUSDT", "1h"))
        self.save_log.assert_not_called()

    async def test_free_text_logs_both_sides_and_schedules_profile(self):
        message = _message("status")
        thinking = SimpleNamespace(delete=AsyncMock())
        message.answer.side_effect = [thinking, None]

        await self.handlers.text(message)
        for _ in range(20):
            if self.extract_profile.called:
                break
            await asyncio.sleep(0.001)

        self.assertEqual(
            self.save_log.call_args_list[0].args, (7, "user", "status")
        )
        self.assertEqual(
            self.save_log.call_args_list[1].args,
            (7, "assistant", "reply:7:Vlad:status"),
        )
        self.extract_profile.assert_called_once()

    async def test_new_member_welcome_uses_injected_sender(self):
        event = SimpleNamespace(
            old_chat_member=SimpleNamespace(status="left"),
            new_chat_member=SimpleNamespace(
                status="member", user=SimpleNamespace(id=8, first_name="Alex")
            ),
            chat=SimpleNamespace(id=99),
        )

        await self.handlers.member(event)

        self.send_message.assert_awaited_once_with(99, "Добро пожаловать в APEX!")


if __name__ == "__main__":
    unittest.main()
