"""Respect the server's minimum wait before aiogram's polling backoff."""
import asyncio
from aiogram.client.session.middlewares.base import BaseRequestMiddleware
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import GetUpdates


class PollingRetryAfter(BaseRequestMiddleware):
    async def __call__(self, make_request, bot, method):
        try:
            return await make_request(bot, method)
        except TelegramRetryAfter as exc:
            if isinstance(method, GetUpdates):
                await asyncio.sleep(max(0, float(exc.retry_after)))
            raise
