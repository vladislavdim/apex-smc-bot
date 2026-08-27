"""Bounded, retrying JSON requests. No provider error may escape the scanner."""

from __future__ import annotations

import asyncio
import json
import random
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class ExternalHTTPError(RuntimeError):
    pass


class ExternalHTTPClient:
    def __init__(self, timeout_seconds: float = 7.0, retries: int = 2, concurrency: int = 6) -> None:
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self._semaphore = asyncio.Semaphore(concurrency)

    def _fetch_sync(self, url: str, params: dict[str, Any] | None, headers: dict[str, str] | None) -> Any:
        if params:
            url = f"{url}?{urlencode(params)}"
        request_headers = {"User-Agent": "APEX-SMC-Bot/external-context"}
        if headers:
            request_headers.update(headers)
        request = Request(url, headers=request_headers)
        with urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    async def get_json(
        self, url: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None
    ) -> Any:
        last_error: Exception | None = None
        async with self._semaphore:
            for attempt in range(self.retries + 1):
                try:
                    return await asyncio.to_thread(self._fetch_sync, url, params, headers)
                except Exception as exc:
                    last_error = exc
                    if attempt < self.retries:
                        await asyncio.sleep(0.25 * (2**attempt) + random.uniform(0, 0.1))
        raise ExternalHTTPError(type(last_error).__name__ if last_error else "request failed")


http_client = ExternalHTTPClient()
