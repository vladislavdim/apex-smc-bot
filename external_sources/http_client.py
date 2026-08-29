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
    def __init__(self, timeout_seconds: float = 4.0, retries: int = 1, concurrency: int = 6) -> None:
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self._semaphore = asyncio.Semaphore(concurrency)

    def _fetch_sync(
        self, url: str, params: dict[str, Any] | None,
        headers: dict[str, str] | None, method: str = "GET", payload: Any = None,
    ) -> Any:
        if params:
            url = f"{url}?{urlencode(params)}"
        request_headers = {"User-Agent": "APEX-SMC-Bot/external-context"}
        if headers:
            request_headers.update(headers)
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        request = Request(url, data=body, headers=request_headers, method=method)
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

    async def post_json(
        self, url: str, payload: Any, headers: dict[str, str] | None = None,
    ) -> Any:
        last_error: Exception | None = None
        async with self._semaphore:
            for attempt in range(self.retries + 1):
                try:
                    return await asyncio.to_thread(
                        self._fetch_sync, url, None, headers, "POST", payload,
                    )
                except Exception as exc:
                    last_error = exc
                    if attempt < self.retries:
                        await asyncio.sleep(0.25 * (2**attempt) + random.uniform(0, 0.1))
        raise ExternalHTTPError(type(last_error).__name__ if last_error else "request failed")


http_client = ExternalHTTPClient()
