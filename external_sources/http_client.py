"""Bounded, retrying JSON requests. No provider error may escape the scanner."""

from __future__ import annotations

import asyncio
import json
import random
from typing import Any
from urllib.error import HTTPError
from email.utils import parsedate_to_datetime
import time
from .budget import budget, BudgetDenied, request_scope
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

    async def _request(self, url, params=None, headers=None, method="GET", payload=None):
        source, units = request_scope(url, params, payload)
        last_error = None
        async with self._semaphore:
            for attempt in range(self.retries + 1):
                try:
                    await asyncio.to_thread(budget.reserve, source, units)
                except Exception as exc:
                    # A failed ledger must never permit unmetered traffic.
                    raise ExternalHTTPError("budget_unavailable_or_exhausted") from exc
                try:
                    result = await asyncio.to_thread(
                        self._fetch_sync, url, params, headers, method, payload)
                except Exception as exc:
                    last_error = exc
                    limited = isinstance(exc, HTTPError) and exc.code in (418, 429)
                    retry_after = 0
                    if limited:
                        raw = exc.headers.get("Retry-After", "60") if exc.headers else "60"
                        try:
                            retry_after = max(0, float(raw))
                        except (ValueError, TypeError):
                            try:
                                retry_after = max(0, parsedate_to_datetime(raw).timestamp()-time.time())
                            except Exception:
                                retry_after = 60
                    await asyncio.to_thread(budget.outcome, source, failed=True,
                                            rate_limited=limited, retry_after=retry_after)
                    if limited or (isinstance(exc, HTTPError) and 400 <= exc.code < 500):
                        break
                    if attempt < self.retries:
                        await asyncio.sleep(0.25 * (2**attempt) + random.uniform(0, 0.1))
                else:
                    await asyncio.to_thread(budget.outcome, source)
                    return result
        raise ExternalHTTPError(type(last_error).__name__ if last_error else "request failed")

    async def get_json(self, url: str, params=None, headers=None):
        return await self._request(url, params, headers)

    async def post_json(self, url: str, payload: Any, headers=None):
        return await self._request(url, headers=headers, method="POST", payload=payload)


http_client = ExternalHTTPClient()
