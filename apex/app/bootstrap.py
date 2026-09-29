"""Transport-only production launcher shared by webhook and polling.

Trading bootstrap and shutdown remain injected callbacks.  This module owns
only process fencing, Telegram transport selection and HTTP health routing.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from apex.config.settings import ApexConfig


AsyncTransportHook = Callable[[str], Awaitable[Any]]
AsyncShutdownHook = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class ProductionDependencies:
    config: ApexConfig
    runtime: Any
    telegram_bot: Any
    dispatcher: Any
    update_type: Any
    web: Any
    initialize: AsyncTransportHook
    shutdown: AsyncShutdownHook
    token_snapshot: Callable[[], Mapping[str, Any]]


def _acquire_process_lock(path: str = "/tmp/apex_bot.lock") -> Any | None:
    # Keep this descriptor open for the lifetime of the process. Unlinking the
    # file would let a second process lock a different inode during rollout.
    lock_file = open(path, "w", encoding="utf-8")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_file.close()
        return None
    return lock_file


def build_webhook_application(deps: ProductionDependencies) -> Any:
    app = deps.web.Application()

    async def health(request: Any) -> Any:
        snapshot = deps.runtime.public_snapshot()
        if request.path == "/health/ready":
            return deps.web.json_response(
                {
                    "ready": snapshot["ready"],
                    "status": snapshot["status"],
                    "health": snapshot["health"],
                },
                status=200 if snapshot["ready"] else 503,
            )
        if request.path == "/health/system":
            return deps.web.json_response(snapshot)
        return deps.web.json_response(
            {"alive": True, "status": snapshot["status"], "health": snapshot["health"]}
        )

    async def handle_webhook(request: Any) -> Any:
        secret = deps.config.integrations.telegram_webhook_secret
        supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not secret or not hmac.compare_digest(supplied, secret):
            return deps.web.Response(text="Forbidden", status=403)
        try:
            update = deps.update_type(**json.loads(await request.read()))
        except (ValueError, TypeError):
            return deps.web.Response(text="Invalid update", status=400)
        try:
            await deps.dispatcher.feed_update(deps.telegram_bot, update)
        except Exception as exc:
            logging.error("Webhook processing failed: %s", type(exc).__name__)
            # Do not acknowledge an update that was not successfully processed.
            return deps.web.Response(text="Retry later", status=503)
        return deps.web.Response(text="OK")

    async def token_stats(_request: Any) -> Any:
        return deps.web.json_response(dict(deps.token_snapshot()))

    async def startup(_app: Any) -> None:
        await deps.initialize("webhook")

    async def shutdown(_app: Any) -> None:
        await deps.shutdown("render_sigterm")

    app.router.add_get("/", health, allow_head=False)
    for path in ("/health", "/health/live", "/health/ready", "/health/system"):
        app.router.add_get(path, health)
    app.router.add_head("/", health)
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_get("/tokens", token_stats)
    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)
    return app


async def _run_polling(deps: ProductionDependencies) -> None:
    try:
        await deps.initialize("polling")
        await deps.dispatcher.start_polling(
            deps.telegram_bot,
            allowed_updates=deps.dispatcher.resolve_used_update_types(),
        )
    finally:
        await deps.shutdown("polling_shutdown")


def run_production(deps: ProductionDependencies, *, max_polling_restarts: int = 10) -> None:
    lock_file = _acquire_process_lock()
    if lock_file is None:
        logging.error("Другой инстанс уже запущен — выходим")
        return
    # Keep the descriptor bound to this frame until the selected transport exits.
    webhook_url = deps.config.integrations.webhook_url
    if webhook_url:
        app = build_webhook_application(deps)
        logging.info("Запуск в webhook режиме на порту %s", deps.config.runtime.port)
        deps.web.run_app(app, host="0.0.0.0", port=deps.config.runtime.port)
        return

    for restart_count in range(max(1, int(max_polling_restarts))):
        try:
            asyncio.run(_run_polling(deps))
        except Exception as exc:
            attempt = restart_count + 1
            logging.error("Polling упал (%s/%s): %s", attempt, max_polling_restarts, exc)
            if attempt >= max_polling_restarts:
                return
            time.sleep(10)
            logging.info("Перезапускаем polling...")
        else:
            return


__all__ = [
    "ProductionDependencies", "build_webhook_application", "run_production",
]
