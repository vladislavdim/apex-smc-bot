from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from apex.app.bootstrap import ProductionDependencies, build_webhook_application, _run_polling
from apex.config.settings import ApexConfig


class FakeRouter:
    def __init__(self):
        self.routes = []

    def add_get(self, path, handler, **options):
        self.routes.append(("GET", path, handler, options))

    def add_head(self, path, handler):
        self.routes.append(("HEAD", path, handler, {}))

    def add_post(self, path, handler):
        self.routes.append(("POST", path, handler, {}))


class FakeApplication:
    def __init__(self):
        self.router = FakeRouter()
        self.on_startup = []
        self.on_shutdown = []


class FakeWeb:
    Application = FakeApplication

    @staticmethod
    def json_response(payload, status=200):
        return SimpleNamespace(payload=payload, status=status)

    @staticmethod
    def Response(*, text):
        return SimpleNamespace(text=text)


class FakeRuntime:
    def public_snapshot(self):
        return {"ready": False, "status": "STARTING", "health": "DEGRADED"}


class FakeDispatcher:
    def __init__(self):
        self.feed_update = AsyncMock()
        self.start_polling = AsyncMock()

    def resolve_used_update_types(self):
        return ["message"]


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    def dependencies(self):
        return ProductionDependencies(
            config=ApexConfig.from_env({}),
            runtime=FakeRuntime(),
            telegram_bot=object(),
            dispatcher=FakeDispatcher(),
            update_type=lambda **payload: payload,
            web=FakeWeb,
            initialize=AsyncMock(),
            shutdown=AsyncMock(),
            token_snapshot=lambda: {"available": True},
        )

    async def test_webhook_app_exposes_distinct_health_and_lifecycle(self):
        deps = self.dependencies()
        app = build_webhook_application(deps)
        routes = {(method, path): (handler, options) for method, path, handler, options in app.router.routes}
        self.assertEqual(
            set(routes),
            {
                ("GET", "/"), ("HEAD", "/"), ("GET", "/health"),
                ("GET", "/health/live"), ("GET", "/health/ready"),
                ("GET", "/health/system"), ("POST", "/webhook"),
                ("GET", "/tokens"),
            },
        )
        self.assertFalse(routes[("GET", "/")][1]["allow_head"])
        ready = await routes[("GET", "/health/ready")][0](SimpleNamespace(path="/health/ready"))
        live = await routes[("GET", "/health/live")][0](SimpleNamespace(path="/health/live"))
        self.assertEqual(ready.status, 503)
        self.assertFalse(ready.payload["ready"])
        self.assertEqual(live.status, 200)
        self.assertTrue(live.payload["alive"])
        await app.on_startup[0](app)
        await app.on_shutdown[0](app)
        deps.initialize.assert_awaited_once_with("webhook")
        deps.shutdown.assert_awaited_once_with("render_sigterm")

    async def test_polling_uses_the_same_injected_lifecycle(self):
        deps = self.dependencies()
        await _run_polling(deps)
        deps.initialize.assert_awaited_once_with("polling")
        deps.dispatcher.start_polling.assert_awaited_once_with(
            deps.telegram_bot, allowed_updates=["message"],
        )
        deps.shutdown.assert_awaited_once_with("polling_shutdown")


if __name__ == "__main__":
    unittest.main()
