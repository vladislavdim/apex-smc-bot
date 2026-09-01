import os
import unittest
from pathlib import Path
from unittest.mock import patch

from core import external_market_context


class ExecutionOnlyBinanceTests(unittest.IsolatedAsyncioTestCase):
    def test_binance_hosts_exist_only_in_execution_module(self):
        root = Path(__file__).resolve().parents[1]
        violations = []
        for path in root.rglob("*.py"):
            relative = path.relative_to(root)
            if relative.parts[0] in {"tests", "brain-backups"}:
                continue
            content = path.read_text(encoding="utf-8")
            if any(host in content for host in (
                "fapi.binance.com",
                "api.binance.com",
                "fstream.binance.com",
            )) and relative.as_posix() != "core/trade_execution.py":
                violations.append(relative.as_posix())
        self.assertEqual(violations, [])

    async def test_external_context_ignores_binance_market_provider_setting(self):
        requested_urls = []

        async def get_json(url, params=None):
            requested_urls.append(url)
            if "gateio" in url:
                return []
            raise AssertionError(f"unexpected market provider request: {url}")

        external_market_context._CACHE.clear()
        with patch.dict(
            os.environ,
            {"APEX_MARKET_DATA_PROVIDERS": "gate,binance"},
            clear=True,
        ), patch.object(external_market_context, "_get_json", new=get_json):
            context = await external_market_context.collect_external_market_context(
                "BTCUSDT"
            )

        self.assertEqual(context["available_sources"], ["gateio"])
        self.assertTrue(requested_urls)
        self.assertFalse(any("binance.com" in url for url in requested_urls))


if __name__ == "__main__":
    unittest.main()
