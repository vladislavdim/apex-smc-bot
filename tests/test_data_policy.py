import unittest
from core.data_policy import configured_market_data_providers, provider_enabled


class DataPolicyTests(unittest.TestCase):
    def test_default_policy_is_gate_only(self):
        self.assertEqual(configured_market_data_providers({}), ("gate",))
        self.assertTrue(provider_enabled("gate", {}))
        self.assertFalse(provider_enabled("binance", {}))
        self.assertFalse(provider_enabled("bybit", {}))
        self.assertFalse(provider_enabled("hyperliquid", {}))

    def test_explicit_diagnostic_providers_are_opt_in(self):
        env = {"APEX_MARKET_DATA_PROVIDERS": "gate,binance,bybit,hyperliquid"}
        self.assertEqual(
            configured_market_data_providers(env),
            ("gate", "binance", "bybit", "hyperliquid"),
        )


if __name__ == "__main__":
    unittest.main()
