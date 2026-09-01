import unittest
from core.data_policy import configured_market_data_providers, provider_enabled


class DataPolicyTests(unittest.TestCase):
    def test_default_policy_is_gate_only(self):
        self.assertEqual(configured_market_data_providers({}), ("gate",))
        self.assertTrue(provider_enabled("gate", {}))
        self.assertFalse(provider_enabled("binance", {}))
        self.assertFalse(provider_enabled("bybit", {}))
        self.assertFalse(provider_enabled("hyperliquid", {}))

    def test_other_exchanges_cannot_be_enabled_as_market_data_providers(self):
        env = {"APEX_MARKET_DATA_PROVIDERS": "gate,binance,bybit,hyperliquid"}
        self.assertEqual(
            configured_market_data_providers(env),
            ("gate",),
        )
        self.assertFalse(provider_enabled("binance", env))
        self.assertFalse(provider_enabled("bybit", env))
        self.assertFalse(provider_enabled("hyperliquid", env))


if __name__ == "__main__":
    unittest.main()
