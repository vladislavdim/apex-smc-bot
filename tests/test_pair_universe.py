import unittest

from core.pair_universe import FALLBACK_COMMON_PAIRS, select_common_pairs


class PairUniverseTests(unittest.TestCase):
    def test_fallback_has_exactly_120_unique_pairs(self):
        self.assertEqual(len(FALLBACK_COMMON_PAIRS), 120)
        self.assertEqual(len(set(FALLBACK_COMMON_PAIRS)), 120)

    def test_selects_only_liquid_exact_common_perpetuals(self):
        gate = [
            {"contract": "BTC_USDT", "volume_24h_quote": "900000", "highest_bid": "100", "lowest_ask": "100.1"},
            {"contract": "ETH_USDT", "volume_24h_quote": "800000", "highest_bid": "50", "lowest_ask": "50.05"},
            {"contract": "PEPE_USDT", "volume_24h_quote": "999999", "highest_bid": "1", "lowest_ask": "1.001"},
            {"contract": "WIDE_USDT", "volume_24h_quote": "999999", "highest_bid": "1", "lowest_ask": "1.1"},
            {"contract": "LOW_USDT", "volume_24h_quote": "100", "highest_bid": "1", "lowest_ask": "1.001"},
        ]
        binance = {"symbols": [
            {"symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
            {"symbol": "ETHUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
            {"symbol": "1000PEPEUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
            {"symbol": "WIDEUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
            {"symbol": "LOWUSDT", "contractType": "PERPETUAL", "status": "TRADING", "quoteAsset": "USDT", "marginAsset": "USDT"},
        ]}
        self.assertEqual(select_common_pairs(gate, binance, limit=120), ["BTCUSDT", "ETHUSDT"])

    def test_rejects_non_trading_and_non_usdt_contracts(self):
        gate = [{"contract": "BTC_USDT", "volume_24h_quote": "900000", "highest_bid": "100", "lowest_ask": "100.1"}]
        for status, quote in (("BREAK", "USDT"), ("TRADING", "USDC")):
            info = {"symbols": [{
                "symbol": "BTCUSDT", "contractType": "PERPETUAL", "status": status,
                "quoteAsset": quote, "marginAsset": quote,
            }]}
            self.assertEqual(select_common_pairs(gate, info), [])


if __name__ == "__main__":
    unittest.main()
