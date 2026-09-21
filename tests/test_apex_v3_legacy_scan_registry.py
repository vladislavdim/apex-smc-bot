import unittest

from apex.strategies.legacy_scan_registry import (
    analyze_trade_type,
    full_scan,
    raw_scan_handler_registered,
    register_raw_scan_handler,
    run_raw_scan,
)


class LegacyScanRegistryTests(unittest.TestCase):
    def tearDown(self):
        register_raw_scan_handler(None)

    def test_missing_handler_fails_closed(self):
        register_raw_scan_handler(None)
        self.assertFalse(raw_scan_handler_registered())
        self.assertIsNone(run_raw_scan("BTCUSDT", "1h"))

    def test_manual_and_automatic_paths_share_exact_handler(self):
        calls = []

        def scanner(symbol, timeframe, passive_watch):
            calls.append((symbol, timeframe, passive_watch))
            return {"symbol": symbol, "timeframe": timeframe}

        register_raw_scan_handler(scanner)

        self.assertTrue(raw_scan_handler_registered())
        self.assertEqual(
            run_raw_scan("ETHUSDT", "4h", True),
            {"symbol": "ETHUSDT", "timeframe": "4h"},
        )
        self.assertEqual(calls, [("ETHUSDT", "4h", True)])

    def test_legacy_facades_delegate_without_fabricated_geometry(self):
        calls = []
        register_raw_scan_handler(
            lambda *args: calls.append(args) or {"entry": 100, "sl": 95}
        )

        self.assertEqual(analyze_trade_type("BTCUSDT", "scalp")["entry"], 100)
        self.assertEqual(full_scan("ETHUSDT", "4h")["sl"], 95)
        self.assertEqual(calls, [
            ("BTCUSDT", "15m", False),
            ("ETHUSDT", "4h", False),
        ])


if __name__ == "__main__":
    unittest.main()
