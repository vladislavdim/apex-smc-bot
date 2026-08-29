import unittest
import sys
import types
from unittest.mock import patch

try:
    import requests  # noqa: F401
except ImportError:
    sys.modules["requests"] = types.SimpleNamespace()

from core.smc_engine import smc_tf


class ClosedCandleStructureTests(unittest.TestCase):
    def test_smc_structure_excludes_mutable_exchange_candle(self):
        candles = [
            {"open": i + 1, "high": i + 2, "low": i, "close": i + 1.5, "volume": 10}
            for i in range(20)
        ]
        mutable = {"open": 20, "high": 10_000, "low": 0.1, "close": 9_999, "volume": 999_999}
        response = {
            "candles": candles + [mutable],
            "source": "test", "quality": "high", "is_synthetic": False, "error": "",
        }
        with patch("core.smc_engine.get_candles_smart", return_value=response):
            result = smc_tf("BTCUSDT", "1h")
        self.assertEqual(result["candles_count"], 20)
        self.assertNotIn(mutable, result["candles"])


if __name__ == "__main__":
    unittest.main()
