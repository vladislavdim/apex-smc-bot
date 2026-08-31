import unittest
from unittest.mock import patch

import brain_router


class BrainRouterGateTests(unittest.TestCase):
    def test_monthly_candles_use_30_day_api_interval(self):
        class Response:
            @staticmethod
            def raise_for_status():
                return None

            @staticmethod
            def json():
                candle = {"o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"}
                return [candle.copy() for _ in range(5)]

        with patch("brain_router.requests.get", return_value=Response()) as request:
            brain_router._fetch_gateio("BTCUSDT", "1M", 5)

        self.assertEqual(request.call_args.kwargs["params"]["interval"], "30d")


if __name__ == "__main__":
    unittest.main()
