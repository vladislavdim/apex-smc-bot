import unittest
from unittest.mock import Mock, patch

from apex.market import liquidation_context


class LiquidationContextTests(unittest.TestCase):
    def _response(self, long_size, short_size):
        response = Mock(status_code=200)
        response.json.return_value = [{
            "long_liq_size": long_size,
            "short_liq_size": short_size,
        }]
        return response

    def test_long_liquidation_dominance_is_bearish(self):
        response = self._response("30", "10")
        with patch.object(
            liquidation_context, "get_pair",
            return_value={"gate_symbol": "BTC_USDT"},
        ), patch.object(
            liquidation_context, "_http_get", return_value=response,
        ) as request:
            result = liquidation_context.get_liquidation_ratio("BTCUSDT")
        self.assertEqual(result["ratio"], 3.0)
        self.assertEqual(result["signal"], "BEARISH")
        self.assertEqual(result["long_pct"], 0.75)
        request.assert_called_once_with(
            "https://api.gateio.ws/api/v4/futures/usdt/contract_stats",
            params={"contract": "BTC_USDT", "interval": "1h", "limit": 3},
            headers={"User-Agent": "APEX-SMC/1.0"},
            timeout=8,
        )

    def test_short_liquidation_dominance_is_bullish(self):
        with patch.object(
            liquidation_context, "get_pair", return_value={},
        ), patch.object(
            liquidation_context, "_http_get",
            return_value=self._response(1, 4),
        ):
            result = liquidation_context.get_liquidation_ratio("ETHUSDT")
        self.assertEqual(result["signal"], "BULLISH")
        self.assertEqual(result["desc"], "Gate short liquidations dominate (4.00x)")

    def test_zero_liquidations_preserve_neutral_ratio(self):
        with patch.object(
            liquidation_context, "get_pair", return_value={},
        ), patch.object(
            liquidation_context, "_http_get",
            return_value=self._response(0, 0),
        ):
            result = liquidation_context.get_liquidation_ratio("SOLUSDT")
        self.assertEqual(result["ratio"], 1.0)
        self.assertEqual(result["signal"], "NEUTRAL")
        self.assertEqual((result["long_pct"], result["short_pct"]), (0.5, 0.5))

    def test_provider_failure_is_fail_safe(self):
        with patch.object(
            liquidation_context, "get_pair", side_effect=RuntimeError("offline"),
        ):
            self.assertEqual(
                liquidation_context.get_liquidation_ratio("AAVEUSDT"),
                {"ratio": 1.0, "signal": "NEUTRAL", "desc": "", "ok": False},
            )


if __name__ == "__main__":
    unittest.main()
