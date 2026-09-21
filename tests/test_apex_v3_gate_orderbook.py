import unittest
from unittest.mock import Mock, patch

from apex.market import gate_orderbook


class GateOrderbookProviderTests(unittest.TestCase):
    def test_registered_contract_and_multiplier_are_preserved(self):
        response = Mock()
        response.json.return_value = {
            "bids": [{"p": "10", "s": "-3"}],
            "asks": [{"p": "12", "s": "1"}],
        }
        with patch(
            "external_sources.pair_registry.get_pair",
            return_value={"gate_symbol": "1000PEPE_USDT", "gate_multiplier": 1000},
        ), patch.object(gate_orderbook, "_request_get", return_value=response) as get:
            result = gate_orderbook.get_orderbook("PEPEUSDT")

        self.assertEqual(result, {"bids": 30000.0, "asks": 12000.0, "bias": "BUY"})
        response.raise_for_status.assert_called_once_with()
        self.assertEqual(get.call_args.kwargs["params"], {
            "contract": "1000PEPE_USDT", "limit": 20,
        })

    def test_failure_remains_unknown_instead_of_zero(self):
        with patch(
            "external_sources.pair_registry.get_pair",
            side_effect=RuntimeError("registry unavailable"),
        ):
            self.assertIsNone(gate_orderbook.get_orderbook("BTCUSDT"))


if __name__ == "__main__":
    unittest.main()
