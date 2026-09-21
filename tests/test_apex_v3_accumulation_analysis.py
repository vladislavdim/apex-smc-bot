import unittest
from unittest.mock import Mock

from apex.market.accumulation_analysis import AccumulationAnalysis


def _candles(count):
    return [
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.1, "volume": 10.0}
        for _ in range(count)
    ]


class AccumulationAnalysisTests(unittest.TestCase):
    def test_insufficient_history_fails_closed_before_context_calls(self):
        get_candles = Mock(side_effect=[_candles(23), _candles(96)])
        orderbook = Mock()
        groq = Mock()
        self.assertIsNone(AccumulationAnalysis(get_candles, orderbook, groq).detect("BTCUSDT"))
        orderbook.assert_not_called()
        groq.assert_not_called()

    def test_high_score_preserves_advisory_contract_and_groq_target(self):
        get_candles = Mock(side_effect=[_candles(48), _candles(96)])
        orderbook = Mock(return_value={"bids": 2.0, "asks": 1.0})
        groq = Mock(return_value='{"target": 112, "target_pct": 11.9, "logic": "range expansion"}')
        result = AccumulationAnalysis(get_candles, orderbook, groq).detect("ETHUSDT")
        self.assertEqual(result["score"], 80)
        self.assertEqual(result["pump_target"], 112.0)
        self.assertEqual(result["pump_target_pct"], 11.9)
        self.assertEqual(result["pump_logic"], "range expansion")
        self.assertEqual(result["price"], 100.1)
        orderbook.assert_called_once_with("ETHUSDT")
        groq.assert_called_once()
        self.assertEqual(groq.call_args.kwargs, {"max_tokens": 100})

    def test_missing_orderbook_applies_penalty_and_rejects(self):
        get_candles = Mock(side_effect=[_candles(48), _candles(96)])
        groq = Mock()
        result = AccumulationAnalysis(get_candles, Mock(return_value=None), groq).detect("AAVEUSDT")
        self.assertIsNone(result)
        groq.assert_not_called()


if __name__ == "__main__":
    unittest.main()
