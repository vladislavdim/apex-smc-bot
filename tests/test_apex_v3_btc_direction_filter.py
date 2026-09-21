import unittest
from unittest.mock import Mock

from apex.market.btc_direction_filter import BtcDirectionFilter


def _candles(closes):
    return [{"close": float(close)} for close in closes]


class BtcDirectionFilterTests(unittest.TestCase):
    def test_change_uses_average_of_latest_three_transitions(self):
        get_candles = Mock(return_value=_candles([100, 101, 103, 106, 110]))
        result = BtcDirectionFilter(get_candles).one_hour_change()
        expected = round(((103 / 101 - 1) + (106 / 103 - 1) + (110 / 106 - 1)) / 3 * 100, 3)
        self.assertEqual(result, expected)
        get_candles.assert_called_once_with("BTCUSDT", "1h", 5)

    def test_falling_btc_blocks_bullish_signal(self):
        provider = BtcDirectionFilter(Mock(return_value=_candles([104, 103, 102, 101])))
        allowed, reason = provider.allows_signal("BULLISH")
        self.assertFalse(allowed)
        self.assertIn("1h", reason)
        self.assertIn("лонги опасны", reason)

    def test_rising_btc_blocks_bearish_signal_on_four_hours(self):
        get_candles = Mock(return_value=_candles([100, 101, 102, 103]))
        allowed, reason = BtcDirectionFilter(get_candles).allows_signal(
            "BEARISH", use_4h=True,
        )
        self.assertFalse(allowed)
        self.assertIn("4h", reason)
        get_candles.assert_called_once_with("BTCUSDT", "4h", 5)

    def test_missing_data_is_neutral_and_fail_open(self):
        provider = BtcDirectionFilter(Mock(return_value=[]))
        self.assertEqual(provider.one_hour_change(), 0.0)
        self.assertEqual(provider.allows_signal("BULLISH"), (True, ""))


if __name__ == "__main__":
    unittest.main()
