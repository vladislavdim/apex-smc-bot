import unittest
from unittest.mock import patch

from apex.market import structure_bridge


class StructureBridgeTests(unittest.TestCase):
    def test_equal_levels_preserve_first_match_and_tolerance(self):
        candles = [
            {"high": 100.0, "low": 90.0},
            {"high": 100.19, "low": 85.0},
            {"high": 110.0, "low": 90.17},
        ]
        self.assertEqual(
            structure_bridge.find_equal_highs_lows(candles),
            (100.095, 90.08500000000001),
        )

    def test_equal_levels_use_only_requested_tail(self):
        candles = [
            {"high": 100.0, "low": 90.0},
            {"high": 100.1, "low": 90.1},
            {"high": 120.0, "low": 70.0},
            {"high": 130.0, "low": 60.0},
        ]
        self.assertEqual(
            structure_bridge.find_equal_highs_lows(candles, lookback=2),
            (None, None),
        )

    def test_bos_choch_boolean_delegates_to_event_contract(self):
        with patch.object(
            structure_bridge,
            "get_bos_choch_event",
            side_effect=[{"type": "BOS"}, None],
        ) as event:
            self.assertTrue(structure_bridge.detect_bos_choch([{}], "BULLISH", 7))
            self.assertFalse(structure_bridge.detect_bos_choch([{}], "BEARISH", 8))
        self.assertEqual(event.call_args_list[0].args, ([{}], "BULLISH"))
        self.assertEqual(event.call_args_list[0].kwargs, {"lookback": 7})
        self.assertEqual(event.call_args_list[1].kwargs, {"lookback": 8})

    def test_bos_choch_rejects_invalid_or_insufficient_input(self):
        with patch.object(structure_bridge, "_analyze_market_structure") as analyze:
            self.assertIsNone(structure_bridge.get_bos_choch_event([{}] * 20, "SIDEWAYS"))
            self.assertIsNone(structure_bridge.get_bos_choch_event([{}] * 17, "BULLISH"))
        analyze.assert_not_called()

    def test_bos_choch_returns_only_matching_closed_structural_event(self):
        event = {"type": "CHoCH", "direction": "BULLISH", "closed": True}
        with patch.object(
            structure_bridge,
            "_analyze_market_structure",
            return_value={"event": event},
        ) as analyze:
            self.assertIs(
                structure_bridge.get_bos_choch_event(
                    [{}] * 20, "BULLISH", lookback=15, max_break_age=4
                ),
                event,
            )
        analyze.assert_called_once_with([{}] * 20, swing_lookback=2, max_break_age=4)

    def test_bos_choch_uses_wider_pivots_for_long_samples(self):
        event = {"type": "BOS", "direction": "BEARISH", "closed": True}
        candles = [{}] * 50
        with patch.object(
            structure_bridge,
            "_analyze_market_structure",
            return_value={"event": event},
        ) as analyze:
            self.assertIs(
                structure_bridge.get_bos_choch_event(candles, "BEARISH"),
                event,
            )
        analyze.assert_called_once_with(candles, swing_lookback=3, max_break_age=1)

    def test_bos_choch_rejects_wrong_direction_open_or_unknown_event(self):
        invalid_events = (
            {"type": "BOS", "direction": "BEARISH", "closed": True},
            {"type": "BOS", "direction": "BULLISH", "closed": False},
            {"type": "SWING", "direction": "BULLISH", "closed": True},
        )
        for event in invalid_events:
            with self.subTest(event=event), patch.object(
                structure_bridge,
                "_analyze_market_structure",
                return_value={"event": event},
            ):
                self.assertIsNone(
                    structure_bridge.get_bos_choch_event([{}] * 20, "BULLISH")
                )

    def test_find_swings_delegates_with_exact_lookback(self):
        candles = [{"high": 2, "low": 1}]
        with patch.object(structure_bridge, "_find_swings", return_value=([], [])) as find:
            self.assertEqual(structure_bridge.find_swings(candles, lookback=7), ([], []))
        find.assert_called_once_with(candles, lookback=7)

    def test_classification_uses_canonical_engine(self):
        with patch.object(structure_bridge, "_classify_swings", return_value=[{"kind": "H"}]) as classify:
            result = structure_bridge.classify_swings([(1, 2.0)], [(2, 1.0)])
        self.assertEqual(result, [{"kind": "H"}])
        classify.assert_called_once_with([(1, 2.0)], [(2, 1.0)])

    def test_events_are_bounded_to_latest_break(self):
        candles = [{"close": 1.0}]
        classified = [{"kind": "H"}]
        with patch.object(structure_bridge, "_events_with_trend_fallback", return_value=[]) as events:
            self.assertEqual(structure_bridge.detect_events(candles, classified), [])
        events.assert_called_once_with(candles, classified, max_break_age=1)

    def test_empty_structure_does_not_call_engine(self):
        with patch.object(structure_bridge, "_events_with_trend_fallback") as events:
            self.assertEqual(structure_bridge.detect_events([], []), [])
        events.assert_not_called()


if __name__ == "__main__":
    unittest.main()
