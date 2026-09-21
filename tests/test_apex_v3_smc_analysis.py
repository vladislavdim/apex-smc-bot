import unittest
from unittest.mock import Mock

from apex.market.smc_analysis import LegacySmcAnalysis


def _analysis(*, engine_available=False, smc_tf=None, smart_multi_tf=None):
    return LegacySmcAnalysis(
        engine_available=lambda: engine_available,
        smart_multi_tf=smart_multi_tf or Mock(),
        get_candles=Mock(return_value=[{"close": 1}] * 20),
        get_confirmed_candles=lambda candles: candles,
        find_swings=Mock(return_value=([], [])),
        classify_swings=Mock(return_value={"trend": "BULLISH"}),
        detect_events=Mock(return_value=[{"direction": "BULLISH"}]),
        timeframe_labels={"1h": "1 час", "4h": "4 часа"},
        smc_tf=smc_tf or Mock(),
    )


class LegacySmcAnalysisTests(unittest.TestCase):
    def test_canonical_engine_has_priority(self):
        smc_tf = Mock(return_value={"direction": "BEARISH"})
        analysis = _analysis(engine_available=True, smc_tf=smc_tf)
        self.assertEqual(analysis.smc_on_tf("BTCUSDT", "1h"), "BEARISH")
        smc_tf.assert_called_once_with("BTCUSDT", "1h")

    def test_failed_engine_uses_confirmed_structure_fallback(self):
        analysis = _analysis(
            engine_available=True,
            smc_tf=Mock(side_effect=RuntimeError("unavailable")),
        )
        self.assertEqual(analysis.smc_on_tf("BTCUSDT", "1h"), "BULLISH")

    def test_multi_tf_delegates_when_engine_is_available(self):
        expected = {"direction": "BULLISH"}
        smart = Mock(return_value=expected)
        analysis = _analysis(engine_available=True, smart_multi_tf=smart)
        self.assertIs(analysis.multi_tf_analysis("BTCUSDT", ["1h"]), expected)
        smart.assert_called_once_with("BTCUSDT", ["1h"])

    def test_fallback_grading_and_labels_are_preserved(self):
        analysis = _analysis(engine_available=False)
        analysis.smc_on_tf = Mock(side_effect=["BULLISH", "BULLISH"])
        result = analysis.multi_tf_analysis("BTCUSDT", ["1h", "4h"])
        self.assertEqual(result["grade"], "ХОРОШАЯ")
        self.assertEqual(result["stars"], "⭐⭐⭐")
        self.assertIn("1 час: BULLISH", result["tf_status"])


if __name__ == "__main__":
    unittest.main()
