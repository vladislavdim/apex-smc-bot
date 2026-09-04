import unittest
from pathlib import Path
from unittest.mock import patch

import stats_server


class StrategyFunnelCleanupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.market = Path('market.py').read_text(encoding='utf-8')
        cls.setup = Path('core/setup_evidence.py').read_text(encoding='utf-8')
        cls.audit = Path('core/setup_audit.py').read_text(encoding='utf-8')
        cls.catalog = Path('core/strategy_catalog.py').read_text(encoding='utf-8')
        cls.stats = Path('stats_server.py').read_text(encoding='utf-8')

    def test_swing_4h_structure_is_context_but_1h_is_mandatory(self):
        self.assertIn('4h BOS/CHoCH context after trigger (non-blocking)', self.market)
        self.assertIn('LTF: fresh 1h BOS/CHoCH', self.market)
        self.assertIn('fresh 1h BOS/CHoCH trigger', self.setup)
        self.assertNotIn('4h plus fresh 1h BOS/CHoCH trigger', self.setup)
        self.assertIn('4H BOS/CHoCH after trigger strengthens thesis', self.catalog)

    def test_fast_preliminary_volume_is_context_and_trigger_stays_16x(self):
        self.assertIn('15m preliminary impulse volume >= 1.1x average (non-blocking)', self.market)
        self.assertIn('_vol_threshold = 1.6', self.market)
        self.assertNotIn("return _audit_fail('FAST_DETECT_FAST_DEAL_R9250'", self.market)
        self.assertIn('Telemetry/context only; executable trigger still requires 1.6× volume.', self.catalog)

    def test_wyckoff_distribution_range_has_symmetric_telemetry(self):
        self.assertIn("'WYCKOFF_DIST_RANGE'", self.market)
        self.assertIn('dist_range_pct >= 25', self.market)
        self.assertIn('wyckoff_dist_range', self.stats)

    def test_release_sha_is_attached_to_telemetry(self):
        self.assertIn('RENDER_GIT_COMMIT', self.audit)
        self.assertIn('payload_data.setdefault("release_sha", RELEASE_SHA)', self.audit)
        self.assertIn('После последнего deploy', self.stats)

    def test_latest_release_filter_and_funnel_are_cohort_safe(self):
        events = [
            {
                'event_key': 'new1', 'kind': 'attempt', 'strategy': 'SWING', 'symbol': 'BTCUSDT',
                'occurred_at': '2026-09-04T19:00:00+00:00',
                'payload': {
                    'attempt_key': 'new1', 'release_sha': 'newsha', 'strategy': 'SWING', 'symbol': 'BTCUSDT',
                    'outcome': 'FILTERED', 'checks': [
                        {'label': 'direction', 'state': 'PASS'},
                        {'label': 'fresh 1h BOS/CHoCH', 'state': 'FAIL'},
                    ],
                    'stop': {'label': 'fresh 1h BOS/CHoCH', 'snapshot': {}},
                },
            },
            {
                'event_key': 'old1', 'kind': 'attempt', 'strategy': 'SWING', 'symbol': 'ETHUSDT',
                'occurred_at': '2026-09-04T18:00:00+00:00',
                'payload': {
                    'attempt_key': 'old1', 'release_sha': 'oldsha', 'strategy': 'SWING', 'symbol': 'ETHUSDT',
                    'outcome': 'CANDIDATE', 'checks': [{'label': 'direction', 'state': 'PASS'}], 'candidate': {'rr': 2.5},
                },
            },
        ]
        with patch.object(stats_server, '_fetch', return_value=events):
            data = stats_server.build_dashboard(days=1, release='latest')
        self.assertEqual(data['release_sha'], 'newsha')
        self.assertEqual(data['summary']['attempts'], 1)
        self.assertEqual(data['summary']['candidates'], 0)
        self.assertEqual(data['funnels'][0]['strategy'], 'SWING')
        self.assertEqual(data['funnels'][0]['attempts'], 1)

    def test_wyckoff_range_percentiles_use_observed_values(self):
        events = []
        for i, value in enumerate([20.0, 25.0, 30.0, 40.0]):
            events.append({
                'event_key': f'w{i}', 'kind': 'attempt', 'strategy': 'WYCKOFF', 'symbol': f'X{i}USDT',
                'occurred_at': f'2026-09-04T19:0{i}:00+00:00',
                'payload': {
                    'attempt_key': f'w{i}', 'release_sha': 'sha', 'strategy': 'WYCKOFF', 'subtype': 'DISTRIBUTION',
                    'symbol': f'X{i}USDT', 'outcome': 'FILTERED', 'checks': [],
                    'stop': {'label': 'range', 'snapshot': {'dist_range_pct': value}},
                },
            })
        with patch.object(stats_server, '_fetch', return_value=events):
            data = stats_server.build_dashboard(days=1, release='latest')
        metric = data['wyckoff_dist_range']
        self.assertEqual(metric['count'], 4)
        self.assertEqual(metric['median'], 27.5)
        self.assertEqual(metric['p75'], 32.5)


if __name__ == '__main__':
    unittest.main()
