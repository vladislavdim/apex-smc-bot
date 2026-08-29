import unittest
from datetime import datetime, timezone

from core.session_clock import fast_session


class SessionClockTests(unittest.TestCase):
    def test_london_window_tracks_summer_and_winter_time(self):
        self.assertEqual(fast_session(datetime(2026, 7, 1, 7, 15, tzinfo=timezone.utc)), "LONDON")
        self.assertEqual(fast_session(datetime(2026, 1, 7, 8, 15, tzinfo=timezone.utc)), "LONDON")

    def test_new_york_window_tracks_summer_and_winter_time(self):
        self.assertEqual(fast_session(datetime(2026, 7, 1, 13, 45, tzinfo=timezone.utc)), "NEW_YORK")
        self.assertEqual(fast_session(datetime(2026, 1, 7, 14, 45, tzinfo=timezone.utc)), "NEW_YORK")

    def test_dead_hours_have_no_fast_session(self):
        self.assertIsNone(fast_session(datetime(2026, 7, 1, 2, 0, tzinfo=timezone.utc)))


if __name__ == "__main__":
    unittest.main()
