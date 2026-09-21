from __future__ import annotations

import unittest
from unittest.mock import patch

from apex.ops.resource_guard import memory_snapshot
from apex.ops.watchdog import EventLoopLagMonitor, ProcessCpuMonitor


class MutableClock:
    def __init__(self):
        self.wall = 0.0
        self.cpu = 0.0

    def monotonic(self):
        return self.wall

    def process_time(self):
        return self.cpu


class WatchdogTests(unittest.IsolatedAsyncioTestCase):
    def test_cpu_monitor_uses_process_over_wall_ratio(self):
        clock = MutableClock()
        monitor = ProcessCpuMonitor(monotonic=clock.monotonic, process_time=clock.process_time)
        clock.wall, clock.cpu = 10.0, 9.5
        snapshot = monitor.sample()
        self.assertEqual(snapshot.state, "DEGRADED")
        self.assertEqual(snapshot.ratio, 0.95)

    def test_memory_guard_uses_central_config_thresholds(self):
        with patch("apex.ops.resource_guard._rss_bytes", return_value=70), patch(
            "apex.ops.resource_guard._memory_limit_bytes", return_value=100,
        ):
            self.assertEqual(memory_snapshot(watch_ratio=0.50, degraded_ratio=0.60, stop_ratio=0.80).state, "DEGRADED")
            self.assertEqual(memory_snapshot(watch_ratio=0.75, degraded_ratio=0.80, stop_ratio=0.90).state, "NORMAL")
        with self.assertRaisesRegex(ValueError, "invalid_memory_thresholds"):
            memory_snapshot(watch_ratio=0.8, degraded_ratio=0.7, stop_ratio=0.9)

    def test_memory_guard_accepts_the_typed_config_limit(self):
        with patch("apex.ops.resource_guard._rss_bytes", return_value=70):
            snapshot = memory_snapshot(
                watch_ratio=0.50,
                degraded_ratio=0.60,
                stop_ratio=0.80,
                limit_bytes=100,
            )
        self.assertEqual(snapshot.limit_bytes, 100)
        self.assertEqual(snapshot.state, "DEGRADED")

    async def test_event_loop_lag_requires_sustained_breaches(self):
        clock = MutableClock()

        async def slow_sleep(interval):
            clock.wall += interval + 0.6

        monitor = EventLoopLagMonitor(
            interval_seconds=1, sla_ms=500, window=10,
            monotonic=clock.monotonic, sleeper=slow_sleep,
        )
        first = await monitor.sample()
        await monitor.sample()
        third = await monitor.sample()
        self.assertEqual(first.state, "NORMAL")
        self.assertEqual(third.state, "DEGRADED")
        self.assertEqual(third.breach_count, 3)

    async def test_old_lag_breaches_age_out_of_the_window(self):
        clock = MutableClock()
        extra = [0.6, 0.6, 0.6] + [0.0] * 10

        async def variable_sleep(interval):
            clock.wall += interval + extra.pop(0)

        monitor = EventLoopLagMonitor(
            interval_seconds=1, sla_ms=500, window=10,
            monotonic=clock.monotonic, sleeper=variable_sleep,
        )
        snapshots = [await monitor.sample() for _ in range(13)]
        self.assertEqual(snapshots[2].state, "DEGRADED")
        self.assertEqual(snapshots[-1].breach_count, 0)
        self.assertEqual(snapshots[-1].state, "NORMAL")


if __name__ == "__main__":
    unittest.main()
