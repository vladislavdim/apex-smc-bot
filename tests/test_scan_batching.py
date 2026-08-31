import unittest

from core.scan_batching import RotatingBatcher


class ScanBatchingTests(unittest.TestCase):
    def test_rotating_batches_cover_the_universe_without_overlap(self):
        batcher = RotatingBatcher()
        universe = [f"PAIR{i}" for i in range(120)]

        first = batcher.take("swing", universe, 60)
        second = batcher.take("swing", universe, 60)

        self.assertEqual(len(first), 60)
        self.assertEqual(len(second), 60)
        self.assertFalse(set(first) & set(second))
        self.assertEqual(set(first + second), set(universe))

    def test_batch_wraps_and_never_duplicates_one_cycle(self):
        batcher = RotatingBatcher()
        universe = list(range(5))

        self.assertEqual(batcher.take("zone", universe, 3), [0, 1, 2])
        self.assertEqual(batcher.take("zone", universe, 3), [3, 4, 0])
        self.assertEqual(batcher.take("zone", universe, 3), [1, 2, 3])

    def test_oversized_batch_returns_each_item_once(self):
        self.assertEqual(RotatingBatcher().take("all", [1, 2, 3], 10), [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
