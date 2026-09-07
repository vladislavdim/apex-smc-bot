import unittest
from unittest.mock import Mock

try:
    from core import coingecko_guard as guard
except Exception as exc:  # local minimal runtime; Render installs requests
    guard = None
    _IMPORT_ERROR = exc


@unittest.skipIf(guard is None, "requests is not installed in this test runtime")
class CoinGeckoGuardTests(unittest.TestCase):
    def setUp(self):
        guard._CACHE.clear()
        guard._KEY_LOCKS.clear()
        self.original = guard._ORIGINAL_GET

    def tearDown(self):
        guard._ORIGINAL_GET = self.original

    def test_identical_requests_are_cached(self):
        response = Mock(status_code=200, content=b"{}")
        calls = []
        guard._ORIGINAL_GET = lambda *args, **kwargs: calls.append((args, kwargs)) or response
        first = guard.guarded_get("https://api.coingecko.com/api/v3/global")
        second = guard.guarded_get("https://api.coingecko.com/api/v3/global")
        self.assertEqual(len(calls), 1)
        self.assertIsNot(first, second)

    def test_different_endpoints_remain_independent(self):
        response = Mock(status_code=200, content=b"{}")
        calls = []
        guard._ORIGINAL_GET = lambda *args, **kwargs: calls.append(args[0]) or response
        guard.guarded_get("https://api.coingecko.com/api/v3/global")
        guard.guarded_get("https://api.coingecko.com/api/v3/search/trending")
        self.assertEqual(len(calls), 2)

    def test_429_uses_last_successful_response(self):
        good = Mock(status_code=200, content=b"good")
        limited = Mock(status_code=429, content=b"limited")
        guard._ORIGINAL_GET = Mock(side_effect=[good, limited])
        url = "https://api.coingecko.com/api/v3/simple/price"
        guard.guarded_get(url, params={"ids": "bitcoin"})
        # Expire the short cache without sleeping.
        key = guard._cache_key(url, {"ids": "bitcoin"})
        guard._CACHE[key] = (0.0, good)
        result = guard.guarded_get(url, params={"ids": "bitcoin"})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.content, b"good")

    def test_non_coingecko_requests_are_untouched(self):
        response = Mock(status_code=200)
        guard._ORIGINAL_GET = Mock(return_value=response)
        result = guard.guarded_get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        self.assertIs(result, response)
        guard._ORIGINAL_GET.assert_called_once()


if __name__ == "__main__":
    unittest.main()
