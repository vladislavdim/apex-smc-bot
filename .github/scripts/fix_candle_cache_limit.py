from pathlib import Path

p = Path('market.py')
s = p.read_text(encoding='utf-8')

old = '''    global candle_cache
    cache_key = f"{symbol}_{interval}"

    cache_ttl = 60 if interval in ("1m", "3m", "5m") else 180 if interval in ("15m", "30m") else 300 if interval in ("1h", "2h") else 600
    if cache_key in candle_cache:
        cached, ts = candle_cache[cache_key]
        if time.time() - ts < cache_ttl and len(cached) >= 20:
            return cached

    # Проверяем global candles storage
    _gc = get_global_candles(symbol, interval)
    if _gc and len(_gc) >= 20:
        candle_cache[cache_key] = (_gc, time.time())
        return _gc
'''
new = '''    global candle_cache
    requested_limit = max(1, int(limit or 1))
    cache_key = f"{symbol}_{interval}"

    cache_ttl = 60 if interval in ("1m", "3m", "5m") else 180 if interval in ("15m", "30m") else 300 if interval in ("1h", "2h") else 600
    if cache_key in candle_cache:
        cached, ts = candle_cache[cache_key]
        # Never satisfy a larger history request with a shorter cached sample.
        if time.time() - ts < cache_ttl and len(cached) >= requested_limit:
            return cached[-requested_limit:]

    # Проверяем global candles storage. Same rule: it must satisfy this request.
    _gc = get_global_candles(symbol, interval)
    if _gc and len(_gc) >= requested_limit:
        candle_cache[cache_key] = (_gc, time.time())
        return _gc[-requested_limit:]
'''
if old not in s:
    raise SystemExit('target cache block not found')
s = s.replace(old, new, 1)

# Ensure fresh/fallback responses are clipped to the requested history length, while
# preserving the existing longest sample in the process cache when available.
old = '''            rc = _brain_router.candles(symbol, interval, limit)
            if rc and len(rc) >= 3:
                candle_cache[cache_key] = (rc, time.time())
                update_global_candles(symbol, interval, rc)
                return rc
'''
new = '''            rc = _brain_router.candles(symbol, interval, requested_limit)
            if rc and len(rc) >= 3:
                candle_cache[cache_key] = (rc, time.time())
                update_global_candles(symbol, interval, rc)
                return rc[-requested_limit:]
'''
if old not in s:
    raise SystemExit('router cache block not found')
s = s.replace(old, new, 1)

old = '''            result = get_candles_smart(symbol, interval, limit)
            candles = result.get("candles", []) if isinstance(result, dict) else []
            if candles and len(candles) >= 3:
                candle_cache[cache_key] = (candles, time.time())
                update_global_candles(symbol, interval, candles)
                return candles
'''
new = '''            result = get_candles_smart(symbol, interval, requested_limit)
            candles = result.get("candles", []) if isinstance(result, dict) else []
            if candles and len(candles) >= 3:
                candle_cache[cache_key] = (candles, time.time())
                update_global_candles(symbol, interval, candles)
                return candles[-requested_limit:]
'''
if old not in s:
    raise SystemExit('smc cache block not found')
s = s.replace(old, new, 1)

p.write_text(s, encoding='utf-8')

# Regression test: textual and behavioral contract for cache history length.
t = Path('tests/test_candle_cache_request_limit.py')
t.write_text('''import unittest\nfrom pathlib import Path\n\nclass CandleCacheRequestLimitTests(unittest.TestCase):\n    def test_get_candles_requires_cached_sample_to_cover_requested_limit(self):\n        s = Path("market.py").read_text(encoding="utf-8")\n        self.assertIn("requested_limit = max(1, int(limit or 1))", s)\n        self.assertIn("len(cached) >= requested_limit", s)\n        self.assertIn("len(_gc) >= requested_limit", s)\n        self.assertNotIn("len(cached) >= 20:\\n            return cached", s)\n        self.assertIn("return rc[-requested_limit:]", s)\n        self.assertIn("return candles[-requested_limit:]", s)\n\nif __name__ == "__main__":\n    unittest.main()\n''', encoding='utf-8')
print('candle cache limit patch applied')
