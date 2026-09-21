"""Optional, bounded positioning context. Never becomes a strategy hard gate.

Instrument IDs must be explicitly mapped from Coinalyze's future-markets
catalog: a ticker alone does not identify exchange, multiplier or contract.
"""
import asyncio
import json
import time
from apex.config.settings import ApexConfig
from .cache import cache
from .http_client import http_client

SOURCE = "coinalyze"


async def collect(symbol):
    settings = ApexConfig.from_env().integrations
    key = settings.coinalyze_api_key
    if not key:
        return {"source": SOURCE, "status": "not_configured", "mode": "LIVE_CONTEXT"}
    try:
        mapping = json.loads(settings.coinalyze_symbol_map_json)
        if not isinstance(mapping, dict) or not 1 <= len(mapping) <= 10:
            raise ValueError("maximum_ten_explicit_instruments")
        if any(not isinstance(k, str) or not isinstance(v, str) or not v or ',' in v for k,v in mapping.items()):
            raise ValueError("invalid_instrument_map")
        if symbol not in mapping:
            return {"source": SOURCE, "status": "outside_live_context_universe", "mode": "LIVE_CONTEXT"}
        instruments = sorted(set(mapping.values()))
        async def fetch():
            # Sequential endpoints keep the batch under the 24-unit APEX minute cap.
            result = {}
            for endpoint in ('open-interest', 'funding-rate'):
                params = {'symbols': ','.join(instruments)}
                if endpoint == 'open-interest': params['convert_to_usd'] = 'true'
                rows = await http_client.get_json('https://api.coinalyze.net/v1/'+endpoint, params, {'api_key': key})
                if not isinstance(rows, list): raise ValueError('invalid_response')
                result[endpoint] = rows
            return result
        payload, status, age = await cache.get_or_fetch(SOURCE+':'+','.join(instruments), 900, 1800, fetch)
        rows = {}
        now = time.time()
        for endpoint, values in payload.items():
            row = next((x for x in values if isinstance(x, dict) and x.get('symbol') == mapping[symbol]), None)
            if row is None: raise ValueError('missing_instrument')
            event_time = float(row['update'])
            value = float(row['value'])
            import math
            if not math.isfinite(value) or not math.isfinite(event_time) or not 0 <= now-event_time <= 1800:
                raise ValueError('stale_or_invalid_source_data')
            rows[endpoint] = {'value': value, 'event_time': event_time}
        return {'source': SOURCE, 'symbol': symbol, 'instrument': mapping[symbol],
                'status': status, 'age_seconds': age, 'mode': 'LIVE_CONTEXT', 'normalized': rows,
                'provenance': 'Coinalyze exchange-specific contract; not institutional intent'}
    except Exception as exc:
        return {'source': SOURCE, 'status': 'unavailable', 'mode': 'LIVE_CONTEXT', 'error': type(exc).__name__}
