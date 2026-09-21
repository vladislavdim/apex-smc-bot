"""Shared immutable market UI and strategy constants."""

TF_CATEGORIES = {
    "scalp": ["1m", "5m", "15m"],
    "swing": ["1h", "4h"],
    "long": ["1d", "1w", "1M"],
}

TF_LABELS = {
    "1m": "1 мин", "3m": "3 мин", "5m": "5 мин", "15m": "15 мин",
    "30m": "30 мин", "1h": "1 час", "2h": "2 часа", "4h": "4 часа",
    "1d": "1 день", "3d": "3 дня", "1w": "1 неделя", "1M": "1 месяц",
}

FAST_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT", "DOTUSDT",
    "MATICUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
    "SUIUSDT", "INJUSDT", "FETUSDT", "WIFUSDT", "PEPEUSDT",
]

COINGECKO_IDS = {
    "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin", "XRPUSDT": "ripple", "DOGEUSDT": "dogecoin",
    "AVAXUSDT": "avalanche-2", "LINKUSDT": "chainlink", "TONUSDT": "toncoin",
    "ARBUSDT": "arbitrum", "SUIUSDT": "sui", "NEARUSDT": "near",
    "INJUSDT": "injective-protocol", "APTUSDT": "aptos",
    "DOTUSDT": "polkadot", "ADAUSDT": "cardano", "MATICUSDT": "matic-network",
    "LTCUSDT": "litecoin", "ATOMUSDT": "cosmos", "UNIUSDT": "uniswap",
    "OPUSDT": "optimism", "STXUSDT": "blockstack",
    "RENDERUSDT": "render-token", "FETUSDT": "fetch-ai", "WIFUSDT": "dogwifcoin",
    "PEPEUSDT": "pepe", "SHIBUSDT": "shiba-inu", "TRXUSDT": "tron",
    "XLMUSDT": "stellar", "HBARUSDT": "hedera-hashgraph",
}

SYMBOL_ALIASES = {
    "btc": "BTCUSDT", "биткоин": "BTCUSDT", "бтк": "BTCUSDT",
    "bitcoin": "BTCUSDT", "биток": "BTCUSDT", "битка": "BTCUSDT",
    "бит": "BTCUSDT", "eth": "ETHUSDT", "эфир": "ETHUSDT",
    "эфириум": "ETHUSDT", "ethereum": "ETHUSDT", "эф": "ETHUSDT",
    "ефир": "ETHUSDT", "sol": "SOLUSDT", "соль": "SOLUSDT",
    "солана": "SOLUSDT", "solana": "SOLUSDT", "сол": "SOLUSDT",
    "bnb": "BNBUSDT", "бнб": "BNBUSDT", "бинанс коин": "BNBUSDT",
    "xrp": "XRPUSDT", "рипл": "XRPUSDT", "ripple": "XRPUSDT",
    "хрп": "XRPUSDT", "xrpusdt": "XRPUSDT", "doge": "DOGEUSDT",
    "додж": "DOGEUSDT", "dogecoin": "DOGEUSDT", "доге": "DOGEUSDT",
    "avax": "AVAXUSDT", "авакс": "AVAXUSDT", "avalanche": "AVAXUSDT",
    "link": "LINKUSDT", "линк": "LINKUSDT", "chainlink": "LINKUSDT",
    "ton": "TONUSDT", "тон": "TONUSDT", "toncoin": "TONUSDT",
    "тонкоин": "TONUSDT", "arb": "ARBUSDT", "арб": "ARBUSDT",
    "arbitrum": "ARBUSDT", "sui": "SUIUSDT", "суи": "SUIUSDT",
    "dot": "DOTUSDT", "полкадот": "DOTUSDT", "polkadot": "DOTUSDT",
    "ada": "ADAUSDT", "кардано": "ADAUSDT", "cardano": "ADAUSDT",
    "matic": "MATICUSDT", "матик": "MATICUSDT", "polygon": "MATICUSDT",
    "ltc": "LTCUSDT", "лайткоин": "LTCUSDT", "litecoin": "LTCUSDT",
    "atom": "ATOMUSDT", "космос": "ATOMUSDT", "cosmos": "ATOMUSDT",
    "near": "NEARUSDT", "ниар": "NEARUSDT", "pepe": "PEPEUSDT",
    "пепе": "PEPEUSDT", "shib": "SHIBUSDT", "шиб": "SHIBUSDT",
    "shiba": "SHIBUSDT", "trx": "TRXUSDT", "трон": "TRXUSDT",
    "tron": "TRXUSDT", "wif": "WIFUSDT", "render": "RENDERUSDT",
    "рендер": "RENDERUSDT", "fet": "FETUSDT", "fetch": "FETUSDT",
    "inj": "INJUSDT", "injective": "INJUSDT", "apt": "APTUSDT",
    "aptos": "APTUSDT", "op": "OPUSDT", "optimism": "OPUSDT",
    "uni": "UNIUSDT", "uniswap": "UNIUSDT", "юни": "UNIUSDT",
    "stx": "STXUSDT", "stacks": "STXUSDT", "hbar": "HBARUSDT",
    "hedera": "HBARUSDT", "xlm": "XLMUSDT", "stellar": "XLMUSDT",
    "стеллар": "XLMUSDT", "ldo": "LDOUSDT", "lido": "LDOUSDT",
    "aave": "AAVEUSDT", "аав": "AAVEUSDT", "mkr": "MKRUSDT",
    "maker": "MKRUSDT", "crv": "CRVUSDT", "curve": "CRVUSDT",
    "floki": "FLOKIUSDT", "флоки": "FLOKIUSDT", "bonk": "BONKUSDT",
    "бонк": "BONKUSDT", "jup": "JUPUSDT", "jupiter": "JUPUSDT",
    "sei": "SEIUSDT", "tia": "TIAUSDT", "celestia": "TIAUSDT",
    "pyth": "PYTHUSDT", "wld": "WLDUSDT", "worldcoin": "WLDUSDT",
}

__all__ = [
    "COINGECKO_IDS", "FAST_PAIRS", "SYMBOL_ALIASES", "TF_CATEGORIES", "TF_LABELS",
]
