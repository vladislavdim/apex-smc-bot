"""
APEX Brain Builder — отдельный скрипт самообучения
Groq каждый час сам наполняет brain.db знаниями:
  - SMC паттерны (Order Block, FVG, BOS)
  - Правила торговли по монетам
  - История сделок и результаты
  - Макро тренды (BTC доминанция, DXY)
  - Новости и анализ рынка

Запуск: python brain_builder.py
Или подключается к боту через import и вызов run_brain_builder()
"""

import os
import sqlite3
import requests
import logging
import json
import time
import re
from datetime import datetime
from groq import Groq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

GROQ_KEY = os.environ.get("GROQ_API_KEY", "")
DB_PATH = os.environ.get("BRAIN_DB_PATH", "brain.db")

groq_client = Groq(api_key=GROQ_KEY) if GROQ_KEY else None

# ──────────────────────────────────────────────
# БАЗА ДАННЫХ
# ──────────────────────────────────────────────

def init_brain_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Основное хранилище знаний
    c.execute("""CREATE TABLE IF NOT EXISTS knowledge (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT,
        content TEXT,
        source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # SMC паттерны
    c.execute("""CREATE TABLE IF NOT EXISTS smc_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pattern_type TEXT,
        symbol TEXT,
        timeframe TEXT,
        description TEXT,
        success_rate REAL DEFAULT 0.0,
        examples TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Правила торговли по монетам
    c.execute("""CREATE TABLE IF NOT EXISTS coin_rules (
        symbol TEXT PRIMARY KEY,
        best_timeframe TEXT,
        best_setup TEXT,
        avoid_conditions TEXT,
        avg_move_pct REAL DEFAULT 0.0,
        volatility TEXT,
        notes TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # История сделок (синхронизируется с основным bot)
    c.execute("""CREATE TABLE IF NOT EXISTS trade_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        direction TEXT,
        entry REAL,
        exit_price REAL,
        result TEXT,
        pnl_pct REAL,
        timeframe TEXT,
        setup TEXT,
        lesson TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Макро тренды
    c.execute("""CREATE TABLE IF NOT EXISTS macro_trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        indicator TEXT,
        value TEXT,
        interpretation TEXT,
        impact_on_crypto TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Рыночный контекст (обновляется каждый час)
    c.execute("""CREATE TABLE IF NOT EXISTS market_context (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        btc_dominance REAL,
        fear_greed INTEGER,
        dxy_trend TEXT,
        market_phase TEXT,
        top_movers TEXT,
        groq_summary TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    # Самообучение — правила стратегии
    c.execute("""CREATE TABLE IF NOT EXISTS self_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT,
        rule TEXT,
        confidence REAL DEFAULT 0.5,
        confirmed_by INTEGER DEFAULT 0,
        contradicted_by INTEGER DEFAULT 0,
        source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")

    conn.commit()
    conn.close()
    logging.info(f"Brain DB инициализирована: {DB_PATH}")


def save_knowledge(topic, content, source="brain_builder"):
    try:
        conn = sqlite3.connect(DB_PATH)
        # Обновляем если уже есть
        existing = conn.execute(
            "SELECT id FROM knowledge WHERE topic=? AND source=?", (topic, source)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE knowledge SET content=?, created_at=CURRENT_TIMESTAMP WHERE id=?",
                (content[:2000], existing[0])
            )
        else:
            conn.execute(
                "INSERT INTO knowledge VALUES (NULL,?,?,?,CURRENT_TIMESTAMP)",
                (topic, content[:2000], source)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"save_knowledge: {e}")


def save_smc_pattern(pattern_type, symbol, timeframe, description, examples=""):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT INTO smc_patterns VALUES
            (NULL,?,?,?,?,0.0,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            (pattern_type, symbol, timeframe, description[:500], examples[:300])
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"save_smc_pattern: {e}")


def save_coin_rule(symbol, best_tf, best_setup, avoid, avg_move, volatility, notes):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT OR REPLACE INTO coin_rules VALUES
            (?,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
            (symbol, best_tf, best_setup[:300], avoid[:300], avg_move, volatility, notes[:300])
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"save_coin_rule: {e}")


def save_macro_trend(indicator, value, interpretation, impact):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT INTO macro_trends VALUES
            (NULL,?,?,?,?,CURRENT_TIMESTAMP)""",
            (indicator, str(value)[:100], interpretation[:300], impact[:300])
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"save_macro_trend: {e}")


def save_self_rule(category, rule, confidence=0.5, source="brain_builder"):
    try:
        conn = sqlite3.connect(DB_PATH)
        existing = conn.execute(
            "SELECT id, confidence, confirmed_by FROM self_rules WHERE rule LIKE ? AND category=?",
            (f"%{rule[:50]}%", category)
        ).fetchone()
        if existing:
            new_conf = min(1.0, existing[1] + 0.05)
            conn.execute(
                "UPDATE self_rules SET confidence=?, confirmed_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (new_conf, existing[2] + 1, existing[0])
            )
        else:
            conn.execute(
                "INSERT INTO self_rules (category, rule, rule_type, rule_text, confidence, source, active, created_at, updated_at) VALUES (?,?,?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (category, rule[:300], "auto", rule[:300], confidence, source)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"save_self_rule: {e}")


# ──────────────────────────────────────────────
# ДАННЫЕ ИЗ ИНТЕРНЕТА
# ──────────────────────────────────────────────

def fetch_btc_dominance():
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/global",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json().get("data", {})
        dom = data.get("market_cap_percentage", {}).get("btc", 0)
        total_mcap = data.get("total_market_cap", {}).get("usd", 0)
        return round(dom, 2), total_mcap
    except Exception as e:
        logging.warning(f"BTC dominance: {e}")
        return None, None


def fetch_fear_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
        data = r.json()["data"][0]
        return int(data["value"]), data["value_classification"]
    except Exception as e:
        logging.warning(f"Fear & Greed: {e}")
        return None, None


def fetch_dxy():
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=1d&range=5d",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if len(closes) >= 2:
            change = (closes[-1] - closes[-2]) / closes[-2] * 100
            return round(closes[-1], 2), round(change, 2)
    except Exception as e:
        logging.warning(f"DXY: {e}")
    return None, None


def fetch_top_movers():
    """Топ-5 растущих и падающих монет"""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 50,
                "page": 1,
                "price_change_percentage": "24h"
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12
        )
        coins = r.json()
        if not isinstance(coins, list):
            return [], []
        sorted_coins = sorted(coins, key=lambda x: x.get("price_change_percentage_24h", 0) or 0, reverse=True)
        gainers = [(c["symbol"].upper(), round(c.get("price_change_percentage_24h", 0), 2)) for c in sorted_coins[:5]]
        losers = [(c["symbol"].upper(), round(c.get("price_change_percentage_24h", 0), 2)) for c in sorted_coins[-5:]]
        return gainers, losers
    except Exception as e:
        logging.warning(f"Top movers: {e}")
        return [], []


def parse_rss(url, limit=5):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        items = []
        entries = re.findall(r"<item>(.*?)</item>", r.text, re.DOTALL)
        for entry in entries[:limit]:
            title_m = re.search(r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", entry, re.DOTALL)
            title = title_m.group(1).strip() if title_m else ""
            if title:
                items.append(title)
        return items
    except:
        return []


def fetch_coingecko_trending():
    """CoinGecko Trending — топ-7 поисковых монет за 24ч (без ключа)"""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/search/trending",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code != 200:
            return []
        coins = r.json().get("coins", [])
        result = []
        for item in coins[:7]:
            c = item.get("item", {})
            result.append({
                "symbol": c.get("symbol", "").upper(),
                "name": c.get("name", ""),
                "rank": c.get("market_cap_rank", 999),
                "score": c.get("score", 0),
            })
        return result
    except Exception as e:
        logging.warning(f"CoinGecko Trending: {e}")
        return []


def fetch_blockchair_onchain():
    """Blockchair — on-chain данные BTC и ETH (без ключа)"""
    result = {}
    try:
        # BTC статистика
        r = requests.get(
            "https://api.blockchair.com/bitcoin/stats",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12
        )
        if r.status_code == 200:
            data = r.json().get("data", {})
            result["btc"] = {
                "transactions_24h": data.get("transactions_24h", 0),
                "mempool_size": data.get("mempool_transactions", 0),
                "mempool_bytes": data.get("mempool_size", 0),
                "avg_fee_usd": round(data.get("average_transaction_fee_usd_24h", 0), 2),
                "blocks_24h": data.get("blocks_24h", 0),
            }
            txs = result["btc"]["transactions_24h"]
            activity_btc = "HIGH" if txs > 400000 else "MEDIUM" if txs > 250000 else "LOW"
            result["btc"]["activity"] = activity_btc
        time.sleep(1)
    except Exception as e:
        logging.debug(f"Blockchair BTC: {e}")

    try:
        # ETH статистика
        r = requests.get(
            "https://api.blockchair.com/ethereum/stats",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=12
        )
        if r.status_code == 200:
            data = r.json().get("data", {})
            result["eth"] = {
                "transactions_24h": data.get("transactions_24h", 0),
                "mempool_size": data.get("mempool_transactions", 0),
                "avg_fee_usd": round(data.get("average_transaction_fee_usd_24h", 0), 2),
            }
            txs_eth = result["eth"]["transactions_24h"]
            activity_eth = "HIGH" if txs_eth > 1200000 else "MEDIUM" if txs_eth > 800000 else "LOW"
            result["eth"]["activity"] = activity_eth
    except Exception as e:
        logging.debug(f"Blockchair ETH: {e}")

    return result


def fetch_blockchain_info():
    """Blockchain.info — Hash rate BTC, mempool, активность (без ключа)"""
    result = {}
    try:
        # Статистика сети BTC
        r = requests.get(
            "https://blockchain.info/stats?format=json",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            hash_rate = round(data.get("hash_rate", 0) / 1e9, 1)  # EH/s
            n_tx = data.get("n_tx", 0)
            result = {
                "hash_rate_eh": hash_rate,
                "transactions_24h": n_tx,
                "difficulty": data.get("difficulty", 0),
                "avg_block_size": round(data.get("avg_block_size", 0) / 1000, 1),  # KB
                "miners_revenue_usd": round(data.get("miners_revenue_usd", 0), 0),
                "interpretation": (
                    f"Hash rate BTC: {hash_rate} EH/s | "
                    f"Транзакции 24ч: {n_tx:,} | "
                    f"Доход майнеров: ${data.get('miners_revenue_usd', 0):,.0f}"
                )
            }
    except Exception as e:
        logging.debug(f"Blockchain.info: {e}")
    return result


def fetch_crypto_news():
    """Свежие новости с нескольких источников"""
    all_news = []
    sources = [
        "https://cointelegraph.com/rss",
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://decrypt.co/feed",
    ]
    for url in sources:
        try:
            items = parse_rss(url, limit=5)
            all_news.extend(items)
            time.sleep(0.5)
        except:
            pass
    return all_news[:15]




def fetch_cryptopanic_news():
    """Новости с CryptoPanic — с сентиментом (bullish/bearish/important)"""
    api_key = os.environ.get("CRYPTOPANIC_API_KEY", "")
    if not api_key:
        return []
    try:
        r = requests.get(
            "https://cryptopanic.com/api/v1/posts/",
            params={"auth_token": api_key, "filter": "important", "public": "true", "kind": "news"},
            timeout=10
        )
        if r.status_code != 200:
            return []
        items = r.json().get("results", [])
        news = []
        for item in items[:10]:
            votes = item.get("votes", {})
            sentiment = "BULLISH" if votes.get("positive", 0) > votes.get("negative", 0) else "BEARISH" if votes.get("negative", 0) > votes.get("positive", 0) else "NEUTRAL"
            currencies = [c["code"] for c in item.get("currencies", [])]
            news.append({
                "title": item.get("title", ""),
                "sentiment": sentiment,
                "currencies": currencies,
                "positive": votes.get("positive", 0),
                "negative": votes.get("negative", 0),
            })
        return news
    except Exception as e:
        logging.warning(f"CryptoPanic: {e}")
        return []


def fetch_fred_macro():
    """Макро данные из FRED (ФРС США) — DXY, ставка, инфляция"""
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        return {}
    indicators = {
        "FEDFUNDS": "Ставка ФРС",
        "CPIAUCSL": "Инфляция CPI",
        "DGS10": "Доходность 10-летних облигаций США",
        "DTWEXBGS": "Индекс доллара DXY",
    }
    result = {}
    for series_id, name in indicators.items():
        try:
            r = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "limit": 2,
                    "sort_order": "desc"
                },
                timeout=10
            )
            if r.status_code == 200:
                obs = r.json().get("observations", [])
                if obs:
                    latest = obs[0]
                    prev = obs[1] if len(obs) > 1 else obs[0]
                    try:
                        val = float(latest["value"])
                        prev_val = float(prev["value"])
                        change = round(val - prev_val, 3)
                        result[series_id] = {
                            "name": name,
                            "value": val,
                            "change": change,
                            "date": latest["date"]
                        }
                    except:
                        pass
            time.sleep(0.3)
        except Exception as e:
            logging.debug(f"FRED {series_id}: {e}")
    return result


def fetch_etherscan_activity():
    """On-chain активность Ethereum — газ, транзакции"""
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return {}
    try:
        # Текущая цена газа
        r = requests.get(
            "https://api.etherscan.io/api",
            params={"module": "gastracker", "action": "gasoracle", "apikey": api_key},
            timeout=10
        )
        if r.status_code == 200:
            data = r.json().get("result", {})
            safe_gas = int(data.get("SafeGasPrice", 0))
            fast_gas = int(data.get("FastGasPrice", 0))
            # Интерпретация: высокий газ = высокая on-chain активность
            activity = "HIGH" if fast_gas > 50 else "MEDIUM" if fast_gas > 20 else "LOW"
            return {
                "safe_gas": safe_gas,
                "fast_gas": fast_gas,
                "activity": activity,
                "interpretation": f"On-chain активность ETH: {activity} (газ {fast_gas} gwei)"
            }
    except Exception as e:
        logging.debug(f"Etherscan: {e}")
    return {}

def fetch_prices_snapshot():
    """Быстрый снимок цен топ-монет"""
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": "bitcoin,ethereum,solana,binancecoin,ripple,toncoin,dogecoin",
                "vs_currencies": "usd",
                "include_24hr_change": "true"
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        data = r.json()
        result = {}
        for coin_id, vals in data.items():
            result[coin_id] = {
                "price": vals.get("usd", 0),
                "change_24h": round(vals.get("usd_24h_change", 0), 2)
            }
        return result
    except Exception as e:
        logging.warning(f"Prices snapshot: {e}")
        return {}


# ──────────────────────────────────────────────
# GROQ АНАЛИЗ
# ──────────────────────────────────────────────

def ask_groq(prompt, max_tokens=600):
    if not groq_client:
        logging.error("Groq client не инициализирован")
        return None
    try:
        r = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens
        )
        return r.choices[0].message.content.strip()
    except Exception as e:
        logging.error(f"Groq error: {e}")
        return None


# ──────────────────────────────────────────────
# МОДУЛЬ 1: SMC ПАТТЕРНЫ
# ──────────────────────────────────────────────

def learn_smc_patterns():
    """Groq изучает и записывает SMC паттерны"""
    logging.info("📚 Изучаю SMC паттерны...")

    patterns = [
        ("Order Block", "Зона где крупный игрок открыл большую позицию. Последняя свеча против тренда перед импульсом."),
        ("Fair Value Gap", "Имбаланс между свечами — зона незаполненной ликвидности. Цена стремится вернуться."),
        ("Break of Structure", "Пробой последнего свинг-хая/лоя — подтверждение смены тренда."),
        ("Change of Character", "Первый пробой структуры против тренда — ранний сигнал разворота."),
        ("Liquidity Sweep", "Сбор стопов за очевидными уровнями перед настоящим движением."),
        ("Inducement", "Ложный пробой для привлечения розничных трейдеров в неверную сторону."),
        ("Premium/Discount", "Зоны выше/ниже 50% диапазона — оптимальные точки входа по тренду."),
    ]

    for pattern_name, pattern_desc in patterns:
        prompt = f"""Ты эксперт по Smart Money Concepts (SMC).

Паттерн: {pattern_name}
Базовое описание: {pattern_desc}

Дай структурированный анализ для trading бота:
1. Точное определение (1-2 предложения)
2. Как идентифицировать на графике (конкретные критерии)
3. Лучшие таймфреймы для этого паттерна
4. Win rate ориентировочно (%)
5. Лучшие монеты для этого паттерна (3-5 примеров)
6. Частые ошибки трейдеров

Формат: чёткие пункты, без воды."""

        result = ask_groq(prompt, max_tokens=500)
        if result:
            save_smc_pattern(
                pattern_type=pattern_name,
                symbol="ALL",
                timeframe="1h,4h",
                description=result,
                examples=pattern_desc
            )
            save_knowledge(
                f"smc_{pattern_name.lower().replace(' ', '_')}",
                result,
                "smc_learning"
            )
            logging.info(f"  ✅ {pattern_name} записан")
        time.sleep(2)  # Rate limit Groq


# ──────────────────────────────────────────────
# МОДУЛЬ 2: ПРАВИЛА ПО МОНЕТАМ
# ──────────────────────────────────────────────

TOP_COINS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "TONUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ARBUSDT",
    "ADAUSDT", "DOTUSDT", "NEARUSDT", "INJUSDT", "SUIUSDT",
]

def learn_coin_rules(prices_snapshot=None):
    """Groq анализирует каждую монету и записывает правила торговли"""
    logging.info("🪙 Изучаю правила торговли по монетам...")

    prices_text = ""
    if prices_snapshot:
        lines = []
        for coin_id, data in prices_snapshot.items():
            lines.append(f"{coin_id}: ${data['price']:,.4f} ({data['change_24h']:+.2f}% 24h)")
        prices_text = "\n".join(lines)

    # Анализируем только топ монеты батчем чтобы не тратить много токенов
    batch_prompt = f"""Ты опытный крипто трейдер с 5+ годами опыта.

Текущие цены:
{prices_text if prices_text else "данные недоступны"}

Для каждой монеты из списка дай краткие правила торговли в JSON формате.
Монеты: BTC, ETH, SOL, BNB, XRP, TON, DOGE, AVAX, LINK, ARB

Верни JSON массив:
[
  {{
    "symbol": "BTCUSDT",
    "best_timeframe": "4h",
    "best_setup": "Order Block + FVG confluence на 4h",
    "avoid": "не торговать в боковике 69k-71k без объёма",
    "avg_move_pct": 3.5,
    "volatility": "medium",
    "notes": "реагирует на новости ФРС, DXY корреляция -0.7"
  }},
  ...
]

Только JSON, без пояснений."""

    result = ask_groq(batch_prompt, max_tokens=1500)
    if result:
        try:
            clean = result.replace("```json", "").replace("```", "").strip()
            start = clean.find("[")
            end = clean.rfind("]") + 1
            coins_data = json.loads(clean[start:end])

            for coin in coins_data:
                symbol = coin.get("symbol", "")
                if not symbol:
                    continue
                save_coin_rule(
                    symbol=symbol,
                    best_tf=coin.get("best_timeframe", "1h"),
                    best_setup=coin.get("best_setup", ""),
                    avoid=coin.get("avoid", ""),
                    avg_move=float(coin.get("avg_move_pct", 0)),
                    volatility=coin.get("volatility", "medium"),
                    notes=coin.get("notes", "")
                )
                # Сохраняем как правило самообучения
                if coin.get("best_setup"):
                    save_self_rule(
                        "best_setup",
                        f"{symbol}: {coin['best_setup']}",
                        0.7,
                        "coin_rules"
                    )
                if coin.get("avoid"):
                    save_self_rule(
                        "avoid",
                        f"ИЗБЕГАТЬ {symbol}: {coin['avoid']}",
                        0.65,
                        "coin_rules"
                    )
                logging.info(f"  ✅ {symbol} правила записаны")

        except Exception as e:
            logging.error(f"Парсинг coin rules: {e}")
            # Сохраняем как текст если JSON не распарсился
            save_knowledge("coin_trading_rules", result, "coin_rules")


# ──────────────────────────────────────────────
# МОДУЛЬ 3: МАКРО ТРЕНДЫ
# ──────────────────────────────────────────────

def learn_macro_trends():
    """Groq анализирует макро данные и их влияние на крипту"""
    logging.info("🌍 Анализирую макро тренды...")

    # Собираем данные
    btc_dom, total_mcap = fetch_btc_dominance()
    fg_value, fg_label = fetch_fear_greed()
    dxy_value, dxy_change = fetch_dxy()
    gainers, losers = fetch_top_movers()

    # Формируем контекст
    macro_data = []
    if btc_dom:
        macro_data.append(f"BTC Dominance: {btc_dom}%")
        save_macro_trend(
            "BTC Dominance", btc_dom,
            f"{'высокая' if btc_dom > 55 else 'средняя' if btc_dom > 45 else 'низкая'} доминация",
            "высокая доминация = альты слабее, низкая = альтсезон возможен"
        )

    if fg_value:
        macro_data.append(f"Fear & Greed: {fg_value} ({fg_label})")
        save_macro_trend(
            "Fear & Greed", fg_value,
            fg_label,
            "< 25 = экстремальный страх (покупка), > 75 = жадность (продажа)"
        )

    if dxy_value:
        macro_data.append(f"DXY: {dxy_value} ({dxy_change:+.2f}%)")
        impact = "негативно для крипты" if dxy_change > 0.3 else "позитивно для крипты" if dxy_change < -0.3 else "нейтрально"
        save_macro_trend(
            "DXY Dollar Index", dxy_value,
            f"{'растёт' if dxy_change > 0 else 'падает'} {abs(dxy_change):.2f}%",
            impact
        )

    if gainers:
        macro_data.append(f"Топ-растущие: {', '.join([f'{s}+{c}%' for s,c in gainers[:3]])}")
    if losers:
        macro_data.append(f"Топ-падающие: {', '.join([f'{s}{c}%' for s,c in losers[:3]])}")

    # FRED макро данные
    fred_data = fetch_fred_macro()
    if fred_data:
        for sid, info in fred_data.items():
            chg = f"+{info['change']}" if info['change'] > 0 else str(info['change'])
            macro_data.append(f"📊 {info['name']}: {info['value']} ({chg}) [{info['date']}]")

    # Etherscan on-chain активность
    eth_activity = fetch_etherscan_activity()
    if eth_activity:
        macro_data.append(f"⛓️ {eth_activity.get('interpretation', '')}")

    # Blockchair — on-chain активность BTC и ETH
    blockchair = fetch_blockchair_onchain()
    if blockchair.get("btc"):
        b = blockchair["btc"]
        macro_data.append(
            f"🔗 BTC on-chain: {b['transactions_24h']:,} tx/24h | "
            f"mempool: {b['mempool_size']:,} | "
            f"комиссия: ${b['avg_fee_usd']} | активность: {b['activity']}"
        )
    if blockchair.get("eth"):
        e = blockchair["eth"]
        macro_data.append(
            f"🔗 ETH on-chain: {e['transactions_24h']:,} tx/24h | "
            f"комиссия: ${e['avg_fee_usd']} | активность: {e['activity']}"
        )

    # Blockchain.info — hash rate и здоровье сети BTC
    chain_info = fetch_blockchain_info()
    if chain_info:
        macro_data.append(f"⛏️ {chain_info.get('interpretation', '')}")

    # CoinGecko Trending — что люди ищут прямо сейчас
    trending = fetch_coingecko_trending()
    if trending:
        trending_str = ", ".join([f"{c['symbol']}(#{c['rank']})" for c in trending[:5]])
        macro_data.append(f"🔥 Trending CoinGecko: {trending_str}")

    if not macro_data:
        logging.warning("Нет макро данных для анализа")
        return

    macro_context = "\n".join(macro_data)

    # Groq анализирует макро ситуацию
    prompt = f"""Ты макро аналитик криптовалютного рынка.

ТЕКУЩИЕ ДАННЫЕ ({datetime.now().strftime('%Y-%m-%d %H:%M')}):
{macro_context}

Проведи анализ:
1. ФАЗА РЫНКА: накопление / распределение / рост / падение
2. BTC ДОМИНАЦИЯ: что это значит для альтов прямо сейчас
3. ON-CHAIN СИГНАЛЫ: что говорит активность сети BTC/ETH (транзакции, хэшрейт, комиссии)
4. ХАЙП/ТРЕНДЫ: trending монеты — это возможность или ловушка
5. СТРАХ/ЖАДНОСТЬ + DXY: как влияют на торговые решения
6. СИГНАЛЫ: на что обратить внимание в следующие 24 часа
7. ПРАВИЛО ДНЯ: одно конкретное правило для трейдера

Кратко и по делу, каждый пункт 1-2 предложения."""

    analysis = ask_groq(prompt, max_tokens=600)
    if analysis:
        # Сохраняем в рыночный контекст
        try:
            conn = sqlite3.connect(DB_PATH)
            gainers_str = json.dumps(gainers)
            losers_str = json.dumps(losers)
            conn.execute("""INSERT INTO market_context VALUES
                (NULL,?,?,?,?,?,?,CURRENT_TIMESTAMP)""",
                (
                    btc_dom or 0,
                    fg_value or 0,
                    f"DXY {dxy_value} ({dxy_change:+.2f}%)" if dxy_value else "N/A",
                    "определяется анализом",
                    f"gainers:{gainers_str} losers:{losers_str}",
                    analysis
                )
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Save market_context: {e}")

        save_knowledge(
            f"macro_analysis_{datetime.now().strftime('%Y%m%d_%H')}",
            f"ДАННЫЕ:\n{macro_context}\n\nАНАЛИЗ:\n{analysis}",
            "macro_groq"
        )

        # Извлекаем правило дня (теперь 7й пункт)
        if "ПРАВИЛО" in analysis.upper() or "правило" in analysis.lower():
            lines = analysis.split("\n")
            for line in lines:
                if "правило" in line.lower() or "7." in line:
                    save_self_rule("market", line.replace("7.", "").strip(), 0.6, "macro_daily")
                    break

        # Сохраняем trending монеты как отдельное знание
        if trending:
            trending_knowledge = "TRENDING монеты (CoinGecko): " + ", ".join(
                [f"{c['symbol']}(ранк #{c['rank']})" for c in trending]
            )
            save_knowledge(
                f"trending_{datetime.now().strftime('%Y%m%d_%H')}",
                trending_knowledge,
                "coingecko_trending"
            )
            # Если монета в топ-3 trending И в нашем списке — сохраняем правило
            our_pairs = ["BTC","ETH","SOL","BNB","XRP","TON","DOGE","AVAX","LINK","ARB",
                         "ADA","DOT","POL","LTC","ATOM","NEAR","INJ","SUI","APT","OP",
                         "UNI","PEPE","SHIB","TRX","XLM","WLD","TIA","SEI","JUP","BONK"]
            for c in trending[:3]:
                if c["symbol"] in our_pairs:
                    save_self_rule(
                        "market",
                        f"{c['symbol']} в топ-3 CoinGecko Trending — повышенный интерес рынка, следить за пробоями",
                        0.55,
                        "trending_signal"
                    )

        # Сохраняем on-chain данные как знание
        if blockchair.get("btc"):
            b = blockchair["btc"]
            onchain_note = (
                f"BTC on-chain активность: {b['activity']} | "
                f"{b['transactions_24h']:,} tx за 24ч | "
                f"mempool {b['mempool_size']:,} транзакций | "
                f"{'сеть перегружена — осторожно с входами' if b['activity'] == 'HIGH' else 'сеть в норме'}"
            )
            save_knowledge(
                f"onchain_btc_{datetime.now().strftime('%Y%m%d_%H')}",
                onchain_note,
                "blockchair"
            )

        logging.info(f"  ✅ Макро анализ записан (+ trending + on-chain)")


# ──────────────────────────────────────────────
# МОДУЛЬ 4: НОВОСТИ И АНАЛИЗ РЫНКА
# ──────────────────────────────────────────────

def learn_from_news():
    """Groq читает новости и извлекает торговые инсайты"""
    logging.info("📰 Анализирую новости...")

    news = fetch_crypto_news()
    if not news:
        logging.warning("Новости недоступны")
        return

    news_text = "\n".join([f"• {n}" for n in news[:10]])

    prompt = f"""Ты крипто трейдер анализируешь новости для торговых решений.

НОВОСТИ ({datetime.now().strftime('%Y-%m-%d %H:%M')}):
{news_text}

Дай структурированный анализ:
1. КЛЮЧЕВЫЕ СОБЫТИЯ: топ-3 новости которые влияют на рынок
2. ВЛИЯНИЕ НА BTC: позитив / негатив / нейтрально — почему
3. АЛЬТКОИНЫ: какие монеты могут двигаться на этих новостях
4. РИСКИ: что может пойти не так для лонгов
5. ТОРГОВОЕ ПРАВИЛО: одно конкретное правило из этих новостей

Кратко, практично, без воды."""

    analysis = ask_groq(prompt, max_tokens=500)
    if analysis:
        save_knowledge(
            f"news_analysis_{datetime.now().strftime('%Y%m%d_%H')}",
            f"НОВОСТИ:\n{news_text}\n\nАНАЛИЗ:\n{analysis}",
            "news_groq"
        )

        # Извлекаем торговое правило
        lines = analysis.split("\n")
        for line in lines:
            if "правило" in line.lower() or "5." in line:
                rule = line.replace("5.", "").replace("ТОРГОВОЕ ПРАВИЛО:", "").strip()
                if len(rule) > 15:
                    save_self_rule("market", rule, 0.55, "news_daily")
                break

        logging.info(f"  ✅ Анализ новостей записан")


# ──────────────────────────────────────────────
# МОДУЛЬ 5: АНАЛИЗ ИСТОРИИ СДЕЛОК
# ──────────────────────────────────────────────

def analyze_trade_history():
    """Groq анализирует историю сделок из основной БД бота"""
    logging.info("📊 Анализирую историю сделок...")

    try:
        conn = sqlite3.connect(DB_PATH)

        # Пробуем достать сигналы из основной таблицы
        signals = conn.execute("""
            SELECT symbol, direction, entry, tp1, sl, result, timeframe, grade, created_at
            FROM signals
            WHERE result != 'pending'
            ORDER BY id DESC
            LIMIT 50
        """).fetchall()

        if not signals:
            logging.info("  История сделок пуста — пропускаем")
            conn.close()
            return

        # Статистика
        total = len(signals)
        wins = sum(1 for s in signals if s[5] in ("tp1", "tp2", "tp3"))
        losses = sum(1 for s in signals if s[5] == "sl")
        win_rate = round(wins / total * 100, 1) if total > 0 else 0

        # Статистика по символам
        symbol_stats = {}
        for s in signals:
            sym = s[0]
            if sym not in symbol_stats:
                symbol_stats[sym] = {"wins": 0, "losses": 0, "total": 0}
            symbol_stats[sym]["total"] += 1
            if s[5] in ("tp1", "tp2", "tp3"):
                symbol_stats[sym]["wins"] += 1
            elif s[5] == "sl":
                symbol_stats[sym]["losses"] += 1

        # Топ монеты
        best = sorted(symbol_stats.items(), key=lambda x: x[1]["wins"]/max(x[1]["total"],1), reverse=True)[:5]
        worst = sorted(symbol_stats.items(), key=lambda x: x[1]["losses"]/max(x[1]["total"],1), reverse=True)[:3]

        best_text = "\n".join([f"{s}: {d['wins']}/{d['total']} ({round(d['wins']/d['total']*100)}%)" for s,d in best])
        worst_text = "\n".join([f"{s}: {d['losses']} потерь из {d['total']}" for s,d in worst])

        conn.close()

        # Groq анализирует паттерны ошибок
        recent_losses = [s for s in signals if s[5] == "sl"][:10]
        loss_text = "\n".join([
            f"{s[0]} {s[1]} {s[6]} вход:{s[2]:.4f} стоп:{s[4]:.4f}"
            for s in recent_losses
        ]) if recent_losses else "нет убыточных сделок"

        prompt = f"""Ты анализируешь историю сделок торгового бота.

СТАТИСТИКА (последние {total} сделок):
Win Rate: {win_rate}%
Прибыльных: {wins} | Убыточных: {losses}

ЛУЧШИЕ МОНЕТЫ:
{best_text}

ПРОБЛЕМНЫЕ МОНЕТЫ:
{worst_text}

ПОСЛЕДНИЕ УБЫТКИ:
{loss_text}

Дай анализ:
1. ГЛАВНАЯ ПРОБЛЕМА: почему проигрывает (1-2 предложения)
2. ПАТТЕРН ОШИБОК: что общего в убыточных сделках
3. ПРАВИЛО 1: что перестать делать немедленно
4. ПРАВИЛО 2: что делать больше (работающий паттерн)
5. ПРИОРИТЕТ: топ-3 монеты для торговли на следующей неделе

Конкретно и практично."""

        analysis = ask_groq(prompt, max_tokens=500)
        if analysis:
            save_knowledge(
                f"trade_history_analysis_{datetime.now().strftime('%Y%m%d')}",
                f"СТАТИСТИКА:\nWR:{win_rate}% Wins:{wins} Losses:{losses}\n\nАНАЛИЗ:\n{analysis}",
                "history_groq"
            )

            # Извлекаем правила
            lines = analysis.split("\n")
            for line in lines:
                if "ПРАВИЛО" in line.upper() or line.startswith("3.") or line.startswith("4."):
                    rule = re.sub(r"^[0-9]+\.", "", line).replace("ПРАВИЛО 1:", "").replace("ПРАВИЛО 2:", "").strip()
                    if len(rule) > 15:
                        save_self_rule("entry", rule, 0.7, "history_analysis")

            # Обновляем правила избегания для проблемных монет
            for sym, data in worst:
                if data["total"] >= 3 and data["losses"] / data["total"] > 0.6:
                    save_self_rule(
                        "avoid",
                        f"ИЗБЕГАТЬ {sym} — WR только {round((data['wins']/data['total'])*100)}% за {data['total']} сигналов",
                        0.75,
                        "history_analysis"
                    )

            logging.info(f"  ✅ История сделок проанализирована. WR: {win_rate}%")

    except Exception as e:
        logging.error(f"analyze_trade_history: {e}")


# ──────────────────────────────────────────────
# ГЛАВНЫЙ ЦИКЛ
# ──────────────────────────────────────────────

def run_brain_builder(full=False):
    """
    Основная функция — запускается каждый час из бота или вручную.

    full=True  — полный цикл включая SMC паттерны (1 раз в сутки)
    full=False — быстрый цикл: макро + новости + история (каждый час)
    """
    start = time.time()
    logging.info(f"🧠 Brain Builder запущен | {'ПОЛНЫЙ' if full else 'БЫСТРЫЙ'} цикл")

    init_brain_db()

    # Получаем текущие цены (нужны для анализа монет)
    prices = fetch_prices_snapshot()

    # ── Быстрый цикл (каждый час) ──
    try:
        learn_macro_trends()
    except Exception as e:
        logging.error(f"Macro trends: {e}")

    time.sleep(3)

    try:
        learn_from_news()
    except Exception as e:
        logging.error(f"News learning: {e}")

    time.sleep(3)

    try:
        analyze_trade_history()
    except Exception as e:
        logging.error(f"Trade history: {e}")

    # ── Полный цикл (раз в сутки) ──
    if full:
        time.sleep(3)
        try:
            learn_smc_patterns()
        except Exception as e:
            logging.error(f"SMC patterns: {e}")

        time.sleep(3)
        try:
            learn_coin_rules(prices)
        except Exception as e:
            logging.error(f"Coin rules: {e}")

    elapsed = round(time.time() - start, 1)

    # Итоговая статистика
    try:
        conn = sqlite3.connect(DB_PATH)
        knowledge_count = conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()[0]
        rules_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        patterns_count = conn.execute("SELECT COUNT(*) FROM smc_patterns").fetchone()[0]
        coin_rules_count = conn.execute("SELECT COUNT(*) FROM coin_rules").fetchone()[0]
        conn.close()
        logging.info(
            f"🧠 Brain Builder завершён за {elapsed}с | "
            f"знаний:{knowledge_count} правил:{rules_count} "
            f"паттернов:{patterns_count} монет:{coin_rules_count}"
        )
        return {
            "knowledge": knowledge_count,
            "rules": rules_count,
            "patterns": patterns_count,
            "coins": coin_rules_count,
            "elapsed": elapsed
        }
    except Exception as e:
        logging.error(f"Final stats: {e}")
        return {}


def get_brain_summary():
    """Возвращает текущее состояние мозга для отображения в боте"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        knowledge_count = conn.execute("SELECT COUNT(*) FROM knowledge").fetchone()
        knowledge_count = knowledge_count[0] if knowledge_count else 0
        rules_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()
        rules_count = rules_count[0] if rules_count else 0
        top_rules = conn.execute(
            "SELECT category, rule, confidence FROM self_rules ORDER BY confidence DESC LIMIT 10"
        ).fetchall()
        latest_macro = conn.execute(
            "SELECT groq_summary, created_at FROM market_context ORDER BY id DESC LIMIT 1"
        ).fetchone()
        coin_count = conn.execute("SELECT COUNT(*) FROM coin_rules").fetchone()
        coin_count = coin_count[0] if coin_count else 0
        pattern_count = conn.execute("SELECT COUNT(*) FROM smc_patterns").fetchone()
        pattern_count = pattern_count[0] if pattern_count else 0
        conn.close()

        rules_text = "\n".join([
            f"[{r[0] or '?'}] {(r[1] or '')[:70]} — {float(r[2] or 0):.0%}"
            for r in top_rules
        ]) or "Пока нет правил"

        macro_text = latest_macro[0][:300] if latest_macro else "Нет данных"
        macro_time = latest_macro[1][:16] if latest_macro else ""

        return {
            "knowledge_count": knowledge_count or 0,
            "rules_count": rules_count or 0,
            "coin_count": coin_count or 0,
            "pattern_count": pattern_count or 0,
            "top_rules": rules_text or "Пока нет правил",
            "macro_summary": macro_text or "Нет данных",
            "macro_time": macro_time or "",
        }
    except Exception as e:
        logging.error(f"get_brain_summary: {e}")
        return {
            "knowledge_count": 0,
            "rules_count": 0,
            "coin_count": 0,
            "pattern_count": 0,
            "top_rules": "Ошибка загрузки",
            "macro_summary": "Нет данных",
            "macro_time": "",
        }


# ──────────────────────────────────────────────
# ЗАПУСК НАПРЯМУЮ
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    full_cycle = "--full" in sys.argv
    stats = run_brain_builder(full=full_cycle)
    print("\n✅ Brain Builder завершён:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
