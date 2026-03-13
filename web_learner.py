"""
APEX Web Learner — автономный поиск знаний в интернете
Groq сам решает что искать, ищет, анализирует и сохраняет в brain.db
"""
import requests, sqlite3, logging, json, os, time, re
from datetime import datetime

# ── WAL патч ──
_orig_connect_wl = sqlite3.connect
def _wal_connect_wl(db, timeout=30, **kw):
    kw.setdefault("check_same_thread", False)
    conn = _orig_connect_wl(db, timeout=timeout, **kw)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn
sqlite3.connect = _wal_connect_wl

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brain.db")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.1-8b-instant"

# ═══════════════════════════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ
# ═══════════════════════════════════════════════════════════════

def init_web_learner_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS web_knowledge (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT,
        query TEXT,
        summary TEXT,
        source_url TEXT,
        relevance REAL DEFAULT 0.5,
        applied INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS learning_agenda (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT UNIQUE,
        priority INTEGER DEFAULT 5,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS strategy_library (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        description TEXT,
        conditions TEXT,
        win_rate_expected REAL,
        tested INTEGER DEFAULT 0,
        actual_win_rate REAL,
        source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════
# GROQ ВЫЗОВЫ
# ═══════════════════════════════════════════════════════════════

def _groq(prompt: str, max_tokens: int = 800) -> str:
    if not GROQ_API_KEY:
        return ""
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": max_tokens, "temperature": 0.3},
            timeout=30
        )
        data = r.json()
        if "choices" not in data:
            logging.warning(f"web_learner groq: {data.get('error', {}).get('message', 'no choices')}")
            return ""
        return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logging.warning(f"web_learner groq: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════
# ПОИСК В ИНТЕРНЕТЕ (без внешних API)
# ═══════════════════════════════════════════════════════════════

def _fetch_url(url: str, timeout: int = 10) -> str:
    """Получаем текст страницы"""
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        if r.status_code != 200:
            return ""
        # Простое извлечение текста без beautifulsoup
        text = r.text
        # Убираем HTML теги
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:3000]  # Первые 3000 символов
    except Exception as e:
        logging.debug(f"fetch_url {url}: {e}")
        return ""


def search_duckduckgo(query: str, max_results: int = 3) -> list:
    """Поиск через DuckDuckGo HTML (без API)"""
    try:
        url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            return []

        # Извлекаем ссылки из результатов
        links = re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*class=["\'][^"\']*result__url[^"\']*["\']', r.text)
        if not links:
            # Альтернативный паттерн
            links = re.findall(r'uddg=([^&"\']+)', r.text)
            links = [requests.utils.unquote(l) for l in links]

        # Фильтруем и возвращаем первые N
        clean = [l for l in links if l.startswith("http") and "duckduckgo" not in l]
        return clean[:max_results]
    except Exception as e:
        logging.debug(f"duckduckgo search: {e}")
        return []


def search_crypto_news(query: str) -> list:
    """Поиск по крипто-новостным источникам"""
    sources = [
        f"https://cointelegraph.com/search?query={requests.utils.quote(query)}",
        f"https://cryptonews.com/search/?q={requests.utils.quote(query)}",
    ]
    results = []
    for url in sources:
        text = _fetch_url(url, timeout=8)
        if text and len(text) > 200:
            results.append({"url": url, "text": text[:1500]})
    return results


# ─── RSS фиды — работают стабильно с Render ──────────────────

# Топ источники: новости, анализ, Smart Money трейдеры
RSS_SOURCES = {
    # === НОВОСТИ ===
    "CoinTelegraph":     "https://cointelegraph.com/rss",
    "CoinDesk":          "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "Decrypt":           "https://decrypt.co/feed",
    "TheBlock":          "https://www.theblock.co/rss.xml",
    "CryptoSlate":       "https://cryptoslate.com/feed/",
    "BeInCrypto":        "https://beincrypto.com/feed/",
    "Bitcoinist":        "https://bitcoinist.com/feed/",
    "NewsBTC":           "https://www.newsbtc.com/feed/",
    "CryptoNews":        "https://cryptonews.com/news/feed/",
    "AMBCrypto":         "https://ambcrypto.com/feed/",

    # === АНАЛИТИКА И ИССЛЕДОВАНИЯ ===
    "Glassnode_blog":    "https://insights.glassnode.com/rss/",       # On-chain аналитика
    "Messari":           "https://messari.io/rss/news.xml",            # Фундаментал
    "IntoTheBlock":      "https://blog.intotheblock.com/rss/",         # On-chain
    "CryptoQuant_blog":  "https://cryptoquant.com/community/blog/rss", # Данные бирж
    "DeFiLlama_news":    "https://defillama.com/news/rss",             # DeFi данные

    # === ТРЕЙДИНГ И SMC ===
    "TradingView_ideas": "https://www.tradingview.com/feed/",           # Идеи трейдеров
    "CoinMarketCap_news":"https://coinmarketcap.com/headlines/news/feed/",
    "CryptoCompare":     "https://www.cryptocompare.com/api/data/newsfeeds/?feeds=cryptocompare",

    # === МАКРО / ТРАДФИ ===
    "Investopedia_crypto":"https://www.investopedia.com/cryptocurrency-news-5114163",
    "Reuters_crypto":    "https://feeds.reuters.com/reuters/businessNews",

    # === ON-CHAIN / WHALE DATA ===
    "WhaleAlert":        "https://whale-alert.io/feed",
    "CryptoQuant_alert": "https://cryptoquant.com/community/blog/rss",
}

# Конкретные темы для изучения из источников топ трейдеров
TRADER_KNOWLEDGE_URLS = {
    # Smart Money Concepts
    "ICT_concepts":      "https://www.tradingview.com/scripts/smartmoneyconcepts/",
    "SMC_guide":         "https://www.babypips.com/learn/forex/smart-money-concepts",
    "OB_guide":          "https://www.investopedia.com/terms/o/order-block.asp",
    "FVG_guide":         "https://www.investopedia.com/fair-value-gap-8414362",
    "liquidity_guide":   "https://www.babypips.com/learn/forex/liquidity-in-forex",

    # Стратегии топ крипто трейдеров (публичные блоги/гиды)
    "altcoin_season":    "https://www.coingecko.com/en/methodology",
    "wyckoff_method":    "https://school.stockcharts.com/doku.php?id=market_analysis:the_wyckoff_method",
    "volume_profile":    "https://www.tradingview.com/support/solutions/43000502009/",
    "funding_rates":     "https://www.binance.com/en/blog/futures/what-are-funding-rates-421499824684900420",
    "open_interest":     "https://academy.binance.com/en/articles/what-is-open-interest",
    "supply_demand":     "https://www.babypips.com/learn/forex/support-and-resistance",
    "wyckoff_phases":    "https://school.stockcharts.com/doku.php?id=market_analysis:the_wyckoff_method",
    "liquidations":      "https://academy.binance.com/en/articles/what-is-liquidation",
    "santiment_guide":   "https://academy.santiment.net/",
}


def fetch_rss(source_name: str, url: str, limit: int = 5) -> list:
    """Парсим RSS фид и возвращаем последние записи"""
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            return []
        text = r.text

        # Извлекаем заголовки и описания из XML
        titles = re.findall(r'<title><!\[CDATA\[(.+?)\]\]></title>|<title>(.+?)</title>', text)
        descs = re.findall(r'<description><!\[CDATA\[(.+?)\]\]></description>|<description>(.+?)</description>', text)

        items = []
        for i, (t1, t2) in enumerate(titles[:limit+2]):
            title = (t1 or t2).strip()
            if not title or title == source_name:  # пропускаем заголовок канала
                continue
            desc = ""
            if i < len(descs):
                d1, d2 = descs[i]
                desc = re.sub(r'<[^>]+>', ' ', (d1 or d2)).strip()[:300]
            items.append({"source": source_name, "title": title, "text": desc})
            if len(items) >= limit:
                break
        return items
    except Exception as e:
        logging.debug(f"RSS {source_name}: {e}")
        return []


def get_rss_batch(categories: list = None, limit_per_source: int = 3) -> list:
    """
    Получаем данные из нескольких RSS источников параллельно.
    categories: ['news', 'analysis', 'trading'] или None = все
    """
    import concurrent.futures

    # Выбираем источники по категории
    sources_to_use = dict(list(RSS_SOURCES.items())[:8])  # топ-8 по умолчанию

    all_items = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(fetch_rss, name, url, limit_per_source): name
            for name, url in sources_to_use.items()
        }
        for future in concurrent.futures.as_completed(futures, timeout=20):
            try:
                items = future.result()
                all_items.extend(items)
            except Exception:
                pass

    return all_items


def get_trader_knowledge(topic_key: str) -> str:
    """Получаем знания по конкретной теме из баз трейдеров"""
    url = TRADER_KNOWLEDGE_URLS.get(topic_key, "")
    if not url:
        return ""
    return _fetch_url(url, timeout=12)


# ═══════════════════════════════════════════════════════════════
# GROQ РЕШАЕТ ЧТО ИЗУЧАТЬ
# ═══════════════════════════════════════════════════════════════

def groq_decide_learning_agenda():
    """
    Groq анализирует текущее состояние бота и решает что нужно изучить.
    Результат: список тем для исследования.
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Собираем статистику
        stats = conn.execute("""
            SELECT symbol, win_rate, total FROM symbol_stats
            WHERE total >= 3 ORDER BY win_rate ASC LIMIT 5
        """).fetchall()

        rules_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        patterns_count = conn.execute("SELECT COUNT(*) FROM pattern_memory").fetchone()[0]

        # Незакрытые пробелы в знаниях
        gaps = conn.execute("""
            SELECT query FROM knowledge_gaps
            WHERE resolved=0 ORDER BY id DESC LIMIT 5
        """).fetchall() if conn.execute("SELECT name FROM sqlite_master WHERE name='knowledge_gaps'").fetchone() else []

        conn.close()

        worst = [(s[0], round(s[1], 1)) for s in stats] if stats else []

        prompt = f"""Ты — AI трейдинг-бот APEX. Проанализируй своё состояние и реши что нужно изучить.

ТЕКУЩЕЕ СОСТОЯНИЕ:
- Правил в базе: {rules_count}
- Паттернов: {patterns_count}
- Монеты с низким WR: {worst}
- Незакрытые вопросы: {[g[0] for g in gaps]}

ЗАДАЧА: Составь список из 3-5 тем для самообучения. Учитывай:
1. Какие SMC стратегии улучшат WR на слабых монетах
2. Какие рыночные паттерны не охвачены
3. Актуальные крипто-тренды которые влияют на сигналы
4. Технические паттерны для обнаружения китов

Ответь СТРОГО в JSON формате:
{{
  "agenda": [
    {{"topic": "название темы", "query": "поисковый запрос", "priority": 1-10, "reason": "почему важно"}},
    ...
  ]
}}"""

        response = _groq(prompt, max_tokens=600)
        if not response:
            return []

        clean = re.sub(r'```json|```', '', response).strip()
        data = json.loads(clean)
        items = data.get("agenda", [])

        # Сохраняем в agenda
        conn = sqlite3.connect(DB_PATH)
        for item in items:
            try:
                conn.execute("""INSERT OR IGNORE INTO learning_agenda (topic, priority, reason)
                    VALUES (?,?,?)""",
                    (item["topic"], item.get("priority", 5), item.get("reason", "")))
            except Exception:
                pass
        conn.commit()
        conn.close()

        logging.info(f"[WebLearner] Groq составил агенду: {[i['topic'] for i in items]}")
        return items

    except Exception as e:
        logging.error(f"groq_decide_learning_agenda: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
# GROQ ИССЛЕДУЕТ ТЕМУ
# ═══════════════════════════════════════════════════════════════

def groq_research_topic(topic: str, query: str) -> dict:
    """
    Groq ищет информацию по теме и извлекает полезные знания.
    Приоритет: RSS фиды → DuckDuckGo → крипто-сайты
    """
    # Guard against None args — источник ошибки 'NoneType' has no attribute 'lower'
    topic = (topic or "").strip()
    query = (query or topic).strip()
    if not topic:
        return {}
    try:
        logging.info(f"[WebLearner] Исследую тему: {topic}")

        texts = []

        # 1. Сначала RSS — работает стабильно с Render
        rss_items = get_rss_batch(limit_per_source=3)
        # Фильтруем по релевантности к теме
        query_words = set(query.lower().split())
        relevant_rss = []
        for item in rss_items:
            item_text = (item["title"] + " " + item.get("text", "")).lower()
            matches = sum(1 for w in query_words if w in item_text and len(w) > 3)
            if matches >= 1:
                relevant_rss.append(item)

        if relevant_rss:
            combined_rss = "\n\n".join([
                f"[{i['source']}] {i['title']}\n{i.get('text','')}"
                for i in relevant_rss[:5]
            ])
            texts.append({"url": "rss_feeds", "text": combined_rss[:2000]})
            logging.info(f"[WebLearner] RSS: {len(relevant_rss)} релевантных статей для '{topic}'")

        # 2. DuckDuckGo как дополнение
        if len(texts) < 2:
            try:
                urls = search_duckduckgo(query + " trading strategy crypto")
                for url in urls[:2]:
                    text = _fetch_url(url)
                    if text and len(text) > 300:
                        texts.append({"url": url, "text": text[:1500]})
                    time.sleep(0.5)
            except Exception:
                pass

        # 3. Крипто-сайты как последний резерв
        if not texts:
            texts = search_crypto_news(query)

        # 4. Базы знаний трейдеров — для SMC/технических тем
        if not texts or topic.lower() in ("smc", "order block", "fvg", "liquidity", "wyckoff"):
            for key in TRADER_KNOWLEDGE_URLS:
                if any(w in key for w in query.lower().split()):
                    knowledge = get_trader_knowledge(key)
                    if knowledge:
                        texts.append({"url": TRADER_KNOWLEDGE_URLS[key], "text": knowledge[:1500]})
                        break

        if not texts:
            logging.warning(f"[WebLearner] Нет данных для темы: {topic}")
            return {}

        combined = "\n\n---\n\n".join([f"Источник: {t['url']}\n{t['text']}" for t in texts[:3]])

        prompt = f"""Ты — AI трейдинг-аналитик APEX. Изучи найденные материалы и извлеки полезные знания.

ТЕМА: {topic}
ПОИСКОВЫЙ ЗАПРОС: {query}

НАЙДЕННЫЕ МАТЕРИАЛЫ:
{combined[:2000]}

ЗАДАЧА: Извлеки конкретные торговые знания применимые для криптовалют.

Ответь в JSON формате:
{{
  "summary": "краткое резюме что узнали (2-3 предложения)",
  "key_insights": ["инсайт 1", "инсайт 2", "инсайт 3"],
  "trading_rules": [
    {{"rule": "правило торговли", "type": "PREFER|AVOID|TIMING", "confidence": 0.0-1.0}},
    ...
  ],
  "strategy": {{
    "name": "название стратегии или null",
    "conditions": "когда применять",
    "win_rate_expected": 0.0-1.0 или null
  }},
  "relevance": 0.0-1.0
}}"""

        response = _groq(prompt, max_tokens=700)
        if not response:
            return {}

        clean = re.sub(r'```json|```', '', response).strip()
        data = json.loads(clean)

        # Сохраняем в базу
        conn = sqlite3.connect(DB_PATH)

        # Основные знания — safe insert с fallback
        try:
            conn.execute("""INSERT INTO web_knowledge (topic, query, summary, source_url, relevance)
                VALUES (?,?,?,?,?)""",
                (topic, query, data.get("summary", ""), texts[0]["url"] if texts else "",
                 float(data.get("relevance", 0.5))))
        except Exception:
            conn.execute("""INSERT INTO web_knowledge (topic, summary, source_url, relevance)
                VALUES (?,?,?,?)""",
                (topic, data.get("summary", ""), texts[0]["url"] if texts else "",
                 float(data.get("relevance", 0.5))))

        # Торговые правила → в self_rules
        for rule_item in data.get("trading_rules", []):
            rule_text = rule_item.get("rule", "")
            confidence = float(rule_item.get("confidence", 0.6))
            rule_type = (rule_item.get("type") or "PREFER").lower()
            if rule_text and confidence >= 0.6:
                conn.execute("""INSERT OR IGNORE INTO self_rules (rule_type, rule_text, confidence, source, created_at)
                    VALUES (?,?,?,?,CURRENT_TIMESTAMP)""",
                    (rule_type, rule_text, confidence, f"web_research:{topic}"))

        # Стратегия → в библиотеку
        strat = data.get("strategy", {})
        if strat.get("name"):
            try:
                conn.execute("""INSERT OR IGNORE INTO strategy_library
                    (name, description, conditions, win_rate_expected, source)
                    VALUES (?,?,?,?,?)""",
                    (strat["name"], data.get("summary", ""), strat.get("conditions", ""),
                     strat.get("win_rate_expected"), f"web_research"))
            except Exception:
                pass

        # Отмечаем тему как выполненную
        conn.execute("""UPDATE learning_agenda SET status='done', completed_at=CURRENT_TIMESTAMP
            WHERE topic=?""", (topic,))

        conn.commit()
        conn.close()

        logging.info(f"[WebLearner] ✅ Изучено: {topic} | Релевантность: {data.get('relevance', 0):.1f}")
        return data

    except Exception as e:
        logging.error(f"groq_research_topic {topic}: {e}")
        return {}


# ═══════════════════════════════════════════════════════════════
# ГЛАВНЫЙ ЦИКЛ САМООБУЧЕНИЯ
# ═══════════════════════════════════════════════════════════════

def run_web_learning_cycle():
    """
    Полный цикл автономного самообучения:
    1. Groq решает что изучить
    2. Ищет в интернете
    3. Извлекает и сохраняет знания
    """
    try:
        init_web_learner_db()
        logging.info("[WebLearner] 🔍 Начинаю цикл самообучения...")

        # Добавляем колонку query если нет (миграция)
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute("ALTER TABLE learning_agenda ADD COLUMN query TEXT")
            conn.commit()
            conn.close()
        except Exception:
            pass

        # Сначала проверяем незакрытые темы из агенды
        conn = sqlite3.connect(DB_PATH)
        try:
            pending = conn.execute("""
                SELECT topic, query FROM learning_agenda
                WHERE status='pending' ORDER BY priority DESC LIMIT 3
            """).fetchall()
        except Exception:
            # Старая БД без query — берём только topic
            rows = conn.execute("""
                SELECT topic FROM learning_agenda
                WHERE status='pending' ORDER BY priority DESC LIMIT 3
            """).fetchall()
            pending = [(r[0], r[0]) for r in rows]
        conn.close()

        if not pending:
            # Groq решает что изучать
            agenda = groq_decide_learning_agenda()
            pending = [(item["topic"], item["query"]) for item in agenda[:3]]

        results = []
        for topic, query in pending:
            result = groq_research_topic(topic, query)
            if result:
                results.append({"topic": topic, "result": result})
            time.sleep(2)  # Пауза между запросами

        logging.info(f"[WebLearner] ✅ Цикл завершён: изучено {len(results)} тем")
        return results

    except Exception as e:
        logging.error(f"run_web_learning_cycle: {e}")
        return []


def get_daily_market_digest() -> str:
    """
    Ежедневный дайджест из всех RSS источников.
    Groq анализирует топ новости и выдаёт торговые выводы.
    """
    try:
        logging.info("[WebLearner] 📰 Собираю дайджест рынка...")

        # Берём из всех источников
        all_items = get_rss_batch(limit_per_source=4)
        if not all_items:
            return ""

        # Топ-15 заголовков для Groq
        headlines = "\n".join([
            f"[{i['source']}] {i['title']}"
            for i in all_items[:15]
        ])

        prompt = f"""Ты — AI трейдер APEX. Проанализируй свежие новости крипторынка и дай торговые выводы.

СВЕЖИЕ НОВОСТИ:
{headlines}

Ответь в JSON:
{{
  "market_sentiment": "BULLISH|BEARISH|NEUTRAL",
  "key_events": ["событие 1", "событие 2", "событие 3"],
  "trading_implications": "что это значит для трейдера (2-3 предложения)",
  "coins_mentioned": ["BTC", "ETH", ...],
  "risk_level": "LOW|MEDIUM|HIGH",
  "action": "что делать прямо сейчас"
}}"""

        response = _groq(prompt, max_tokens=500)
        if not response:
            return ""

        clean = re.sub(r'```json|```', '', response).strip()
        data = json.loads(clean)

        # Сохраняем в web_knowledge
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute("""INSERT INTO web_knowledge (topic, query, summary, source_url, relevance)
                VALUES (?,?,?,?,?)""",
                ("daily_digest", "market_news",
                 f"Сентимент: {data.get('market_sentiment')} | {data.get('trading_implications','')}",
                 "rss_feeds", 0.9))
        except Exception:
            conn.execute("""INSERT INTO web_knowledge (topic, summary, source_url, relevance)
                VALUES (?,?,?,?)""",
                ("daily_digest",
                 f"Сентимент: {data.get('market_sentiment')} | {data.get('trading_implications','')}",
                 "rss_feeds", 0.9))
        conn.commit()
        conn.close()

        logging.info(f"[WebLearner] 📰 Дайджест: {data.get('market_sentiment')} | Риск: {data.get('risk_level')}")
        return data.get("trading_implications", "")

    except Exception as e:
        logging.error(f"get_daily_market_digest: {e}")
        return ""


def get_web_knowledge_summary() -> str:
    """Краткая сводка накопленных веб-знаний для отображения"""
    try:
        conn = sqlite3.connect(DB_PATH)
        total = conn.execute("SELECT COUNT(*) FROM web_knowledge").fetchone()[0]
        recent = conn.execute("""
            SELECT topic, summary FROM web_knowledge
            ORDER BY created_at DESC LIMIT 3
        """).fetchall()
        strategies = conn.execute("SELECT COUNT(*) FROM strategy_library").fetchone()[0]
        pending_agenda = conn.execute("""
            SELECT COUNT(*) FROM learning_agenda WHERE status='pending'
        """).fetchone()[0]
        conn.close()

        lines = [f"📚 Веб-знания: {total} тем | 📈 Стратегий: {strategies} | 📋 В очереди: {pending_agenda}"]
        if recent:
            lines.append("\n🔍 Последние изученные темы:")
            for topic, summary in recent:
                lines.append(f"• {topic}: {(summary or '')[:80]}...")
        return "\n".join(lines)
    except Exception as e:
        return f"WebLearner: {e}"


# ═══════════════════════════════════════════════════════════════
# GROQ САМОУЛУЧШЕНИЕ
# ═══════════════════════════════════════════════════════════════

def groq_self_improve():
    """
    Groq анализирует свои результаты и генерирует улучшения —
    новые правила для self_rules на основе статистики сделок.
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # Статистика за 7 дней
        stats = conn.execute("""
            SELECT symbol, result, rr_achieved, timeframe, confluence, regime
            FROM signal_log
            WHERE created_at > datetime('now', '-7 days')
            ORDER BY created_at DESC LIMIT 50
        """).fetchall()

        rules_count = conn.execute("SELECT COUNT(*) FROM self_rules").fetchone()[0]
        wins  = len([s for s in stats if s[1] in ('tp1','tp2','tp3')])
        losses = len([s for s in stats if s[1] == 'sl'])
        total = len(stats)
        wr = round(wins / total * 100, 1) if total > 0 else 0

        best  = [(s[0], s[2]) for s in stats if s[1] in ('tp2','tp3')][:5]
        worst = [(s[0], s[4]) for s in stats if s[1] == 'sl'][:5]

        conn.close()

        prompt = f"""Ты — AI торговый бот APEX. Проанализируй свои результаты и предложи улучшения.

СТАТИСТИКА (7 дней):
- Всего: {total} | WR: {wr}% | Победы: {wins} | Потери: {losses}
- Лучшие сделки (монета, RR): {best}
- Худшие (монета, confluence): {worst}
- Правил в базе: {rules_count}

Придумай 3-5 конкретных правил которые повысят WR.
Ответь в JSON:
{{
  "improvements": [
    {{
      "rule_type": "prefer|avoid|timing|risk",
      "rule_text": "текст правила",
      "confidence": 0.0-1.0
    }}
  ],
  "insight": "главный вывод"
}}"""

        response = _groq(prompt, max_tokens=600)
        if not response:
            return []

        import re, json
        clean = re.sub(r'```json|```', '', response).strip()
        data = json.loads(clean)
        improvements = data.get("improvements", [])

        conn = sqlite3.connect(DB_PATH)
        saved = 0
        for imp in improvements:
            rule_text = imp.get("rule_text")
            if rule_text and float(imp.get("confidence", 0)) >= 0.65:
                conn.execute("""INSERT OR IGNORE INTO self_rules
                    (rule_type, rule_text, confidence, source, created_at)
                    VALUES (?,?,?,?,CURRENT_TIMESTAMP)""",
                    (imp.get("rule_type", "prefer"), rule_text,
                     imp.get("confidence", 0.7), "groq_self_improve"))
                saved += 1
        conn.commit()
        conn.close()

        logging.info(f"[WebLearner] Groq самоулучшение: {saved} правил добавлено")
        return improvements

    except Exception as e:
        logging.error(f"groq_self_improve: {e}")
        return []
