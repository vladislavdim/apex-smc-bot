"""
APEX Web Learner — автономный поиск знаний в интернете
Groq сам решает что искать, ищет, анализирует и сохраняет в brain.db
"""
import requests, sqlite3, logging, json, os, time, re
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brain.db")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = "llama3-70b-8192"

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
        return r.json()["choices"][0]["message"]["content"].strip()
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
    """
    try:
        logging.info(f"[WebLearner] Исследую тему: {topic}")

        # Сначала ищем в DuckDuckGo
        urls = search_duckduckgo(query + " trading strategy crypto")
        texts = []

        for url in urls[:2]:
            text = _fetch_url(url)
            if text and len(text) > 300:
                texts.append({"url": url, "text": text[:1500]})
            time.sleep(0.5)

        # Если ничего не нашли — пробуем крипто-источники
        if not texts:
            texts = search_crypto_news(query)

        if not texts:
            logging.warning(f"[WebLearner] Нет данных для темы: {topic}")
            return {}

        combined = "\n\n---\n\n".join([f"URL: {t['url']}\n{t['text']}" for t in texts])

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

        # Основные знания
        conn.execute("""INSERT INTO web_knowledge (topic, query, summary, source_url, relevance)
            VALUES (?,?,?,?,?)""",
            (topic, query, data.get("summary", ""), texts[0]["url"] if texts else "",
             float(data.get("relevance", 0.5))))

        # Торговые правила → в self_rules
        for rule_item in data.get("trading_rules", []):
            rule_text = rule_item.get("rule", "")
            confidence = float(rule_item.get("confidence", 0.6))
            rule_type = rule_item.get("type", "PREFER").lower()
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

        # Сначала проверяем незакрытые темы из агенды
        conn = sqlite3.connect(DB_PATH)
        pending = conn.execute("""
            SELECT topic, query FROM learning_agenda
            WHERE status='pending' ORDER BY priority DESC LIMIT 3
        """).fetchall()
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
