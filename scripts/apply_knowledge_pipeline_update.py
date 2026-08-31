from pathlib import Path
import re


def replace_once(path: str, old: str, new: str):
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected 1 match, got {count}: {old[:100]!r}")
    p.write_text(text.replace(old, new, 1))


# 1) Replace legacy SMC seed with a conservative, auditable core knowledge base.
p = Path('core/learning.py')
text = p.read_text()
start = text.index('def _seed_smc_knowledge():')
end = text.index('\ndef save_signal(', start)
new_seed = r'''def _seed_smc_knowledge():
    """Install conservative core trading knowledge; archive legacy seed claims.

    Core rules contain definitions and safety principles only. They intentionally
    avoid unsupported hit-rate percentages or deterministic market claims.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # Preserve historical rows for audit, but stop old categorical seed rules
        # from influencing prompts/strategy decisions.
        conn.execute(
            "UPDATE self_rules SET active=0, updated_at=CURRENT_TIMESTAMP "
            "WHERE source='smc_seed' AND active=1"
        )
        existing = conn.execute(
            "SELECT COUNT(*) FROM self_rules WHERE source='core_seed_v2' AND active=1"
        ).fetchone()[0]
        if existing >= 20:
            conn.commit()
            conn.close()
            return

        core_rules = [
            ("structure", "Определяй направление по подтверждённой структуре закрытых свечей; не считай wick-прокол подтверждённым BOS/CHoCH.", 0.95),
            ("structure", "BOS/CHoCH является структурным событием; для входа используй свежесть события и контекст конкретной стратегии.", 0.95),
            ("liquidity", "Liquidity sweep сам по себе не является входом: нужен возврат/реакция и структурное подтверждение в логике стратегии.", 0.94),
            ("zone", "Order Block рассматривай как потенциальную зону реакции, а не гарантию разворота; учитывай свежесть, структуру и invalidation.", 0.92),
            ("zone", "FVG является зоной дисбаланса/контекста, а не гарантией заполнения или разворота; подтверждай его структурой.", 0.93),
            ("zone", "Premium/Discount используй относительно подтверждённого dealing range и направления идеи, а не как самостоятельный сигнал.", 0.91),
            ("entry", "Не догоняй цену: entry должен оставаться связанным с исходной структурной зоной и быть актуальным на момент отправки.", 0.95),
            ("risk", "SL ставится за структурную invalidation сделки; нельзя сдвигать стоп внутрь структуры только ради улучшения RR.", 0.99),
            ("risk", "TP выбирается по реальной структурной цели/ликвидности/opposing zone; нельзя выдумывать TP только ради математического RR.", 0.99),
            ("risk", "После выбора структурных Entry/SL/TP вычисляй фактический RR и отклоняй сделку, если он ниже минимума конкретной стратегии.", 0.99),
            ("mtf", "Старший таймфрейм задаёт контекст, а младший — триггер; обязательность полного совпадения таймфреймов определяется стратегией, а не общим правилом.", 0.93),
            ("confirmation", "Volume и displacement усиливают качество сетапа, но не должны подменять структуру и зону входа.", 0.92),
            ("derivatives", "Open Interest, funding и liquidations — контекст позиционирования и риска; ни один из них не создаёт торговый сигнал самостоятельно.", 0.96),
            ("external", "Whale/smart-money/exchange-flow данные используются как подтверждение или предупреждение только если свежие и достаточно качественные.", 0.95),
            ("external", "Один внешний источник не может создать сигнал; существенные противоречия источников должны снижать уверенность или переводить кандидата в WAIT.", 0.97),
            ("news", "Высоко-impact события и свежие новости меняют риск/волатильность, но направление сделки должно оставаться подтверждено рыночной структурой.", 0.94),
            ("context", "BTC-контекст для альткоинов является фильтром качества/риска; его жёсткость должна зависеть от стратегии и режима рынка.", 0.90),
            ("wyckoff", "Wyckoff-сетап требует распознаваемого диапазона и последовательности фаз/событий; отдельный Spring или UTAD без контекста не достаточен.", 0.94),
            ("fast", "FAST использует только свежие закрытые свечи, ликвидный торговый период и актуальный entry; старые триггеры повторно не использовать.", 0.96),
            ("risk", "Размер позиции определяется допустимым риском и расстоянием до структурного SL; плечо не должно увеличивать заданный риск на сделку.", 0.99),
            ("data", "Если данных нет или источник недоступен, отмечай это явно и не придумывай значения или подтверждения.", 0.99),
            ("learning", "Изменять торговые правила по статистике только на реально активированных сделках с объективным TP/SL исходом; WAIT/REJECT не считать результатом сделки.", 0.99),
        ]
        for category, rule_text, confidence in core_rules:
            conn.execute("""INSERT OR IGNORE INTO self_rules
                (category, rule, rule_type, rule_text, confidence, source, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'core_seed_v2', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                (category, rule_text, category, rule_text, confidence))
        conn.commit()
        conn.close()
        logging.info("[Learning] Core knowledge v2 loaded: %s rules; legacy smc_seed archived", len(core_rules))
    except Exception as e:
        logging.error(f"_seed_smc_knowledge: {e}")
'''
text = text[:start] + new_seed + text[end:]
p.write_text(text)

# 2) Web research becomes candidate knowledge, never an immediate active trading rule.
p = Path('web_learner.py')
text = p.read_text()
marker = '''        conn.execute("""CREATE TABLE IF NOT EXISTS strategy_library (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            name TEXT UNIQUE,\n            description TEXT,\n            conditions TEXT,\n            win_rate_expected REAL,\n            tested INTEGER DEFAULT 0,\n            actual_win_rate REAL,\n            source TEXT,\n            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")\n        conn.commit()\n'''
replacement = '''        conn.execute("""CREATE TABLE IF NOT EXISTS strategy_library (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            name TEXT UNIQUE,\n            description TEXT,\n            conditions TEXT,\n            win_rate_expected REAL,\n            tested INTEGER DEFAULT 0,\n            actual_win_rate REAL,\n            source TEXT,\n            created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")\n\n        conn.execute("""CREATE TABLE IF NOT EXISTS knowledge_candidates (\n            id INTEGER PRIMARY KEY AUTOINCREMENT,\n            topic TEXT,\n            rule_type TEXT,\n            rule_text TEXT,\n            confidence REAL DEFAULT 0.5,\n            source_url TEXT,\n            status TEXT DEFAULT 'candidate',\n            created_at TEXT DEFAULT CURRENT_TIMESTAMP,\n            reviewed_at TEXT,\n            UNIQUE(topic, rule_text))""")\n        conn.commit()\n'''
if marker not in text:
    raise SystemExit('web_learner init marker not found')
text = text.replace(marker, replacement, 1)
old_rules = '''            # Торговые правила → в self_rules\n            for rule_item in data.get("trading_rules", []):\n                rule_text = rule_item.get("rule", "")\n                try:\n                    confidence = float(str(rule_item.get("confidence", 0.6)).split()[0])\n                except (ValueError, TypeError):\n                    confidence = 0.6\n                rule_type = (rule_item.get("type") or "PREFER").lower()\n                if rule_text and confidence >= 0.6:\n                    conn.execute("""INSERT OR IGNORE INTO self_rules (rule_type, rule_text, confidence, source, created_at, active)\n                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1)""",\n                        (rule_type, rule_text, confidence, f"web_research:{topic}"))\n'''
new_rules = '''            # Web findings are candidate knowledge only. Internet text must not\n            # silently become an active trading rule without later validation.\n            for rule_item in data.get("trading_rules", []):\n                rule_text = (rule_item.get("rule") or "").strip()\n                try:\n                    confidence = float(str(rule_item.get("confidence", 0.6)).split()[0])\n                except (ValueError, TypeError):\n                    confidence = 0.6\n                rule_type = (rule_item.get("type") or "PREFER").lower()\n                if rule_text and confidence >= 0.6:\n                    conn.execute("""INSERT OR IGNORE INTO knowledge_candidates\n                        (topic, rule_type, rule_text, confidence, source_url, status, created_at)\n                        VALUES (?, ?, ?, ?, ?, 'candidate', CURRENT_TIMESTAMP)""",\n                        (topic, rule_type, rule_text, confidence, texts[0]["url"] if texts else ""))\n'''
if old_rules not in text:
    raise SystemExit('web_learner self_rules block not found')
text = text.replace(old_rules, new_rules, 1)
p.write_text(text)

# 3) Telegram knowledge screen: count each learning channel honestly, keep execution block untouched.
p = Path('bot.py')
text = p.read_text()
old_counts = '''            rule_count = (conn.execute("SELECT COUNT(*) FROM self_rules").fetchone() or [0])[0]\n            top_rules = conn.execute(\n                "SELECT category, rule, confidence FROM self_rules ORDER BY confidence DESC LIMIT 5"\n            ).fetchall()\n            obs_count = (conn.execute("SELECT COUNT(*) FROM observations").fetchone() or [0])[0]\n            model_count = (conn.execute("SELECT COUNT(*) FROM market_model").fetchone() or [0])[0]\n            # avoid_count — проверяем оба варианта (старый category и новый rule_type)\n            avoid_count = (conn.execute(\n                "SELECT COUNT(*) FROM self_rules WHERE rule_type='avoid' OR category='avoid'"\n            ).fetchone() or [0])[0]\n            # knowledge_count — из таблицы knowledge напрямую\n            knowledge_count = (conn.execute("SELECT COUNT(*) FROM knowledge").fetchone() or [0])[0]\n            # pattern_count — из signal_log\n            try:\n                pattern_count = (conn.execute("SELECT COUNT(*) FROM signal_log").fetchone() or [0])[0]\n            except Exception as e:\n                import logging\n                logging.error(e)\n                pattern_count = 0\n            # coin_count — правила по монетам\n            try:\n                coin_count = (conn.execute(\n                    "SELECT COUNT(DISTINCT symbol) FROM signal_log WHERE symbol IS NOT NULL"\n                ).fetchone() or [0])[0]\n            except Exception as e:\n                import logging\n                logging.error(e)\n                coin_count = 0\n            conn.close()\n'''
new_counts = '''            rule_count = (conn.execute("SELECT COUNT(*) FROM self_rules WHERE active=1").fetchone() or [0])[0]\n            core_rule_count = (conn.execute("SELECT COUNT(*) FROM self_rules WHERE active=1 AND source='core_seed_v2'").fetchone() or [0])[0]\n            trade_rule_count = (conn.execute("SELECT COUNT(*) FROM self_rules WHERE active=1 AND source='groq_trade_analysis'").fetchone() or [0])[0]\n            other_rule_count = max(0, rule_count - core_rule_count - trade_rule_count)\n            top_rules = conn.execute(\n                "SELECT category, rule, confidence FROM self_rules WHERE active=1 ORDER BY confidence DESC LIMIT 5"\n            ).fetchall()\n            obs_count = (conn.execute("SELECT COUNT(*) FROM observations").fetchone() or [0])[0]\n            model_count = (conn.execute("SELECT COUNT(*) FROM market_model").fetchone() or [0])[0]\n            avoid_count = (conn.execute(\n                "SELECT COUNT(*) FROM self_rules WHERE active=1 AND (rule_type='avoid' OR category='avoid')"\n            ).fetchone() or [0])[0]\n            knowledge_count = (conn.execute("SELECT COUNT(*) FROM knowledge").fetchone() or [0])[0]\n            try:\n                web_knowledge_count = (conn.execute("SELECT COUNT(*) FROM web_knowledge").fetchone() or [0])[0]\n                web_24h = (conn.execute("SELECT COUNT(*) FROM web_knowledge WHERE created_at >= datetime('now','-24 hours')").fetchone() or [0])[0]\n                last_web = (conn.execute("SELECT MAX(created_at) FROM web_knowledge").fetchone() or [None])[0]\n            except Exception:\n                web_knowledge_count = web_24h = 0\n                last_web = None\n            try:\n                candidate_count = (conn.execute("SELECT COUNT(*) FROM knowledge_candidates WHERE status='candidate'").fetchone() or [0])[0]\n            except Exception:\n                candidate_count = 0\n            try:\n                pending_topics = (conn.execute("SELECT COUNT(*) FROM learning_agenda WHERE status='pending'").fetchone() or [0])[0]\n            except Exception:\n                pending_topics = 0\n            try:\n                pattern_count = (conn.execute("SELECT COUNT(*) FROM pattern_memory").fetchone() or [0])[0]\n            except Exception:\n                pattern_count = 0\n            try:\n                coin_count = (conn.execute(\n                    "SELECT COUNT(DISTINCT symbol) FROM signal_log WHERE symbol IS NOT NULL AND result IN ('tp1','tp2','tp3','sl')"\n                ).fetchone() or [0])[0]\n            except Exception:\n                coin_count = 0\n            try:\n                last_trade_learning = (conn.execute("SELECT MAX(created_at) FROM brain_log WHERE event_type='trade_analysis'").fetchone() or [None])[0]\n            except Exception:\n                last_trade_learning = None\n            conn.close()\n'''
if old_counts not in text:
    raise SystemExit('bot knowledge count block not found')
text = text.replace(old_counts, new_counts, 1)
old_fallback = '''            rule_count = obs_count = model_count = avoid_count = 0\n            knowledge_count = pattern_count = coin_count = 0\n            top_rules = []\n            macro_summary = bb_rules = macro_time = ""\n'''
new_fallback = '''            rule_count = core_rule_count = trade_rule_count = other_rule_count = 0\n            obs_count = model_count = avoid_count = 0\n            knowledge_count = web_knowledge_count = web_24h = candidate_count = pending_topics = 0\n            pattern_count = coin_count = 0\n            last_web = last_trade_learning = None\n            top_rules = []\n            macro_summary = bb_rules = macro_time = ""\n'''
if old_fallback not in text:
    raise SystemExit('bot knowledge fallback block not found')
text = text.replace(old_fallback, new_fallback, 1)
old_screen = '''            f"📚 Записей знаний: <b>{knowledge_count}</b>\\n"\n            f"📈 SMC-паттернов: <b>{pattern_count}</b>\\n"\n            f"📌 Торговых правил: <b>{rule_count}</b>\\n"\n            f"⛔️ Антипаттернов: <b>{avoid_count}</b>\\n"\n            f"👁 Наблюдений рынка: <b>{obs_count}</b>\\n"\n            f"🗂 Моделей монет: <b>{model_count}</b>\\n"\n            f"🪙 Пар с историей: <b>{coin_count}</b>\\n"\n'''
new_screen = '''            f"📚 База знаний: <b>{knowledge_count + web_knowledge_count}</b>\\n"\n            f"🌐 Web Research: <b>{web_knowledge_count}</b> · за 24ч <b>{web_24h}</b>\\n"\n            f"🧠 Базовые core-правила: <b>{core_rule_count}</b>\\n"\n            f"📈 Правила из сделок: <b>{trade_rule_count}</b>\\n"\n            f"📌 Другие активные правила: <b>{other_rule_count}</b> · всего <b>{rule_count}</b>\\n"\n            f"🧪 Кандидатов из исследований: <b>{candidate_count}</b>\\n"\n            f"⏳ Тем на исследование: <b>{pending_topics}</b>\\n"\n            f"📊 Паттернов из истории: <b>{pattern_count}</b>\\n"\n            f"⛔️ Антипаттернов: <b>{avoid_count}</b>\\n"\n            f"👁 Наблюдений рынка: <b>{obs_count}</b>\\n"\n            f"🗂 Моделей монет: <b>{model_count}</b>\\n"\n            f"🪙 Пар с закрытой историей: <b>{coin_count}</b>\\n"\n            f"🕐 Последний WebLearner: <b>{last_web or '—'}</b>\\n"\n            f"🎓 Последнее обучение по сделке: <b>{last_trade_learning or '—'}</b>\\n"\n'''
if old_screen not in text:
    raise SystemExit('bot knowledge screen block not found')
text = text.replace(old_screen, new_screen, 1)

# 4) Polling mode previously omitted WebLearner/BrainBuilder scheduling. Add the same safe cycles.
old_sched = '''            scheduler.add_job(autonomous_learning_cycle, "interval", hours=1, jitter=120)\n            if _LEARNING_OK:\n'''
new_sched = '''            scheduler.add_job(autonomous_learning_cycle, "interval", hours=1, jitter=120)\n            if BRAIN_BUILDER_AVAILABLE:\n                scheduler.add_job(run_brain_builder_async, "interval", hours=1, jitter=300, max_instances=1, coalesce=True)\n                scheduler.add_job(run_brain_builder_full_async, "cron", hour=3, minute=0, timezone="UTC", max_instances=1, coalesce=True)\n            if _WEB_LEARNER_OK:\n                async def _polling_web_learner():\n                    try:\n                        results = await asyncio.to_thread(_web_learn_cycle)\n                        logging.info("[WebLearner] polling cycle complete: %s topic(s)", len(results or []))\n                        await backup_db_to_github()\n                    except Exception as exc:\n                        logging.warning("[WebLearner] polling cycle failed safely: %s", exc)\n                scheduler.add_job(_polling_web_learner, "interval", hours=1, jitter=300, max_instances=1, coalesce=True)\n                asyncio.get_running_loop().call_later(300, lambda: asyncio.create_task(_polling_web_learner()))\n            if _LEARNING_OK:\n'''
if old_sched not in text:
    raise SystemExit('polling scheduler marker not found')
text = text.replace(old_sched, new_sched, 1)
p.write_text(text)

# 5) Tests for core seed archive and web candidate isolation.
test = r'''import os
import sqlite3
import tempfile
import unittest
from unittest import mock


class KnowledgePipelineTests(unittest.TestCase):
    def test_core_seed_has_no_legacy_pseudo_statistics(self):
        text = Path('core/learning.py').read_text()
        block = text[text.index('def _seed_smc_knowledge():'):text.index('\ndef save_signal(', text.index('def _seed_smc_knowledge():'))]
        self.assertIn("source='core_seed_v2'", block)
        self.assertIn("source='smc_seed' AND active=1", block)
        self.assertNotIn('70-80%', block)
        self.assertNotIn('выше на 30%', block)
        self.assertNotIn('манипуляция гарантирована', block)

    def test_web_research_does_not_activate_self_rules(self):
        text = Path('web_learner.py').read_text()
        self.assertIn('CREATE TABLE IF NOT EXISTS knowledge_candidates', text)
        self.assertIn("'candidate'", text)
        research = text[text.index('def groq_research_topic'):text.index('def run_web_learning_cycle')]
        self.assertNotIn('INSERT OR IGNORE INTO self_rules', research)

    def test_polling_mode_schedules_web_learning(self):
        text = Path('bot.py').read_text()
        polling = text[text.index('async def polling_main():'):]
        self.assertIn('scheduler.add_job(_polling_web_learner, "interval", hours=1', polling)
        self.assertIn('scheduler.add_job(run_brain_builder_async, "interval", hours=1', polling)


from pathlib import Path

if __name__ == '__main__':
    unittest.main()
'''
Path('tests/test_knowledge_pipeline.py').write_text(test)
