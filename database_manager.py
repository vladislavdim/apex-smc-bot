"""
database_manager.py - Исправленный менеджер базы данных с proper connection pooling
Решает проблемы: database is locked, race conditions, отсутствие commit/close
"""

import sqlite3
import threading
import logging
import time
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
from datetime import datetime

# Thread-safe connection pool
class DatabasePool:
    def __init__(self, db_path: str = "brain.db", max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self.pool = []
        self.lock = threading.Lock()
        self.active_connections = 0
        
        # Initialize WAL mode for all connections
        self._init_database()
        
        # Pre-create connections
        for _ in range(max_connections):
            conn = self._create_connection()
            self.pool.append(conn)
    
    def _create_connection(self) -> sqlite3.Connection:
        """Создаёт соединение с правильными настройками WAL"""
        conn = sqlite3.connect(
            self.db_path, 
            timeout=30, 
            check_same_thread=False
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn
    
    def _init_database(self):
        """Инициализирует структуру БД с правильными колонками"""
        conn = self._create_connection()
        try:
            c = conn.cursor()
            
            # ===== СИГНАЛЫ =====
            c.execute("""CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, direction TEXT, signal_type TEXT,
                entry REAL, tp1 REAL, tp2 REAL, tp3 REAL, sl REAL,
                timeframe TEXT, estimated_hours INTEGER, grade TEXT,
                result TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT,
                learning_id INTEGER DEFAULT NULL,
                confluence INTEGER DEFAULT 0,
                regime TEXT DEFAULT 'UNKNOWN')""")
            
            # signal_log — детальный лог
            c.execute("""CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, direction TEXT, grade TEXT,
                entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
                timeframe TEXT, result TEXT DEFAULT 'PENDING',
                hit_tp INTEGER DEFAULT 0, rr_achieved REAL DEFAULT 0,
                hours_open REAL DEFAULT 0, confluence INTEGER DEFAULT 0,
                regime TEXT, source TEXT, notes TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT)""")
            
            # observations — наблюдения бота о рынке
            c.execute("""CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, observation TEXT, context TEXT,
                outcome TEXT DEFAULT '', confirmed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # brain_log — лог всех событий мозга
            c.execute("""CREATE TABLE IF NOT EXISTS brain_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT, title TEXT, description TEXT,
                source TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # alerts
            c.execute("""CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, symbol TEXT, price REAL, direction TEXT,
                price_level REAL, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                triggered INTEGER DEFAULT 0)""")
            
            # knowledge
            c.execute("""CREATE TABLE IF NOT EXISTS knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT, content TEXT, source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # user_memory
            c.execute("""CREATE TABLE IF NOT EXISTS user_memory (
                user_id INTEGER PRIMARY KEY,
                name TEXT, profile TEXT, preferences TEXT,
                coins_mentioned TEXT, deposit REAL DEFAULT 0,
                risk_percent REAL DEFAULT 1.0,
                total_messages INTEGER DEFAULT 0,
                first_seen TEXT, last_seen TEXT)""")
            
            # chat_log
            c.execute("""CREATE TABLE IF NOT EXISTS chat_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, role TEXT, content TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # news_cache
            c.execute("""CREATE TABLE IF NOT EXISTS news_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT, content TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # signal_learning
            c.execute("""CREATE TABLE IF NOT EXISTS signal_learning (
                symbol TEXT PRIMARY KEY,
                total INTEGER DEFAULT 0, wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0, avg_hours_to_tp REAL DEFAULT 0,
                best_timeframe TEXT, worst_timeframe TEXT,
                win_rate REAL DEFAULT 0, last_analysis TEXT)""")
            
            # journal
            c.execute("""CREATE TABLE IF NOT EXISTS journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, symbol TEXT, direction TEXT,
                entry REAL, exit_price REAL, result TEXT,
                note TEXT, pnl_percent REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # bot_errors
            c.execute("""CREATE TABLE IF NOT EXISTS bot_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER, symbol TEXT, direction TEXT,
                entry REAL, sl REAL, result TEXT,
                error_type TEXT, error_description TEXT,
                ai_analysis TEXT, ai_lesson TEXT, ai_next_time TEXT,
                fixed INTEGER DEFAULT 0, fix_description TEXT,
                hours_in_trade REAL, market_context TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP, fixed_at TEXT)""")
            
            # signal_stats
            c.execute("""CREATE TABLE IF NOT EXISTS signal_stats (
                symbol TEXT PRIMARY KEY,
                total INTEGER DEFAULT 0, wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0, tp1_hits INTEGER DEFAULT 0,
                tp2_hits INTEGER DEFAULT 0, tp3_hits INTEGER DEFAULT 0,
                sl_hits INTEGER DEFAULT 0, expired INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0, avg_rr REAL DEFAULT 0.0,
                last_updated TEXT)""")
            
            # auto_rules
            c.execute("""CREATE TABLE IF NOT EXISTS auto_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_type TEXT, target TEXT, condition TEXT,
                confidence REAL DEFAULT 0.5, confirmed INTEGER DEFAULT 0,
                violated INTEGER DEFAULT 0, active INTEGER DEFAULT 1,
                last_check TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # self_analysis
            c.execute("""CREATE TABLE IF NOT EXISTS self_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period TEXT, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0, patterns TEXT, recommendations TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # pattern_history
            c.execute("""CREATE TABLE IF NOT EXISTS pattern_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_type TEXT, symbol TEXT, direction TEXT,
                result TEXT, hours_open REAL, timeframe TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # error_patterns
            c.execute("""CREATE TABLE IF NOT EXISTS error_patterns (
                error_type TEXT PRIMARY KEY,
                count INTEGER DEFAULT 1, last_seen TEXT,
                rule_added TEXT, active INTEGER DEFAULT 1)""")
            
            # self_rules - ИСПРАВЛЕНА СТРУКТУРА
            c.execute("""CREATE TABLE IF NOT EXISTS self_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT, rule TEXT, rule_type TEXT,
                rule_text TEXT, confidence REAL DEFAULT 0.5,
                confirmed_by INTEGER DEFAULT 0,
                contradicted_by INTEGER DEFAULT 0,
                source TEXT, active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # market_model
            c.execute("""CREATE TABLE IF NOT EXISTS market_model (
                symbol TEXT PRIMARY KEY,
                trend TEXT, key_levels TEXT, behavior TEXT,
                best_setup TEXT, avoid TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # learning_history
            c.execute("""CREATE TABLE IF NOT EXISTS learning_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT, title TEXT, description TEXT,
                after_value TEXT, impact_score REAL,
                source TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # session_stats
            c.execute("""CREATE TABLE IF NOT EXISTS session_stats (
                hour_utc INTEGER PRIMARY KEY,
                total INTEGER DEFAULT 0, wins INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0)""")
            
            # btc_correlation
            c.execute("""CREATE TABLE IF NOT EXISTS btc_correlation (
                symbol TEXT PRIMARY KEY,
                correlation REAL DEFAULT 0.0, beta REAL DEFAULT 1.0,
                samples INTEGER DEFAULT 0, last_updated TEXT)""")
            
            # streak_log
            c.execute("""CREATE TABLE IF NOT EXISTS streak_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result TEXT, streak_win INTEGER DEFAULT 0,
                streak_loss INTEGER DEFAULT 0, action_taken TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # grade_accuracy
            c.execute("""CREATE TABLE IF NOT EXISTS grade_accuracy (
                grade TEXT PRIMARY KEY,
                total INTEGER DEFAULT 0, wins INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0, avg_rr REAL DEFAULT 0.0,
                last_updated TEXT)""")
            
            # source_reliability
            c.execute("""CREATE TABLE IF NOT EXISTS source_reliability (
                source TEXT PRIMARY KEY,
                ok INTEGER DEFAULT 0, fail INTEGER DEFAULT 0,
                avg_candles REAL DEFAULT 0.0,
                last_ok TEXT, last_fail TEXT, notes TEXT)""")
            
            # barrier_log
            c.execute("""CREATE TABLE IF NOT EXISTS barrier_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, interval TEXT, source TEXT,
                success INTEGER DEFAULT 0, candles INTEGER DEFAULT 0,
                error TEXT, ts TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            # source_knowledge
            c.execute("""CREATE TABLE IF NOT EXISTS source_knowledge (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fact TEXT, context TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            
            conn.commit()
            logging.info("Database initialized successfully with WAL mode")
            
        except Exception as e:
            logging.error(f"Database initialization failed: {e}")
            raise
        finally:
            conn.close()
    
    @contextmanager
    def get_connection(self):
        """Thread-safe получение соединения из пула"""
        conn = None
        try:
            with self.lock:
                if self.pool:
                    conn = self.pool.pop()
                    self.active_connections += 1
                else:
                    # Если пул пуст, создаём новое соединение
                    conn = self._create_connection()
                    self.active_connections += 1
            
            yield conn
            
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.commit()
                except:
                    pass  # connection might be closed
                
                with self.lock:
                    if len(self.pool) < self.max_connections:
                        # Возвращаем в пул если есть место
                        self.pool.append(conn)
                    else:
                        # Иначе закрываем
                        conn.close()
                    self.active_connections -= 1
    
    def execute_query(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """Безопасное выполнение SELECT запроса"""
        with self.get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return cursor.fetchall()
    
    def execute_update(self, query: str, params: tuple = ()) -> int:
        """Безопасное выполнение INSERT/UPDATE/DELETE"""
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount
    
    def execute_insert(self, query: str, params: tuple = ()) -> int:
        """Безопасное выполнение INSERT с возвратом ID"""
        with self.get_connection() as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.lastrowid
    
    def get_stats(self) -> Dict[str, Any]:
        """Статистика пула соединений"""
        with self.lock:
            return {
                "pool_size": len(self.pool),
                "active_connections": self.active_connections,
                "max_connections": self.max_connections
            }

# Global instance
_db_pool = DatabasePool()

def get_db() -> DatabasePool:
    """Получить глобальный экземпляр пула БД"""
    return _db_pool

# ===== БЕЗОПАСНЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С БД =====

def safe_insert_user_memory(user_id: int, name: str = "", profile: str = "", 
                          preferences: str = "", coins: str = "", 
                          deposit: float = 0, risk_percent: float = 1.0) -> bool:
    """Безопасная вставка в user_memory с явными колонками"""
    try:
        now = datetime.now().isoformat()
        query = """
        INSERT OR REPLACE INTO user_memory 
        (user_id, name, profile, preferences, coins_mentioned, 
         deposit, risk_percent, total_messages, first_seen, last_seen) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (user_id, name, profile, preferences, coins, 
                  deposit, risk_percent, 0, now, now)
        
        _db_pool.execute_update(query, params)
        return True
    except Exception as e:
        logging.error(f"safe_insert_user_memory failed: {e}")
        return False

def safe_insert_self_rules(category: str, rule: str, rule_type: str = "auto",
                          rule_text: str = "", confidence: float = 0.5,
                          source: str = "auto") -> Optional[int]:
    """Безопасная вставка в self_rules с явными колонками"""
    try:
        now = datetime.now().isoformat()
        query = """
        INSERT INTO self_rules 
        (category, rule, rule_type, rule_text, confidence, source, active, created_at, updated_at) 
        VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        """
        params = (category, rule, rule_type, rule_text, confidence, source, now, now)
        
        return _db_pool.execute_insert(query, params)
    except Exception as e:
        logging.error(f"safe_insert_self_rules failed: {e}")
        return None

def safe_fetchone_or_empty(query: str, params: tuple = ()) -> tuple:
    """Безопасное получение одной строки с проверкой на None"""
    try:
        rows = _db_pool.execute_query(query, params)
        return tuple(rows[0]) if rows else ()
    except Exception as e:
        logging.error(f"safe_fetchone_or_empty failed: {e}")
        return ()

def safe_get_signal_confluence_regime(signal_id: int) -> tuple:
    """Безопасное получение confluence и regime для сигнала"""
    query = "SELECT confluence, regime FROM signals WHERE id = ?"
    result = safe_fetchone_or_empty(query, (signal_id,))
    
    if len(result) >= 2:
        confluence = result[0] if result[0] is not None else 0
        regime = result[1] if result[1] is not None else "UNKNOWN"
        return confluence, regime
    
    return 0, "UNKNOWN"

# ===== Миграции и проверки =====

def run_migrations():
    """Запускает миграции для существующих БД"""
    try:
        with _db_pool.get_connection() as conn:
            # Добавляем недостающие колонки если нужно
            migrations = [
                # alerts
                ("ALTER TABLE alerts ADD COLUMN price_level REAL",),
                # signals
                ("ALTER TABLE signals ADD COLUMN confluence INTEGER DEFAULT 0",),
                ("ALTER TABLE signals ADD COLUMN regime TEXT DEFAULT 'UNKNOWN'",),
                ("ALTER TABLE signals ADD COLUMN learning_id INTEGER DEFAULT NULL",),
                # signal_stats
                ("ALTER TABLE signal_stats ADD COLUMN tp1_hits INTEGER DEFAULT 0",),
                ("ALTER TABLE signal_stats ADD COLUMN tp2_hits INTEGER DEFAULT 0",),
                ("ALTER TABLE signal_stats ADD COLUMN tp3_hits INTEGER DEFAULT 0",),
                ("ALTER TABLE signal_stats ADD COLUMN sl_hits INTEGER DEFAULT 0",),
                ("ALTER TABLE signal_stats ADD COLUMN expired INTEGER DEFAULT 0",),
                ("ALTER TABLE signal_stats ADD COLUMN avg_rr REAL DEFAULT 0.0",),
                # self_rules
                ("ALTER TABLE self_rules ADD COLUMN rule_type TEXT",),
                ("ALTER TABLE self_rules ADD COLUMN rule_text TEXT",),
                ("ALTER TABLE self_rules ADD COLUMN source TEXT",),
                ("ALTER TABLE self_rules ADD COLUMN category TEXT",),
                ("ALTER TABLE self_rules ADD COLUMN confirmed_by INTEGER DEFAULT 0",),
                ("ALTER TABLE self_rules ADD COLUMN contradicted_by INTEGER DEFAULT 0",),
                ("ALTER TABLE self_rules ADD COLUMN updated_at TEXT DEFAULT CURRENT_TIMESTAMP",),
                ("ALTER TABLE self_rules ADD COLUMN active INTEGER DEFAULT 1",),
            ]
            
            for migration in migrations:
                try:
                    conn.execute(migration[0])
                except sqlite3.OperationalError:
                    pass  # Колонка уже существует
            
            # Активируем старые записи
            try:
                conn.execute("UPDATE self_rules SET active=1 WHERE active IS NULL")
            except:
                pass
            
            conn.commit()
            logging.info("Database migrations completed successfully")
            
    except Exception as e:
        logging.error(f"Migration failed: {e}")

# Запускаем миграции при импорте
run_migrations()
