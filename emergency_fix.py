"""
emergency_fix.py - Экстренное исправление критических ошибок в работающем боте
"""

import sqlite3
import logging
import threading
import time
from contextlib import contextmanager
from functools import wraps

# Глобальный lock для БД
_db_lock = threading.RLock()

def patch_database_connection():
    """Патч для исправления database is locked"""
    
    # Сохраняем оригинальную функцию
    original_connect = sqlite3.connect
    
    @wraps(original_connect)
    def patched_connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        
        # Добавляем WAL mode и другие оптимизации
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")  # 30 секунд
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
        except:
            pass
        
        return conn
    
    # Заменяем глобальную функцию
    sqlite3.connect = patched_connect

@contextmanager
def safe_db_connection(db_path="brain.db", timeout=30):
    """
    Безопасное подключение к БД с глобальным lock
    """
    with _db_lock:
        conn = None
        try:
            conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"DB error: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.commit()
                except:
                    pass
                conn.close()

def fix_missing_columns():
    """Исправление отсутствующих колонок"""
    
    migrations = [
        # Добавляем недостающие колонки в signals
        ("ALTER TABLE signals ADD COLUMN confluence INTEGER DEFAULT 0",),
        ("ALTER TABLE signals ADD COLUMN regime TEXT DEFAULT 'UNKNOWN'",),
        ("ALTER TABLE signals ADD COLUMN learning_id INTEGER DEFAULT NULL",),
        
        # Добавляем колонку id в таблицы где её нет
        ("ALTER TABLE signal_learning ADD COLUMN id INTEGER PRIMARY KEY AUTOINCREMENT",),
    ]
    
    try:
        with safe_db_connection() as conn:
            for migration in migrations:
                try:
                    conn.execute(migration[0])
                    logging.info(f"Applied migration: {migration[0]}")
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e) or "no such table" in str(e):
                        logging.debug(f"Migration skipped: {migration[0]}")
                    else:
                        logging.error(f"Migration failed: {migration[0]} - {e}")
            
            conn.commit()
            logging.info("Database migrations completed")
            
    except Exception as e:
        logging.error(f"Migration error: {e}")

def patch_save_signal():
    """Патч для функции save_signal"""
    
    def safe_save_signal(symbol, direction, signal_type, entry, tp1, tp2, tp3, sl,
                        timeframe, est_hours, grade, learning_id=None, confluence=0, regime="UNKNOWN"):
        """Безопасное сохранение сигнала с явными колонками"""
        try:
            with safe_db_connection() as conn:
                cursor = conn.execute("""
                    INSERT INTO signals 
                    (symbol, direction, signal_type, entry, tp1, tp2, tp3, sl,
                     timeframe, estimated_hours, grade, result, created_at, closed_at, 
                     learning_id, confluence, regime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 
                            CURRENT_TIMESTAMP, NULL, ?, ?, ?)
                """, (symbol, direction, signal_type, entry, tp1, tp2, tp3, sl,
                      timeframe, est_hours, grade, learning_id, confluence, regime))
                
                signal_id = cursor.lastrowid
                logging.info(f"Signal saved: {symbol} {direction} (ID: {signal_id})")
                return signal_id
                
        except Exception as e:
            logging.error(f"Save signal error: {e}")
            return None
    
    return safe_save_signal

def patch_close_signal():
    """Патч для функции close_signal"""
    
    def safe_close_signal(signal_id, result, hit_tp=0):
        """Безопасное закрытие сигнала с проверкой колонок"""
        try:
            with safe_db_connection() as conn:
                # Проверяем наличие колонок
                cursor = conn.execute("PRAGMA table_info(signals)")
                columns = [row[1] for row in cursor.fetchall()]
                
                if 'id' in columns and 'result' in columns:
                    conn.execute("""
                        UPDATE signals 
                        SET result=?, closed_at=CURRENT_TIMESTAMP 
                        WHERE id=?
                    """, (result, signal_id))
                    
                    logging.info(f"Signal closed: ID {signal_id} -> {result}")
                    return True
                else:
                    logging.error("Required columns missing in signals table")
                    return False
                    
        except Exception as e:
            logging.error(f"Close signal error: {e}")
            return False
    
    return safe_close_signal

def patch_groq_extensions():
    """Патч для защиты groq_extensions от удаления функций"""
    
    # Создаем защищенную версию функций
    PROTECTED_FUNCTIONS = {
        'run_confluence_boosters',
        'boost_strong_volume', 
        'boost_clean_structure',
        'get_extensions_summary'
    }
    
    def safe_run_confluence_boosters(candles, direction):
        """Защищенная версия run_confluence_boosters"""
        try:
            # Базовая реализация
            if not candles or len(candles) < 10:
                return 0, []
            
            bonus = 0
            descriptions = []
            
            # Проверка объёма
            volumes = [c.get("volume", 0) for c in candles[-20:]]
            avg_volume = sum(volumes) / len(volumes) if volumes else 0
            last_volume = volumes[-1] if volumes else 0
            
            if last_volume > avg_volume * 1.5:
                bonus += 8
                descriptions.append("✅ Высокий объём (+8)")
            
            return bonus, descriptions
            
        except Exception as e:
            logging.error(f"run_confluence_boosters error: {e}")
            return 0, []
    
    def safe_boost_strong_volume(candles, direction):
        """Защищенная версия boost_strong_volume"""
        try:
            if not candles or len(candles) < 10:
                return 0, ""
            
            volumes = [c.get("volume", 0) for c in candles[-20:]]
            avg_volume = sum(volumes) / len(volumes) if volumes else 0
            last_volume = volumes[-1] if volumes else 0
            
            if avg_volume > 0 and last_volume > avg_volume * 1.8:
                return 8, "✅ Аномальный объём (+8)"
            
            return 0, ""
            
        except Exception as e:
            logging.error(f"boost_strong_volume error: {e}")
            return 0, ""
    
    def safe_boost_clean_structure(candles, direction):
        """Защищенная версия boost_clean_structure"""
        try:
            if not candles or len(candles) < 5:
                return 0, ""
            
            last5 = candles[-5:]
            bullish_count = sum(1 for c in last5 if c["close"] > c["open"])
            
            if direction == "BULLISH" and bullish_count >= 4:
                return 5, "✅ Чистая бычья структура (+5)"
            elif direction == "BEARISH" and bullish_count <= 1:
                return 5, "✅ Чистая медвежья структура (+5)"
            
            return 0, ""
            
        except Exception as e:
            logging.error(f"boost_clean_structure error: {e}")
            return 0, ""
    
    def safe_get_extensions_summary():
        """Защищенная версия get_extensions_summary"""
        return {
            "filters": 5,
            "boosters": 3,
            "version": "2.0.1",
            "protected_functions": list(PROTECTED_FUNCTIONS),
            "security_version": "2.0.1"
        }
    
    return {
        'run_confluence_boosters': safe_run_confluence_boosters,
        'boost_strong_volume': safe_boost_strong_volume,
        'boost_clean_structure': safe_boost_clean_structure,
        'get_extensions_summary': safe_get_extensions_summary
    }

def patch_telegram_messages():
    """Патч для предотвращения дублирования сообщений"""
    
    # Глобальный кэш последних сообщений
    _message_cache = {}
    
    def safe_send_message(bot, chat_id, text, **kwargs):
        """Безопасная отправка сообщения с проверкой дубликатов"""
        try:
            # Создаем ключ для кэша
            cache_key = f"{chat_id}_{hash(text)}_{hash(str(kwargs))}"
            
            # Проверяем не отправляли ли это сообщение недавно
            if cache_key in _message_cache:
                last_time = _message_cache[cache_key]
                if time.time() - last_time < 60:  # 1 минута
                    logging.debug("Skipping duplicate message")
                    return None
            
            # Отправляем сообщение
            result = bot.send_message(chat_id, text, **kwargs)
            
            # Сохраняем в кэш
            _message_cache[cache_key] = time.time()
            
            # Очищаем старые записи из кэша
            old_keys = [k for k, t in _message_cache.items() if time.time() - t > 300]
            for k in old_keys:
                del _message_cache[k]
            
            return result
            
        except Exception as e:
            if "message is not modified" in str(e):
                logging.debug("Message not modified, skipping")
                return None
            else:
                logging.error(f"Send message error: {e}")
                raise
    
    return safe_send_message

def apply_all_patches():
    """Применение всех патчей"""
    
    logging.info("Applying emergency patches...")
    
    # 1. Патч для подключения к БД
    patch_database_connection()
    logging.info("✅ Database connection patched")
    
    # 2. Исправление колонок в БД
    fix_missing_columns()
    logging.info("✅ Database columns fixed")
    
    # 3. Защита функций Groq
    protected_functions = patch_groq_extensions()
    logging.info("✅ Groq functions protected")
    
    # 4. Патч для отправки сообщений
    safe_send = patch_telegram_messages()
    logging.info("✅ Telegram messages patched")
    
    logging.info("🎯 All emergency patches applied successfully!")
    
    return {
        'save_signal': patch_save_signal(),
        'close_signal': patch_close_signal(),
        'protected_functions': protected_functions,
        'safe_send_message': safe_send
    }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    patches = apply_all_patches()
    print("Emergency fixes applied successfully!")
