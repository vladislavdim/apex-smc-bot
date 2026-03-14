"""
bot_fixed.py - Исправленная версия бота с интеграцией всех оптимизаций
Решает проблемы: database locking, race conditions, API rate limiting, scanner performance
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

# Импортируем наши новые модули
from database_manager import get_db
from market_data import get_candles_cached, get_top_pairs
from ai_manager import get_ai_manager, groq_call_async
from scheduler_manager import get_scheduler, add_scheduled_task, scheduled_task
from market_scanner import get_scanner, scan_market_async, SignalQuality

# Telegram импорты
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiohttp import web

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация компонентов
db = get_db()
ai_manager = get_ai_manager()
scheduler = get_scheduler()
scanner = get_scanner()

# Telegram конфигурация
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
SIGNAL_CHANNEL = int(os.environ.get("SIGNAL_CHANNEL", 0))

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ===== КЛАВИАТУРЫ =====

def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Найти сделки", callback_data="menu_find_deals"),
         InlineKeyboardButton(text="📊 Рынок сейчас", callback_data="menu_market")],
        [InlineKeyboardButton(text="🔍 Сканировать рынок", callback_data="menu_scan"),
         InlineKeyboardButton(text="📈 Статистика", callback_data="menu_stats")],
        [InlineKeyboardButton(text="🧠 Мозг APEX", callback_data="menu_brain")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu_settings")]
    ])

def tf_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5 мин", callback_data="tf_5m"),
         InlineKeyboardButton(text="15 мин", callback_data="tf_15m"),
         InlineKeyboardButton(text="1 час", callback_data="tf_1h")],
        [InlineKeyboardButton(text="4 часа", callback_data="tf_4h"),
         InlineKeyboardButton(text="1 день", callback_data="tf_1d")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="menu_back")]
    ])

# ===== ОБРАБОТЧИКИ КОМАНД =====

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Команда /start"""
    await message.answer(
        "🤖 <b>APEX SMC Bot v2.0</b>\n\n"
        "Исправленная версия с:\n"
        "✅ Защитой от database locking\n"
        "✅ Оптимизированным сканером 200+ пар\n"
        "✅ Rate limiting для AI API\n"
        "✅ Защитой от race conditions\n\n"
        "Выберите действие:",
        reply_markup=main_menu(),
        parse_mode="HTML"
    )

@dp.message(Command("scan"))
async def cmd_scan(message: types.Message):
    """Команда /scan - сканирование рынка"""
    await message.answer("🔍 Сканирую рынок... Это может занять до 2 минут.")
    
    try:
        # Запускаем сканирование в фоне
        results = await scan_market_async(
            timeframes=["15m", "1h", "4h"],
            min_quality=SignalQuality.GOOD
        )
        
        if not results:
            await message.answer("😴 Сигналы не найдены. Рынок спокоен.")
            return
        
        # Формируем сообщение с топ сигналами
        response = f"🎯 <b>Найдено сигналов: {len(results)}</b>\n\n"
        
        for i, result in enumerate(results[:5], 1):
            direction_emoji = "🟢" if result.direction == "BULLISH" else "🔴"
            response += (
                f"{i}. {direction_emoji} <b>{result.symbol}</b> {result.grade}\n"
                f"Entry: {result.entry:.6f}\n"
                f"SL: {result.sl:.6f} | TP1: {result.tp1:.6f}\n"
                f"Confluence: {result.confluence} | Timeframe: {result.timeframe}\n\n"
            )
        
        await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Scan command error: {e}")
        await message.answer("❌ Ошибка при сканировании. Попробуйте позже.")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Команда /stats - статистика"""
    try:
        # Статистика сканера
        scan_stats = scanner.get_scan_stats()
        
        # Статистика AI
        ai_stats = ai_manager.get_stats()
        
        # Статистика БД
        db_stats = db.get_stats()
        
        response = (
            f"📊 <b>Статистика APEX Bot</b>\n\n"
            f"🔍 <b>Сканер:</b>\n"
            f"• Всего символов: {scan_stats.total_symbols}\n"
            f"• Успешных сканов: {scan_stats.successful_scans}\n"
            f"• Топ сигналов: {scan_stats.top_signals}\n"
            f"• Время сканирования: {scan_stats.scan_time:.2f}s\n\n"
            f"🤖 <b>AI:</b>\n"
            f"• Всего вызовов: {ai_stats['usage']['total_calls']}\n"
            f"• Успешных: {ai_stats['usage']['successful_calls']}\n"
            f"• Токенов сегодня: {ai_stats['usage']['daily_tokens']}\n\n"
            f"💾 <b>База данных:</b>\n"
            f"• Активных соединений: {db_stats['active_connections']}\n"
            f"• В пуле: {db_stats['pool_size']}"
        )
        
        await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")
        await message.answer("❌ Ошибка при получении статистики.")

@dp.message(Command("symbol"))
async def cmd_symbol(message: types.Message):
    """Команда /symbol - анализ одного символа"""
    try:
        # Получаем символ из команды
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Использование: /symbol BTCUSDT")
            return
        
        symbol = parts[1].upper()
        await message.answer(f"🔍 Анализирую {symbol}...")
        
        # Сканируем один символ
        result = scanner.scan_single_symbol(symbol)
        
        if not result:
            await message.answer(f"😴 Нет сигнала по {symbol}")
            return
        
        # Формируем ответ
        direction_emoji = "🟢" if result.direction == "BULLISH" else "🔴"
        response = (
            f"{direction_emoji} <b>{symbol}</b> {result.grade}\n\n"
            f"📍 Entry: {result.entry:.6f}\n"
            f"🛡️ SL: {result.sl:.6f}\n"
            f"🎯 TP1: {result.tp1:.6f}\n"
            f"📊 Confluence: {result.confluence}\n"
            f"📈 Regime: {result.regime}\n"
            f"⏰ Timeframe: {result.timeframe}\n"
            f"🔍 Source: {result.source}\n"
            f"⚡ Analysis time: {result.analysis_time:.3f}s"
        )
        
        await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Symbol command error: {e}")
        await message.answer("❌ Ошибка при анализе символа.")

# ===== ОБРАБОТЧИКИ CALLBACK =====

@dp.callback_query(lambda c: c.data.startswith("menu_"))
async def handle_menu(callback: CallbackQuery):
    """Обработка меню"""
    action = callback.data.replace("menu_", "")
    
    if action == "find_deals":
        await callback.message.edit_text("🔍 Ищу лучшие сделки...")
        try:
            results = await scan_market_async(min_quality=SignalQuality.TOP)
            
            if not results:
                await callback.message.edit_text("😴 Топ сигналы не найдены")
                return
            
            response = f"🎯 <b>Топ сигналы ({len(results)})</b>\n\n"
            
            for i, result in enumerate(results[:3], 1):
                direction_emoji = "🟢" if result.direction == "BULLISH" else "🔴"
                response += (
                    f"{i}. {direction_emoji} <b>{result.symbol}</b> {result.grade}\n"
                    f"Entry: {result.entry:.6f}\n"
                    f"Confluence: {result.confluence}\n\n"
                )
            
            await callback.message.edit_text(response, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Find deals error: {e}")
            await callback.message.edit_text("❌ Ошибка при поиске сделок")
    
    elif action == "scan":
        await callback.message.edit_text("🔍 Запускаю полное сканирование...")
        # Перенаправляем на команду /scan
        await cmd_scan(callback.message)
    
    elif action == "stats":
        await cmd_stats(callback.message)
    
    elif action == "market":
        await callback.message.edit_text(
            "📊 <b>Рынок</b>\n\n"
            "Используйте /symbol <PAIR> для анализа конкретной пары\n"
            "Или /scan для полного сканирования рынка"
        )
    
    elif action == "brain":
        try:
            ai_stats = ai_manager.get_stats()
            response = (
                f"🧠 <b>Мозг APEX</b>\n\n"
                f"📊 Использование AI:\n"
                f"• Всего вызовов: {ai_stats['usage']['total_calls']}\n"
                f"• Успешность: {(ai_stats['usage']['successful_calls']/max(1,ai_stats['usage']['total_calls'])*100):.1f}%\n"
                f"• Токенов сегодня: {ai_stats['usage']['daily_tokens']}/{ai_manager.daily_token_limit}\n"
                f"• Дневной лимит: {ai_manager.get_daily_usage_percent():.1f}%\n\n"
                f"🤖 Доступные модели:\n"
            )
            
            for name, model in ai_stats['models'].items():
                status_emoji = "✅" if model['status'] == "available" else "❌"
                response += f"{status_emoji} {name}\n"
            
            await callback.message.edit_text(response, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Brain menu error: {e}")
            await callback.message.edit_text("❌ Ошибка получения данных AI")
    
    elif action == "settings":
        await callback.message.edit_text(
            "⚙️ <b>Настройки</b>\n\n"
            "Доступные команды:\n"
            "/scan - Сканирование рынка\n"
            "/stats - Статистика\n"
            "/symbol <PAIR> - Анализ символа\n\n"
            "Бот работает в оптимизированном режиме v2.0"
        )
    
    await callback.answer()

# ===== ЗАДАЧИ SCHEDULER =====

@scheduled_task(name="market_scan", interval=900, timeout=300)  # Каждые 15 минут
async def scheduled_market_scan():
    """Плановое сканирование рынка"""
    try:
        logger.info("Starting scheduled market scan")
        
        results = await scan_market_async(
            timeframes=["15m", "1h", "4h"],
            min_quality=SignalQuality.TOP
        )
        
        if results and SIGNAL_CHANNEL:
            # Отправляем топ сигнал в канал
            top_signal = results[0]
            direction_emoji = "🟢" if top_signal.direction == "BULLISH" else "🔴"
            
            message = (
                f"🚀 <b>APEX SIGNAL</b>\n\n"
                f"{direction_emoji} <b>{top_signal.symbol}</b> {top_signal.grade}\n\n"
                f"📍 Entry: {top_signal.entry:.6f}\n"
                f"🛡️ SL: {top_signal.sl:.6f}\n"
                f"🎯 TP1: {top_signal.tp1:.6f}\n"
                f"📊 Confluence: {top_signal.confluence}\n"
                f"📈 Regime: {top_signal.regime}\n"
                f"⏰ Timeframe: {top_signal.timeframe}\n\n"
                f"#APEX #{top_signal.symbol}"
            )
            
            await bot.send_message(SIGNAL_CHANNEL, message, parse_mode="HTML")
            logger.info(f"Sent signal to channel: {top_signal.symbol}")
        
    except Exception as e:
        logger.error(f"Scheduled scan error: {e}")

@scheduled_task(name="cleanup", interval=3600, timeout=60)  # Каждый час
async def scheduled_cleanup():
    """Очистка кэшей и старых данных"""
    try:
        logger.info("Starting scheduled cleanup")
        
        # Очистка кэша свечей
        from market_data import clear_cache
        clear_cache()
        
        # Очистка старых результатов сканирования
        scanner.result_cache.clear()
        
        logger.info("Cleanup completed")
        
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

# ===== HEALTH CHECK ENDPOINT =====

async def health_check(request):
    """Health check для Render/деплоя"""
    try:
        # Проверяем компоненты
        db_status = "healthy" if db.get_stats()["active_connections"] >= 0 else "error"
        ai_status = "healthy" if ai_manager._check_daily_limit() else "limit_reached"
        scanner_status = "healthy" if scanner.symbols else "no_symbols"
        
        health_data = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {
                "database": db_status,
                "ai": ai_status,
                "scanner": scanner_status,
                "scheduler": "running" if scheduler.running else "stopped"
            },
            "stats": {
                "symbols_count": len(scanner.symbols),
                "daily_ai_usage": ai_manager.get_daily_usage_percent(),
                "db_connections": db.get_stats()["active_connections"]
            }
        }
        
        return web.json_response(health_data)
        
    except Exception as e:
        return web.json_response({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, status=500)

# ===== ЗАПУСК БОТА =====

async def on_startup(dispatcher):
    """Действия при запуске бота"""
    logger.info("Starting APEX Bot v2.0...")
    
    # Инициализация компонентов
    try:
        # Обновляем список символов
        scanner.update_symbols()
        
        # Запускаем scheduler
        scheduler.start()
        
        # Добавляем задачи если их нет
        if "market_scan" not in scheduler.tasks:
            add_scheduled_task("market_scan", scheduled_market_scan, 900)
        
        if "cleanup" not in scheduler.tasks:
            add_scheduled_task("cleanup", scheduled_cleanup, 3600)
        
        logger.info("Bot initialized successfully")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")

async def on_shutdown(dispatcher):
    """Действия при остановке бота"""
    logger.info("Shutting down APEX Bot...")
    
    try:
        # Останавливаем scheduler
        scheduler.shutdown()
        
        # Закрываем соединения БД
        # (пул закроется автоматически при выходе)
        
        logger.info("Bot shutdown complete")
        
    except Exception as e:
        logger.error(f"Shutdown error: {e}")

# ===== WEB SERVER ДЛЯ DEPLOY =====

async def create_web_app():
    """Создание web приложения для health check"""
    app = web.Application()
    app.router.add_get("/health", health_check)
    return app

# ===== ОСНОВНАЯ ФУНКЦИЯ ЗАПУСКА =====

async def main():
    """Основная функция запуска"""
    # Создаём web приложение
    web_app = await create_web_app()
    
    # Запускаем бота
    await dp.start_polling(
        bot,
        on_startup=on_startup,
        on_shutdown=on_shutdown
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        # Graceful shutdown
        scheduler.shutdown()
