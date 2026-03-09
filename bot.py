"""
APEX SMC Bot — Telegram бот с сигналами по Smart Money Concepts
Биржа: Bybit Futures | Полностью бесплатный стек
"""

import os
import asyncio
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from core.smc_engine import BybitData
from core.learning import LearningEngine
from signals.generator import SignalGenerator

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))       # Твой Telegram ID
CHANNEL_ID = os.getenv("CHANNEL_ID", "")         # Канал для сигналов (опц.)

SCAN_INTERVAL_MINUTES = 30  # Сканировать каждые 30 минут

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ─── ИНИЦИАЛИЗАЦИЯ ────────────────────────────────────────────────────────────
bybit = BybitData()
scanner = SignalGenerator()
learner = LearningEngine()

# Хранилище подписчиков
subscribers: set[int] = set()
if ADMIN_ID:
    subscribers.add(ADMIN_ID)


# ─── ФОРМАТИРОВАНИЕ СИГНАЛА ───────────────────────────────────────────────────

def format_signal(signal, signal_id: int = None) -> str:
    emoji = "🟢" if signal.direction == "LONG" else "🔴"
    stars = "⭐" * signal.strength

    confluence_text = "\n".join(signal.confluence)

    msg = f"""
{'━' * 30}
{emoji} *{signal.symbol}* — {signal.direction} [{signal.timeframe}]
{'━' * 30}

📐 *Сетап:* {signal.setup}
{stars}

💰 *Вход:* `{signal.entry:.4f}`
🛑 *Стоп:* `{signal.sl:.4f}`
🎯 *TP1:* `{signal.tp1:.4f}` (1:2)
🎯 *TP2:* `{signal.tp2:.4f}` (1:3)
🎯 *TP3:* `{signal.tp3:.4f}` (1:5)

📊 *Confluences ({len(signal.confluence)}):*
{confluence_text}

⏰ {signal.time.strftime('%H:%M %d.%m.%Y')}
{'━' * 30}
⚠️ _Не финансовый совет. DYOR._
"""
    return msg.strip()


def format_update(update_data: dict) -> str:
    signal = update_data["signal"]
    result = update_data["result"]
    pnl = update_data["pnl_rr"]

    icons = {
        "tp1": "🎯 TP1 достигнут! +2R",
        "tp2": "🎯🎯 TP2 достигнут! +3R",
        "tp3": "🎯🎯🎯 TP3 достигнут! +5R",
        "sl": "💀 Стоп сработал. -1R"
    }

    emoji = "✅" if result != "sl" else "❌"
    return f"""{emoji} *{signal['symbol']}* {signal['direction']} — {icons.get(result, result)}
PnL: *{'+' if pnl > 0 else ''}{pnl}R*"""


# ─── КОМАНДЫ ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    subscribers.add(user_id)

    keyboard = [
        [InlineKeyboardButton("📡 Сканировать рынок", callback_data="scan"),
         InlineKeyboardButton("📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton("📈 Топ монеты", callback_data="top_symbols"),
         InlineKeyboardButton("❓ Как читать сигнал", callback_data="howto")]
    ]

    await update.message.reply_text(
        "⚡ *APEX SMC — Фьючерсные сигналы*\n\n"
        "Анализирую рынок по *Smart Money Concepts*:\n"
        "• Break of Structure (BOS)\n"
        "• Change of Character (CHoCH)\n"
        "• Order Blocks (OB)\n"
        "• Fair Value Gaps (FVG)\n"
        "• Liquidity Zones\n\n"
        "Биржа: *Bybit Futures*\n"
        "Учусь на каждом сигнале 🧠\n\n"
        "Сканирование каждые 30 минут автоматически.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("🔍 Сканирую рынок... (~30 сек)")

    signals = await asyncio.get_event_loop().run_in_executor(
        None, lambda: scanner.scan_market(timeframes=["60", "240"])
    )

    if not signals:
        await msg.edit_text("😴 Чистый рынок — нет сигналов с достаточным confluence.\nПодожди следующего скана.")
        return

    await msg.edit_text(f"✅ Найдено *{len(signals)}* сигналов", parse_mode="Markdown")

    for signal in signals[:5]:  # Топ 5
        signal_id = learner.save_signal(signal)
        text = format_signal(signal, signal_id)
        await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    report = learner.get_performance_report()

    if report["total"] == 0:
        await update.message.reply_text(
            "📊 Статистика пока пустая — нет закрытых сигналов.\n"
            "Запусти /scan и подожди пока позиции закроются."
        )
        return

    top_symbols_text = "\n".join(
        [f"• {s[0]}: {s[1]}% ({s[2]} сделок)" for s in report["top_symbols"]]
    ) or "нет данных"

    top_factors_text = "\n".join(
        [f"• {f[0][:40]}: {f[1]}%" for f in report["top_factors"]]
    ) or "нет данных"

    pnl_emoji = "✅" if report["avg_rr"] > 0 else "❌"

    await update.message.reply_text(
        f"📊 *Статистика APEX SMC*\n\n"
        f"📈 Всего сигналов: *{report['total']}*\n"
        f"✅ Выигрыши: *{report['wins']}*\n"
        f"❌ Стопы: *{report['losses']}*\n"
        f"🎯 Win Rate: *{report['win_rate']}%*\n"
        f"{pnl_emoji} Средний RR: *{report['avg_rr']}R*\n\n"
        f"🏆 *Лучшие монеты:*\n{top_symbols_text}\n\n"
        f"🧠 *Лучшие confluences:*\n{top_factors_text}\n\n"
        f"_Бот адаптирует стратегию на основе этих данных_",
        parse_mode="Markdown"
    )


async def cmd_top(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📡 Получаю топ монеты Bybit...")

    symbols = await asyncio.get_event_loop().run_in_executor(
        None, bybit.get_top_symbols, 10
    )

    text = "🔥 *Топ монеты по объёму (Bybit Futures):*\n\n"
    for i, s in enumerate(symbols, 1):
        price = bybit.get_price(s)
        text += f"{i}. `{s}` — ${price:,.4f}\n"

    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_howto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Как читать сигналы APEX SMC*\n\n"
        "*Вход* — цена входа в позицию\n"
        "*Стоп* — уровень стоп-лосса (ниже OB для лонга)\n"
        "*TP1/2/3* — цели (RR 1:2, 1:3, 1:5)\n\n"
        "⭐ *Сила сигнала (1-5 звёзд):*\n"
        "⭐⭐⭐ = минимум 3 фактора совпало\n"
        "⭐⭐⭐⭐⭐ = максимальный confluence\n\n"
        "📐 *SMC концепции:*\n"
        "• *BOS* — Break of Structure (продолжение тренда)\n"
        "• *CHoCH* — Change of Character (разворот)\n"
        "• *OB* — Order Block (зона входа крупных игроков)\n"
        "• *FVG* — Fair Value Gap (незакрытый дисбаланс)\n"
        "• *Liquidity* — зона ликвидности (стопы толпы)\n\n"
        "💡 *Рекомендуемый риск:* 1-2% от депозита на сделку\n"
        "📊 Всегда дожидайся закрытия свечи перед входом!",
        parse_mode="Markdown"
    )


# ─── CALLBACK КНОПКИ ──────────────────────────────────────────────────────────

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "scan":
        await cmd_scan(query, ctx)
    elif query.data == "stats":
        await cmd_stats(query, ctx)
    elif query.data == "top_symbols":
        await cmd_top(query, ctx)
    elif query.data == "howto":
        await cmd_howto(query, ctx)


# ─── АВТО-СКАНИРОВАНИЕ ────────────────────────────────────────────────────────

async def auto_scan(app: Application):
    """Фоновое сканирование каждые 30 минут"""
    while True:
        await asyncio.sleep(SCAN_INTERVAL_MINUTES * 60)

        log.info("🔍 Авто-скан...")

        try:
            # Проверяем результаты прошлых сигналов
            updates = await asyncio.get_event_loop().run_in_executor(
                None, lambda: learner.check_pending_signals(bybit)
            )

            # Отправляем апдейты подписчикам
            for upd in updates:
                text = format_update(upd)
                for user_id in subscribers:
                    try:
                        await app.bot.send_message(user_id, text, parse_mode="Markdown")
                    except:
                        pass

            # Адаптивный min confluence
            min_conf = learner.get_adaptive_min_confluence()
            log.info(f"Адаптивный min_confluence: {min_conf}")

            # Предпочтительные символы (если есть история)
            best = learner.get_best_symbols()
            symbols = best if best else None

            # Сканируем
            signals = await asyncio.get_event_loop().run_in_executor(
                None, lambda: scanner.scan_market(symbols=symbols, timeframes=["60", "240"])
            )

            if signals:
                log.info(f"Найдено {len(signals)} сигналов")
                for signal in signals[:3]:
                    signal_id = learner.save_signal(signal)
                    text = format_signal(signal, signal_id)

                    for user_id in subscribers:
                        try:
                            await app.bot.send_message(
                                user_id, text, parse_mode="Markdown"
                            )
                        except Exception as e:
                            log.error(f"Send error {user_id}: {e}")
            else:
                log.info("Сигналов нет — рынок в зоне ожидания")

        except Exception as e:
            log.error(f"Авто-скан ошибка: {e}")


# ─── ЗАПУСК ───────────────────────────────────────────────────────────────────

async def post_init(app: Application):
    asyncio.create_task(auto_scan(app))
    log.info("⚡ APEX SMC запущен. Авто-скан каждые 30 минут.")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("top", cmd_top))
    app.add_handler(CommandHandler("howto", cmd_howto))
    app.add_handler(CallbackQueryHandler(handle_callback))

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
