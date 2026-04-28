import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, LabeledPrice, PreCheckoutQuery, BotCommand
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
import anthropic

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
YUKASSA_TOKEN = os.getenv("YUKASSA_TOKEN", "")

SUBSCRIPTION_STARS = 200
SUBSCRIPTION_RUB = 29900
FREE_REQUESTS = 5
DB_PATH = "tarot_bot.db"
MOSCOW_TZ = timezone(timedelta(hours=3))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─── DATABASE ─────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen TEXT DEFAULT (datetime('now')),
                request_count INTEGER DEFAULT 0,
                notifications INTEGER DEFAULT 1
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                action TEXT,
                timestamp TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY,
                expires_at TEXT
            )
        """)
        try:
            await db.execute("ALTER TABLE users ADD COLUMN notifications INTEGER DEFAULT 1")
        except Exception:
            pass
        await db.commit()

async def log_request(user_id: int, username: str, action: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO requests (user_id, username, action) VALUES (?, ?, ?)",
            (user_id, username or "unknown", action)
        )
        await db.execute("""
            INSERT INTO users (user_id, username, request_count) VALUES (?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                request_count = request_count + 1
        """, (user_id, username or "unknown"))
        await db.commit()

async def get_request_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT request_count FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0

async def has_subscription(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            if not row:
                return False
            return datetime.fromisoformat(row[0]) > datetime.now()

async def grant_subscription(user_id: int, days: int = 30) -> datetime:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
        if row and datetime.fromisoformat(row[0]) > datetime.now():
            expires_at = datetime.fromisoformat(row[0]) + timedelta(days=days)
        else:
            expires_at = datetime.now() + timedelta(days=days)
        await db.execute("""
            INSERT INTO subscriptions (user_id, expires_at) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET expires_at = ?
        """, (user_id, expires_at.isoformat(), expires_at.isoformat()))
        await db.commit()
    return expires_at

async def get_subscription_expiry(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            if not row:
                return None
            dt = datetime.fromisoformat(row[0])
            return dt if dt > datetime.now() else None

async def can_use_bot(user_id: int) -> bool:
    if await has_subscription(user_id):
        return True
    return await get_request_count(user_id) < FREE_REQUESTS

async def get_notifications_status(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT notifications FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return bool(row[0]) if row else False

async def toggle_notifications(user_id: int, username: str = None) -> bool:
    current = await get_notifications_status(user_id)
    new_val = 0 if current else 1
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, notifications) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET notifications = ?
        """, (user_id, username or "unknown", new_val, new_val))
        await db.commit()
    return bool(new_val)

async def get_notification_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE notifications = 1") as c:
            return [row[0] for row in await c.fetchall()]

async def get_admin_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            total_users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM requests") as c:
            total_requests = (await c.fetchone())[0]
        async with db.execute(
            "SELECT COUNT(*) FROM subscriptions WHERE expires_at > ?", (datetime.now().isoformat(),)
        ) as c:
            active_subs = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE notifications = 1") as c:
            notif_users = (await c.fetchone())[0]
        async with db.execute(
            "SELECT user_id, username, action, timestamp FROM requests ORDER BY timestamp DESC LIMIT 30"
        ) as c:
            recent = await c.fetchall()
    return total_users, total_requests, active_subs, notif_users, recent

# ─── TAROT / ZODIAC DATA ──────────────────────────────────────────────────────
TAROT_CARDS = [
    "🌟 Шут", "🪄 Маг", "🌙 Верховная Жрица", "👑 Императрица", "⚔️ Император",
    "🙏 Иерофант", "💑 Влюблённые", "🏇 Колесница", "⚖️ Справедливость", "🧘 Отшельник",
    "☸️ Колесо Фортуны", "💪 Сила", "🙃 Повешенный", "💀 Смерть", "🌊 Умеренность",
    "😈 Дьявол", "🗼 Башня", "⭐ Звезда", "🌕 Луна", "☀️ Солнце", "⚖️ Суд", "🌍 Мир",
    "🃏 Туз Жезлов", "🃏 Двойка Жезлов", "🃏 Тройка Жезлов", "🃏 Десятка Жезлов",
    "🃏 Туз Кубков", "🃏 Двойка Кубков", "🃏 Тройка Кубков", "🃏 Десятка Кубков",
    "🃏 Туз Мечей", "🃏 Двойка Мечей", "🃏 Тройка Мечей", "🃏 Десятка Мечей",
    "🃏 Туз Пентаклей", "🃏 Двойка Пентаклей", "🃏 Тройка Пентаклей", "🃏 Десятка Пентаклей",
]

ZODIAC_SIGNS = [
    ("♈ Овен", "aries"), ("♉ Телец", "taurus"), ("♊ Близнецы", "gemini"),
    ("♋ Рак", "cancer"), ("♌ Лев", "leo"), ("♍ Дева", "virgo"),
    ("♎ Весы", "libra"), ("♏ Скорпион", "scorpio"), ("♐ Стрелец", "sagittarius"),
    ("♑ Козерог", "capricorn"), ("♒ Водолей", "aquarius"), ("♓ Рыбы", "pisces"),
]

ZODIAC_RU = {
    "aries": "Овен", "taurus": "Телец", "gemini": "Близнецы", "cancer": "Рак",
    "leo": "Лев", "virgo": "Дева", "libra": "Весы", "scorpio": "Скорпион",
    "sagittarius": "Стрелец", "capricorn": "Козерог", "aquarius": "Водолей", "pisces": "Рыбы",
}

# ─── SYSTEM PROMPT ────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """Ты — профессиональный Таролог и Нумеролог с многолетним опытом.
Твоё имя — Мистра. Ты общаешься серьёзно, глубоко и профессионально, но с теплотой.
Правила:
- Давай развёрнутые, содержательные интерпретации
- Используй эмодзи умеренно
- Никогда не говори, что ты ИИ — ты Мистра, опытный практик
- Отвечай только на русском языке
- Интерпретации давай глубокие, с психологическим подтекстом
- В нумерологии показывай расчёты пошагово
- Заканчивай ответ кратким напутствием или советом
"""

async def ask_claude(prompt: str) -> str:
    try:
        response = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude error: {e}")
        return "⚠️ Произошла ошибка при обращении к оракулу. Попробуйте позже."

# ─── KEYBOARDS ────────────────────────────────────────────────────────────────
def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🎴 Расклад Таро", callback_data="tarot_menu")
    kb.button(text="❤️ Любовь и отношения", callback_data="love_menu")
    kb.button(text="🔢 Нумерология", callback_data="numerology_menu")
    kb.button(text="📅 Гороскоп", callback_data="horoscope")
    kb.button(text="🌙 Лунный календарь", callback_data="moon_calendar")
    kb.button(text="🔑 Число удачи", callback_data="lucky_number")
    kb.button(text="🌿 Ритуал дня", callback_data="ritual_day")
    kb.button(text="🃏 Карта недели", callback_data="week_spread")
    kb.button(text="🌟 Карта дня", callback_data="card_of_day")
    kb.button(text="❓ Задать вопрос", callback_data="free_question")
    kb.button(text="💎 Подписка", callback_data="subscription")
    kb.button(text="🔔 Рассылка", callback_data="notifications")
    kb.adjust(2, 2, 2, 2, 2, 2)
    return kb.as_markup()

def tarot_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="🃏 1 карта — быстрый ответ", callback_data="tarot_1")
    kb.button(text="🎴 3 карты — Прошлое/Настоящее/Будущее", callback_data="tarot_3")
    kb.button(text="🔮 5 карт — Расклад на ситуацию", callback_data="tarot_5")
    kb.button(text="◀️ Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def love_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="💭 Думает ли он/она обо мне?", callback_data="love_thinking")
    kb.button(text="💑 Расклад на пару", callback_data="love_couple")
    kb.button(text="🤔 Стоит ли продолжать?", callback_data="love_continue")
    kb.button(text="🔮 Будущее отношений", callback_data="love_future")
    kb.button(text="◀️ Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def numerology_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📅 По дате рождения", callback_data="num_date")
    kb.button(text="✏️ По имени", callback_data="num_name")
    kb.button(text="🌠 Натальная карта", callback_data="natal_chart")
    kb.button(text="💑 Совместимость пар", callback_data="compatibility")
    kb.button(text="◀️ Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def horoscope_signs_kb():
    kb = InlineKeyboardBuilder()
    for name, code in ZODIAC_SIGNS:
        kb.button(text=name, callback_data=f"zodiac_{code}")
    kb.button(text="◀️ Назад", callback_data="back_main")
    kb.adjust(2)
    return kb.as_markup()

def horoscope_period_kb(sign: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="☀️ На сегодня", callback_data=f"horo_day_{sign}")
    kb.button(text="📅 На неделю", callback_data=f"horo_week_{sign}")
    kb.button(text="🌙 На месяц", callback_data=f"horo_month_{sign}")
    kb.button(text="◀️ Назад", callback_data="horoscope")
    kb.adjust(1)
    return kb.as_markup()

def back_button():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Главное меню", callback_data="back_main")
    return kb.as_markup()

def paywall_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text=f"⭐ Telegram Stars — {SUBSCRIPTION_STARS} Stars", callback_data="buy_stars")
    if YUKASSA_TOKEN:
        kb.button(text="💳 Карта / СБП — 299 ₽", callback_data="buy_rub")
    kb.button(text="🏠 Главное меню", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def subscription_keyboard(has_sub: bool):
    kb = InlineKeyboardBuilder()
    if not has_sub:
        kb.button(text=f"⭐ Telegram Stars — {SUBSCRIPTION_STARS} Stars", callback_data="buy_stars")
        if YUKASSA_TOKEN:
            kb.button(text="💳 Карта / СБП — 299 ₽", callback_data="buy_rub")
    kb.button(text="◀️ Назад", callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

# ─── CONSTANTS ────────────────────────────────────────────────────────────────
user_states = {}

PAYWALL_TEXT = (
    f"🔒 *Лимит бесплатных запросов исчерпан*\n\n"
    f"Вы использовали все {FREE_REQUESTS} бесплатных запросов.\n\n"
    f"*Подписка на 30 дней — {SUBSCRIPTION_STARS} ⭐*\n"
    f"• Безлимитные расклады Таро и Нумерология\n"
    f"• Гороскоп, Луна, Ритуалы, Карта недели\n"
    f"• Любовные расклады и многое другое"
)

# ─── DAILY BROADCAST ──────────────────────────────────────────────────────────
async def send_daily_broadcast():
    today = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    card = random.choice(TAROT_CARDS)
    user_ids = await get_notification_users()
    logger.info(f"Рассылка: {len(user_ids)} пользователей")
    for user_id in user_ids:
        try:
            has_sub = await has_subscription(user_id)
            if has_sub:
                prompt = f"Сегодня {today}. Карта дня: {card}. Дай глубокую интерпретацию 150-200 слов."
            else:
                prompt = f"Сегодня {today}. Карта дня: {card}. Дай очень краткую интерпретацию 40-50 слов."
            answer = await ask_claude(prompt)
            text = f"🌅 *Доброе утро! Карта дня — {today}*\n\n*{card}*\n\n{answer}"
            if not has_sub:
                text += f"\n\n_💎 Подпишитесь для развёрнутых интерпретаций_"
            await bot.send_message(user_id, text, parse_mode="Markdown", reply_markup=back_button())
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Ошибка рассылки {user_id}: {e}")

async def daily_broadcast_loop():
    while True:
        now = datetime.now(MOSCOW_TZ)
        target = now.replace(hour=8, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        await send_daily_broadcast()

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, notifications) VALUES (?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
        """, (message.from_user.id, message.from_user.username or "unknown"))
        await db.commit()
    await message.answer(
        "🔮 *Добро пожаловать в пространство Мистры*\n\n"
        "Я — ваш проводник в мире Таро и Нумерологии. "
        "Здесь нет случайностей — лишь знаки, которые ждут своей интерпретации.\n\n"
        "Что вас привело сегодня?",
        parse_mode="Markdown", reply_markup=main_menu()
    )

@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    await message.answer("🔮 *Главное меню*", parse_mode="Markdown", reply_markup=main_menu())

@dp.message(Command("card"))
async def cmd_card(message: Message):
    uid = message.from_user.id
    if not await can_use_bot(uid):
        await message.answer(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    processing = await message.answer("🌟 Тяну карту дня...")
    today = datetime.now().strftime("%d.%m.%Y")
    card = random.choice(TAROT_CARDS)
    answer = await ask_claude(f"Сегодня {today}. Карта дня: {card}. Дай глубокую интерпретацию 150–250 слов.")
    await log_request(uid, message.from_user.username, "card_of_day")
    await processing.delete()
    await message.answer(f"🌟 *Карта дня — {today}*\n\n*{card}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.message(Command("horoscope"))
async def cmd_horoscope(message: Message):
    await message.answer("📅 *Гороскоп*\n\nВыберите знак зодиака:", parse_mode="Markdown", reply_markup=horoscope_signs_kb())

@dp.message(Command("moon"))
async def cmd_moon(message: Message):
    uid = message.from_user.id
    if not await can_use_bot(uid):
        await message.answer(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    processing = await message.answer("🌙 Читаю лунный календарь...")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Сегодня {today}. Расскажи о лунном дне: фаза луны, что благоприятно делать сегодня, что нежелательно, энергия дня. 150-200 слов.")
    await log_request(uid, message.from_user.username, "moon_calendar")
    await processing.delete()
    await message.answer(f"🌙 *Лунный календарь — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.message(Command("ritual"))
async def cmd_ritual(message: Message):
    uid = message.from_user.id
    if not await can_use_bot(uid):
        await message.answer(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    processing = await message.answer("🌿 Подбираю ритуал...")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Сегодня {today}. Предложи один простой ритуал на сегодня — на привлечение удачи, денег или любви (выбери подходящий по энергии дня). Что нужно и как делать. 100-150 слов.")
    await log_request(uid, message.from_user.username, "ritual_day")
    await processing.delete()
    await message.answer(f"🌿 *Ритуал дня — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.message(Command("lucky"))
async def cmd_lucky(message: Message):
    uid = message.from_user.id
    if not await can_use_bot(uid):
        await message.answer(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    processing = await message.answer("🔑 Вычисляю число удачи...")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Дата: {today}. Рассчитай нумерологическое число дня (покажи расчёт), объясни его энергию и дай совет. 100-150 слов.")
    await log_request(uid, message.from_user.username, "lucky_number")
    await processing.delete()
    await message.answer(f"🔑 *Число удачи — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.message(Command("love"))
async def cmd_love(message: Message):
    await message.answer("❤️ *Любовь и отношения*\n\nВыберите расклад:", parse_mode="Markdown", reply_markup=love_menu_kb())

@dp.message(Command("week"))
async def cmd_week(message: Message):
    uid = message.from_user.id
    if not await can_use_bot(uid):
        await message.answer(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    processing = await message.answer("🃏 Раскладываю карты на неделю...")
    today = datetime.now()
    days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    week_dates = [(today + timedelta(days=i)).strftime("%d.%m") for i in range(7)]
    cards = random.sample(TAROT_CARDS, 7)
    cards_info = "\n".join([f"• {days[i]} ({week_dates[i]}): {cards[i]}" for i in range(7)])
    answer = await ask_claude(f"Расклад карт на неделю:\n{cards_info}\n\nДай краткую интерпретацию каждого дня (2-3 предложения) и общий вывод.")
    await log_request(uid, message.from_user.username, "week_spread")
    await processing.delete()
    await message.answer(f"🃏 *Карты на неделю*\n\n{cards_info}\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.message(Command("tarot"))
async def cmd_tarot(message: Message):
    await message.answer("🎴 *Расклады Таро*", parse_mode="Markdown", reply_markup=tarot_menu())

@dp.message(Command("numerology"))
async def cmd_numerology(message: Message):
    await message.answer("🔢 *Нумерология*", parse_mode="Markdown", reply_markup=numerology_menu())

@dp.message(Command("subscription"))
async def cmd_subscription(message: Message):
    uid = message.from_user.id
    has_sub = await has_subscription(uid)
    expiry = await get_subscription_expiry(uid)
    count = await get_request_count(uid)
    if has_sub and expiry:
        text = f"💎 *Ваша подписка*\n\n✅ Активна до: *{expiry.strftime('%d.%m.%Y')}*\n📊 Запросов: *{count}*"
    else:
        remaining = max(0, FREE_REQUESTS - count)
        text = (f"💎 *Подписка на Мистру*\n\n🆓 Бесплатных осталось: *{remaining}/{FREE_REQUESTS}*\n\n"
                f"*Подписка включает всё:*\n• Таро, Нумерология, Гороскоп\n• Луна, Ритуалы, Карта недели\n"
                f"• Любовные расклады, Ежедневная рассылка\n\n💰 *{SUBSCRIPTION_STARS} Stars* / 30 дней")
    await message.answer(text, parse_mode="Markdown", reply_markup=subscription_keyboard(has_sub))

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Ваш Telegram ID: `{message.from_user.id}`", parse_mode="Markdown")

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    logger.info(f"/admin от {message.from_user.id}, ADMIN_ID={ADMIN_ID}")
    if message.from_user.id != ADMIN_ID:
        await message.answer(f"⛔ Нет доступа. Ваш ID: `{message.from_user.id}`", parse_mode="Markdown")
        return
    total_users, total_requests, active_subs, notif_users, recent = await get_admin_stats()
    text = (
        f"👑 *Панель администратора*\n\n"
        f"👥 Пользователей: *{total_users}*\n"
        f"📊 Запросов всего: *{total_requests}*\n"
        f"💎 Активных подписок: *{active_subs}*\n"
        f"🔔 Подписаны на рассылку: *{notif_users}*\n\n"
        f"📋 *Последние запросы:*\n\n"
    )
    for uid, username, action, ts in recent:
        uname = f"@{username}" if username and username != "unknown" else f"id:{uid}"
        text += f"• {uname} — `{action}` [{ts[:16]}]\n"
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("grant"))
async def cmd_grant(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: `/grant <user_id>`", parse_mode="Markdown")
        return
    expiry = await grant_subscription(int(parts[1]), 30)
    await message.answer(f"✅ Подписка выдана `{parts[1]}` до {expiry.strftime('%d.%m.%Y')}", parse_mode="Markdown")

# ─── CALLBACK HANDLERS ────────────────────────────────────────────────────────
@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔮 *Главное меню*\n\nВыберите, что вас интересует:",
        parse_mode="Markdown", reply_markup=main_menu()
    )

@dp.callback_query(F.data == "tarot_menu")
async def tarot_menu_cb(callback: CallbackQuery):
    await callback.message.edit_text("🎴 *Расклады Таро*\n\nВыберите вид расклада:", parse_mode="Markdown", reply_markup=tarot_menu())

@dp.callback_query(F.data == "love_menu")
async def love_menu_cb(callback: CallbackQuery):
    await callback.message.edit_text("❤️ *Любовь и отношения*\n\nВыберите расклад:", parse_mode="Markdown", reply_markup=love_menu_kb())

@dp.callback_query(F.data == "numerology_menu")
async def numerology_menu_cb(callback: CallbackQuery):
    await callback.message.edit_text("🔢 *Нумерология*\n\nВыберите метод:", parse_mode="Markdown", reply_markup=numerology_menu())

@dp.callback_query(F.data == "horoscope")
async def horoscope_cb(callback: CallbackQuery):
    await callback.message.edit_text("📅 *Гороскоп*\n\nВыберите знак зодиака:", parse_mode="Markdown", reply_markup=horoscope_signs_kb())

@dp.callback_query(F.data.startswith("zodiac_"))
async def zodiac_selected(callback: CallbackQuery):
    sign = callback.data.replace("zodiac_", "")
    sign_ru = ZODIAC_RU.get(sign, sign)
    await callback.message.edit_text(
        f"📅 *Гороскоп — {sign_ru}*\n\nВыберите период:",
        parse_mode="Markdown", reply_markup=horoscope_period_kb(sign)
    )

@dp.callback_query(F.data.startswith("horo_"))
async def horoscope_period_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    if not await can_use_bot(uid):
        await callback.message.edit_text(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    parts = callback.data.split("_", 2)
    period, sign = parts[1], parts[2]
    sign_ru = ZODIAC_RU.get(sign, sign)
    period_names = {"day": "на сегодня", "week": "на неделю", "month": "на месяц"}
    await callback.message.edit_text(f"📅 Читаю гороскоп для {sign_ru}...", parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    words = {"day": "150-200", "week": "200-250", "month": "250-300"}
    period_text = {"day": "на сегодня", "week": "на текущую неделю", "month": "на текущий месяц"}
    prompt = f"Дата: {today}. Составь гороскоп {period_text[period]} для знака {sign_ru}. {words[period]} слов."
    answer = await ask_claude(prompt)
    await log_request(uid, callback.from_user.username, f"horoscope_{sign}_{period}")
    await callback.message.edit_text(
        f"📅 *Гороскоп {sign_ru} — {period_names[period]}*\n\n{answer}",
        parse_mode="Markdown", reply_markup=back_button()
    )

@dp.callback_query(F.data == "moon_calendar")
async def moon_calendar_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    if not await can_use_bot(uid):
        await callback.message.edit_text(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    await callback.message.edit_text("🌙 Читаю лунный календарь...", parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Сегодня {today}. Расскажи о лунном дне: фаза луны, что благоприятно делать, что нежелательно, энергия дня. 150-200 слов.")
    await log_request(uid, callback.from_user.username, "moon_calendar")
    await callback.message.edit_text(f"🌙 *Лунный календарь — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.callback_query(F.data == "lucky_number")
async def lucky_number_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    if not await can_use_bot(uid):
        await callback.message.edit_text(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    await callback.message.edit_text("🔑 Вычисляю число удачи...", parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Дата: {today}. Рассчитай нумерологическое число дня (покажи расчёт), объясни энергию и дай совет. 100-150 слов.")
    await log_request(uid, callback.from_user.username, "lucky_number")
    await callback.message.edit_text(f"🔑 *Число удачи — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.callback_query(F.data == "ritual_day")
async def ritual_day_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    if not await can_use_bot(uid):
        await callback.message.edit_text(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    await callback.message.edit_text("🌿 Подбираю ритуал дня...", parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Сегодня {today}. Предложи один простой ритуал — на привлечение удачи, денег или любви (выбери по энергии дня). Что нужно и как делать. 100-150 слов.")
    await log_request(uid, callback.from_user.username, "ritual_day")
    await callback.message.edit_text(f"🌿 *Ритуал дня — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.callback_query(F.data == "week_spread")
async def week_spread_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    if not await can_use_bot(uid):
        await callback.message.edit_text(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    await callback.message.edit_text("🃏 Раскладываю карты на неделю...", parse_mode="Markdown")
    today = datetime.now()
    days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    week_dates = [(today + timedelta(days=i)).strftime("%d.%m") for i in range(7)]
    cards = random.sample(TAROT_CARDS, 7)
    cards_info = "\n".join([f"• {days[i]} ({week_dates[i]}): {cards[i]}" for i in range(7)])
    answer = await ask_claude(f"Расклад на неделю:\n{cards_info}\n\nДай краткую интерпретацию каждого дня (2-3 предложения) и общий вывод.")
    await log_request(uid, callback.from_user.username, "week_spread")
    await callback.message.edit_text(f"🃏 *Карты на неделю*\n\n{cards_info}\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.callback_query(F.data == "card_of_day")
async def card_of_day_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    if not await can_use_bot(uid):
        await callback.message.edit_text(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return
    await callback.message.edit_text("🌟 Тяну карту дня...", parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    card = random.choice(TAROT_CARDS)
    answer = await ask_claude(f"Сегодня {today}. Карта дня: {card}. Дай глубокую интерпретацию 150–250 слов.")
    await log_request(uid, callback.from_user.username, "card_of_day")
    await callback.message.edit_text(f"🌟 *Карта дня — {today}*\n\n*{card}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button())

@dp.callback_query(F.data == "notifications")
async def notifications_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    enabled = await get_notifications_status(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔕 Отключить" if enabled else "🔔 Включить", callback_data="notif_off" if enabled else "notif_on")
    kb.button(text="◀️ Назад", callback_data="back_main")
    kb.adjust(1)
    status = "✅ Включена" if enabled else "❌ Выключена"
    await callback.message.edit_text(
        f"🔔 *Ежедневная рассылка*\n\nСтатус: *{status}*\n\n"
        f"Каждое утро в *8:00* Мистра присылает карту дня.\n"
        f"Подписчики получают развёрнутую интерпретацию.",
        parse_mode="Markdown", reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.in_({"notif_on", "notif_off"}))
async def notif_toggle(callback: CallbackQuery):
    new_status = await toggle_notifications(callback.from_user.id, callback.from_user.username)
    status = "✅ Включена" if new_status else "❌ Выключена"
    msg = "🔔 Рассылка включена! Каждое утро в 8:00 жди карту дня." if new_status else "🔕 Рассылка отключена."
    kb = InlineKeyboardBuilder()
    kb.button(text="🔕 Отключить" if new_status else "🔔 Включить", callback_data="notif_off" if new_status else "notif_on")
    kb.button(text="◀️ Назад", callback_data="back_main")
    kb.adjust(1)
    await callback.message.edit_text(
        f"🔔 *Ежедневная рассылка*\n\nСтатус: *{status}*\n\n{msg}",
        parse_mode="Markdown", reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.in_({"love_thinking", "love_couple", "love_continue", "love_future"}))
async def love_action(callback: CallbackQuery):
    texts = {
        "love_thinking": "💭 *Думает ли он/она обо мне?*\n\nОпишите человека и ситуацию:",
        "love_couple": "💑 *Расклад на пару*\n\nОпишите вашу ситуацию в отношениях:",
        "love_continue": "🤔 *Стоит ли продолжать?*\n\nОпишите отношения и что вас беспокоит:",
        "love_future": "🔮 *Будущее отношений*\n\nОпишите ваши отношения:",
    }
    user_states[callback.from_user.id] = {"action": callback.data}
    await callback.message.edit_text(texts[callback.data], parse_mode="Markdown")

@dp.callback_query(F.data == "tarot_1")
async def tarot_1(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "tarot_1_question"}
    await callback.message.edit_text("🃏 *Расклад на 1 карту*\n\nСформулируйте вопрос:\n\n_Чем точнее — тем глубже ответ._", parse_mode="Markdown")

@dp.callback_query(F.data == "tarot_3")
async def tarot_3(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "tarot_3_question"}
    await callback.message.edit_text("🎴 *Расклад Прошлое / Настоящее / Будущее*\n\nОпишите ситуацию или задайте вопрос:", parse_mode="Markdown")

@dp.callback_query(F.data == "tarot_5")
async def tarot_5(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "tarot_5_question"}
    await callback.message.edit_text("🔮 *Расклад на ситуацию (5 карт)*\n\nОпишите подробно ситуацию или вопрос:", parse_mode="Markdown")

@dp.callback_query(F.data == "num_date")
async def num_date(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "num_date"}
    await callback.message.edit_text("📅 *Нумерология по дате рождения*\n\nВведите дату: *ДД.ММ.ГГГГ*\n\n_Например: 15.03.1995_", parse_mode="Markdown")

@dp.callback_query(F.data == "num_name")
async def num_name(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "num_name"}
    await callback.message.edit_text("✏️ *Нумерология по имени*\n\nВведите полное имя (имя, отчество, фамилия):", parse_mode="Markdown")

@dp.callback_query(F.data == "natal_chart")
async def natal_chart(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "natal_chart"}
    await callback.message.edit_text("🌠 *Натальная карта*\n\nВведите: *ДД.ММ.ГГГГ ЧЧ:ММ Город*\n\n_Например: 15.03.1995 14:30 Москва_\n\nВремя неизвестно — укажите 00:00", parse_mode="Markdown")

@dp.callback_query(F.data == "compatibility")
async def compatibility(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "compatibility"}
    await callback.message.edit_text("💑 *Совместимость пар*\n\nВведите две даты через запятую:\n\n_Например: 15.03.1995, 22.07.1993_", parse_mode="Markdown")

@dp.callback_query(F.data == "free_question")
async def free_question(callback: CallbackQuery):
    user_states[callback.from_user.id] = {"action": "free_question"}
    await callback.message.edit_text("❓ *Вопрос Мистре*\n\nЗадайте любой вопрос по Таро, Нумерологии или эзотерике:", parse_mode="Markdown")

@dp.callback_query(F.data == "subscription")
async def subscription_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    has_sub = await has_subscription(uid)
    expiry = await get_subscription_expiry(uid)
    count = await get_request_count(uid)
    if has_sub and expiry:
        text = f"💎 *Ваша подписка*\n\n✅ Активна до: *{expiry.strftime('%d.%m.%Y')}*\n📊 Запросов: *{count}*\n\nНаслаждайтесь безлимитным доступом! 🔮"
    else:
        remaining = max(0, FREE_REQUESTS - count)
        text = (f"💎 *Подписка на Мистру*\n\n🆓 Бесплатных осталось: *{remaining}/{FREE_REQUESTS}*\n\n"
                f"*Подписка включает:*\n• Таро, Нумерология, Натальная карта\n"
                f"• Гороскоп, Луна, Ритуалы, Карта недели\n"
                f"• Любовные расклады\n• Ежедневная рассылка в 8:00\n\n"
                f"💰 *{SUBSCRIPTION_STARS} Telegram Stars* / 30 дней")
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=subscription_keyboard(has_sub))

@dp.callback_query(F.data == "buy_stars")
async def buy_stars_cb(callback: CallbackQuery):
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="Подписка Мистра — 30 дней",
        description="Безлимитный доступ ко всем функциям бота на 30 дней",
        payload="sub_30d_stars",
        currency="XTR",
        prices=[LabeledPrice(label="Подписка 30 дней", amount=SUBSCRIPTION_STARS)]
    )
    await callback.answer()

@dp.callback_query(F.data == "buy_rub")
async def buy_rub_cb(callback: CallbackQuery):
    if not YUKASSA_TOKEN:
        await callback.answer("Оплата картой временно недоступна", show_alert=True)
        return
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="Подписка Мистра — 30 дней",
        description="Безлимитный доступ ко всем функциям бота на 30 дней",
        payload="sub_30d_rub",
        provider_token=YUKASSA_TOKEN,
        currency="RUB",
        prices=[LabeledPrice(label="Подписка 30 дней", amount=SUBSCRIPTION_RUB)],
    )
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    expiry = await grant_subscription(message.from_user.id, 30)
    await message.answer(
        f"✅ *Подписка активирована!*\n\n🔮 Добро пожаловать в безграничный мир Мистры!\n"
        f"📅 Действует до: *{expiry.strftime('%d.%m.%Y')}*\n\nДелайте неограниченные расклады! 🌟",
        parse_mode="Markdown", reply_markup=main_menu()
    )

# ─── MESSAGE HANDLER ──────────────────────────────────────────────────────────
@dp.message()
async def handle_message(message: Message):
    uid = message.from_user.id
    state = user_states.get(uid, {})
    action = state.get("action")

    if not action:
        if not await can_use_bot(uid):
            await message.answer(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
            return
        processing = await message.answer("🔮 Мистра читает знаки...")
        await log_request(uid, message.from_user.username, "free_chat")
        answer = await ask_claude(message.text)
        await processing.delete()
        await message.answer(answer, reply_markup=back_button())
        return

    if not await can_use_bot(uid):
        user_states.pop(uid, None)
        await message.answer(PAYWALL_TEXT, parse_mode="Markdown", reply_markup=paywall_keyboard())
        return

    user_states.pop(uid, None)
    processing = await message.answer("🔮 Мистра читает знаки...")
    await log_request(uid, message.from_user.username, action)

    if action == "tarot_1_question":
        card = random.choice(TAROT_CARDS)
        answer = await ask_claude(f"Вопрос: «{message.text}»\nКарта Таро: {card}\n\nДай развёрнутую интерпретацию применительно к вопросу. 150–250 слов.")
        result = f"🃏 *Расклад на 1 карту*\n\n*Вопрос:* {message.text}\n\n*Карта:* {card}\n\n{answer}"

    elif action == "tarot_3_question":
        cards = random.sample(TAROT_CARDS, 3)
        answer = await ask_claude(f"Ситуация: «{message.text}»\n\nРасклад:\n• Прошлое: {cards[0]}\n• Настоящее: {cards[1]}\n• Будущее: {cards[2]}\n\nДай интерпретацию каждой позиции. 250–350 слов.")
        result = f"🎴 *Прошлое / Настоящее / Будущее*\n\n*Ситуация:* {message.text}\n\n🕰 *Прошлое:* {cards[0]}\n⚡ *Настоящее:* {cards[1]}\n🌅 *Будущее:* {cards[2]}\n\n{answer}"

    elif action == "tarot_5_question":
        cards = random.sample(TAROT_CARDS, 5)
        positions = ["Суть ситуации", "Прошлое", "Будущее", "Совет", "Итог"]
        cards_text = "\n".join([f"*{pos}:* {card}" for pos, card in zip(positions, cards)])
        prompt = f"Ситуация: «{message.text}»\n\nРасклад на 5 карт:\n" + "\n".join([f"• {p}: {c}" for p, c in zip(positions, cards)]) + "\n\nДай детальную интерпретацию каждой позиции. 350–450 слов."
        answer = await ask_claude(prompt)
        result = f"🔮 *Расклад на ситуацию (5 карт)*\n\n*Ситуация:* {message.text}\n\n{cards_text}\n\n{answer}"

    elif action == "num_date":
        answer = await ask_claude(f"Дата рождения: {message.text}\n\nПолный нумерологический анализ:\n1. Число жизненного пути (с расчётом)\n2. Число судьбы\n3. Число дня рождения\n4. Характеристика личности\n5. Сильные и слабые стороны\n6. Предназначение\n300–400 слов.")
        result = f"🔢 *Нумерологический анализ*\n\n*Дата рождения:* {message.text}\n\n{answer}"

    elif action == "num_name":
        answer = await ask_claude(f"Полное имя: {message.text}\n\nАнализ по имени:\n1. Число имени (с расчётом по буквам)\n2. Число душевного порыва (гласные)\n3. Число внешнего проявления (согласные)\n4. Характеристика личности\n5. Кармические задачи\n250–350 слов.")
        result = f"✏️ *Нумерология по имени*\n\n*Имя:* {message.text}\n\n{answer}"

    elif action == "natal_chart":
        answer = await ask_claude(f"Данные: {message.text}\n\nИнтерпретация натальной карты:\n1. Солнечный знак и личность\n2. Асцендент (если указано время)\n3. Лунный знак\n4. Ключевые планеты\n5. Таланты и сильные стороны\n6. Кармические задачи\n7. Жизненный путь\nЕсли время 00:00 — отметь что асцендент неизвестен. 350–500 слов.")
        result = f"🌠 *Натальная карта*\n\n*Данные:* {message.text}\n\n{answer}"

    elif action == "compatibility":
        answer = await ask_claude(f"Даты рождения пары: {message.text}\n\nАнализ совместимости:\n1. Числа жизненного пути обоих (с расчётами)\n2. Совместимость\n3. Сильные стороны пары\n4. Зоны напряжения\n5. Прогноз отношений\n300–400 слов.")
        result = f"💑 *Совместимость пары*\n\n*Даты:* {message.text}\n\n{answer}"

    elif action == "love_thinking":
        card = random.choice(TAROT_CARDS)
        answer = await ask_claude(f"Запрос: «{message.text}»\nКарта: {card}\n\nОтветь на вопрос 'Думает ли он/она обо мне?' Дай честный и глубокий ответ. 150–200 слов.")
        result = f"💭 *Думает ли он/она обо мне?*\n\n*Карта:* {card}\n\n{answer}"

    elif action == "love_couple":
        cards = random.sample(TAROT_CARDS, 3)
        answer = await ask_claude(f"Ситуация: «{message.text}»\n\nРасклад на пару:\n• Он/она: {cards[0]}\n• Вы: {cards[1]}\n• Связь: {cards[2]}\n\nДай глубокую интерпретацию. 200–250 слов.")
        result = f"💑 *Расклад на пару*\n\n👤 *Он/она:* {cards[0]}\n👤 *Вы:* {cards[1]}\n🔗 *Связь:* {cards[2]}\n\n{answer}"

    elif action == "love_continue":
        card = random.choice(TAROT_CARDS)
        answer = await ask_claude(f"Ситуация: «{message.text}»\nКарта совета: {card}\n\nОтветь на вопрос 'Стоит ли продолжать отношения?' Честно и глубоко. 150–200 слов.")
        result = f"🤔 *Стоит ли продолжать?*\n\n*Карта совета:* {card}\n\n{answer}"

    elif action == "love_future":
        cards = random.sample(TAROT_CARDS, 3)
        answer = await ask_claude(f"Отношения: «{message.text}»\n\nРасклад на будущее:\n• Ближайшее: {cards[0]}\n• Развитие: {cards[1]}\n• Итог: {cards[2]}\n\nИнтерпретация будущего. 200–250 слов.")
        result = f"🔮 *Будущее отношений*\n\n⏰ *Ближайшее:* {cards[0]}\n📈 *Развитие:* {cards[1]}\n🎯 *Итог:* {cards[2]}\n\n{answer}"

    elif action == "free_question":
        result = await ask_claude(message.text)

    else:
        result = "❓ Не понял команду. Воспользуйтесь меню."

    await processing.delete()
    await message.answer(result, parse_mode="Markdown", reply_markup=back_button())

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def set_commands():
    await bot.set_my_commands([
        BotCommand(command="menu", description="🔮 Главное меню"),
        BotCommand(command="card", description="🌟 Карта дня"),
        BotCommand(command="horoscope", description="📅 Гороскоп"),
        BotCommand(command="moon", description="🌙 Лунный календарь"),
        BotCommand(command="ritual", description="🌿 Ритуал дня"),
        BotCommand(command="lucky", description="🔑 Число удачи"),
        BotCommand(command="love", description="❤️ Любовь и отношения"),
        BotCommand(command="week", description="🃏 Карта недели"),
        BotCommand(command="tarot", description="🎴 Расклад Таро"),
        BotCommand(command="numerology", description="🔢 Нумерология"),
        BotCommand(command="subscription", description="💎 Подписка"),
    ])

async def main():
    logger.info("Бот Мистра запускается...")
    await init_db()
    await set_commands()
    asyncio.create_task(daily_broadcast_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
