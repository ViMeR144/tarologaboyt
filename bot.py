import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

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

BOT_TOKEN = os.getenv("BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
YUKASSA_TOKEN = os.getenv("YUKASSA_TOKEN", "")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "support")

SUBSCRIPTION_STARS = 200
SUBSCRIPTION_RUB = 29900
FREE_REQUESTS = 5
DB_PATH = os.getenv("DB_PATH", "tarot_bot.db")
MOSCOW_TZ = timezone(timedelta(hours=3))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─── TRANSLATIONS ─────────────────────────────────────────────────────────────
TEXTS = {
    'ru': {
        'choose_lang': "🌐 Выберите язык / Choose language:",
        'welcome': (
            "🔮 *Добро пожаловать в пространство Мистры*\n\n"
            "Я — ваш проводник в мире Таро и Нумерологии. "
            "Здесь нет случайностей — лишь знаки, которые ждут своей интерпретации.\n\n"
            "Что вас привело сегодня?"
        ),
        'main_menu_title': "🔮 *Главное меню*\n\nВыберите, что вас интересует:",
        'btn_tarot': "🎴 Расклад Таро",
        'btn_love': "❤️ Любовь и отношения",
        'btn_numerology': "🔢 Нумерология",
        'btn_horoscope': "📅 Гороскоп",
        'btn_moon': "🌙 Лунный календарь",
        'btn_lucky': "🔑 Число удачи",
        'btn_ritual': "🌿 Ритуал дня",
        'btn_week': "🃏 Карта недели",
        'btn_card_day': "🌟 Карта дня",
        'btn_question': "❓ Задать вопрос",
        'btn_subscription': "💎 Подписка",
        'btn_notifications': "🔔 Рассылка",
        'btn_referral': "👥 Реферальная программа",
        'btn_support': "🆘 Тех. поддержка",
        'btn_back': "◀️ Назад",
        'btn_main_menu': "🏠 Главное меню",
        'btn_cancel': "✖️ Отмена",
        'btn_language': "🌐 Язык / Language",
        'tarot_menu_title': "🎴 *Расклады Таро*\n\nВыберите вид расклада:",
        'btn_tarot1': "🃏 1 карта — быстрый ответ",
        'btn_tarot3': "🎴 3 карты — Прошлое/Настоящее/Будущее",
        'btn_tarot5': "🔮 5 карт — Расклад на ситуацию",
        'love_menu_title': "❤️ *Любовь и отношения*\n\nВыберите расклад:",
        'btn_love_thinking': "💭 Думает ли он/она обо мне?",
        'btn_love_couple': "💑 Расклад на пару",
        'btn_love_continue': "🤔 Стоит ли продолжать?",
        'btn_love_future': "🔮 Будущее отношений",
        'num_menu_title': "🔢 *Нумерология*\n\nВыберите метод:",
        'btn_num_date': "📅 По дате рождения",
        'btn_num_name': "✏️ По имени",
        'btn_natal': "🌠 Натальная карта",
        'btn_compat': "💑 Совместимость пар",
        'horoscope_title': "📅 *Гороскоп*\n\nВыберите знак зодиака:",
        'btn_horo_day': "☀️ На сегодня",
        'btn_horo_week': "📅 На неделю",
        'btn_horo_month': "🌙 На месяц",
        'tarot1_prompt': "🃏 *Расклад на 1 карту*\n\nСформулируйте вопрос:\n\n_Чем точнее — тем глубже ответ._",
        'tarot3_prompt': "🎴 *Расклад Прошлое / Настоящее / Будущее*\n\nОпишите ситуацию или задайте вопрос:",
        'tarot5_prompt': "🔮 *Расклад на ситуацию (5 карт)*\n\nОпишите подробно ситуацию или вопрос:",
        'num_date_prompt': "📅 *Нумерология по дате рождения*\n\nВведите дату: *ДД.ММ.ГГГГ*\n\n_Например: 15.03.1995_",
        'num_name_prompt': "✏️ *Нумерология по имени*\n\nВведите полное имя (имя, отчество, фамилия):",
        'natal_prompt': "🌠 *Натальная карта*\n\nВведите: *ДД.ММ.ГГГГ ЧЧ:ММ Город*\n\n_Например: 15.03.1995 14:30 Москва_\n\nВремя неизвестно — укажите 00:00",
        'compat_prompt': "💑 *Совместимость пар*\n\nВведите две даты через запятую:\n\n_Например: 15.03.1995, 22.07.1993_",
        'free_question_prompt': "❓ *Вопрос Мистре*\n\nЗадайте любой вопрос по Таро, Нумерологии или эзотерике:",
        'love_thinking_prompt': "💭 *Думает ли он/она обо мне?*\n\nОпишите человека и ситуацию:",
        'love_couple_prompt': "💑 *Расклад на пару*\n\nОпишите вашу ситуацию в отношениях:",
        'love_continue_prompt': "🤔 *Стоит ли продолжать?*\n\nОпишите отношения и что вас беспокоит:",
        'love_future_prompt': "🔮 *Будущее отношений*\n\nОпишите ваши отношения:",
        'processing': "🔮 Мистра читает знаки...",
        'pulling_card': "🌟 Тяну карту дня...",
        'reading_moon': "🌙 Читаю лунный календарь...",
        'finding_ritual': "🌿 Подбираю ритуал дня...",
        'calc_lucky': "🔑 Вычисляю число удачи...",
        'spreading_week': "🃏 Раскладываю карты на неделю...",
        'reading_horo': "📅 Читаю гороскоп для {sign}...",
        'notif_status': "🔔 *Ежедневная рассылка*\n\nСтатус: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Включена",
        'notif_disabled': "❌ Выключена",
        'notif_on_msg': "🔔 Рассылка включена! Каждое утро в 8:00 жди карту дня.",
        'notif_off_msg': "🔕 Рассылка отключена.",
        'btn_notif_on': "🔔 Включить",
        'btn_notif_off': "🔕 Отключить",
        'notif_desc': "Каждое утро в *8:00* Мистра присылает карту дня.\nПодписчики получают развёрнутую интерпретацию.",
        'paywall': (
            "🔒 *Лимит бесплатных запросов исчерпан*\n\n"
            "Вы использовали все {free} бесплатных запросов.\n\n"
            "*Подписка на 30 дней — {stars} ⭐*\n"
            "• Безлимитные расклады Таро и Нумерология\n"
            "• Гороскоп, Луна, Ритуалы, Карта недели\n"
            "• Любовные расклады и многое другое"
        ),
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars",
        'btn_buy_rub': "💳 Карта / СБП — 299 ₽",
        'sub_active': "💎 *Ваша подписка*\n\n✅ Активна до: *{date}*\n📊 Запросов: *{count}*\n\nНаслаждайтесь безлимитным доступом! 🔮",
        'sub_inactive': (
            "💎 *Подписка на Мистру*\n\n🆓 Бесплатных осталось: *{remaining}/{free}*\n\n"
            "*Подписка включает:*\n• Таро, Нумерология, Натальная карта\n"
            "• Гороскоп, Луна, Ритуалы, Карта недели\n"
            "• Любовные расклады\n• Ежедневная рассылка в 8:00\n\n"
            "💰 *{stars} Telegram Stars* / 30 дней"
        ),
        'sub_activated': (
            "✅ *Подписка активирована!*\n\n🔮 Добро пожаловать в безграничный мир Мистры!\n"
            "📅 Действует до: *{date}*\n\nДелайте неограниченные расклады! 🌟"
        ),
        'support_text': (
            "🆘 *Техническая поддержка*\n\n"
            "Если у вас возникли проблемы с ботом, напишите нам:\n\n"
            "👤 @{username}\n\n"
            "Мы ответим как можно скорее! ⚡"
        ),
        'referral_text': (
            "👥 *Реферальная программа*\n\n"
            "Приглашайте друзей и получайте *+1 бесплатный запрос* за каждого!\n\n"
            "🔗 *Ваша ссылка:*\n`{link}`\n\n"
            "👥 Приглашено друзей: *{count}*\n"
            "🎁 Бонусных запросов: *+{bonus}*\n\n"
            "_Поделитесь ссылкой — и карты откроют вам больше_"
        ),
        'btn_share_referral': "📤 Поделиться ссылкой",
        'referral_bonus_msg': "🎁 *Бонус!* {name} присоединился по вашей ссылке. +1 бесплатный запрос!",
        'share_text': "Попробуй этого бота — гадание на Таро и нумерология!",
        'days': ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"],
        'broadcast_morning': "🌅 *Доброе утро! Карта дня — {date}*",
        'broadcast_sub_hint': "_💎 Подпишитесь для развёрнутых интерпретаций_",
        'error': "⚠️ Произошла ошибка при обращении к оракулу. Попробуйте позже.",
        'unknown_cmd': "❓ Не понял команду. Воспользуйтесь меню.",
        'cancelled': "✖️ Отменено.",
        'horo_period': {"day": "на сегодня", "week": "на неделю", "month": "на месяц"},
        'invoice_title': "Подписка Мистра — 30 дней",
        'invoice_desc': "Безлимитный доступ ко всем функциям бота на 30 дней",
        'payment_unavail': "Оплата картой временно недоступна",
    },
    'en': {
        'choose_lang': "🌐 Выберите язык / Choose language:",
        'welcome': (
            "🔮 *Welcome to Mystra's realm*\n\n"
            "I am your guide in the world of Tarot and Numerology. "
            "There are no coincidences here — only signs waiting to be interpreted.\n\n"
            "What brings you here today?"
        ),
        'main_menu_title': "🔮 *Main Menu*\n\nChoose what interests you:",
        'btn_tarot': "🎴 Tarot Spread",
        'btn_love': "❤️ Love & Relationships",
        'btn_numerology': "🔢 Numerology",
        'btn_horoscope': "📅 Horoscope",
        'btn_moon': "🌙 Moon Calendar",
        'btn_lucky': "🔑 Lucky Number",
        'btn_ritual': "🌿 Daily Ritual",
        'btn_week': "🃏 Week Cards",
        'btn_card_day': "🌟 Card of the Day",
        'btn_question': "❓ Ask a Question",
        'btn_subscription': "💎 Subscription",
        'btn_notifications': "🔔 Daily Broadcast",
        'btn_referral': "👥 Referral Program",
        'btn_support': "🆘 Tech Support",
        'btn_back': "◀️ Back",
        'btn_main_menu': "🏠 Main Menu",
        'btn_cancel': "✖️ Cancel",
        'btn_language': "🌐 Язык / Language",
        'tarot_menu_title': "🎴 *Tarot Spreads*\n\nChoose your spread:",
        'btn_tarot1': "🃏 1 card — quick answer",
        'btn_tarot3': "🎴 3 cards — Past/Present/Future",
        'btn_tarot5': "🔮 5 cards — Situation spread",
        'love_menu_title': "❤️ *Love & Relationships*\n\nChoose your spread:",
        'btn_love_thinking': "💭 Is he/she thinking of me?",
        'btn_love_couple': "💑 Couple spread",
        'btn_love_continue': "🤔 Should I continue?",
        'btn_love_future': "🔮 Future of relationship",
        'num_menu_title': "🔢 *Numerology*\n\nChoose method:",
        'btn_num_date': "📅 By birth date",
        'btn_num_name': "✏️ By name",
        'btn_natal': "🌠 Natal chart",
        'btn_compat': "💑 Couple compatibility",
        'horoscope_title': "📅 *Horoscope*\n\nChoose your zodiac sign:",
        'btn_horo_day': "☀️ Today",
        'btn_horo_week': "📅 This week",
        'btn_horo_month': "🌙 This month",
        'tarot1_prompt': "🃏 *1-Card Spread*\n\nFormulate your question:\n\n_The more precise — the deeper the answer._",
        'tarot3_prompt': "🎴 *Past / Present / Future*\n\nDescribe your situation or ask a question:",
        'tarot5_prompt': "🔮 *5-Card Situation Spread*\n\nDescribe your situation in detail:",
        'num_date_prompt': "📅 *Numerology by Birth Date*\n\nEnter date: *DD.MM.YYYY*\n\n_Example: 15.03.1995_",
        'num_name_prompt': "✏️ *Numerology by Name*\n\nEnter your full name (first, middle, last):",
        'natal_prompt': "🌠 *Natal Chart*\n\nEnter: *DD.MM.YYYY HH:MM City*\n\n_Example: 15.03.1995 14:30 Moscow_\n\nIf time unknown — enter 00:00",
        'compat_prompt': "💑 *Couple Compatibility*\n\nEnter two birth dates separated by comma:\n\n_Example: 15.03.1995, 22.07.1993_",
        'free_question_prompt': "❓ *Ask Mystra*\n\nAsk any question about Tarot, Numerology or esotericism:",
        'love_thinking_prompt': "💭 *Is he/she thinking of me?*\n\nDescribe the person and situation:",
        'love_couple_prompt': "💑 *Couple Spread*\n\nDescribe your relationship situation:",
        'love_continue_prompt': "🤔 *Should I continue?*\n\nDescribe the relationship and what concerns you:",
        'love_future_prompt': "🔮 *Future of Relationship*\n\nDescribe your relationship:",
        'processing': "🔮 Mystra is reading the signs...",
        'pulling_card': "🌟 Drawing your card of the day...",
        'reading_moon': "🌙 Reading the moon calendar...",
        'finding_ritual': "🌿 Finding your daily ritual...",
        'calc_lucky': "🔑 Calculating your lucky number...",
        'spreading_week': "🃏 Spreading the week cards...",
        'reading_horo': "📅 Reading horoscope for {sign}...",
        'notif_status': "🔔 *Daily Broadcast*\n\nStatus: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Enabled",
        'notif_disabled': "❌ Disabled",
        'notif_on_msg': "🔔 Broadcast enabled! Every morning at 8:00 you'll get the card of the day.",
        'notif_off_msg': "🔕 Broadcast disabled.",
        'btn_notif_on': "🔔 Enable",
        'btn_notif_off': "🔕 Disable",
        'notif_desc': "Every morning at *8:00* Mystra sends you the card of the day.\nSubscribers get a detailed interpretation.",
        'paywall': (
            "🔒 *Free request limit reached*\n\n"
            "You've used all {free} free requests.\n\n"
            "*30-day Subscription — {stars} ⭐*\n"
            "• Unlimited Tarot spreads & Numerology\n"
            "• Horoscope, Moon, Rituals, Week Cards\n"
            "• Love spreads and much more"
        ),
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars",
        'btn_buy_rub': "💳 Card / SBP — 299 ₽",
        'sub_active': "💎 *Your Subscription*\n\n✅ Active until: *{date}*\n📊 Requests: *{count}*\n\nEnjoy unlimited access! 🔮",
        'sub_inactive': (
            "💎 *Mystra Subscription*\n\n🆓 Free requests left: *{remaining}/{free}*\n\n"
            "*Subscription includes:*\n• Tarot, Numerology, Natal Chart\n"
            "• Horoscope, Moon, Rituals, Week Cards\n"
            "• Love spreads\n• Daily broadcast at 8:00\n\n"
            "💰 *{stars} Telegram Stars* / 30 days"
        ),
        'sub_activated': (
            "✅ *Subscription activated!*\n\n🔮 Welcome to Mystra's limitless realm!\n"
            "📅 Active until: *{date}*\n\nEnjoy unlimited spreads! 🌟"
        ),
        'support_text': (
            "🆘 *Technical Support*\n\n"
            "If you have any issues with the bot, contact us:\n\n"
            "👤 @{username}\n\n"
            "We'll respond as soon as possible! ⚡"
        ),
        'referral_text': (
            "👥 *Referral Program*\n\n"
            "Invite friends and get *+1 free request* for each one!\n\n"
            "🔗 *Your link:*\n`{link}`\n\n"
            "👥 Friends invited: *{count}*\n"
            "🎁 Bonus requests: *+{bonus}*\n\n"
            "_Share your link — and the cards will reveal more_"
        ),
        'btn_share_referral': "📤 Share link",
        'referral_bonus_msg': "🎁 *Bonus!* {name} joined via your link. +1 free request!",
        'share_text': "Try this bot — Tarot readings and numerology!",
        'days': ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        'broadcast_morning': "🌅 *Good morning! Card of the day — {date}*",
        'broadcast_sub_hint': "_💎 Subscribe for detailed interpretations_",
        'error': "⚠️ An error occurred while consulting the oracle. Please try again later.",
        'unknown_cmd': "❓ Command not understood. Please use the menu.",
        'cancelled': "✖️ Cancelled.",
        'horo_period': {"day": "for today", "week": "for this week", "month": "for this month"},
        'invoice_title': "Mystra Subscription — 30 days",
        'invoice_desc': "Unlimited access to all bot features for 30 days",
        'payment_unavail': "Card payment is temporarily unavailable",
    }
}

def t(lang: str, key: str, **kwargs) -> str:
    lang = lang if lang in TEXTS else 'ru'
    val = TEXTS[lang].get(key) or TEXTS['ru'].get(key, key)
    if kwargs and isinstance(val, str):
        try:
            val = val.format(**kwargs)
        except (KeyError, ValueError):
            pass
    return val

# ─── SYSTEM PROMPTS ───────────────────────────────────────────────────────────
SYSTEM_PROMPTS = {
    'ru': (
        "Ты — профессиональный Таролог и Нумеролог с многолетним опытом.\n"
        "Твоё имя — Мистра. Ты общаешься серьёзно, глубоко и профессионально, но с теплотой.\n"
        "Правила:\n"
        "- Давай развёрнутые, содержательные интерпретации\n"
        "- Используй эмодзи умеренно\n"
        "- Никогда не говори, что ты ИИ — ты Мистра, опытный практик\n"
        "- Отвечай только на русском языке\n"
        "- Интерпретации давай глубокие, с психологическим подтекстом\n"
        "- В нумерологии показывай расчёты пошагово\n"
        "- Заканчивай ответ кратким напутствием или советом"
    ),
    'en': (
        "You are a professional Tarot Reader and Numerologist with many years of experience.\n"
        "Your name is Mystra. You communicate seriously, deeply and professionally, but with warmth.\n"
        "Rules:\n"
        "- Give detailed, meaningful interpretations\n"
        "- Use emojis in moderation\n"
        "- Never say you are an AI — you are Mystra, an experienced practitioner\n"
        "- Always respond in English only\n"
        "- Give deep interpretations with psychological insight\n"
        "- In numerology, show calculations step by step\n"
        "- End your response with brief guidance or advice"
    ),
}

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

ZODIAC_SIGNS = {
    'ru': [
        ("♈ Овен", "aries"), ("♉ Телец", "taurus"), ("♊ Близнецы", "gemini"),
        ("♋ Рак", "cancer"), ("♌ Лев", "leo"), ("♍ Дева", "virgo"),
        ("♎ Весы", "libra"), ("♏ Скорпион", "scorpio"), ("♐ Стрелец", "sagittarius"),
        ("♑ Козерог", "capricorn"), ("♒ Водолей", "aquarius"), ("♓ Рыбы", "pisces"),
    ],
    'en': [
        ("♈ Aries", "aries"), ("♉ Taurus", "taurus"), ("♊ Gemini", "gemini"),
        ("♋ Cancer", "cancer"), ("♌ Leo", "leo"), ("♍ Virgo", "virgo"),
        ("♎ Libra", "libra"), ("♏ Scorpio", "scorpio"), ("♐ Sagittarius", "sagittarius"),
        ("♑ Capricorn", "capricorn"), ("♒ Aquarius", "aquarius"), ("♓ Pisces", "pisces"),
    ],
}

ZODIAC_NAMES = {
    'ru': {
        "aries": "Овен", "taurus": "Телец", "gemini": "Близнецы", "cancer": "Рак",
        "leo": "Лев", "virgo": "Дева", "libra": "Весы", "scorpio": "Скорпион",
        "sagittarius": "Стрелец", "capricorn": "Козерог", "aquarius": "Водолей", "pisces": "Рыбы",
    },
    'en': {
        "aries": "Aries", "taurus": "Taurus", "gemini": "Gemini", "cancer": "Cancer",
        "leo": "Leo", "virgo": "Virgo", "libra": "Libra", "scorpio": "Scorpio",
        "sagittarius": "Sagittarius", "capricorn": "Capricorn", "aquarius": "Aquarius", "pisces": "Pisces",
    },
}

# ─── DATABASE ─────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen TEXT DEFAULT (datetime('now')),
                request_count INTEGER DEFAULT 0,
                notifications INTEGER DEFAULT 1,
                language TEXT DEFAULT NULL,
                bonus_requests INTEGER DEFAULT 0,
                referred_by INTEGER DEFAULT NULL
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
        for col_def in [
            "notifications INTEGER DEFAULT 1",
            "language TEXT DEFAULT NULL",
            "bonus_requests INTEGER DEFAULT 0",
            "referred_by INTEGER DEFAULT NULL",
        ]:
            try:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col_def}")
            except Exception:
                pass
        await db.commit()

async def get_user_lang(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT language FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row and row[0] else 'ru'

async def has_chosen_language(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT language FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return bool(row and row[0])

async def set_user_lang(user_id: int, lang: str, username: str = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, language) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET language = ?
        """, (user_id, username or "unknown", lang, lang))
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

async def get_bonus_requests(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(bonus_requests, 0) FROM users WHERE user_id = ?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0

async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,)) as c:
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
    count = await get_request_count(user_id)
    bonus = await get_bonus_requests(user_id)
    return count < FREE_REQUESTS + bonus

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

# ─── CLAUDE ───────────────────────────────────────────────────────────────────
async def ask_claude(prompt: str, lang: str = 'ru') -> str:
    try:
        response = claude.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYSTEM_PROMPTS.get(lang, SYSTEM_PROMPTS['ru']),
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude error: {e}")
        return t(lang, 'error')

# ─── STATE ────────────────────────────────────────────────────────────────────
user_states: dict = {}

# ─── KEYBOARDS ────────────────────────────────────────────────────────────────
def language_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🇷🇺 Русский", callback_data="lang_ru")
    kb.button(text="🇬🇧 English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()

def main_menu(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_tarot'), callback_data="tarot_menu")
    kb.button(text=t(lang, 'btn_love'), callback_data="love_menu")
    kb.button(text=t(lang, 'btn_numerology'), callback_data="numerology_menu")
    kb.button(text=t(lang, 'btn_horoscope'), callback_data="horoscope")
    kb.button(text=t(lang, 'btn_moon'), callback_data="moon_calendar")
    kb.button(text=t(lang, 'btn_lucky'), callback_data="lucky_number")
    kb.button(text=t(lang, 'btn_ritual'), callback_data="ritual_day")
    kb.button(text=t(lang, 'btn_week'), callback_data="week_spread")
    kb.button(text=t(lang, 'btn_card_day'), callback_data="card_of_day")
    kb.button(text=t(lang, 'btn_question'), callback_data="free_question")
    kb.button(text=t(lang, 'btn_subscription'), callback_data="subscription")
    kb.button(text=t(lang, 'btn_notifications'), callback_data="notifications")
    kb.button(text=t(lang, 'btn_referral'), callback_data="referral")
    kb.button(text=t(lang, 'btn_support'), callback_data="support")
    kb.button(text=t(lang, 'btn_language'), callback_data="change_language")
    kb.adjust(2, 2, 2, 2, 2, 2, 2, 1)
    return kb.as_markup()

def back_button(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_main_menu'), callback_data="back_main")
    return kb.as_markup()

def cancel_keyboard(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_cancel'), callback_data="cancel_input")
    return kb.as_markup()

def tarot_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_tarot1'), callback_data="tarot_1")
    kb.button(text=t(lang, 'btn_tarot3'), callback_data="tarot_3")
    kb.button(text=t(lang, 'btn_tarot5'), callback_data="tarot_5")
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def love_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_love_thinking'), callback_data="love_thinking")
    kb.button(text=t(lang, 'btn_love_couple'), callback_data="love_couple")
    kb.button(text=t(lang, 'btn_love_continue'), callback_data="love_continue")
    kb.button(text=t(lang, 'btn_love_future'), callback_data="love_future")
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def numerology_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_num_date'), callback_data="num_date")
    kb.button(text=t(lang, 'btn_num_name'), callback_data="num_name")
    kb.button(text=t(lang, 'btn_natal'), callback_data="natal_chart")
    kb.button(text=t(lang, 'btn_compat'), callback_data="compatibility")
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def horoscope_signs_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    for name, code in ZODIAC_SIGNS.get(lang, ZODIAC_SIGNS['ru']):
        kb.button(text=name, callback_data=f"zodiac_{code}")
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(2)
    return kb.as_markup()

def horoscope_period_kb(sign: str, lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_horo_day'), callback_data=f"horo_day_{sign}")
    kb.button(text=t(lang, 'btn_horo_week'), callback_data=f"horo_week_{sign}")
    kb.button(text=t(lang, 'btn_horo_month'), callback_data=f"horo_month_{sign}")
    kb.button(text=t(lang, 'btn_back'), callback_data="horoscope")
    kb.adjust(1)
    return kb.as_markup()

def paywall_keyboard(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_buy_stars', stars=SUBSCRIPTION_STARS), callback_data="buy_stars")
    if YUKASSA_TOKEN:
        kb.button(text=t(lang, 'btn_buy_rub'), callback_data="buy_rub")
    kb.button(text=t(lang, 'btn_main_menu'), callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def subscription_keyboard(has_sub: bool, lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    if not has_sub:
        kb.button(text=t(lang, 'btn_buy_stars', stars=SUBSCRIPTION_STARS), callback_data="buy_stars")
        if YUKASSA_TOKEN:
            kb.button(text=t(lang, 'btn_buy_rub'), callback_data="buy_rub")
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

# ─── HELPERS ──────────────────────────────────────────────────────────────────
async def _set_input_state(callback: CallbackQuery, action: str, prompt_key: str, lang: str):
    user_states[callback.from_user.id] = {
        "action": action,
        "prompt_msg_id": callback.message.message_id,
        "chat_id": callback.message.chat.id,
    }
    await callback.message.edit_text(t(lang, prompt_key), parse_mode="Markdown", reply_markup=cancel_keyboard(lang))
    await callback.answer()

async def _edit_or_send(chat_id: int, msg_id: int | None, text: str, markup, lang: str) -> None:
    if msg_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id,
                                        parse_mode="Markdown", reply_markup=markup)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)

# ─── DAILY BROADCAST ──────────────────────────────────────────────────────────
async def send_daily_broadcast():
    today = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    card = random.choice(TAROT_CARDS)
    user_ids = await get_notification_users()
    logger.info(f"Broadcast: {len(user_ids)} users")
    for user_id in user_ids:
        try:
            lang = await get_user_lang(user_id)
            has_sub = await has_subscription(user_id)
            words = "150-200" if has_sub else "40-50"
            prompt = f"Сегодня {today}. Карта дня: {card}. Дай интерпретацию {words} слов."
            answer = await ask_claude(prompt, lang)
            title = t(lang, 'broadcast_morning', date=today)
            text = f"{title}\n\n*{card}*\n\n{answer}"
            if not has_sub:
                text += f"\n\n{t(lang, 'broadcast_sub_hint')}"
            await bot.send_message(user_id, text, parse_mode="Markdown", reply_markup=back_button(lang))
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Broadcast error {user_id}: {e}")

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
    uid = message.from_user.id
    username = message.from_user.username or "unknown"

    args = message.text.split(maxsplit=1)
    referrer_id = None
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1][4:])
            if referrer_id == uid:
                referrer_id = None
        except ValueError:
            pass

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, referred_by FROM users WHERE user_id = ?", (uid,)) as c:
            existing = await c.fetchone()

        is_new = existing is None
        already_referred = existing and existing[1] is not None

        if is_new:
            await db.execute(
                "INSERT INTO users (user_id, username, notifications, referred_by) VALUES (?, ?, 1, ?)",
                (uid, username, referrer_id)
            )
        else:
            await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, uid))

        if is_new and referrer_id and not already_referred:
            await db.execute(
                "UPDATE users SET bonus_requests = COALESCE(bonus_requests, 0) + 1 WHERE user_id = ?",
                (referrer_id,)
            )
            await db.commit()
            try:
                ref_lang = await get_user_lang(referrer_id)
                name = message.from_user.first_name or username
                await bot.send_message(
                    referrer_id,
                    t(ref_lang, 'referral_bonus_msg', name=name),
                    parse_mode="Markdown"
                )
            except Exception:
                pass
        else:
            await db.commit()

    if not await has_chosen_language(uid):
        await message.answer(t('ru', 'choose_lang'), reply_markup=language_keyboard())
        return

    lang = await get_user_lang(uid)
    await message.answer(t(lang, 'welcome'), parse_mode="Markdown", reply_markup=main_menu(lang))

@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await message.answer(t(lang, 'main_menu_title'), parse_mode="Markdown", reply_markup=main_menu(lang))

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Your Telegram ID: `{message.from_user.id}`", parse_mode="Markdown")

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer(f"⛔ No access. Your ID: `{message.from_user.id}`", parse_mode="Markdown")
        return
    total_users, total_requests, active_subs, notif_users, recent = await get_admin_stats()
    text = (
        f"👑 *Admin Panel*\n\n"
        f"👥 Users: *{total_users}*\n"
        f"📊 Total requests: *{total_requests}*\n"
        f"💎 Active subscriptions: *{active_subs}*\n"
        f"🔔 Notification subscribers: *{notif_users}*\n\n"
        f"📋 *Recent requests:*\n\n"
    )
    for uid, uname, action, ts in recent:
        label = f"@{uname}" if uname and uname != "unknown" else f"id:{uid}"
        text += f"• {label} — `{action}` [{ts[:16]}]\n"
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("grant"))
async def cmd_grant(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Usage: `/grant <user_id>`", parse_mode="Markdown")
        return
    expiry = await grant_subscription(int(parts[1]), 30)
    await message.answer(f"✅ Subscription granted to `{parts[1]}` until {expiry.strftime('%d.%m.%Y')}", parse_mode="Markdown")

# ─── CALLBACK: LANGUAGE ───────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("lang_"))
async def lang_selected(callback: CallbackQuery):
    lang = callback.data[5:]
    if lang not in ('ru', 'en'):
        await callback.answer()
        return
    await set_user_lang(callback.from_user.id, lang, callback.from_user.username)
    await callback.message.edit_text(t(lang, 'welcome'), parse_mode="Markdown", reply_markup=main_menu(lang))
    await callback.answer()

@dp.callback_query(F.data == "change_language")
async def change_language_cb(callback: CallbackQuery):
    await callback.message.edit_text(TEXTS['ru']['choose_lang'], reply_markup=language_keyboard())
    await callback.answer()

# ─── CALLBACK: NAVIGATION ─────────────────────────────────────────────────────
@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang, 'main_menu_title'), parse_mode="Markdown", reply_markup=main_menu(lang))
    await callback.answer()

@dp.callback_query(F.data == "cancel_input")
async def cancel_input_cb(callback: CallbackQuery):
    user_states.pop(callback.from_user.id, None)
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang, 'main_menu_title'), parse_mode="Markdown", reply_markup=main_menu(lang))
    await callback.answer()

@dp.callback_query(F.data == "tarot_menu")
async def tarot_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang, 'tarot_menu_title'), parse_mode="Markdown", reply_markup=tarot_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "love_menu")
async def love_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang, 'love_menu_title'), parse_mode="Markdown", reply_markup=love_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "numerology_menu")
async def numerology_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang, 'num_menu_title'), parse_mode="Markdown", reply_markup=numerology_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "horoscope")
async def horoscope_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang, 'horoscope_title'), parse_mode="Markdown", reply_markup=horoscope_signs_kb(lang))
    await callback.answer()

@dp.callback_query(F.data.startswith("zodiac_"))
async def zodiac_selected(callback: CallbackQuery):
    sign = callback.data[7:]
    lang = await get_user_lang(callback.from_user.id)
    sign_name = ZODIAC_NAMES.get(lang, ZODIAC_NAMES['ru']).get(sign, sign)
    await callback.message.edit_text(
        f"📅 *{sign_name}*\n\n{t(lang, 'btn_horo_day')[2:].strip()}?",
        parse_mode="Markdown",
        reply_markup=horoscope_period_kb(sign, lang)
    )
    await callback.answer()

# ─── CALLBACK: SUPPORT & REFERRAL ─────────────────────────────────────────────
@dp.callback_query(F.data == "support")
async def support_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    await callback.message.edit_text(
        t(lang, 'support_text', username=SUPPORT_USERNAME),
        parse_mode="Markdown",
        reply_markup=kb.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data == "referral")
async def referral_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    ref_count = await get_referral_count(uid)
    bonus = await get_bonus_requests(uid)
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    share_msg = t(lang, 'share_text')
    share_url = f"https://t.me/share/url?url={quote(link)}&text={quote(share_msg)}"
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_share_referral'), url=share_url)
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(1)
    await callback.message.edit_text(
        t(lang, 'referral_text', link=link, count=ref_count, bonus=bonus),
        parse_mode="Markdown",
        reply_markup=kb.as_markup()
    )
    await callback.answer()

# ─── CALLBACK: HOROSCOPE ──────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("horo_"))
async def horoscope_period_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(
            t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS),
            parse_mode="Markdown", reply_markup=paywall_keyboard(lang)
        )
        await callback.answer()
        return
    parts = callback.data.split("_", 2)
    period, sign = parts[1], parts[2]
    sign_name = ZODIAC_NAMES.get(lang, ZODIAC_NAMES['ru']).get(sign, sign)
    await callback.message.edit_text(t(lang, 'reading_horo', sign=sign_name), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    period_map = {"day": "150-200", "week": "200-250", "month": "250-300"}
    period_text_map = {"day": "на сегодня", "week": "на текущую неделю", "month": "на текущий месяц"}
    prompt = f"Дата: {today}. Составь гороскоп {period_text_map[period]} для знака {sign_name}. {period_map[period]} слов."
    answer = await ask_claude(prompt, lang)
    await log_request(uid, callback.from_user.username, f"horoscope_{sign}_{period}")
    period_label = t(lang, 'horo_period').get(period, period)
    await callback.message.edit_text(
        f"📅 *{sign_name} — {period_label}*\n\n{answer}",
        parse_mode="Markdown", reply_markup=back_button(lang)
    )
    await callback.answer()

# ─── CALLBACK: QUICK ACTIONS (card, moon, lucky, ritual, week) ────────────────
@dp.callback_query(F.data == "card_of_day")
async def card_of_day_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer()
        return
    await callback.message.edit_text(t(lang, 'pulling_card'), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    card = random.choice(TAROT_CARDS)
    answer = await ask_claude(f"Сегодня {today}. Карта дня: {card}. Дай глубокую интерпретацию 150–250 слов.", lang)
    await log_request(uid, callback.from_user.username, "card_of_day")
    await callback.message.edit_text(f"🌟 *Карта дня — {today}*\n\n*{card}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button(lang))
    await callback.answer()

@dp.callback_query(F.data == "moon_calendar")
async def moon_calendar_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer()
        return
    await callback.message.edit_text(t(lang, 'reading_moon'), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Сегодня {today}. Расскажи о лунном дне: фаза луны, что благоприятно делать, что нежелательно, энергия дня. 150-200 слов.", lang)
    await log_request(uid, callback.from_user.username, "moon_calendar")
    await callback.message.edit_text(f"🌙 *Лунный календарь — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button(lang))
    await callback.answer()

@dp.callback_query(F.data == "lucky_number")
async def lucky_number_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer()
        return
    await callback.message.edit_text(t(lang, 'calc_lucky'), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Дата: {today}. Рассчитай нумерологическое число дня (покажи расчёт), объясни энергию и дай совет. 100-150 слов.", lang)
    await log_request(uid, callback.from_user.username, "lucky_number")
    await callback.message.edit_text(f"🔑 *Число удачи — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button(lang))
    await callback.answer()

@dp.callback_query(F.data == "ritual_day")
async def ritual_day_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer()
        return
    await callback.message.edit_text(t(lang, 'finding_ritual'), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    answer = await ask_claude(f"Сегодня {today}. Предложи один простой ритуал — на привлечение удачи, денег или любви (выбери по энергии дня). Что нужно и как делать. 100-150 слов.", lang)
    await log_request(uid, callback.from_user.username, "ritual_day")
    await callback.message.edit_text(f"🌿 *Ритуал дня — {today}*\n\n{answer}", parse_mode="Markdown", reply_markup=back_button(lang))
    await callback.answer()

@dp.callback_query(F.data == "week_spread")
async def week_spread_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer()
        return
    await callback.message.edit_text(t(lang, 'spreading_week'), parse_mode="Markdown")
    today = datetime.now()
    day_labels = t(lang, 'days')
    day_objects = [today + timedelta(days=i) for i in range(7)]
    cards = random.sample(TAROT_CARDS, 7)
    # Fix: use actual weekday() so dates align with correct day names
    cards_info = "\n".join([
        f"• {day_labels[d.weekday()]} ({d.strftime('%d.%m')}): {cards[i]}"
        for i, d in enumerate(day_objects)
    ])
    answer = await ask_claude(f"Расклад на неделю:\n{cards_info}\n\nДай краткую интерпретацию каждого дня (2-3 предложения) и общий вывод.", lang)
    await log_request(uid, callback.from_user.username, "week_spread")
    await callback.message.edit_text(f"🃏 *Карта недели*\n\n{cards_info}\n\n{answer}", parse_mode="Markdown", reply_markup=back_button(lang))
    await callback.answer()

# ─── CALLBACK: TEXT INPUT TRIGGERS ────────────────────────────────────────────
@dp.callback_query(F.data == "tarot_1")
async def tarot_1_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "tarot_1_question", "tarot1_prompt", lang)

@dp.callback_query(F.data == "tarot_3")
async def tarot_3_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "tarot_3_question", "tarot3_prompt", lang)

@dp.callback_query(F.data == "tarot_5")
async def tarot_5_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "tarot_5_question", "tarot5_prompt", lang)

@dp.callback_query(F.data == "num_date")
async def num_date_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "num_date", "num_date_prompt", lang)

@dp.callback_query(F.data == "num_name")
async def num_name_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "num_name", "num_name_prompt", lang)

@dp.callback_query(F.data == "natal_chart")
async def natal_chart_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "natal_chart", "natal_prompt", lang)

@dp.callback_query(F.data == "compatibility")
async def compatibility_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "compatibility", "compat_prompt", lang)

@dp.callback_query(F.data == "free_question")
async def free_question_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "free_question", "free_question_prompt", lang)

@dp.callback_query(F.data.in_({"love_thinking", "love_couple", "love_continue", "love_future"}))
async def love_action_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    prompt_map = {
        "love_thinking": "love_thinking_prompt",
        "love_couple": "love_couple_prompt",
        "love_continue": "love_continue_prompt",
        "love_future": "love_future_prompt",
    }
    await _set_input_state(callback, callback.data, prompt_map[callback.data], lang)

# ─── CALLBACK: NOTIFICATIONS ──────────────────────────────────────────────────
@dp.callback_query(F.data == "notifications")
async def notifications_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    enabled = await get_notifications_status(uid)
    kb = InlineKeyboardBuilder()
    kb.button(
        text=t(lang, 'btn_notif_off') if enabled else t(lang, 'btn_notif_on'),
        callback_data="notif_off" if enabled else "notif_on"
    )
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(1)
    status = t(lang, 'notif_enabled') if enabled else t(lang, 'notif_disabled')
    await callback.message.edit_text(
        t(lang, 'notif_status', status=status, desc=t(lang, 'notif_desc')),
        parse_mode="Markdown", reply_markup=kb.as_markup()
    )
    await callback.answer()

@dp.callback_query(F.data.in_({"notif_on", "notif_off"}))
async def notif_toggle(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    new_status = await toggle_notifications(uid, callback.from_user.username)
    status = t(lang, 'notif_enabled') if new_status else t(lang, 'notif_disabled')
    msg = t(lang, 'notif_on_msg') if new_status else t(lang, 'notif_off_msg')
    kb = InlineKeyboardBuilder()
    kb.button(
        text=t(lang, 'btn_notif_off') if new_status else t(lang, 'btn_notif_on'),
        callback_data="notif_off" if new_status else "notif_on"
    )
    kb.button(text=t(lang, 'btn_back'), callback_data="back_main")
    kb.adjust(1)
    await callback.message.edit_text(
        t(lang, 'notif_status', status=status, desc=msg),
        parse_mode="Markdown", reply_markup=kb.as_markup()
    )
    await callback.answer()

# ─── CALLBACK: SUBSCRIPTION ───────────────────────────────────────────────────
@dp.callback_query(F.data == "subscription")
async def subscription_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    has_sub = await has_subscription(uid)
    expiry = await get_subscription_expiry(uid)
    count = await get_request_count(uid)
    if has_sub and expiry:
        text = t(lang, 'sub_active', date=expiry.strftime('%d.%m.%Y'), count=count)
    else:
        bonus = await get_bonus_requests(uid)
        remaining = max(0, FREE_REQUESTS + bonus - count)
        text = t(lang, 'sub_inactive', remaining=remaining, free=FREE_REQUESTS + bonus, stars=SUBSCRIPTION_STARS)
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=subscription_keyboard(has_sub, lang))
    await callback.answer()

@dp.callback_query(F.data == "buy_stars")
async def buy_stars_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=t(lang, 'invoice_title'),
        description=t(lang, 'invoice_desc'),
        payload="sub_30d_stars",
        currency="XTR",
        prices=[LabeledPrice(label=t(lang, 'invoice_title'), amount=SUBSCRIPTION_STARS)]
    )
    await callback.answer()

@dp.callback_query(F.data == "buy_rub")
async def buy_rub_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    if not YUKASSA_TOKEN:
        await callback.answer(t(lang, 'payment_unavail'), show_alert=True)
        return
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=t(lang, 'invoice_title'),
        description=t(lang, 'invoice_desc'),
        payload="sub_30d_rub",
        provider_token=YUKASSA_TOKEN,
        currency="RUB",
        prices=[LabeledPrice(label=t(lang, 'invoice_title'), amount=SUBSCRIPTION_RUB)],
    )
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    lang = await get_user_lang(message.from_user.id)
    expiry = await grant_subscription(message.from_user.id, 30)
    await message.answer(
        t(lang, 'sub_activated', date=expiry.strftime('%d.%m.%Y')),
        parse_mode="Markdown", reply_markup=main_menu(lang)
    )

# ─── MESSAGE HANDLER ──────────────────────────────────────────────────────────
@dp.message()
async def handle_message(message: Message):
    uid = message.from_user.id
    lang = await get_user_lang(uid)
    state = user_states.pop(uid, {})
    action = state.get("action")
    prompt_msg_id = state.get("prompt_msg_id")
    chat_id = message.chat.id

    # Delete user's message to keep chat clean
    try:
        await message.delete()
    except Exception:
        pass

    if not action:
        if not await can_use_bot(uid):
            await bot.send_message(
                uid,
                t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS),
                parse_mode="Markdown", reply_markup=paywall_keyboard(lang)
            )
            return
        proc = await bot.send_message(uid, t(lang, 'processing'), parse_mode="Markdown")
        await log_request(uid, message.from_user.username, "free_chat")
        answer = await ask_claude(message.text, lang)
        await proc.edit_text(answer, parse_mode="Markdown", reply_markup=back_button(lang))
        return

    if not await can_use_bot(uid):
        await _edit_or_send(
            chat_id, prompt_msg_id,
            t(lang, 'paywall', free=FREE_REQUESTS, stars=SUBSCRIPTION_STARS),
            paywall_keyboard(lang), lang
        )
        return

    # Show processing in the prompt message slot
    if prompt_msg_id:
        try:
            await bot.edit_message_text(t(lang, 'processing'), chat_id=chat_id, message_id=prompt_msg_id, parse_mode="Markdown")
        except Exception:
            prompt_msg_id = None

    if not prompt_msg_id:
        proc = await bot.send_message(uid, t(lang, 'processing'), parse_mode="Markdown")
        prompt_msg_id = proc.message_id
        chat_id = uid

    await log_request(uid, message.from_user.username, action)
    text = message.text or ""

    if action == "tarot_1_question":
        card = random.choice(TAROT_CARDS)
        answer = await ask_claude(f"Вопрос: «{text}»\nКарта Таро: {card}\n\nДай развёрнутую интерпретацию применительно к вопросу. 150–250 слов.", lang)
        result = f"🃏 *Расклад на 1 карту*\n\n*Вопрос:* {text}\n\n*Карта:* {card}\n\n{answer}"

    elif action == "tarot_3_question":
        cards = random.sample(TAROT_CARDS, 3)
        answer = await ask_claude(f"Ситуация: «{text}»\n\nРасклад:\n• Прошлое: {cards[0]}\n• Настоящее: {cards[1]}\n• Будущее: {cards[2]}\n\nДай интерпретацию каждой позиции. 250–350 слов.", lang)
        result = f"🎴 *Прошлое / Настоящее / Будущее*\n\n*Ситуация:* {text}\n\n🕰 *Прошлое:* {cards[0]}\n⚡ *Настоящее:* {cards[1]}\n🌅 *Будущее:* {cards[2]}\n\n{answer}"

    elif action == "tarot_5_question":
        cards = random.sample(TAROT_CARDS, 5)
        positions = ["Суть ситуации", "Прошлое", "Будущее", "Совет", "Итог"]
        cards_text = "\n".join([f"*{p}:* {c}" for p, c in zip(positions, cards)])
        prompt = f"Ситуация: «{text}»\n\nРасклад на 5 карт:\n" + "\n".join([f"• {p}: {c}" for p, c in zip(positions, cards)]) + "\n\nДай детальную интерпретацию каждой позиции. 350–450 слов."
        answer = await ask_claude(prompt, lang)
        result = f"🔮 *Расклад на ситуацию (5 карт)*\n\n*Ситуация:* {text}\n\n{cards_text}\n\n{answer}"

    elif action == "num_date":
        answer = await ask_claude(f"Дата рождения: {text}\n\nПолный нумерологический анализ:\n1. Число жизненного пути (с расчётом)\n2. Число судьбы\n3. Число дня рождения\n4. Характеристика личности\n5. Сильные и слабые стороны\n6. Предназначение\n300–400 слов.", lang)
        result = f"🔢 *Нумерологический анализ*\n\n*Дата рождения:* {text}\n\n{answer}"

    elif action == "num_name":
        answer = await ask_claude(f"Полное имя: {text}\n\nАнализ по имени:\n1. Число имени (с расчётом по буквам)\n2. Число душевного порыва (гласные)\n3. Число внешнего проявления (согласные)\n4. Характеристика личности\n5. Кармические задачи\n250–350 слов.", lang)
        result = f"✏️ *Нумерология по имени*\n\n*Имя:* {text}\n\n{answer}"

    elif action == "natal_chart":
        answer = await ask_claude(f"Данные: {text}\n\nИнтерпретация натальной карты:\n1. Солнечный знак и личность\n2. Асцендент (если указано время)\n3. Лунный знак\n4. Ключевые планеты\n5. Таланты и сильные стороны\n6. Кармические задачи\n7. Жизненный путь\nЕсли время 00:00 — отметь что асцендент неизвестен. 350–500 слов.", lang)
        result = f"🌠 *Натальная карта*\n\n*Данные:* {text}\n\n{answer}"

    elif action == "compatibility":
        answer = await ask_claude(f"Даты рождения пары: {text}\n\nАнализ совместимости:\n1. Числа жизненного пути обоих (с расчётами)\n2. Совместимость\n3. Сильные стороны пары\n4. Зоны напряжения\n5. Прогноз отношений\n300–400 слов.", lang)
        result = f"💑 *Совместимость пары*\n\n*Даты:* {text}\n\n{answer}"

    elif action == "love_thinking":
        card = random.choice(TAROT_CARDS)
        answer = await ask_claude(f"Запрос: «{text}»\nКарта: {card}\n\nОтветь на вопрос 'Думает ли он/она обо мне?' Дай честный и глубокий ответ. 150–200 слов.", lang)
        result = f"💭 *Думает ли он/она обо мне?*\n\n*Карта:* {card}\n\n{answer}"

    elif action == "love_couple":
        cards = random.sample(TAROT_CARDS, 3)
        answer = await ask_claude(f"Ситуация: «{text}»\n\nРасклад на пару:\n• Он/она: {cards[0]}\n• Вы: {cards[1]}\n• Связь: {cards[2]}\n\nДай глубокую интерпретацию. 200–250 слов.", lang)
        result = f"💑 *Расклад на пару*\n\n👤 *Он/она:* {cards[0]}\n👤 *Вы:* {cards[1]}\n🔗 *Связь:* {cards[2]}\n\n{answer}"

    elif action == "love_continue":
        card = random.choice(TAROT_CARDS)
        answer = await ask_claude(f"Ситуация: «{text}»\nКарта совета: {card}\n\nОтветь на вопрос 'Стоит ли продолжать отношения?' Честно и глубоко. 150–200 слов.", lang)
        result = f"🤔 *Стоит ли продолжать?*\n\n*Карта совета:* {card}\n\n{answer}"

    elif action == "love_future":
        cards = random.sample(TAROT_CARDS, 3)
        answer = await ask_claude(f"Отношения: «{text}»\n\nРасклад на будущее:\n• Ближайшее: {cards[0]}\n• Развитие: {cards[1]}\n• Итог: {cards[2]}\n\nИнтерпретация будущего. 200–250 слов.", lang)
        result = f"🔮 *Будущее отношений*\n\n⏰ *Ближайшее:* {cards[0]}\n📈 *Развитие:* {cards[1]}\n🎯 *Итог:* {cards[2]}\n\n{answer}"

    elif action == "free_question":
        result = await ask_claude(text, lang)

    else:
        result = t(lang, 'unknown_cmd')

    await _edit_or_send(chat_id, prompt_msg_id, result, back_button(lang), lang)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def set_commands():
    await bot.set_my_commands([
        BotCommand(command="menu", description="🔮 Главное меню / Main menu"),
        BotCommand(command="myid", description="🆔 Ваш Telegram ID"),
    ])

async def main():
    logger.info("Бот Мистра запускается...")
    await init_db()
    await set_commands()
    asyncio.create_task(daily_broadcast_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
