import asyncio
import base64
import csv
import io
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, LabeledPrice, PreCheckoutQuery, BotCommand, BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
import anthropic

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
YUKASSA_TOKEN = os.getenv("YUKASSA_TOKEN", "")
# YK_SBP format: "shopId:secretKey" — for direct YuKassa API (SBP payments)
_yk_sbp = os.getenv("YK_SBP", "")
YUKASSA_SHOP_ID = ""
YUKASSA_SECRET_KEY = ""
if _yk_sbp and ":" in _yk_sbp:
    _yp = _yk_sbp.split(":", 1)
    YUKASSA_SHOP_ID = _yp[0]
    YUKASSA_SECRET_KEY = _yp[1]
STRIPE_TOKEN = os.getenv("STRIPE_TOKEN", "")
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "")
CRYPTOBOT_API = "https://pay.crypt.bot/api"
WELCOME_PHOTO = os.getenv("WELCOME_PHOTO", "")
SITE_URL = os.getenv("SITE_URL", "").strip().rstrip("/")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "support")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")

SUBSCRIPTION_STARS = 100
SUBSCRIPTION_RUB = 25000
SUBSCRIPTION_USD = 300  # cents ($4.99)
SUBSCRIPTION_USDT = "3.00"
FREE_REQUESTS = 5
DB_PATH = os.getenv("DB_PATH", "tarot_bot.db")
MOSCOW_TZ = timezone(timedelta(hours=3))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
BOT_USERNAME = ""  # set in main()

# ─── TRANSLATIONS ─────────────────────────────────────────────────────────────
TEXTS = {
    'ru': {
        'choose_lang': "🌐 Выберите язык / Choose language:",
        'welcome': "🔮 *Добро пожаловать в пространство Мистры*\n\nПолноценный расклад, как у живого таролога. Карты тасуются случайно — ИИ читает именно ваш расклад с учётом знака зодиака и вопроса.\n\n🎴 Таро · ❤️ Любовь · 💼 Карьера\n🔢 Нумерология · 🪨 Руны · 🖐 Хиромантия\n🌙 Луна · 📅 Гороскоп · 💭 Сны\n\n_Первые 5 раскладов — бесплатно._",
        'terms_accept_btn': "✅ Принимаю",
        'terms_view_btn': "📜 Соглашение",
        'terms_read_btn': "🌐 Читать на сайте",
        'main_menu_title': "🔮 *Главное меню*\n\nВыберите, что вас интересует:",
        'btn_tarot': "🎴 Таро", 'btn_love': "❤️ Любовь",
        'btn_numerology': "🔢 Нумерология", 'btn_horoscope': "📅 Гороскоп",
        'btn_moon': "🌙 Луна", 'btn_lucky': "🔑 Число удачи",
        'btn_ritual': "🌿 Ритуал дня", 'btn_week': "🃏 Карта недели",
        'btn_card_day': "🌟 Карта дня", 'btn_question': "❓ Задать вопрос",
        'btn_runes': "🪨 Руны", 'btn_dream': "💭 Толкование сна",
        'btn_palmistry': "🖐 Хиромантия",
        'btn_subscription': "💎 Подписка", 'btn_promo': "🎟 Промокод",
        'btn_notifications': "🔔 Рассылка", 'btn_referral': "👥 Реферал",
        'btn_profile': "👤 Мой профиль", 'btn_support': "🆘 Поддержка",
        'btn_back': "◀️ Назад", 'btn_main_menu': "🏠 Главное меню",
        'btn_cancel': "✖️ Отмена", 'btn_language': "🌐 Язык / Language",
        'btn_readings_menu': "🔮 Гадания",
        'btn_esoterics_menu': "✨ Нумерология & Эзотерика",
        'btn_account_menu': "👤 Аккаунт",
        'readings_menu_title': "🔮 *Гадания*\n\nВыберите вид гадания:",
        'esoterics_menu_title': "✨ *Нумерология & Эзотерика*\n\nВыберите раздел:",
        'account_menu_title': "👤 *Аккаунт*\n\nВыберите раздел:",
        'btn_tarot_cc': "✡️ Кельтский крест (10 карт)", 'btn_tarot_yn': "☯️ Да / Нет",
        'btn_career': "💼 Карьера", 'btn_card_year': "🗓 Карта года",
        'btn_my_horo': "♈ Мой гороскоп", 'btn_history': "📜 История раскладов",
        'btn_tarot_library': "📚 Библиотека Таро", 'btn_gift_sub': "🎁 Подарить подписку",
        'btn_career_money': "💰 Деньги и финансы", 'btn_career_job': "💼 Карьера / Работа",
        'btn_career_biz': "🚀 Бизнес и проекты",
        'career_menu_title': "💼 *Карьера и деньги*\n\nВыберите тему:",
        'tarot_cc_prompt': "✡️ *Кельтский крест (10 карт)*\n\nОпишите ситуацию или задайте главный вопрос:",
        'tarot_yn_prompt': "☯️ *Да или Нет?*\n\nСформулируйте вопрос чётко:",
        'career_money_prompt': "💰 *Деньги и финансы*\n\nОпишите ситуацию или задайте вопрос:",
        'career_job_prompt': "💼 *Карьера и работа*\n\nОпишите ситуацию или задайте вопрос:",
        'career_biz_prompt': "🚀 *Бизнес и проекты*\n\nОпишите ваш бизнес или проект:",
        'card_year_prompt': "🗓 *Карта года*\n\nВведите дату рождения *ДД.ММ.ГГГГ*:\n\n_Или укажите её в профиле — тогда карта откроется сразу._",
        'tarot_library_prompt': "📚 *Библиотека Таро*\n\nНапишите название карты или тему:\n\n_Например: «Шут», «Башня», «Масть Кубков»_",
        'history_title': "📜 *История раскладов*\n\n",
        'history_empty': "📜 *История раскладов*\n\nУ вас пока нет сохранённых раскладов.\n\nСделайте первый расклад! 🔮",
        'moon_new_msg': "🌑 *Новолуние*\n\nНастало время новых начинаний!\n\nСегодня мощная энергия для загадывания желаний и постановки целей. Отличное время для нового расклада Таро. 🌙",
        'moon_full_msg': "🌕 *Полнолуние*\n\nВремя кульминации и завершения!\n\nСегодня обострена интуиция, эмоции на пике. Отпустите старое и подведите итоги. ✨",
        'inactive_reminder': "🔮 *Мистра скучает по вам...*\n\nВы не заглядывали уже несколько дней. Карты ждут — возможно, сегодня именно тот день, когда знаки готовы открыться вам. 🌟",
        'gift_sub_created': "🎁 *Подарочный промокод создан!*\n\nПередайте другу этот код:\n\n`{code}`\n\nОн даёт *30 дней* подписки на Мистру. ✨",
        'gift_sub_btn': "🎁 Подарить подписку другу — {stars} Stars",
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
        'btn_num_date': "📅 По дате рождения", 'btn_num_name': "✏️ По имени",
        'btn_natal': "🌠 Натальная карта", 'btn_compat': "💑 Совместимость пар",
        'btn_num_fate': "🔮 Число судьбы", 'btn_num_square': "📊 Пифагорейский квадрат",
        'btn_num_address': "🏠 Нумерология адреса", 'btn_num_year': "🗓 Личный год",
        'btn_num_trio': "👨‍👩‍👦 Треугольник отношений", 'btn_num_biz': "💼 Нумерология бизнеса",
        'btn_page_next': "➡️ Следующая страница", 'btn_page_prev': "⬅️ Предыдущая страница",
        'num_fate_prompt': "🔮 *Число судьбы*\n\nВведите полное имя и дату рождения:\n\n_Например: Иван Иванов Иванович, 15.03.1995_",
        'num_square_prompt': "📊 *Пифагорейский квадрат*\n\nВведите дату рождения: *ДД.ММ.ГГГГ*\n\n_Например: 15.03.1995_",
        'num_address_prompt': "🏠 *Нумерология адреса*\n\nВведите адрес (улица и номер дома, квартира):\n\n_Например: Ленина 42, кв 7_",
        'num_year_prompt': "🗓 *Личный год*\n\nВведите дату рождения: *ДД.ММ.ГГГГ*",
        'num_trio_prompt': "👨‍👩‍👦 *Треугольник отношений*\n\nВведите три даты рождения через запятую:\n\n_Например: 15.03.1995, 22.07.1993, 01.01.2000_",
        'num_biz_prompt': "💼 *Нумерология бизнеса*\n\nВведите название компании или проекта и дату основания:\n\n_Например: ООО Мистра, 01.01.2020_",
        'horoscope_title': "📅 *Гороскоп*\n\nВыберите знак зодиака:",
        'btn_horo_day': "☀️ На сегодня", 'btn_horo_week': "📅 На неделю", 'btn_horo_month': "🌙 На месяц",
        'rune_menu_title': "🪨 *Расклад на рунах*\n\nВыберите вид:",
        'btn_rune1': "🪨 1 руна — ответ на вопрос", 'btn_rune3': "🪨 3 руны — расклад ситуации",
        'tarot1_prompt': "🃏 *Расклад на 1 карту*\n\nСформулируйте вопрос:\n\n_Чем точнее — тем глубже ответ._",
        'tarot3_prompt': "🎴 *Прошлое / Настоящее / Будущее*\n\nОпишите ситуацию или задайте вопрос:",
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
        'rune1_prompt': "🪨 *1 руна*\n\nСформулируйте вопрос:",
        'rune3_prompt': "🪨 *3 руны*\n\nОпишите ситуацию:",
        'palmistry_prompt': "🖐 *Хиромантия*\n\nОтправьте фото вашей ладони (рабочей руки).\n\n_Убедитесь, что ладонь хорошо освещена — так линии будут видны чётче._",
        'palmistry_no_photo': "📷 Пожалуйста, отправьте *фото ладони* (не текст).",
        'reading_palm': "🖐 Читаю линии вашей ладони...",
        'dream_prompt': "💭 *Толкование сна*\n\nОпишите ваш сон подробно:",
        'promo_prompt': "🎟 *Промокод*\n\nВведите промокод:",
        'profile_birthdate_prompt': "📅 Введите дату рождения: *ДД.ММ.ГГГГ*\n\n_Например: 15.03.1995_",
        'profile_name_prompt': "✏️ Введите ваше полное имя:",
        'processing': "🔮 Мистра читает знаки...",
        'pulling_card': "🌟 Тяну карту дня...",
        'reading_moon': "🌙 Читаю лунный календарь...",
        'finding_ritual': "🌿 Подбираю ритуал дня...",
        'calc_lucky': "🔑 Вычисляю число удачи...",
        'spreading_week': "🃏 Раскладываю карты на неделю...",
        'reading_horo': "📅 Читаю гороскоп для {sign}...",
        'notif_status': "🔔 *Ежедневная рассылка*\n\nСтатус: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Включена", 'notif_disabled': "❌ Выключена",
        'notif_on_msg': "🔔 Рассылка включена! Каждое утро в 8:00 жди карту дня.",
        'notif_off_msg': "🔕 Рассылка отключена.",
        'btn_notif_on': "🔔 Включить", 'btn_notif_off': "🔕 Отключить",
        'notif_desc': "Каждое утро в *8:00* Мистра присылает карту дня.\nПодписчики получают развёрнутую интерпретацию.",
        'paywall': "🔒 *Лимит бесплатных запросов исчерпан*\n\nВы использовали все {free} бесплатных запросов.\n\n*Подписка на 30 дней — {stars} ⭐*\n• Безлимитные расклады Таро и Нумерология\n• Гороскоп, Луна, Ритуалы, Карта недели\n• Любовные расклады и многое другое",
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars", 'btn_buy_rub': "💳 ЮКасса (карта/ЮMoney) — 250 ₽",
        'btn_buy_sbp': "📱 СБП — 250 ₽",
        'btn_buy_card': "💳 Visa / Mastercard — $4.99",
        'btn_buy_crypto': "💎 Crypto (USDT/TON) — $4.99",
        'sbp_payment_msg': "📱 *Оплата через СБП*\n\nНажмите кнопку ниже — откроется страница ЮКассы для оплаты через приложение вашего банка.\n\n✅ Подписка активируется автоматически в течение ~30 секунд после оплаты.",
        'sbp_btn_pay': "📱 Открыть форму оплаты СБП",
        'sbp_error': "❌ Ошибка создания платежа. Попробуйте позже или выберите другой способ оплаты.",
        'sub_active': "💎 *Ваша подписка*\n\n✅ Активна до: *{date}*\n📊 Запросов: *{count}*\n🔥 Серия: *{streak} дней*\n\nНаслаждайтесь безлимитным доступом! 🔮",
        'sub_inactive': "💎 *Подписка на Мистру*\n\n🆓 Бесплатных осталось: *{remaining}/{free}*\n🔥 Серия: *{streak} дней*\n\n*Подписка включает:*\n• Таро, Нумерология, Натальная карта\n• Гороскоп, Луна, Ритуалы, Руны\n• Любовные расклады\n• Ежедневная рассылка в 8:00\n\n💰 *{stars} Telegram Stars* / 30 дней",
        'sub_activated': "✅ *Подписка активирована!*\n\n🔮 Добро пожаловать в безграничный мир Мистры!\n📅 Действует до: *{date}*\n\nДелайте неограниченные расклады! 🌟",
        'support_text': "🆘 *Техническая поддержка*\n\nЕсли у вас возникли проблемы с ботом, напишите нам:\n\n👤 @{username}\n\nМы ответим как можно скорее! ⚡",
        'referral_text': "👥 *Реферальная программа*\n\nПриглашайте друзей и получайте *+1 бесплатный запрос* за каждого!\n\n🔗 *Ваша ссылка:*\n`{link}`\n\n👥 Приглашено друзей: *{count}*\n🎁 Бонусных запросов: *+{bonus}*\n\n_Поделитесь ссылкой — и карты откроют вам больше_",
        'btn_share_referral': "📤 Поделиться ссылкой",
        'referral_bonus_msg': "🎁 *Бонус!* {name} присоединился по вашей ссылке. +1 бесплатный запрос!",
        'share_text': "Попробуй этого бота — гадание на Таро и нумерология!",
        'promo_success': "✅ *Промокод активирован!*\n\nПодписка продлена на *{days} дней*. 🎉",
        'promo_invalid': "❌ Промокод не найден.", 'promo_used': "❌ Вы уже использовали этот промокод.",
        'promo_exhausted': "❌ Промокод больше не действует.",
        'profile_title': "👤 *Ваш профиль*\n\n✏️ Имя: *{name}*\n📅 Дата рождения: *{birth}*\n♈ Знак зодиака: *{zodiac}*\n⚧ Пол: *{gender}*\n🌆 Город: *{city}*\n🕐 Часовой пояс: *{timezone}*\n🔥 Серия: *{streak} дней*\n🎁 Бонусных запросов: *+{bonus}*",
        'profile_saved': "✅ Сохранено!", 'profile_empty': "не указано",
        'btn_set_birthdate': "📅 Дата рождения", 'btn_set_name': "✏️ Имя",
        'btn_set_zodiac': "♈ Знак зодиака", 'btn_set_gender': "⚧ Пол",
        'btn_set_city': "🌆 Город", 'btn_set_timezone': "🕐 Часовой пояс",
        'btn_clear_profile': "🗑 Очистить профиль",
        'set_gender_prompt': "⚧ *Укажите ваш пол:*",
        'btn_gender_m': "👨 Мужской", 'btn_gender_f': "👩 Женский", 'btn_gender_o': "🌈 Другой",
        'set_city_prompt': "🌆 *Введите ваш город:*\n\nНапример: _Киев_, _Москва_, _Варшава_",
        'set_timezone_prompt': "🕐 *Введите ваш часовой пояс:*\n\nНапример: _UTC+2_, _UTC+3_, _UTC+0_",
        'history_item_btn': "🔸 {title} | {date}",
        'streak_bonus': "🔥 *{days}-дневная серия!*\n\nЗа верность Мистре — *+1 бесплатный запрос* в подарок! 🎁",
        'banned_msg': "⛔ Ваш аккаунт заблокирован. Обратитесь в техническую поддержку.",
        'days': ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"],
        'broadcast_morning': "🌅 *Доброе утро! Карта дня — {date}*",
        'broadcast_sub_hint': "_💎 Подпишитесь для развёрнутых интерпретаций_",
        'error': "⚠️ Произошла ошибка при обращении к оракулу. Попробуйте позже.",
        'unknown_cmd': "❓ Не понял команду. Воспользуйтесь меню.",
        'horo_period': {"day": "на сегодня", "week": "на неделю", "month": "на месяц"},
        'invoice_title': "Подписка Мистра — 30 дней",
        'invoice_desc': "Безлимитный доступ ко всем функциям бота на 30 дней",
        'payment_unavail': "Оплата картой временно недоступна",
        'btn_refund': "💳 Возврат средств",
        'refund_request_msg': "💳 *Запрос возврата средств*\n\nВы оплатили подписку через СБП/ЮКасса на сумму *250 ₽*.\n\n⚠️ После возврата:\n• Подписка будет немедленно отменена\n• Средства вернутся в течение 5-10 рабочих дней\n\nПодтвердить возврат?",
        'refund_no_payment': "❌ Возврат недоступен\n\nДля возврата обратитесь в поддержку: @{support}",
        'refund_success': "✅ Возврат оформлен!\n\nСредства вернутся в течение 5-10 рабочих дней.\nПодписка отменена.",
        'refund_error': "❌ Не удалось оформить возврат автоматически.\n\nОбратитесь в поддержку: @{support}",
        'btn_refund_confirm': "✅ Да, вернуть деньги",
        'btn_refund_cancel': "❌ Отмена",
        'btn_my_stats': "📊 Моя статистика",
        'btn_my_payments': "📜 История платежей",
        'btn_delete_account': "🗑 Удалить аккаунт",
        'btn_notify_time': "⏰ Время рассылки",
        'my_stats_title': "📊 *Ваша статистика*\n\n📆 В боте с: *{since}*\n🔢 Всего раскладов: *{total}*\n🏆 Любимый тип: *{fav}*\n🔥 Серия: *{streak} дней*\n👥 Приглашено друзей: *{refs}*\n🎁 Бонусных запросов: *+{bonus}*",
        'my_payments_title': "📜 *История платежей*\n\n",
        'my_payments_empty': "📜 *История платежей*\n\nПлатежей не найдено.",
        'delete_account_confirm_msg': "🗑 *Удаление аккаунта*\n\n⚠️ Это действие *необратимо*!\n\nБудут удалены:\n• Профиль и личные данные\n• История раскладов\n• Подписка и бонусы\n• Реферальная статистика\n\nВы уверены?",
        'delete_account_done': "✅ *Данные удалены*\n\nВаш аккаунт и все данные успешно удалены из системы.",
        'btn_delete_confirm': "⚠️ Да, удалить всё",
        'notify_time_title': "⏰ *Время рассылки*\n\nВыберите час отправки карты дня (московское время):\n\nТекущий: *{hour}:00 МСК*",
        'notify_time_set': "✅ Время рассылки: *{hour}:00 МСК*",
    },
    'en': {
        'choose_lang': "🌐 Выберите язык / Choose language:",
        'welcome': "🔮 *Welcome to Mystra*\n\nA full tarot reading, just like a real tarot reader. Cards are shuffled randomly — AI reads your unique spread based on your zodiac sign and question.\n\n🎴 Tarot · ❤️ Love · 💼 Career\n🔢 Numerology · 🪨 Runes · 🖐 Palmistry\n🌙 Moon · 📅 Horoscope · 💭 Dreams\n\n_First 5 readings are free._",
        'terms_accept_btn': "✅ I Agree",
        'terms_view_btn': "📜 Terms",
        'terms_read_btn': "🌐 Read on website",
        'main_menu_title': "🔮 *Main Menu*\n\nChoose what interests you:",
        'btn_tarot': "🎴 Tarot", 'btn_love': "❤️ Love",
        'btn_numerology': "🔢 Numerology", 'btn_horoscope': "📅 Horoscope",
        'btn_moon': "🌙 Moon", 'btn_lucky': "🔑 Lucky Number",
        'btn_ritual': "🌿 Daily Ritual", 'btn_week': "🃏 Week Cards",
        'btn_card_day': "🌟 Card of the Day", 'btn_question': "❓ Ask a Question",
        'btn_runes': "🪨 Runes", 'btn_dream': "💭 Dream Interp.",
        'btn_palmistry': "🖐 Palmistry",
        'btn_subscription': "💎 Subscription", 'btn_promo': "🎟 Promo Code",
        'btn_notifications': "🔔 Broadcast", 'btn_referral': "👥 Referral",
        'btn_profile': "👤 My Profile", 'btn_support': "🆘 Support",
        'btn_back': "◀️ Back", 'btn_main_menu': "🏠 Main Menu",
        'btn_cancel': "✖️ Cancel", 'btn_language': "🌐 Язык / Language",
        'btn_readings_menu': "🔮 Readings",
        'btn_esoterics_menu': "✨ Numerology & Esoterics",
        'btn_account_menu': "👤 Account",
        'readings_menu_title': "🔮 *Readings*\n\nChoose your reading type:",
        'esoterics_menu_title': "✨ *Numerology & Esoterics*\n\nChoose a section:",
        'account_menu_title': "👤 *Account*\n\nChoose a section:",
        'btn_tarot_cc': "✡️ Celtic Cross (10 cards)", 'btn_tarot_yn': "☯️ Yes / No",
        'btn_career': "💼 Career", 'btn_card_year': "🗓 Card of the Year",
        'btn_my_horo': "♈ My Horoscope", 'btn_history': "📜 Reading History",
        'btn_tarot_library': "📚 Tarot Library", 'btn_gift_sub': "🎁 Gift Subscription",
        'btn_career_money': "💰 Money & Finance", 'btn_career_job': "💼 Career / Work",
        'btn_career_biz': "🚀 Business & Projects",
        'career_menu_title': "💼 *Career & Money*\n\nChoose a topic:",
        'tarot_cc_prompt': "✡️ *Celtic Cross (10 cards)*\n\nDescribe your situation or ask your main question:",
        'tarot_yn_prompt': "☯️ *Yes or No?*\n\nFormulate your question clearly:",
        'career_money_prompt': "💰 *Money & Finance*\n\nDescribe your situation or ask a question:",
        'career_job_prompt': "💼 *Career & Work*\n\nDescribe your situation or ask a question:",
        'career_biz_prompt': "🚀 *Business & Projects*\n\nDescribe your business or project:",
        'card_year_prompt': "🗓 *Card of the Year*\n\nEnter your birth date *DD.MM.YYYY*:\n\n_Or set it in your profile — the card will open instantly._",
        'tarot_library_prompt': "📚 *Tarot Library*\n\nEnter a card name or topic:\n\n_Example: «The Fool», «The Tower», «Suit of Cups»_",
        'history_title': "📜 *Reading History*\n\n",
        'history_empty': "📜 *Reading History*\n\nYou have no saved readings yet.\n\nMake your first spread! 🔮",
        'moon_new_msg': "🌑 *New Moon*\n\nTime for new beginnings!\n\nPowerful energy today for setting intentions and new goals. A great time for a Tarot reading. 🌙",
        'moon_full_msg': "🌕 *Full Moon*\n\nTime for culmination and completion!\n\nIntuition is heightened, emotions at their peak. Release the old and reflect. ✨",
        'inactive_reminder': "🔮 *Mystra misses you...*\n\nYou haven't visited in a few days. The cards are waiting — perhaps today is the day the signs are ready to reveal themselves to you. 🌟",
        'gift_sub_created': "🎁 *Gift promo code created!*\n\nShare this code with your friend:\n\n`{code}`\n\nIt gives *30 days* of Mystra subscription. ✨",
        'gift_sub_btn': "🎁 Gift subscription to a friend — {stars} Stars",
        'tarot_menu_title': "🎴 *Tarot Spreads*\n\nChoose your spread:",
        'btn_tarot1': "🃏 1 card — quick answer",
        'btn_tarot3': "🎴 3 cards — Past/Present/Future",
        'btn_tarot5': "🔮 5 cards — Situation spread",
        'love_menu_title': "❤️ *Love & Relationships*\n\nChoose your spread:",
        'btn_love_thinking': "💭 Is he/she thinking of me?",
        'btn_love_couple': "💑 Couple spread", 'btn_love_continue': "🤔 Should I continue?",
        'btn_love_future': "🔮 Future of relationship",
        'num_menu_title': "🔢 *Numerology*\n\nChoose method:",
        'btn_num_date': "📅 By birth date", 'btn_num_name': "✏️ By name",
        'btn_natal': "🌠 Natal chart", 'btn_compat': "💑 Couple compatibility",
        'btn_num_fate': "🔮 Destiny Number", 'btn_num_square': "📊 Pythagorean Square",
        'btn_num_address': "🏠 Address Numerology", 'btn_num_year': "🗓 Personal Year",
        'btn_num_trio': "👨‍👩‍👦 Relationship Triangle", 'btn_num_biz': "💼 Business Numerology",
        'btn_page_next': "➡️ Next page", 'btn_page_prev': "⬅️ Previous page",
        'num_fate_prompt': "🔮 *Destiny Number*\n\nEnter your full name and birth date:\n\n_Example: John Smith, 15.03.1995_",
        'num_square_prompt': "📊 *Pythagorean Square*\n\nEnter birth date: *DD.MM.YYYY*\n\n_Example: 15.03.1995_",
        'num_address_prompt': "🏠 *Address Numerology*\n\nEnter your address (street, house/apartment number):\n\n_Example: Main St 42, apt 7_",
        'num_year_prompt': "🗓 *Personal Year*\n\nEnter your birth date: *DD.MM.YYYY*",
        'num_trio_prompt': "👨‍👩‍👦 *Relationship Triangle*\n\nEnter three birth dates separated by commas:\n\n_Example: 15.03.1995, 22.07.1993, 01.01.2000_",
        'num_biz_prompt': "💼 *Business Numerology*\n\nEnter the company/project name and founding date:\n\n_Example: Mystra LLC, 01.01.2020_",
        'horoscope_title': "📅 *Horoscope*\n\nChoose your zodiac sign:",
        'btn_horo_day': "☀️ Today", 'btn_horo_week': "📅 This week", 'btn_horo_month': "🌙 This month",
        'rune_menu_title': "🪨 *Rune Reading*\n\nChoose type:",
        'btn_rune1': "🪨 1 rune — answer to a question", 'btn_rune3': "🪨 3 runes — situation spread",
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
        'rune1_prompt': "🪨 *1 Rune*\n\nFormulate your question:",
        'rune3_prompt': "🪨 *3 Runes*\n\nDescribe your situation:",
        'palmistry_prompt': "🖐 *Palmistry*\n\nSend a photo of your palm (dominant hand).\n\n_Make sure the palm is well-lit — the lines will be clearer._",
        'palmistry_no_photo': "📷 Please send a *photo of your palm* (not text).",
        'reading_palm': "🖐 Reading the lines of your palm...",
        'dream_prompt': "💭 *Dream Interpretation*\n\nDescribe your dream in detail:",
        'promo_prompt': "🎟 *Promo Code*\n\nEnter your promo code:",
        'profile_birthdate_prompt': "📅 Enter your birth date: *DD.MM.YYYY*\n\n_Example: 15.03.1995_",
        'profile_name_prompt': "✏️ Enter your full name:",
        'processing': "🔮 Mystra is reading the signs...",
        'pulling_card': "🌟 Drawing your card of the day...",
        'reading_moon': "🌙 Reading the moon calendar...",
        'finding_ritual': "🌿 Finding your daily ritual...",
        'calc_lucky': "🔑 Calculating your lucky number...",
        'spreading_week': "🃏 Spreading the week cards...",
        'reading_horo': "📅 Reading horoscope for {sign}...",
        'notif_status': "🔔 *Daily Broadcast*\n\nStatus: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Enabled", 'notif_disabled': "❌ Disabled",
        'notif_on_msg': "🔔 Broadcast enabled! Every morning at 8:00 you'll get the card of the day.",
        'notif_off_msg': "🔕 Broadcast disabled.",
        'btn_notif_on': "🔔 Enable", 'btn_notif_off': "🔕 Disable",
        'notif_desc': "Every morning at *8:00* Mystra sends you the card of the day.\nSubscribers get a detailed interpretation.",
        'paywall': "🔒 *Free request limit reached*\n\nYou've used all {free} free requests.\n\n*30-day Subscription — {stars} ⭐*\n• Unlimited Tarot spreads & Numerology\n• Horoscope, Moon, Rituals, Week Cards\n• Love spreads and much more",
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars", 'btn_buy_rub': "💳 YuKassa (card/YuMoney) — 250 ₽",
        'btn_buy_sbp': "📱 SBP (Fast Pay) — 250 ₽",
        'btn_buy_card': "💳 Visa / Mastercard — $4.99",
        'btn_buy_crypto': "💎 Crypto (USDT/TON) — $4.99",
        'sbp_payment_msg': "📱 *Pay via SBP*\n\nClick the button below — you will be redirected to YuKassa's payment page to pay through your banking app.\n\n✅ Subscription activates automatically within ~30 seconds after payment.",
        'sbp_btn_pay': "📱 Open SBP Payment Form",
        'sbp_error': "❌ Payment creation failed. Please try later or choose a different payment method.",
        'sub_active': "💎 *Your Subscription*\n\n✅ Active until: *{date}*\n📊 Requests: *{count}*\n🔥 Streak: *{streak} days*\n\nEnjoy unlimited access! 🔮",
        'sub_inactive': "💎 *Mystra Subscription*\n\n🆓 Free requests left: *{remaining}/{free}*\n🔥 Streak: *{streak} days*\n\n*Subscription includes:*\n• Tarot, Numerology, Natal Chart, Runes\n• Horoscope, Moon, Rituals, Week Cards\n• Love spreads\n• Daily broadcast at 8:00\n\n💰 *{stars} Telegram Stars* / 30 days",
        'sub_activated': "✅ *Subscription activated!*\n\n🔮 Welcome to Mystra's limitless realm!\n📅 Active until: *{date}*\n\nEnjoy unlimited spreads! 🌟",
        'support_text': "🆘 *Technical Support*\n\nIf you have any issues with the bot, contact us:\n\n👤 @{username}\n\nWe'll respond as soon as possible! ⚡",
        'referral_text': "👥 *Referral Program*\n\nInvite friends and get *+1 free request* for each one!\n\n🔗 *Your link:*\n`{link}`\n\n👥 Friends invited: *{count}*\n🎁 Bonus requests: *+{bonus}*\n\n_Share your link — and the cards will reveal more_",
        'btn_share_referral': "📤 Share link",
        'referral_bonus_msg': "🎁 *Bonus!* {name} joined via your link. +1 free request!",
        'share_text': "Try this bot — Tarot readings and numerology!",
        'promo_success': "✅ *Promo code activated!*\n\nSubscription extended by *{days} days*. 🎉",
        'promo_invalid': "❌ Promo code not found.", 'promo_used': "❌ You have already used this promo code.",
        'promo_exhausted': "❌ This promo code is no longer valid.",
        'profile_title': "👤 *Your Profile*\n\n✏️ Name: *{name}*\n📅 Birth date: *{birth}*\n♈ Zodiac: *{zodiac}*\n⚧ Gender: *{gender}*\n🌆 City: *{city}*\n🕐 Timezone: *{timezone}*\n🔥 Streak: *{streak} days*\n🎁 Bonus requests: *+{bonus}*",
        'profile_saved': "✅ Saved!", 'profile_empty': "not set",
        'btn_set_birthdate': "📅 Birth date", 'btn_set_name': "✏️ Name",
        'btn_set_zodiac': "♈ Zodiac sign", 'btn_set_gender': "⚧ Gender",
        'btn_set_city': "🌆 City", 'btn_set_timezone': "🕐 Timezone",
        'btn_clear_profile': "🗑 Clear profile",
        'set_gender_prompt': "⚧ *Choose your gender:*",
        'btn_gender_m': "👨 Male", 'btn_gender_f': "👩 Female", 'btn_gender_o': "🌈 Other",
        'set_city_prompt': "🌆 *Enter your city:*\n\nExample: _Kyiv_, _Warsaw_, _London_",
        'set_timezone_prompt': "🕐 *Enter your timezone:*\n\nExample: _UTC+2_, _UTC+3_, _UTC+0_",
        'history_item_btn': "🔸 {title} | {date}",
        'streak_bonus': "🔥 *{days}-day streak!*\n\nFor your loyalty to Mystra — *+1 free request* as a gift! 🎁",
        'banned_msg': "⛔ Your account has been banned. Contact technical support.",
        'days': ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        'broadcast_morning': "🌅 *Good morning! Card of the day — {date}*",
        'broadcast_sub_hint': "_💎 Subscribe for detailed interpretations_",
        'error': "⚠️ An error occurred while consulting the oracle. Please try again later.",
        'unknown_cmd': "❓ Command not understood. Please use the menu.",
        'horo_period': {"day": "for today", "week": "for this week", "month": "for this month"},
        'invoice_title': "Mystra Subscription — 30 days",
        'invoice_desc': "Unlimited access to all bot features for 30 days",
        'payment_unavail': "Card payment is temporarily unavailable",
        'btn_refund': "💳 Refund",
        'refund_request_msg': "💳 *Refund Request*\n\nYou paid for a subscription via SBP/YuKassa for *250 ₽*.\n\n⚠️ After refund:\n• Subscription will be immediately cancelled\n• Funds will be returned within 5-10 business days\n\nConfirm refund?",
        'refund_no_payment': "❌ Refund unavailable\n\nTo get a refund, contact support: @{support}",
        'refund_success': "✅ Refund processed!\n\nFunds will be returned within 5-10 business days.\nSubscription cancelled.",
        'refund_error': "❌ Could not process refund automatically.\n\nContact support: @{support}",
        'btn_refund_confirm': "✅ Yes, refund",
        'btn_refund_cancel': "❌ Cancel",
        'btn_my_stats': "📊 My Statistics",
        'btn_my_payments': "📜 Payment History",
        'btn_delete_account': "🗑 Delete Account",
        'btn_notify_time': "⏰ Broadcast Time",
        'my_stats_title': "📊 *Your Statistics*\n\n📆 In bot since: *{since}*\n🔢 Total readings: *{total}*\n🏆 Favorite type: *{fav}*\n🔥 Streak: *{streak} days*\n👥 Friends invited: *{refs}*\n🎁 Bonus requests: *+{bonus}*",
        'my_payments_title': "📜 *Payment History*\n\n",
        'my_payments_empty': "📜 *Payment History*\n\nNo payments found.",
        'delete_account_confirm_msg': "🗑 *Delete Account*\n\n⚠️ This action is *irreversible*!\n\nThe following will be deleted:\n• Profile and personal data\n• Reading history\n• Subscription and bonuses\n• Referral statistics\n\nAre you sure?",
        'delete_account_done': "✅ *Data Deleted*\n\nYour account and all data have been successfully deleted.",
        'btn_delete_confirm': "⚠️ Yes, delete everything",
        'notify_time_title': "⏰ *Broadcast Time*\n\nChoose the hour for your daily card (Moscow time):\n\nCurrent: *{hour}:00 MSK*",
        'notify_time_set': "✅ Broadcast time: *{hour}:00 MSK*",
    },
    'uk': {
        'choose_lang': "🌐 Виберіть мову / Choose language:",
        'welcome': "🔮 *Ласкаво просимо до простору Містри*\n\nПовноцінний розклад, як у живого таролога. Карти тасуються випадково — ШІ читає саме ваш розклад з урахуванням знаку зодіаку і питання.\n\n🎴 Таро · ❤️ Кохання · 💼 Кар'єра\n🔢 Нумерологія · 🪨 Руни · 🖐 Хіромантія\n🌙 Місяць · 📅 Гороскоп · 💭 Сни\n\n_Перші 5 розкладів — безкоштовно._",
        'terms_accept_btn': "✅ Приймаю",
        'terms_view_btn': "📜 Угода",
        'terms_read_btn': "🌐 Читати на сайті",
        'main_menu_title': "🔮 *Головне меню*\n\nОберіть, що вас цікавить:",
        'btn_tarot': "🎴 Таро", 'btn_love': "❤️ Кохання",
        'btn_numerology': "🔢 Нумерологія", 'btn_horoscope': "📅 Гороскоп",
        'btn_moon': "🌙 Місяць", 'btn_lucky': "🔑 Число удачі",
        'btn_ritual': "🌿 Ритуал дня", 'btn_week': "🃏 Карта тижня",
        'btn_card_day': "🌟 Карта дня", 'btn_question': "❓ Поставити питання",
        'btn_runes': "🪨 Руни", 'btn_dream': "💭 Тлумачення сну",
        'btn_palmistry': "🖐 Хіромантія",
        'btn_subscription': "💎 Підписка", 'btn_promo': "🎟 Промокод",
        'btn_notifications': "🔔 Розсилка", 'btn_referral': "👥 Реферал",
        'btn_profile': "👤 Мій профіль", 'btn_support': "🆘 Підтримка",
        'btn_back': "◀️ Назад", 'btn_main_menu': "🏠 Головне меню",
        'btn_cancel': "✖️ Скасувати", 'btn_language': "🌐 Мова / Language",
        'btn_readings_menu': "🔮 Ворожіння",
        'btn_esoterics_menu': "✨ Нумерологія & Езотерика",
        'btn_account_menu': "👤 Акаунт",
        'readings_menu_title': "🔮 *Ворожіння*\n\nОберіть вид ворожіння:",
        'esoterics_menu_title': "✨ *Нумерологія & Езотерика*\n\nОберіть розділ:",
        'account_menu_title': "👤 *Акаунт*\n\nОберіть розділ:",
        'btn_tarot_cc': "✡️ Кельтський хрест (10 карт)", 'btn_tarot_yn': "☯️ Так / Ні",
        'btn_career': "💼 Кар'єра", 'btn_card_year': "🗓 Карта року",
        'btn_my_horo': "♈ Мій гороскоп", 'btn_history': "📜 Історія розкладів",
        'btn_tarot_library': "📚 Бібліотека Таро", 'btn_gift_sub': "🎁 Подарувати підписку",
        'btn_career_money': "💰 Гроші та фінанси", 'btn_career_job': "💼 Кар'єра / Робота",
        'btn_career_biz': "🚀 Бізнес і проєкти",
        'career_menu_title': "💼 *Кар'єра та гроші*\n\nОберіть тему:",
        'tarot_cc_prompt': "✡️ *Кельтський хрест (10 карт)*\n\nОпишіть ситуацію або поставте головне питання:",
        'tarot_yn_prompt': "☯️ *Так чи Ні?*\n\nСформулюйте питання чітко:",
        'career_money_prompt': "💰 *Гроші та фінанси*\n\nОпишіть ситуацію або поставте питання:",
        'career_job_prompt': "💼 *Кар'єра та робота*\n\nОпишіть ситуацію або поставте питання:",
        'career_biz_prompt': "🚀 *Бізнес і проєкти*\n\nОпишіть ваш бізнес або проєкт:",
        'card_year_prompt': "🗓 *Карта року*\n\nВведіть дату народження *ДД.ММ.РРРР*:\n\n_Або вкажіть її в профілі — карта відкриється одразу._",
        'tarot_library_prompt': "📚 *Бібліотека Таро*\n\nНапишіть назву карти або тему:\n\n_Наприклад: «Блазень», «Вежа», «Масть Кубків»_",
        'history_title': "📜 *Історія розкладів*\n\n",
        'history_empty': "📜 *Історія розкладів*\n\nУ вас ще немає збережених розкладів.\n\nЗробіть перший розклад! 🔮",
        'moon_new_msg': "🌑 *Новолуння*\n\nНастав час нових починань!\n\nСьогодні потужна енергія для загадування бажань і постановки цілей. 🌙",
        'moon_full_msg': "🌕 *Повнолуння*\n\nЧас кульмінації та завершення!\n\nСьогодні загострена інтуїція, емоції на піку. Відпустіть старе і підведіть підсумки. ✨",
        'inactive_reminder': "🔮 *Містра сумує за вами...*\n\nВи не заходили вже кілька днів. Карти чекають — можливо, сьогодні саме той день. 🌟",
        'gift_sub_created': "🎁 *Подарунковий промокод створено!*\n\nПередайте другу цей код:\n\n`{code}`\n\nВін дає *30 днів* підписки на Містру. ✨",
        'gift_sub_btn': "🎁 Подарувати підписку другу — {stars} Stars",
        'tarot_menu_title': "🎴 *Розклади Таро*\n\nОберіть вид розкладу:",
        'btn_tarot1': "🃏 1 карта — швидка відповідь",
        'btn_tarot3': "🎴 3 карти — Минуле/Теперішнє/Майбутнє",
        'btn_tarot5': "🔮 5 карт — Розклад на ситуацію",
        'love_menu_title': "❤️ *Кохання та стосунки*\n\nОберіть розклад:",
        'btn_love_thinking': "💭 Думає він/вона про мене?",
        'btn_love_couple': "💑 Розклад на пару",
        'btn_love_continue': "🤔 Чи варто продовжувати?",
        'btn_love_future': "🔮 Майбутнє стосунків",
        'num_menu_title': "🔢 *Нумерологія*\n\nОберіть метод:",
        'btn_num_date': "📅 За датою народження", 'btn_num_name': "✏️ За іменем",
        'btn_natal': "🌠 Натальна карта", 'btn_compat': "💑 Сумісність пар",
        'btn_num_fate': "🔮 Число долі", 'btn_num_square': "📊 Піфагорійський квадрат",
        'btn_num_address': "🏠 Нумерологія адреси", 'btn_num_year': "🗓 Особистий рік",
        'btn_num_trio': "👨‍👩‍👦 Трикутник стосунків", 'btn_num_biz': "💼 Нумерологія бізнесу",
        'btn_page_next': "➡️ Наступна сторінка", 'btn_page_prev': "⬅️ Попередня сторінка",
        'num_fate_prompt': "🔮 *Число долі*\n\nВведіть повне ім'я та дату народження:\n\n_Наприклад: Іван Іваненко, 15.03.1995_",
        'num_square_prompt': "📊 *Піфагорійський квадрат*\n\nВведіть дату народження: *ДД.ММ.РРРР*",
        'num_address_prompt': "🏠 *Нумерологія адреси*\n\nВведіть адресу (вулиця, номер будинку, квартира):",
        'num_year_prompt': "🗓 *Особистий рік*\n\nВведіть дату народження: *ДД.ММ.РРРР*",
        'num_trio_prompt': "👨‍👩‍👦 *Трикутник стосунків*\n\nВведіть три дати народження через кому:",
        'num_biz_prompt': "💼 *Нумерологія бізнесу*\n\nВведіть назву компанії та дату заснування:",
        'horoscope_title': "📅 *Гороскоп*\n\nОберіть знак зодіаку:",
        'btn_horo_day': "☀️ На сьогодні", 'btn_horo_week': "📅 На тиждень", 'btn_horo_month': "🌙 На місяць",
        'rune_menu_title': "🪨 *Розклад на рунах*\n\nОберіть вид:",
        'btn_rune1': "🪨 1 руна — відповідь на питання", 'btn_rune3': "🪨 3 руни — розклад ситуації",
        'tarot1_prompt': "🃏 *Розклад на 1 карту*\n\nСформулюйте питання:\n\n_Чим точніше — тим глибша відповідь._",
        'tarot3_prompt': "🎴 *Минуле / Теперішнє / Майбутнє*\n\nОпишіть ситуацію або поставте питання:",
        'tarot5_prompt': "🔮 *Розклад на ситуацію (5 карт)*\n\nОпишіть детально ситуацію або питання:",
        'num_date_prompt': "📅 *Нумерологія за датою народження*\n\nВведіть дату: *ДД.ММ.РРРР*",
        'num_name_prompt': "✏️ *Нумерологія за іменем*\n\nВведіть повне ім'я:",
        'natal_prompt': "🌠 *Натальна карта*\n\nВведіть: *ДД.ММ.РРРР ГГ:ХХ Місто*\n\nЧас невідомий — вкажіть 00:00",
        'compat_prompt': "💑 *Сумісність пар*\n\nВведіть дві дати через кому:",
        'free_question_prompt': "❓ *Питання Містрі*\n\nПоставте будь-яке питання з Таро, Нумерології або езотерики:",
        'love_thinking_prompt': "💭 *Думає він/вона про мене?*\n\nОпишіть людину і ситуацію:",
        'love_couple_prompt': "💑 *Розклад на пару*\n\nОпишіть вашу ситуацію у стосунках:",
        'love_continue_prompt': "🤔 *Чи варто продовжувати?*\n\nОпишіть стосунки і що вас турбує:",
        'love_future_prompt': "🔮 *Майбутнє стосунків*\n\nОпишіть ваші стосунки:",
        'rune1_prompt': "🪨 *1 руна*\n\nСформулюйте питання:",
        'rune3_prompt': "🪨 *3 руни*\n\nОпишіть ситуацію:",
        'palmistry_prompt': "🖐 *Хіромантія*\n\nНадішліть фото вашої долоні (робочої руки).\n\n_Переконайтеся, що долоня добре освітлена._",
        'palmistry_no_photo': "📷 Будь ласка, надішліть *фото долоні* (не текст).",
        'reading_palm': "🖐 Читаю лінії вашої долоні...",
        'dream_prompt': "💭 *Тлумачення сну*\n\nОпишіть ваш сон детально:",
        'promo_prompt': "🎟 *Промокод*\n\nВведіть промокод:",
        'profile_birthdate_prompt': "📅 Введіть дату народження: *ДД.ММ.РРРР*",
        'profile_name_prompt': "✏️ Введіть ваше повне ім'я:",
        'processing': "🔮 Містра читає знаки...",
        'pulling_card': "🌟 Тягну карту дня...",
        'reading_moon': "🌙 Читаю місячний календар...",
        'finding_ritual': "🌿 Підбираю ритуал дня...",
        'calc_lucky': "🔑 Обчислюю число удачі...",
        'spreading_week': "🃏 Розкладаю карти на тиждень...",
        'reading_horo': "📅 Читаю гороскоп для {sign}...",
        'notif_status': "🔔 *Щоденна розсилка*\n\nСтатус: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Увімкнена", 'notif_disabled': "❌ Вимкнена",
        'notif_on_msg': "🔔 Розсилку увімкнено! Щоранку о 8:00 чекайте карту дня.",
        'notif_off_msg': "🔕 Розсилку вимкнено.",
        'btn_notif_on': "🔔 Увімкнути", 'btn_notif_off': "🔕 Вимкнути",
        'notif_desc': "Щоранку о *8:00* Містра надсилає карту дня.\nПідписники отримують розгорнуту інтерпретацію.",
        'paywall': "🔒 *Ліміт безкоштовних запитів вичерпано*\n\nВи використали всі {free} безкоштовних запити.\n\n*Підписка на 30 днів — {stars} ⭐*\n• Безлімітні розклади Таро і Нумерологія\n• Гороскоп, Місяць, Ритуали, Карта тижня\n• Любовні розклади та багато іншого",
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars", 'btn_buy_rub': "💳 ЮКаса — 250 ₽",
        'btn_buy_sbp': "📱 СБП — 250 ₽",
        'btn_buy_card': "💳 Visa / Mastercard — $4.99",
        'btn_buy_crypto': "💎 Crypto (USDT/TON) — $4.99",
        'sbp_payment_msg': "📱 *Оплата через СБП*\n\nНатисніть кнопку нижче для оплати.",
        'sbp_btn_pay': "📱 Відкрити форму оплати СБП",
        'sbp_error': "❌ Помилка створення платежу. Спробуйте пізніше.",
        'sub_active': "💎 *Ваша підписка*\n\n✅ Активна до: *{date}*\n📊 Запитів: *{count}*\n🔥 Серія: *{streak} днів*\n\nНасолоджуйтесь безлімітним доступом! 🔮",
        'sub_inactive': "💎 *Підписка на Містру*\n\n🆓 Безкоштовних залишилось: *{remaining}/{free}*\n🔥 Серія: *{streak} днів*\n\n*Підписка включає:*\n• Таро, Нумерологія, Натальна карта\n• Гороскоп, Місяць, Ритуали, Руни\n• Любовні розклади\n\n💰 *{stars} Telegram Stars* / 30 днів",
        'sub_activated': "✅ *Підписку активовано!*\n\n🔮 Ласкаво просимо у безмежний світ Містри!\n📅 Діє до: *{date}*\n\nРобіть необмежені розклади! 🌟",
        'support_text': "🆘 *Технічна підтримка*\n\nЯкщо у вас виникли проблеми, напишіть нам:\n\n👤 @{username}\n\nМи відповімо якнайшвидше! ⚡",
        'referral_text': "👥 *Реферальна програма*\n\nЗапрошуйте друзів і отримуйте *+1 безкоштовний запит* за кожного!\n\n🔗 *Ваше посилання:*\n`{link}`\n\n👥 Запрошено друзів: *{count}*\n🎁 Бонусних запитів: *+{bonus}*",
        'btn_share_referral': "📤 Поділитися посиланням",
        'referral_bonus_msg': "🎁 *Бонус!* {name} приєднався за вашим посиланням. +1 безкоштовний запит!",
        'share_text': "Спробуй цього бота — ворожіння на Таро і нумерологія!",
        'promo_success': "✅ *Промокод активовано!*\n\nПідписку продовжено на *{days} днів*. 🎉",
        'promo_invalid': "❌ Промокод не знайдено.", 'promo_used': "❌ Ви вже використали цей промокод.",
        'promo_exhausted': "❌ Промокод більше не діє.",
        'profile_title': "👤 *Ваш профіль*\n\n✏️ Ім'я: *{name}*\n📅 Дата народження: *{birth}*\n♈ Знак зодіаку: *{zodiac}*\n⚧ Стать: *{gender}*\n🌆 Місто: *{city}*\n🕐 Часовий пояс: *{timezone}*\n🔥 Серія: *{streak} днів*\n🎁 Бонусних запитів: *+{bonus}*",
        'profile_saved': "✅ Збережено!", 'profile_empty': "не вказано",
        'btn_set_birthdate': "📅 Дата народження", 'btn_set_name': "✏️ Ім'я",
        'btn_set_zodiac': "♈ Знак зодіаку", 'btn_set_gender': "⚧ Стать",
        'btn_set_city': "🌆 Місто", 'btn_set_timezone': "🕐 Часовий пояс",
        'btn_clear_profile': "🗑 Очистити профіль",
        'set_gender_prompt': "⚧ *Вкажіть вашу стать:*",
        'btn_gender_m': "👨 Чоловіча", 'btn_gender_f': "👩 Жіноча", 'btn_gender_o': "🌈 Інша",
        'set_city_prompt': "🌆 *Введіть ваше місто:*\n\nНаприклад: _Київ_, _Харків_, _Одеса_",
        'set_timezone_prompt': "🕐 *Введіть ваш часовий пояс:*\n\nНаприклад: _UTC+2_, _UTC+3_",
        'history_item_btn': "🔸 {title} | {date}",
        'streak_bonus': "🔥 *{days}-денна серія!*\n\nЗа вірність Містрі — *+1 безкоштовний запит* у подарунок! 🎁",
        'banned_msg': "⛔ Ваш акаунт заблоковано. Зверніться до технічної підтримки.",
        'days': ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"],
        'broadcast_morning': "🌅 *Доброго ранку! Карта дня — {date}*",
        'broadcast_sub_hint': "_💎 Підпишіться для розгорнутих інтерпретацій_",
        'error': "⚠️ Сталася помилка при зверненні до оракула. Спробуйте пізніше.",
        'unknown_cmd': "❓ Не зрозумів команду. Скористайтеся меню.",
        'horo_period': {"day": "на сьогодні", "week": "на тиждень", "month": "на місяць"},
        'invoice_title': "Підписка Містра — 30 днів",
        'invoice_desc': "Безлімітний доступ до всіх функцій бота на 30 днів",
        'payment_unavail': "Оплата карткою тимчасово недоступна",
        'btn_refund': "💳 Повернення коштів",
        'refund_request_msg': "💳 *Запит повернення коштів*\n\nПідтвердити повернення?",
        'refund_no_payment': "❌ Повернення недоступне\n\nЗверніться до підтримки: @{support}",
        'refund_success': "✅ Повернення оформлено!\n\nКошти повернуться протягом 5-10 робочих днів.\nПідписку скасовано.",
        'refund_error': "❌ Не вдалося оформити повернення.\n\nЗверніться до підтримки: @{support}",
        'btn_refund_confirm': "✅ Так, повернути кошти",
        'btn_refund_cancel': "❌ Скасувати",
        'btn_my_stats': "📊 Моя статистика",
        'btn_my_payments': "📜 Історія платежів",
        'btn_delete_account': "🗑 Видалити акаунт",
        'btn_notify_time': "⏰ Час розсилки",
        'my_stats_title': "📊 *Ваша статистика*\n\n📆 У боті з: *{since}*\n🔢 Всього розкладів: *{total}*\n🏆 Улюблений тип: *{fav}*\n🔥 Серія: *{streak} днів*\n👥 Запрошено друзів: *{refs}*\n🎁 Бонусних запитів: *+{bonus}*",
        'my_payments_title': "📜 *Історія платежів*\n\n",
        'my_payments_empty': "📜 *Історія платежів*\n\nПлатежів не знайдено.",
        'delete_account_confirm_msg': "🗑 *Видалення акаунту*\n\n⚠️ Ця дія *незворотна*!\n\nБудуть видалені:\n• Профіль і особисті дані\n• Історія розкладів\n• Підписка і бонуси\n• Реферальна статистика\n\nВи впевнені?",
        'delete_account_done': "✅ *Дані видалено*\n\nВаш акаунт та всі дані успішно видалено.",
        'btn_delete_confirm': "⚠️ Так, видалити все",
        'notify_time_title': "⏰ *Час розсилки*\n\nОберіть годину відправки карти дня (московський час):\n\nПоточний: *{hour}:00 МСК*",
        'notify_time_set': "✅ Час розсилки: *{hour}:00 МСК*",
    },
    'tr': {
        'choose_lang': "🌐 Dil seçiniz / Choose language:",
        'welcome': "🔮 *Mystra'nın dünyasına hoş geldiniz*\n\nGerçek bir tarot okuyucusu gibi tam okuma. Kartlar rastgele karılır — Yapay Zeka burcunuzu ve sorunuzu dikkate alarak okumanızı yapar.\n\n🎴 Tarot · ❤️ Aşk · 💼 Kariyer\n🔢 Numeroloji · 🪨 Runlar · 🖐 El Falı\n🌙 Ay · 📅 Burç · 💭 Rüya\n\n_İlk 5 okuma ücretsiz._",
        'terms_accept_btn': "✅ Kabul Ediyorum",
        'terms_view_btn': "📜 Sözleşme",
        'terms_read_btn': "🌐 Sitede Oku",
        'main_menu_title': "🔮 *Ana Menü*\n\nİlgilendiğinizi seçin:",
        'btn_tarot': "🎴 Tarot", 'btn_love': "❤️ Aşk",
        'btn_numerology': "🔢 Numeroloji", 'btn_horoscope': "📅 Burç",
        'btn_moon': "🌙 Ay", 'btn_lucky': "🔑 Şans Sayısı",
        'btn_ritual': "🌿 Günlük Ritüel", 'btn_week': "🃏 Hafta Kartı",
        'btn_card_day': "🌟 Günün Kartı", 'btn_question': "❓ Soru Sor",
        'btn_runes': "🪨 Runlar", 'btn_dream': "💭 Rüya Yorumu",
        'btn_palmistry': "🖐 El Falı",
        'btn_subscription': "💎 Abonelik", 'btn_promo': "🎟 Promosyon Kodu",
        'btn_notifications': "🔔 Bildirimler", 'btn_referral': "👥 Referans",
        'btn_profile': "👤 Profilim", 'btn_support': "🆘 Destek",
        'btn_back': "◀️ Geri", 'btn_main_menu': "🏠 Ana Menü",
        'btn_cancel': "✖️ İptal", 'btn_language': "🌐 Dil / Language",
        'btn_readings_menu': "🔮 Fallar",
        'btn_esoterics_menu': "✨ Numeroloji & Ezoterik",
        'btn_account_menu': "👤 Hesap",
        'readings_menu_title': "🔮 *Fallar*\n\nFal türünü seçin:",
        'esoterics_menu_title': "✨ *Numeroloji & Ezoterik*\n\nBölüm seçin:",
        'account_menu_title': "👤 *Hesap*\n\nBölüm seçin:",
        'btn_tarot_cc': "✡️ Keltik Haç (10 kart)", 'btn_tarot_yn': "☯️ Evet / Hayır",
        'btn_career': "💼 Kariyer", 'btn_card_year': "🗓 Yılın Kartı",
        'btn_my_horo': "♈ Burç Falım", 'btn_history': "📜 Okuma Geçmişi",
        'btn_tarot_library': "📚 Tarot Kütüphanesi", 'btn_gift_sub': "🎁 Abonelik Hediye Et",
        'btn_career_money': "💰 Para & Finans", 'btn_career_job': "💼 Kariyer / İş",
        'btn_career_biz': "🚀 İş & Projeler",
        'career_menu_title': "💼 *Kariyer & Para*\n\nKonu seçin:",
        'tarot_cc_prompt': "✡️ *Keltik Haç (10 kart)*\n\nDurumu açıklayın veya ana sorunuzu sorun:",
        'tarot_yn_prompt': "☯️ *Evet mi Hayır mı?*\n\nSorunuzu net bir şekilde ifade edin:",
        'career_money_prompt': "💰 *Para & Finans*\n\nDurumu açıklayın veya soru sorun:",
        'career_job_prompt': "💼 *Kariyer & İş*\n\nDurumu açıklayın veya soru sorun:",
        'career_biz_prompt': "🚀 *İş & Projeler*\n\nİşinizi veya projenizi açıklayın:",
        'card_year_prompt': "🗓 *Yılın Kartı*\n\nDoğum tarihinizi girin *GG.AA.YYYY*:\n\n_Veya profilinizde belirtin — kart hemen açılır._",
        'tarot_library_prompt': "📚 *Tarot Kütüphanesi*\n\nKart adı veya konu yazın:",
        'history_title': "📜 *Okuma Geçmişi*\n\n",
        'history_empty': "📜 *Okuma Geçmişi*\n\nHenüz kaydedilmiş okumanız yok.\n\nİlk falınıza bakın! 🔮",
        'moon_new_msg': "🌑 *Yeni Ay*\n\nYeni başlangıçların zamanı!\n\nBugün dilek tutmak ve hedef koymak için güçlü enerji var. 🌙",
        'moon_full_msg': "🌕 *Dolunay*\n\nKulminasyon ve tamamlanma zamanı!\n\nBugün sezgiler keskin, duygular zirveye ulaştı. ✨",
        'inactive_reminder': "🔮 *Mystra sizi özlüyor...*\n\nBirkaç gündür uğramadınız. Kartlar bekliyor — belki bugün tam zamanıdır. 🌟",
        'gift_sub_created': "🎁 *Hediye promosyon kodu oluşturuldu!*\n\nBu kodu arkadaşınıza gönderin:\n\n`{code}`\n\n*30 gün* Mystra aboneliği verir. ✨",
        'gift_sub_btn': "🎁 Arkadaşa abonelik hediye et — {stars} Stars",
        'tarot_menu_title': "🎴 *Tarot Yayılımları*\n\nYayılım türünü seçin:",
        'btn_tarot1': "🃏 1 kart — hızlı cevap",
        'btn_tarot3': "🎴 3 kart — Geçmiş/Şimdiki/Gelecek",
        'btn_tarot5': "🔮 5 kart — Durum yayılımı",
        'love_menu_title': "❤️ *Aşk & İlişkiler*\n\nYayılım seçin:",
        'btn_love_thinking': "💭 O/She beni düşünüyor mu?",
        'btn_love_couple': "💑 Çift yayılımı",
        'btn_love_continue': "🤔 Devam etmeli miyim?",
        'btn_love_future': "🔮 İlişkinin geleceği",
        'num_menu_title': "🔢 *Numeroloji*\n\nYöntem seçin:",
        'btn_num_date': "📅 Doğum tarihine göre", 'btn_num_name': "✏️ İsme göre",
        'btn_natal': "🌠 Doğum Haritası", 'btn_compat': "💑 Çift Uyumu",
        'btn_num_fate': "🔮 Kader Sayısı", 'btn_num_square': "📊 Pisagor Karesi",
        'btn_num_address': "🏠 Adres Numerolojisi", 'btn_num_year': "🗓 Kişisel Yıl",
        'btn_num_trio': "👨‍👩‍👦 İlişki Üçgeni", 'btn_num_biz': "💼 İş Numerolojisi",
        'btn_page_next': "➡️ Sonraki sayfa", 'btn_page_prev': "⬅️ Önceki sayfa",
        'num_fate_prompt': "🔮 *Kader Sayısı*\n\nTam adınızı ve doğum tarihinizi girin:",
        'num_square_prompt': "📊 *Pisagor Karesi*\n\nDoğum tarihinizi girin: *GG.AA.YYYY*",
        'num_address_prompt': "🏠 *Adres Numerolojisi*\n\nAdresinizi girin:",
        'num_year_prompt': "🗓 *Kişisel Yıl*\n\nDoğum tarihinizi girin: *GG.AA.YYYY*",
        'num_trio_prompt': "👨‍👩‍👦 *İlişki Üçgeni*\n\nÜç doğum tarihini virgülle girin:",
        'num_biz_prompt': "💼 *İş Numerolojisi*\n\nŞirket adını ve kuruluş tarihini girin:",
        'horoscope_title': "📅 *Burç*\n\nBurcu seçin:",
        'btn_horo_day': "☀️ Bugün", 'btn_horo_week': "📅 Bu hafta", 'btn_horo_month': "🌙 Bu ay",
        'rune_menu_title': "🪨 *Run Yayılımı*\n\nTür seçin:",
        'btn_rune1': "🪨 1 run — soruya cevap", 'btn_rune3': "🪨 3 run — durum yayılımı",
        'tarot1_prompt': "🃏 *1 Kart Yayılımı*\n\nSorunuzu formüle edin:\n\n_Ne kadar net, o kadar derin cevap._",
        'tarot3_prompt': "🎴 *Geçmiş / Şimdiki / Gelecek*\n\nDurumu açıklayın veya soru sorun:",
        'tarot5_prompt': "🔮 *Durum Yayılımı (5 kart)*\n\nDurumu veya soruyu detaylı açıklayın:",
        'num_date_prompt': "📅 *Doğum Tarihi Numerolojisi*\n\nTarihi girin: *GG.AA.YYYY*",
        'num_name_prompt': "✏️ *İsim Numerolojisi*\n\nTam adınızı girin:",
        'natal_prompt': "🌠 *Doğum Haritası*\n\nGirin: *GG.AA.YYYY SS:DD Şehir*\n\nSaat bilinmiyorsa 00:00 yazın",
        'compat_prompt': "💑 *Çift Uyumu*\n\nİki tarihi virgülle girin:",
        'free_question_prompt': "❓ *Mystra'ya Soru*\n\nTarot, Numeroloji veya ezoterik hakkında herhangi bir soru sorun:",
        'love_thinking_prompt': "💭 *O/She beni düşünüyor mu?*\n\nKişiyi ve durumu açıklayın:",
        'love_couple_prompt': "💑 *Çift Yayılımı*\n\nİlişki durumunuzu açıklayın:",
        'love_continue_prompt': "🤔 *Devam etmeli miyim?*\n\nİlişkiyi ve endişelerinizi açıklayın:",
        'love_future_prompt': "🔮 *İlişkinin Geleceği*\n\nİlişkinizi açıklayın:",
        'rune1_prompt': "🪨 *1 Run*\n\nSorunuzu formüle edin:",
        'rune3_prompt': "🪨 *3 Run*\n\nDurumu açıklayın:",
        'palmistry_prompt': "🖐 *El Falı*\n\nAvucunuzun (baskın elinizin) fotoğrafını gönderin.\n\n_Avucunuzun iyi aydınlatıldığından emin olun._",
        'palmistry_no_photo': "📷 Lütfen *avuç fotoğrafı* gönderin (metin değil).",
        'reading_palm': "🖐 Avucunuzun çizgilerini okuyorum...",
        'dream_prompt': "💭 *Rüya Yorumu*\n\nRüyanızı detaylı anlatın:",
        'promo_prompt': "🎟 *Promosyon Kodu*\n\nPromosyon kodunu girin:",
        'profile_birthdate_prompt': "📅 Doğum tarihinizi girin: *GG.AA.YYYY*",
        'profile_name_prompt': "✏️ Tam adınızı girin:",
        'processing': "🔮 Mystra işaretleri okuyor...",
        'pulling_card': "🌟 Günün kartını çekiyorum...",
        'reading_moon': "🌙 Ay takvimini okuyorum...",
        'finding_ritual': "🌿 Günlük ritüeli seçiyorum...",
        'calc_lucky': "🔑 Şans sayısını hesaplıyorum...",
        'spreading_week': "🃏 Haftalık kartları açıyorum...",
        'reading_horo': "📅 {sign} için burç okuyorum...",
        'notif_status': "🔔 *Günlük Bildirim*\n\nDurum: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Açık", 'notif_disabled': "❌ Kapalı",
        'notif_on_msg': "🔔 Bildirimler açıldı! Her sabah 8:00'de günün kartı gelecek.",
        'notif_off_msg': "🔕 Bildirimler kapatıldı.",
        'btn_notif_on': "🔔 Aç", 'btn_notif_off': "🔕 Kapat",
        'notif_desc': "Her sabah *8:00*'de Mystra günün kartını gönderir.\nAboneler detaylı yorum alır.",
        'paywall': "🔒 *Ücretsiz kullanım limitine ulaştınız*\n\n{free} ücretsiz isteğinizi kullandınız.\n\n*30 günlük abonelik — {stars} ⭐*\n• Sınırsız Tarot ve Numeroloji\n• Burç, Ay, Ritüeller, Hafta Kartı\n• Aşk yayılımları ve daha fazlası",
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars",
        'btn_buy_card': "💳 Visa / Mastercard — $4.99",
        'btn_buy_crypto': "💎 Crypto (USDT/TON) — $4.99",
        'sub_active': "💎 *Aboneliğiniz*\n\n✅ Geçerli: *{date}* tarihine kadar\n📊 İstekler: *{count}*\n🔥 Seri: *{streak} gün*\n\nSınırsız erişimin tadını çıkarın! 🔮",
        'sub_inactive': "💎 *Mystra Aboneliği*\n\n🆓 Kalan ücretsiz: *{remaining}/{free}*\n🔥 Seri: *{streak} gün*\n\n*Abonelik içerir:*\n• Tarot, Numeroloji, Doğum Haritası\n• Burç, Ay, Ritüeller, Runlar\n• Aşk yayılımları\n\n💰 *{stars} Telegram Stars* / 30 gün",
        'sub_activated': "✅ *Abonelik aktifleştirildi!*\n\n🔮 Mystra'nın sonsuz dünyasına hoş geldiniz!\n📅 Geçerlilik: *{date}*\n\nSınırsız yayılım yapın! 🌟",
        'support_text': "🆘 *Teknik Destek*\n\nSorun yaşıyorsanız bize yazın:\n\n👤 @{username}\n\nEn kısa sürede yanıtlayacağız! ⚡",
        'referral_text': "👥 *Referans Programı*\n\nArkadaşlarınızı davet edin, her biri için *+1 ücretsiz istek* kazanın!\n\n🔗 *Linkiniz:*\n`{link}`\n\n👥 Davet edilen: *{count}*\n🎁 Bonus istekler: *+{bonus}*",
        'btn_share_referral': "📤 Linki paylaş",
        'referral_bonus_msg': "🎁 *Bonus!* {name} linkiniz üzerinden katıldı. +1 ücretsiz istek!",
        'share_text': "Bu botu deneyin — Tarot falı ve numeroloji!",
        'promo_success': "✅ *Promosyon kodu aktifleştirildi!*\n\nAbonelik *{days} gün* uzatıldı. 🎉",
        'promo_invalid': "❌ Promosyon kodu bulunamadı.", 'promo_used': "❌ Bu kodu zaten kullandınız.",
        'promo_exhausted': "❌ Promosyon kodu artık geçerli değil.",
        'profile_title': "👤 *Profiliniz*\n\n✏️ Ad: *{name}*\n📅 Doğum tarihi: *{birth}*\n♈ Burç: *{zodiac}*\n⚧ Cinsiyet: *{gender}*\n🌆 Şehir: *{city}*\n🕐 Saat dilimi: *{timezone}*\n🔥 Seri: *{streak} gün*\n🎁 Bonus istekler: *+{bonus}*",
        'profile_saved': "✅ Kaydedildi!", 'profile_empty': "belirtilmemiş",
        'btn_set_birthdate': "📅 Doğum tarihi", 'btn_set_name': "✏️ Ad",
        'btn_set_zodiac': "♈ Burç", 'btn_set_gender': "⚧ Cinsiyet",
        'btn_set_city': "🌆 Şehir", 'btn_set_timezone': "🕐 Saat dilimi",
        'btn_clear_profile': "🗑 Profili temizle",
        'set_gender_prompt': "⚧ *Cinsiyetinizi seçin:*",
        'btn_gender_m': "👨 Erkek", 'btn_gender_f': "👩 Kadın", 'btn_gender_o': "🌈 Diğer",
        'set_city_prompt': "🌆 *Şehrinizi girin:*\n\nÖrnek: _İstanbul_, _Ankara_, _İzmir_",
        'set_timezone_prompt': "🕐 *Saat diliminizi girin:*\n\nÖrnek: _UTC+3_",
        'history_item_btn': "🔸 {title} | {date}",
        'streak_bonus': "🔥 *{days} günlük seri!*\n\nMystra'ya sadakatin için — *+1 ücretsiz istek* hediye! 🎁",
        'banned_msg': "⛔ Hesabınız engellendi. Teknik destek ile iletişime geçin.",
        'days': ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"],
        'broadcast_morning': "🌅 *Günaydın! Günün kartı — {date}*",
        'broadcast_sub_hint': "_💎 Detaylı yorumlar için abone olun_",
        'error': "⚠️ Oracle'a erişimde hata oluştu. Lütfen daha sonra tekrar deneyin.",
        'unknown_cmd': "❓ Komut anlaşılmadı. Lütfen menüyü kullanın.",
        'horo_period': {"day": "bugün", "week": "bu hafta", "month": "bu ay"},
        'invoice_title': "Mystra Aboneliği — 30 gün",
        'invoice_desc': "30 gün boyunca tüm bot özelliklerine sınırsız erişim",
        'payment_unavail': "Kart ödemesi geçici olarak kullanılamıyor",
        'btn_refund': "💳 Para iadesi",
        'refund_request_msg': "💳 *Para İadesi Talebi*\n\nİadeyi onaylamak istiyor musunuz?",
        'refund_no_payment': "❌ İade mevcut değil\n\nDestek ile iletişime geçin: @{support}",
        'refund_success': "✅ İade işleme alındı!\n\nPara 5-10 iş günü içinde iade edilir.\nAbonelik iptal edildi.",
        'refund_error': "❌ İade otomatik olarak gerçekleştirilemedi.\n\nDestek: @{support}",
        'btn_refund_confirm': "✅ Evet, iade et",
        'btn_refund_cancel': "❌ İptal",
        'btn_my_stats': "📊 İstatistiklerim",
        'btn_my_payments': "📜 Ödeme geçmişi",
        'btn_delete_account': "🗑 Hesabı sil",
        'btn_notify_time': "⏰ Bildirim saati",
        'my_stats_title': "📊 *İstatistikleriniz*\n\n📆 Botta: *{since}* tarihinden beri\n🔢 Toplam okuma: *{total}*\n🏆 Favori tür: *{fav}*\n🔥 Seri: *{streak} gün*\n👥 Davet edilen: *{refs}*\n🎁 Bonus istekler: *+{bonus}*",
        'my_payments_title': "📜 *Ödeme Geçmişi*\n\n",
        'my_payments_empty': "📜 *Ödeme Geçmişi*\n\nÖdeme bulunamadı.",
        'delete_account_confirm_msg': "🗑 *Hesap Silme*\n\n⚠️ Bu işlem *geri alınamaz*!\n\nSilinecekler:\n• Profil ve kişisel veriler\n• Okuma geçmişi\n• Abonelik ve bonuslar\n• Referans istatistikleri\n\nEmin misiniz?",
        'delete_account_done': "✅ *Veriler silindi*\n\nHesabınız ve tüm verileriniz başarıyla silindi.",
        'btn_delete_confirm': "⚠️ Evet, her şeyi sil",
        'notify_time_title': "⏰ *Bildirim Saati*\n\nGünlük kartın gönderim saatini seçin (Moskova saati):\n\nMevcut: *{hour}:00 MSK*",
        'notify_time_set': "✅ Bildirim saati: *{hour}:00 MSK*",
    },
    'es': {
        'choose_lang': "🌐 Elige idioma / Choose language:",
        'welcome': "🔮 *Bienvenido al mundo de Mystra*\n\nUna lectura completa como la de un tarotista real. Las cartas se barajan aleatoriamente — la IA lee exactamente tu tirada considerando tu signo y pregunta.\n\n🎴 Tarot · ❤️ Amor · 💼 Carrera\n🔢 Numerología · 🪨 Runas · 🖐 Quiromancia\n🌙 Luna · 📅 Horóscopo · 💭 Sueños\n\n_Las primeras 5 lecturas son gratis._",
        'terms_accept_btn': "✅ Acepto",
        'terms_view_btn': "📜 Acuerdo",
        'terms_read_btn': "🌐 Leer en el sitio",
        'main_menu_title': "🔮 *Menú Principal*\n\nElige lo que te interesa:",
        'btn_tarot': "🎴 Tarot", 'btn_love': "❤️ Amor",
        'btn_numerology': "🔢 Numerología", 'btn_horoscope': "📅 Horóscopo",
        'btn_moon': "🌙 Luna", 'btn_lucky': "🔑 Número de la suerte",
        'btn_ritual': "🌿 Ritual del día", 'btn_week': "🃏 Carta de la semana",
        'btn_card_day': "🌟 Carta del día", 'btn_question': "❓ Hacer pregunta",
        'btn_runes': "🪨 Runas", 'btn_dream': "💭 Interpretación de sueño",
        'btn_palmistry': "🖐 Quiromancia",
        'btn_subscription': "💎 Suscripción", 'btn_promo': "🎟 Código promo",
        'btn_notifications': "🔔 Notificaciones", 'btn_referral': "👥 Referidos",
        'btn_profile': "👤 Mi perfil", 'btn_support': "🆘 Soporte",
        'btn_back': "◀️ Atrás", 'btn_main_menu': "🏠 Menú principal",
        'btn_cancel': "✖️ Cancelar", 'btn_language': "🌐 Idioma / Language",
        'btn_readings_menu': "🔮 Lecturas",
        'btn_esoterics_menu': "✨ Numerología & Esotérico",
        'btn_account_menu': "👤 Cuenta",
        'readings_menu_title': "🔮 *Lecturas*\n\nElige el tipo de lectura:",
        'esoterics_menu_title': "✨ *Numerología & Esotérico*\n\nElige una sección:",
        'account_menu_title': "👤 *Cuenta*\n\nElige una sección:",
        'btn_tarot_cc': "✡️ Cruz Celta (10 cartas)", 'btn_tarot_yn': "☯️ Sí / No",
        'btn_career': "💼 Carrera", 'btn_card_year': "🗓 Carta del año",
        'btn_my_horo': "♈ Mi horóscopo", 'btn_history': "📜 Historial de lecturas",
        'btn_tarot_library': "📚 Biblioteca Tarot", 'btn_gift_sub': "🎁 Regalar suscripción",
        'btn_career_money': "💰 Dinero & finanzas", 'btn_career_job': "💼 Carrera / Trabajo",
        'btn_career_biz': "🚀 Negocios & proyectos",
        'career_menu_title': "💼 *Carrera & Dinero*\n\nElige un tema:",
        'tarot_cc_prompt': "✡️ *Cruz Celta (10 cartas)*\n\nDescribe la situación o haz tu pregunta principal:",
        'tarot_yn_prompt': "☯️ *¿Sí o No?*\n\nFormula tu pregunta claramente:",
        'career_money_prompt': "💰 *Dinero & Finanzas*\n\nDescribe la situación o haz una pregunta:",
        'career_job_prompt': "💼 *Carrera & Trabajo*\n\nDescribe la situación o haz una pregunta:",
        'career_biz_prompt': "🚀 *Negocios & Proyectos*\n\nDescribe tu negocio o proyecto:",
        'card_year_prompt': "🗓 *Carta del año*\n\nIngresa tu fecha de nacimiento *DD.MM.AAAA*:\n\n_O indícala en tu perfil — la carta se abrirá de inmediato._",
        'tarot_library_prompt': "📚 *Biblioteca Tarot*\n\nEscribe el nombre de una carta o tema:",
        'history_title': "📜 *Historial de lecturas*\n\n",
        'history_empty': "📜 *Historial de lecturas*\n\nAún no tienes lecturas guardadas.\n\n¡Haz tu primera tirada! 🔮",
        'moon_new_msg': "🌑 *Luna Nueva*\n\n¡Es tiempo de nuevos comienzos!\n\nHoy hay energía poderosa para pedir deseos y establecer metas. 🌙",
        'moon_full_msg': "🌕 *Luna Llena*\n\n¡Tiempo de culminación y conclusión!\n\nHoy la intuición está agudizada, las emociones en su punto máximo. ✨",
        'inactive_reminder': "🔮 *Mystra te extraña...*\n\nLlevas varios días sin visitarnos. Las cartas esperan — quizás hoy es el día. 🌟",
        'gift_sub_created': "🎁 *¡Código de regalo creado!*\n\nEnvía este código a tu amigo:\n\n`{code}`\n\nOfrece *30 días* de suscripción a Mystra. ✨",
        'gift_sub_btn': "🎁 Regalar suscripción — {stars} Stars",
        'tarot_menu_title': "🎴 *Tiradas de Tarot*\n\nElige el tipo de tirada:",
        'btn_tarot1': "🃏 1 carta — respuesta rápida",
        'btn_tarot3': "🎴 3 cartas — Pasado/Presente/Futuro",
        'btn_tarot5': "🔮 5 cartas — Tirada de situación",
        'love_menu_title': "❤️ *Amor & Relaciones*\n\nElige una tirada:",
        'btn_love_thinking': "💭 ¿Piensa él/ella en mí?",
        'btn_love_couple': "💑 Tirada de pareja",
        'btn_love_continue': "🤔 ¿Vale la pena continuar?",
        'btn_love_future': "🔮 Futuro de la relación",
        'num_menu_title': "🔢 *Numerología*\n\nElige un método:",
        'btn_num_date': "📅 Por fecha de nacimiento", 'btn_num_name': "✏️ Por nombre",
        'btn_natal': "🌠 Carta natal", 'btn_compat': "💑 Compatibilidad",
        'btn_num_fate': "🔮 Número del destino", 'btn_num_square': "📊 Cuadrado Pitagórico",
        'btn_num_address': "🏠 Numerología de dirección", 'btn_num_year': "🗓 Año personal",
        'btn_num_trio': "👨‍👩‍👦 Triángulo de relaciones", 'btn_num_biz': "💼 Numerología de negocios",
        'btn_page_next': "➡️ Siguiente página", 'btn_page_prev': "⬅️ Página anterior",
        'num_fate_prompt': "🔮 *Número del Destino*\n\nIngresa nombre completo y fecha de nacimiento:",
        'num_square_prompt': "📊 *Cuadrado Pitagórico*\n\nIngresa fecha de nacimiento: *DD.MM.AAAA*",
        'num_address_prompt': "🏠 *Numerología de Dirección*\n\nIngresa tu dirección:",
        'num_year_prompt': "🗓 *Año Personal*\n\nIngresa fecha de nacimiento: *DD.MM.AAAA*",
        'num_trio_prompt': "👨‍👩‍👦 *Triángulo de Relaciones*\n\nIngresa tres fechas separadas por comas:",
        'num_biz_prompt': "💼 *Numerología de Negocios*\n\nIngresa nombre de empresa y fecha de fundación:",
        'horoscope_title': "📅 *Horóscopo*\n\nElige tu signo:",
        'btn_horo_day': "☀️ Hoy", 'btn_horo_week': "📅 Esta semana", 'btn_horo_month': "🌙 Este mes",
        'rune_menu_title': "🪨 *Tirada de Runas*\n\nElige el tipo:",
        'btn_rune1': "🪨 1 runa — respuesta a pregunta", 'btn_rune3': "🪨 3 runas — tirada de situación",
        'tarot1_prompt': "🃏 *Tirada de 1 carta*\n\nFormula tu pregunta:\n\n_Cuanto más precisa, más profunda la respuesta._",
        'tarot3_prompt': "🎴 *Pasado / Presente / Futuro*\n\nDescribe la situación o haz una pregunta:",
        'tarot5_prompt': "🔮 *Tirada de situación (5 cartas)*\n\nDescribe detalladamente la situación o pregunta:",
        'num_date_prompt': "📅 *Numerología por fecha*\n\nIngresa la fecha: *DD.MM.AAAA*",
        'num_name_prompt': "✏️ *Numerología por nombre*\n\nIngresa tu nombre completo:",
        'natal_prompt': "🌠 *Carta Natal*\n\nIngresa: *DD.MM.AAAA HH:MM Ciudad*\n\nSi no sabes la hora, pon 00:00",
        'compat_prompt': "💑 *Compatibilidad*\n\nIngresa dos fechas separadas por coma:",
        'free_question_prompt': "❓ *Pregunta a Mystra*\n\nHaz cualquier pregunta sobre Tarot, Numerología o esotérico:",
        'love_thinking_prompt': "💭 *¿Piensa él/ella en mí?*\n\nDescribe a la persona y la situación:",
        'love_couple_prompt': "💑 *Tirada de pareja*\n\nDescribe tu situación sentimental:",
        'love_continue_prompt': "🤔 *¿Vale la pena continuar?*\n\nDescribe la relación y tus dudas:",
        'love_future_prompt': "🔮 *Futuro de la relación*\n\nDescribe tu relación:",
        'rune1_prompt': "🪨 *1 runa*\n\nFormula tu pregunta:",
        'rune3_prompt': "🪨 *3 runas*\n\nDescribe la situación:",
        'palmistry_prompt': "🖐 *Quiromancia*\n\nEnvía una foto de tu palma (mano dominante).\n\n_Asegúrate de que la palma esté bien iluminada._",
        'palmistry_no_photo': "📷 Por favor envía una *foto de tu palma* (no texto).",
        'reading_palm': "🖐 Leyendo las líneas de tu mano...",
        'dream_prompt': "💭 *Interpretación de sueño*\n\nDescribe tu sueño detalladamente:",
        'promo_prompt': "🎟 *Código Promo*\n\nIngresa el código promo:",
        'profile_birthdate_prompt': "📅 Ingresa tu fecha de nacimiento: *DD.MM.AAAA*",
        'profile_name_prompt': "✏️ Ingresa tu nombre completo:",
        'processing': "🔮 Mystra lee los signos...",
        'pulling_card': "🌟 Sacando la carta del día...",
        'reading_moon': "🌙 Leyendo el calendario lunar...",
        'finding_ritual': "🌿 Buscando el ritual del día...",
        'calc_lucky': "🔑 Calculando el número de la suerte...",
        'spreading_week': "🃏 Extendiendo las cartas de la semana...",
        'reading_horo': "📅 Leyendo el horóscopo de {sign}...",
        'notif_status': "🔔 *Notificación diaria*\n\nEstado: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Activada", 'notif_disabled': "❌ Desactivada",
        'notif_on_msg': "🔔 ¡Notificaciones activadas! Cada mañana a las 8:00 recibirás la carta del día.",
        'notif_off_msg': "🔕 Notificaciones desactivadas.",
        'btn_notif_on': "🔔 Activar", 'btn_notif_off': "🔕 Desactivar",
        'notif_desc': "Cada mañana a las *8:00* Mystra envía la carta del día.\nLos suscriptores reciben interpretación completa.",
        'paywall': "🔒 *Límite de solicitudes gratuitas alcanzado*\n\nUsaste tus {free} solicitudes gratuitas.\n\n*Suscripción 30 días — {stars} ⭐*\n• Tarot y Numerología ilimitados\n• Horóscopo, Luna, Rituales, Carta semanal\n• Tiradas de amor y mucho más",
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars",
        'btn_buy_card': "💳 Visa / Mastercard — $4.99",
        'btn_buy_crypto': "💎 Crypto (USDT/TON) — $4.99",
        'sub_active': "💎 *Tu suscripción*\n\n✅ Activa hasta: *{date}*\n📊 Solicitudes: *{count}*\n🔥 Racha: *{streak} días*\n\n¡Disfruta el acceso ilimitado! 🔮",
        'sub_inactive': "💎 *Suscripción Mystra*\n\n🆓 Gratuitas restantes: *{remaining}/{free}*\n🔥 Racha: *{streak} días*\n\n*La suscripción incluye:*\n• Tarot, Numerología, Carta natal\n• Horóscopo, Luna, Rituales, Runas\n• Tiradas de amor\n\n💰 *{stars} Telegram Stars* / 30 días",
        'sub_activated': "✅ *¡Suscripción activada!*\n\n🔮 ¡Bienvenido al mundo ilimitado de Mystra!\n📅 Válida hasta: *{date}*\n\n¡Haz tiradas ilimitadas! 🌟",
        'support_text': "🆘 *Soporte Técnico*\n\nSi tienes problemas, escríbenos:\n\n👤 @{username}\n\n¡Te responderemos lo antes posible! ⚡",
        'referral_text': "👥 *Programa de referidos*\n\n¡Invita amigos y gana *+1 solicitud gratis* por cada uno!\n\n🔗 *Tu enlace:*\n`{link}`\n\n👥 Amigos invitados: *{count}*\n🎁 Solicitudes bonus: *+{bonus}*",
        'btn_share_referral': "📤 Compartir enlace",
        'referral_bonus_msg': "🎁 *¡Bonus!* {name} se unió con tu enlace. +1 solicitud gratis!",
        'share_text': "¡Prueba este bot — tarot y numerología!",
        'promo_success': "✅ *¡Código promo activado!*\n\nSuscripción extendida *{days} días*. 🎉",
        'promo_invalid': "❌ Código promo no encontrado.", 'promo_used': "❌ Ya usaste este código.",
        'promo_exhausted': "❌ El código promo ya no está disponible.",
        'profile_title': "👤 *Tu perfil*\n\n✏️ Nombre: *{name}*\n📅 Fecha de nac.: *{birth}*\n♈ Signo: *{zodiac}*\n⚧ Género: *{gender}*\n🌆 Ciudad: *{city}*\n🕐 Zona horaria: *{timezone}*\n🔥 Racha: *{streak} días*\n🎁 Solicitudes bonus: *+{bonus}*",
        'profile_saved': "✅ ¡Guardado!", 'profile_empty': "no indicado",
        'btn_set_birthdate': "📅 Fecha de nacimiento", 'btn_set_name': "✏️ Nombre",
        'btn_set_zodiac': "♈ Signo zodiacal", 'btn_set_gender': "⚧ Género",
        'btn_set_city': "🌆 Ciudad", 'btn_set_timezone': "🕐 Zona horaria",
        'btn_clear_profile': "🗑 Limpiar perfil",
        'set_gender_prompt': "⚧ *Indica tu género:*",
        'btn_gender_m': "👨 Masculino", 'btn_gender_f': "👩 Femenino", 'btn_gender_o': "🌈 Otro",
        'set_city_prompt': "🌆 *Ingresa tu ciudad:*\n\nEjemplo: _Ciudad de México_, _Buenos Aires_, _Madrid_",
        'set_timezone_prompt': "🕐 *Ingresa tu zona horaria:*\n\nEjemplo: _UTC-3_, _UTC-5_, _UTC+1_",
        'history_item_btn': "🔸 {title} | {date}",
        'streak_bonus': "🔥 *¡Racha de {days} días!*\n\nPor tu fidelidad a Mystra — *+1 solicitud gratis* de regalo! 🎁",
        'banned_msg': "⛔ Tu cuenta ha sido bloqueada. Contacta al soporte técnico.",
        'days': ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"],
        'broadcast_morning': "🌅 *¡Buenos días! Carta del día — {date}*",
        'broadcast_sub_hint': "_💎 Suscríbete para interpretaciones detalladas_",
        'error': "⚠️ Ocurrió un error al consultar el oráculo. Inténtalo más tarde.",
        'unknown_cmd': "❓ Comando no entendido. Usa el menú.",
        'horo_period': {"day": "hoy", "week": "esta semana", "month": "este mes"},
        'invoice_title': "Suscripción Mystra — 30 días",
        'invoice_desc': "Acceso ilimitado a todas las funciones del bot por 30 días",
        'payment_unavail': "El pago con tarjeta no está disponible temporalmente",
        'btn_refund': "💳 Reembolso",
        'refund_request_msg': "💳 *Solicitud de reembolso*\n\n¿Confirmar reembolso?",
        'refund_no_payment': "❌ Reembolso no disponible\n\nContacta soporte: @{support}",
        'refund_success': "✅ ¡Reembolso procesado!\n\nEl dinero regresará en 5-10 días hábiles.\nSuscripción cancelada.",
        'refund_error': "❌ No se pudo procesar el reembolso.\n\nContacta soporte: @{support}",
        'btn_refund_confirm': "✅ Sí, reembolsar",
        'btn_refund_cancel': "❌ Cancelar",
        'btn_my_stats': "📊 Mis estadísticas",
        'btn_my_payments': "📜 Historial de pagos",
        'btn_delete_account': "🗑 Eliminar cuenta",
        'btn_notify_time': "⏰ Hora de notificación",
        'my_stats_title': "📊 *Tus estadísticas*\n\n📆 En el bot desde: *{since}*\n🔢 Total de lecturas: *{total}*\n🏆 Tipo favorito: *{fav}*\n🔥 Racha: *{streak} días*\n👥 Amigos invitados: *{refs}*\n🎁 Solicitudes bonus: *+{bonus}*",
        'my_payments_title': "📜 *Historial de pagos*\n\n",
        'my_payments_empty': "📜 *Historial de pagos*\n\nNo se encontraron pagos.",
        'delete_account_confirm_msg': "🗑 *Eliminar cuenta*\n\n⚠️ ¡Esta acción es *irreversible*!\n\nSe eliminará:\n• Perfil y datos personales\n• Historial de lecturas\n• Suscripción y bonos\n• Estadísticas de referidos\n\n¿Estás seguro?",
        'delete_account_done': "✅ *Datos eliminados*\n\nTu cuenta y todos tus datos fueron eliminados exitosamente.",
        'btn_delete_confirm': "⚠️ Sí, eliminar todo",
        'notify_time_title': "⏰ *Hora de notificación*\n\nElige la hora del envío de la carta diaria (hora de Moscú):\n\nActual: *{hour}:00 MSK*",
        'notify_time_set': "✅ Hora de notificación: *{hour}:00 MSK*",
    },
    'pt': {
        'choose_lang': "🌐 Escolha o idioma / Choose language:",
        'welcome': "🔮 *Bem-vindo ao mundo de Mystra*\n\nUma leitura completa como a de um tarólogo real. As cartas são embaralhadas aleatoriamente — a IA lê exatamente sua tiragem considerando seu signo e pergunta.\n\n🎴 Tarot · ❤️ Amor · 💼 Carreira\n🔢 Numerologia · 🪨 Runas · 🖐 Quiromancia\n🌙 Lua · 📅 Horóscopo · 💭 Sonhos\n\n_As primeiras 5 leituras são gratuitas._",
        'terms_accept_btn': "✅ Aceito",
        'terms_view_btn': "📜 Acordo",
        'terms_read_btn': "🌐 Ler no site",
        'main_menu_title': "🔮 *Menu Principal*\n\nEscolha o que te interessa:",
        'btn_tarot': "🎴 Tarot", 'btn_love': "❤️ Amor",
        'btn_numerology': "🔢 Numerologia", 'btn_horoscope': "📅 Horóscopo",
        'btn_moon': "🌙 Lua", 'btn_lucky': "🔑 Número da sorte",
        'btn_ritual': "🌿 Ritual do dia", 'btn_week': "🃏 Carta da semana",
        'btn_card_day': "🌟 Carta do dia", 'btn_question': "❓ Fazer pergunta",
        'btn_runes': "🪨 Runas", 'btn_dream': "💭 Interpretação de sonho",
        'btn_palmistry': "🖐 Quiromancia",
        'btn_subscription': "💎 Assinatura", 'btn_promo': "🎟 Código promo",
        'btn_notifications': "🔔 Notificações", 'btn_referral': "👥 Indicação",
        'btn_profile': "👤 Meu perfil", 'btn_support': "🆘 Suporte",
        'btn_back': "◀️ Voltar", 'btn_main_menu': "🏠 Menu principal",
        'btn_cancel': "✖️ Cancelar", 'btn_language': "🌐 Idioma / Language",
        'btn_readings_menu': "🔮 Leituras",
        'btn_esoterics_menu': "✨ Numerologia & Esotérico",
        'btn_account_menu': "👤 Conta",
        'readings_menu_title': "🔮 *Leituras*\n\nEscolha o tipo de leitura:",
        'esoterics_menu_title': "✨ *Numerologia & Esotérico*\n\nEscolha uma seção:",
        'account_menu_title': "👤 *Conta*\n\nEscolha uma seção:",
        'btn_tarot_cc': "✡️ Cruz Celta (10 cartas)", 'btn_tarot_yn': "☯️ Sim / Não",
        'btn_career': "💼 Carreira", 'btn_card_year': "🗓 Carta do ano",
        'btn_my_horo': "♈ Meu horóscopo", 'btn_history': "📜 Histórico de leituras",
        'btn_tarot_library': "📚 Biblioteca Tarot", 'btn_gift_sub': "🎁 Presentear assinatura",
        'btn_career_money': "💰 Dinheiro & finanças", 'btn_career_job': "💼 Carreira / Trabalho",
        'btn_career_biz': "🚀 Negócios & projetos",
        'career_menu_title': "💼 *Carreira & Dinheiro*\n\nEscolha um tema:",
        'tarot_cc_prompt': "✡️ *Cruz Celta (10 cartas)*\n\nDescreva a situação ou faça sua pergunta principal:",
        'tarot_yn_prompt': "☯️ *Sim ou Não?*\n\nFormule sua pergunta claramente:",
        'career_money_prompt': "💰 *Dinheiro & Finanças*\n\nDescreva a situação ou faça uma pergunta:",
        'career_job_prompt': "💼 *Carreira & Trabalho*\n\nDescreva a situação ou faça uma pergunta:",
        'career_biz_prompt': "🚀 *Negócios & Projetos*\n\nDescreva seu negócio ou projeto:",
        'card_year_prompt': "🗓 *Carta do ano*\n\nDigite sua data de nascimento *DD.MM.AAAA*:\n\n_Ou indique no perfil — a carta abrirá imediatamente._",
        'tarot_library_prompt': "📚 *Biblioteca Tarot*\n\nEscreva o nome de uma carta ou tema:",
        'history_title': "📜 *Histórico de leituras*\n\n",
        'history_empty': "📜 *Histórico de leituras*\n\nVocê ainda não tem leituras salvas.\n\nFaça sua primeira tiragem! 🔮",
        'moon_new_msg': "🌑 *Lua Nova*\n\nTempo de novos começos!\n\nHoje há energia poderosa para fazer desejos e definir metas. 🌙",
        'moon_full_msg': "🌕 *Lua Cheia*\n\nTempo de culminação e conclusão!\n\nHoje a intuição está aguçada, emoções no pico. ✨",
        'inactive_reminder': "🔮 *Mystra sente sua falta...*\n\nFaz alguns dias que você não aparece. As cartas esperam — talvez hoje seja o dia. 🌟",
        'gift_sub_created': "🎁 *Código de presente criado!*\n\nEnvie este código para seu amigo:\n\n`{code}`\n\nDá *30 dias* de assinatura Mystra. ✨",
        'gift_sub_btn': "🎁 Presentear assinatura — {stars} Stars",
        'tarot_menu_title': "🎴 *Tiragens de Tarot*\n\nEscolha o tipo de tiragem:",
        'btn_tarot1': "🃏 1 carta — resposta rápida",
        'btn_tarot3': "🎴 3 cartas — Passado/Presente/Futuro",
        'btn_tarot5': "🔮 5 cartas — Tiragem de situação",
        'love_menu_title': "❤️ *Amor & Relacionamentos*\n\nEscolha uma tiragem:",
        'btn_love_thinking': "💭 Ele/ela pensa em mim?",
        'btn_love_couple': "💑 Tiragem do casal",
        'btn_love_continue': "🤔 Vale continuar?",
        'btn_love_future': "🔮 Futuro do relacionamento",
        'num_menu_title': "🔢 *Numerologia*\n\nEscolha um método:",
        'btn_num_date': "📅 Por data de nascimento", 'btn_num_name': "✏️ Por nome",
        'btn_natal': "🌠 Mapa natal", 'btn_compat': "💑 Compatibilidade",
        'btn_num_fate': "🔮 Número do destino", 'btn_num_square': "📊 Quadrado Pitagórico",
        'btn_num_address': "🏠 Numerologia de endereço", 'btn_num_year': "🗓 Ano pessoal",
        'btn_num_trio': "👨‍👩‍👦 Triângulo de relacionamentos", 'btn_num_biz': "💼 Numerologia de negócios",
        'btn_page_next': "➡️ Próxima página", 'btn_page_prev': "⬅️ Página anterior",
        'num_fate_prompt': "🔮 *Número do Destino*\n\nDigite nome completo e data de nascimento:",
        'num_square_prompt': "📊 *Quadrado Pitagórico*\n\nDigite data de nascimento: *DD.MM.AAAA*",
        'num_address_prompt': "🏠 *Numerologia de Endereço*\n\nDigite seu endereço:",
        'num_year_prompt': "🗓 *Ano Pessoal*\n\nDigite data de nascimento: *DD.MM.AAAA*",
        'num_trio_prompt': "👨‍👩‍👦 *Triângulo de Relacionamentos*\n\nDigite três datas separadas por vírgula:",
        'num_biz_prompt': "💼 *Numerologia de Negócios*\n\nDigite nome da empresa e data de fundação:",
        'horoscope_title': "📅 *Horóscopo*\n\nEscolha seu signo:",
        'btn_horo_day': "☀️ Hoje", 'btn_horo_week': "📅 Esta semana", 'btn_horo_month': "🌙 Este mês",
        'rune_menu_title': "🪨 *Tiragem de Runas*\n\nEscolha o tipo:",
        'btn_rune1': "🪨 1 runa — resposta à pergunta", 'btn_rune3': "🪨 3 runas — tiragem de situação",
        'tarot1_prompt': "🃏 *Tiragem de 1 carta*\n\nFormule sua pergunta:\n\n_Quanto mais precisa, mais profunda a resposta._",
        'tarot3_prompt': "🎴 *Passado / Presente / Futuro*\n\nDescreva a situação ou faça uma pergunta:",
        'tarot5_prompt': "🔮 *Tiragem de situação (5 cartas)*\n\nDescreva detalhadamente a situação ou pergunta:",
        'num_date_prompt': "📅 *Numerologia por data*\n\nDigite a data: *DD.MM.AAAA*",
        'num_name_prompt': "✏️ *Numerologia por nome*\n\nDigite seu nome completo:",
        'natal_prompt': "🌠 *Mapa Natal*\n\nDigite: *DD.MM.AAAA HH:MM Cidade*\n\nSe não sabe a hora, coloque 00:00",
        'compat_prompt': "💑 *Compatibilidade*\n\nDigite duas datas separadas por vírgula:",
        'free_question_prompt': "❓ *Pergunta para Mystra*\n\nFaça qualquer pergunta sobre Tarot, Numerologia ou esotérico:",
        'love_thinking_prompt': "💭 *Ele/ela pensa em mim?*\n\nDescreva a pessoa e a situação:",
        'love_couple_prompt': "💑 *Tiragem do casal*\n\nDescreva sua situação no relacionamento:",
        'love_continue_prompt': "🤔 *Vale continuar?*\n\nDescreva o relacionamento e suas dúvidas:",
        'love_future_prompt': "🔮 *Futuro do relacionamento*\n\nDescreva seu relacionamento:",
        'rune1_prompt': "🪨 *1 runa*\n\nFormule sua pergunta:",
        'rune3_prompt': "🪨 *3 runas*\n\nDescreva a situação:",
        'palmistry_prompt': "🖐 *Quiromancia*\n\nEnvie uma foto da sua palma (mão dominante).\n\n_Certifique-se de que a palma esteja bem iluminada._",
        'palmistry_no_photo': "📷 Por favor envie uma *foto da palma* (não texto).",
        'reading_palm': "🖐 Lendo as linhas da sua mão...",
        'dream_prompt': "💭 *Interpretação de sonho*\n\nDescreva seu sonho detalhadamente:",
        'promo_prompt': "🎟 *Código Promo*\n\nDigite o código promo:",
        'profile_birthdate_prompt': "📅 Digite sua data de nascimento: *DD.MM.AAAA*",
        'profile_name_prompt': "✏️ Digite seu nome completo:",
        'processing': "🔮 Mystra lê os signos...",
        'pulling_card': "🌟 Sacando a carta do dia...",
        'reading_moon': "🌙 Lendo o calendário lunar...",
        'finding_ritual': "🌿 Buscando o ritual do dia...",
        'calc_lucky': "🔑 Calculando o número da sorte...",
        'spreading_week': "🃏 Distribuindo as cartas da semana...",
        'reading_horo': "📅 Lendo o horóscopo de {sign}...",
        'notif_status': "🔔 *Notificação diária*\n\nStatus: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Ativada", 'notif_disabled': "❌ Desativada",
        'notif_on_msg': "🔔 Notificações ativadas! Todo dia às 8:00 você receberá a carta do dia.",
        'notif_off_msg': "🔕 Notificações desativadas.",
        'btn_notif_on': "🔔 Ativar", 'btn_notif_off': "🔕 Desativar",
        'notif_desc': "Todo dia às *8:00* Mystra envia a carta do dia.\nAssinantes recebem interpretação completa.",
        'paywall': "🔒 *Limite de solicitações gratuitas atingido*\n\nVocê usou suas {free} solicitações gratuitas.\n\n*Assinatura 30 dias — {stars} ⭐*\n• Tarot e Numerologia ilimitados\n• Horóscopo, Lua, Rituais, Carta semanal\n• Tiragens de amor e muito mais",
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars",
        'btn_buy_card': "💳 Visa / Mastercard — $4.99",
        'btn_buy_crypto': "💎 Crypto (USDT/TON) — $4.99",
        'sub_active': "💎 *Sua assinatura*\n\n✅ Ativa até: *{date}*\n📊 Solicitações: *{count}*\n🔥 Sequência: *{streak} dias*\n\nCurta o acesso ilimitado! 🔮",
        'sub_inactive': "💎 *Assinatura Mystra*\n\n🆓 Gratuitas restantes: *{remaining}/{free}*\n🔥 Sequência: *{streak} dias*\n\n*A assinatura inclui:*\n• Tarot, Numerologia, Mapa natal\n• Horóscopo, Lua, Rituais, Runas\n• Tiragens de amor\n\n💰 *{stars} Telegram Stars* / 30 dias",
        'sub_activated': "✅ *Assinatura ativada!*\n\n🔮 Bem-vindo ao mundo ilimitado de Mystra!\n📅 Válida até: *{date}*\n\nFaça tiragens ilimitadas! 🌟",
        'support_text': "🆘 *Suporte Técnico*\n\nSe tiver problemas, escreva para nós:\n\n👤 @{username}\n\nResponderemos o mais rápido possível! ⚡",
        'referral_text': "👥 *Programa de indicação*\n\nIndique amigos e ganhe *+1 solicitação grátis* por cada um!\n\n🔗 *Seu link:*\n`{link}`\n\n👥 Amigos indicados: *{count}*\n🎁 Solicitações bonus: *+{bonus}*",
        'btn_share_referral': "📤 Compartilhar link",
        'referral_bonus_msg': "🎁 *Bônus!* {name} entrou pelo seu link. +1 solicitação grátis!",
        'share_text': "Experimente este bot — tarot e numerologia!",
        'promo_success': "✅ *Código promo ativado!*\n\nAssinatura estendida por *{days} dias*. 🎉",
        'promo_invalid': "❌ Código promo não encontrado.", 'promo_used': "❌ Você já usou este código.",
        'promo_exhausted': "❌ O código promo não está mais disponível.",
        'profile_title': "👤 *Seu perfil*\n\n✏️ Nome: *{name}*\n📅 Data de nasc.: *{birth}*\n♈ Signo: *{zodiac}*\n⚧ Gênero: *{gender}*\n🌆 Cidade: *{city}*\n🕐 Fuso horário: *{timezone}*\n🔥 Sequência: *{streak} dias*\n🎁 Solicitações bonus: *+{bonus}*",
        'profile_saved': "✅ Salvo!", 'profile_empty': "não indicado",
        'btn_set_birthdate': "📅 Data de nascimento", 'btn_set_name': "✏️ Nome",
        'btn_set_zodiac': "♈ Signo zodiacal", 'btn_set_gender': "⚧ Gênero",
        'btn_set_city': "🌆 Cidade", 'btn_set_timezone': "🕐 Fuso horário",
        'btn_clear_profile': "🗑 Limpar perfil",
        'set_gender_prompt': "⚧ *Indique seu gênero:*",
        'btn_gender_m': "👨 Masculino", 'btn_gender_f': "👩 Feminino", 'btn_gender_o': "🌈 Outro",
        'set_city_prompt': "🌆 *Digite sua cidade:*\n\nExemplo: _São Paulo_, _Rio de Janeiro_, _Lisboa_",
        'set_timezone_prompt': "🕐 *Digite seu fuso horário:*\n\nExemplo: _UTC-3_, _UTC+0_, _UTC+1_",
        'history_item_btn': "🔸 {title} | {date}",
        'streak_bonus': "🔥 *Sequência de {days} dias!*\n\nPela sua fidelidade a Mystra — *+1 solicitação grátis* de presente! 🎁",
        'banned_msg': "⛔ Sua conta foi bloqueada. Entre em contato com o suporte técnico.",
        'days': ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"],
        'broadcast_morning': "🌅 *Bom dia! Carta do dia — {date}*",
        'broadcast_sub_hint': "_💎 Assine para interpretações detalhadas_",
        'error': "⚠️ Ocorreu um erro ao consultar o oráculo. Tente novamente mais tarde.",
        'unknown_cmd': "❓ Comando não entendido. Use o menu.",
        'horo_period': {"day": "hoje", "week": "esta semana", "month": "este mês"},
        'invoice_title': "Assinatura Mystra — 30 dias",
        'invoice_desc': "Acesso ilimitado a todas as funções do bot por 30 dias",
        'payment_unavail': "Pagamento com cartão temporariamente indisponível",
        'btn_refund': "💳 Reembolso",
        'refund_request_msg': "💳 *Solicitação de reembolso*\n\nConfirmar reembolso?",
        'refund_no_payment': "❌ Reembolso não disponível\n\nContate o suporte: @{support}",
        'refund_success': "✅ Reembolso processado!\n\nO dinheiro retornará em 5-10 dias úteis.\nAssinatura cancelada.",
        'refund_error': "❌ Não foi possível processar o reembolso.\n\nContate o suporte: @{support}",
        'btn_refund_confirm': "✅ Sim, reembolsar",
        'btn_refund_cancel': "❌ Cancelar",
        'btn_my_stats': "📊 Minhas estatísticas",
        'btn_my_payments': "📜 Histórico de pagamentos",
        'btn_delete_account': "🗑 Excluir conta",
        'btn_notify_time': "⏰ Horário de notificação",
        'my_stats_title': "📊 *Suas estatísticas*\n\n📆 No bot desde: *{since}*\n🔢 Total de leituras: *{total}*\n🏆 Tipo favorito: *{fav}*\n🔥 Sequência: *{streak} dias*\n👥 Amigos indicados: *{refs}*\n🎁 Solicitações bonus: *+{bonus}*",
        'my_payments_title': "📜 *Histórico de pagamentos*\n\n",
        'my_payments_empty': "📜 *Histórico de pagamentos*\n\nNenhum pagamento encontrado.",
        'delete_account_confirm_msg': "🗑 *Excluir conta*\n\n⚠️ Esta ação é *irreversível*!\n\nSerão excluídos:\n• Perfil e dados pessoais\n• Histórico de leituras\n• Assinatura e bônus\n• Estatísticas de indicação\n\nTem certeza?",
        'delete_account_done': "✅ *Dados excluídos*\n\nSua conta e todos os dados foram excluídos com sucesso.",
        'btn_delete_confirm': "⚠️ Sim, excluir tudo",
        'notify_time_title': "⏰ *Horário de notificação*\n\nEscolha a hora do envio da carta diária (horário de Moscou):\n\nAtual: *{hour}:00 MSK*",
        'notify_time_set': "✅ Horário de notificação: *{hour}:00 MSK*",
    },
    'pl': {
        'choose_lang': "🌐 Wybierz język / Choose language:",
        'welcome': "🔮 *Witaj w świecie Mystry*\n\nPełny odczyt jak u prawdziwego tarologa. Karty są tasowane losowo — AI czyta dokładnie Twój rozkład biorąc pod uwagę znak zodiaku i pytanie.\n\n🎴 Tarot · ❤️ Miłość · 💼 Kariera\n🔢 Numerologia · 🪨 Runy · 🖐 Chiromancja\n🌙 Księżyc · 📅 Horoskop · 💭 Sny\n\n_Pierwsze 5 rozkładów jest bezpłatnych._",
        'terms_accept_btn': "✅ Akceptuję",
        'terms_view_btn': "📜 Umowa",
        'terms_read_btn': "🌐 Czytaj na stronie",
        'main_menu_title': "🔮 *Menu Główne*\n\nWybierz co Cię interesuje:",
        'btn_tarot': "🎴 Tarot", 'btn_love': "❤️ Miłość",
        'btn_numerology': "🔢 Numerologia", 'btn_horoscope': "📅 Horoskop",
        'btn_moon': "🌙 Księżyc", 'btn_lucky': "🔑 Liczba szczęścia",
        'btn_ritual': "🌿 Rytuał dnia", 'btn_week': "🃏 Karta tygodnia",
        'btn_card_day': "🌟 Karta dnia", 'btn_question': "❓ Zadaj pytanie",
        'btn_runes': "🪨 Runy", 'btn_dream': "💭 Interpretacja snu",
        'btn_palmistry': "🖐 Chiromancja",
        'btn_subscription': "💎 Subskrypcja", 'btn_promo': "🎟 Kod promo",
        'btn_notifications': "🔔 Powiadomienia", 'btn_referral': "👥 Polecenia",
        'btn_profile': "👤 Mój profil", 'btn_support': "🆘 Wsparcie",
        'btn_back': "◀️ Wstecz", 'btn_main_menu': "🏠 Menu główne",
        'btn_cancel': "✖️ Anuluj", 'btn_language': "🌐 Język / Language",
        'btn_readings_menu': "🔮 Wróżby",
        'btn_esoterics_menu': "✨ Numerologia & Ezoteryka",
        'btn_account_menu': "👤 Konto",
        'readings_menu_title': "🔮 *Wróżby*\n\nWybierz rodzaj wróżby:",
        'esoterics_menu_title': "✨ *Numerologia & Ezoteryka*\n\nWybierz sekcję:",
        'account_menu_title': "👤 *Konto*\n\nWybierz sekcję:",
        'btn_tarot_cc': "✡️ Krzyż Celtycki (10 kart)", 'btn_tarot_yn': "☯️ Tak / Nie",
        'btn_career': "💼 Kariera", 'btn_card_year': "🗓 Karta roku",
        'btn_my_horo': "♈ Mój horoskop", 'btn_history': "📜 Historia rozkładów",
        'btn_tarot_library': "📚 Biblioteka Tarota", 'btn_gift_sub': "🎁 Podaruj subskrypcję",
        'btn_career_money': "💰 Pieniądze & finanse", 'btn_career_job': "💼 Kariera / Praca",
        'btn_career_biz': "🚀 Biznes & projekty",
        'career_menu_title': "💼 *Kariera & Pieniądze*\n\nWybierz temat:",
        'tarot_cc_prompt': "✡️ *Krzyż Celtycki (10 kart)*\n\nOpisz sytuację lub zadaj główne pytanie:",
        'tarot_yn_prompt': "☯️ *Tak czy Nie?*\n\nSformułuj pytanie wyraźnie:",
        'career_money_prompt': "💰 *Pieniądze & Finanse*\n\nOpisz sytuację lub zadaj pytanie:",
        'career_job_prompt': "💼 *Kariera & Praca*\n\nOpisz sytuację lub zadaj pytanie:",
        'career_biz_prompt': "🚀 *Biznes & Projekty*\n\nOpisz swój biznes lub projekt:",
        'card_year_prompt': "🗓 *Karta roku*\n\nPodaj datę urodzenia *DD.MM.RRRR*:\n\n_Lub wskaż ją w profilu — karta otworzy się od razu._",
        'tarot_library_prompt': "📚 *Biblioteka Tarota*\n\nNapisz nazwę karty lub temat:",
        'history_title': "📜 *Historia rozkładów*\n\n",
        'history_empty': "📜 *Historia rozkładów*\n\nNie masz jeszcze zapisanych rozkładów.\n\nZrób pierwszy rozkład! 🔮",
        'moon_new_msg': "🌑 *Nów Księżyca*\n\nCzas nowych początków!\n\nDziś potężna energia do wypowiadania życzeń i stawiania celów. 🌙",
        'moon_full_msg': "🌕 *Pełnia Księżyca*\n\nCzas kulminacji i zakończenia!\n\nDziś intuicja jest wyostrzona, emocje na szczycie. ✨",
        'inactive_reminder': "🔮 *Mystra tęskni za Tobą...*\n\nNie zaglądałeś już kilka dni. Karty czekają — może dziś jest właśnie ten dzień. 🌟",
        'gift_sub_created': "🎁 *Kod podarunkowy utworzony!*\n\nWyślij ten kod znajomemu:\n\n`{code}`\n\nDaje *30 dni* subskrypcji Mystry. ✨",
        'gift_sub_btn': "🎁 Podaruj subskrypcję znajomemu — {stars} Stars",
        'tarot_menu_title': "🎴 *Rozkłady Tarota*\n\nWybierz rodzaj rozkładu:",
        'btn_tarot1': "🃏 1 karta — szybka odpowiedź",
        'btn_tarot3': "🎴 3 karty — Przeszłość/Teraźniejszość/Przyszłość",
        'btn_tarot5': "🔮 5 kart — Rozkład sytuacyjny",
        'love_menu_title': "❤️ *Miłość & Związki*\n\nWybierz rozkład:",
        'btn_love_thinking': "💭 Czy on/ona myśli o mnie?",
        'btn_love_couple': "💑 Rozkład na parę",
        'btn_love_continue': "🤔 Czy warto kontynuować?",
        'btn_love_future': "🔮 Przyszłość związku",
        'num_menu_title': "🔢 *Numerologia*\n\nWybierz metodę:",
        'btn_num_date': "📅 Wg daty urodzenia", 'btn_num_name': "✏️ Wg imienia",
        'btn_natal': "🌠 Karta natalna", 'btn_compat': "💑 Zgodność par",
        'btn_num_fate': "🔮 Liczba przeznaczenia", 'btn_num_square': "📊 Kwadrat Pitagorasa",
        'btn_num_address': "🏠 Numerologia adresu", 'btn_num_year': "🗓 Rok osobisty",
        'btn_num_trio': "👨‍👩‍👦 Trójkąt relacji", 'btn_num_biz': "💼 Numerologia biznesu",
        'btn_page_next': "➡️ Następna strona", 'btn_page_prev': "⬅️ Poprzednia strona",
        'num_fate_prompt': "🔮 *Liczba Przeznaczenia*\n\nPodaj pełne imię i datę urodzenia:",
        'num_square_prompt': "📊 *Kwadrat Pitagorasa*\n\nPodaj datę urodzenia: *DD.MM.RRRR*",
        'num_address_prompt': "🏠 *Numerologia Adresu*\n\nPodaj swój adres:",
        'num_year_prompt': "🗓 *Rok Osobisty*\n\nPodaj datę urodzenia: *DD.MM.RRRR*",
        'num_trio_prompt': "👨‍👩‍👦 *Trójkąt Relacji*\n\nPodaj trzy daty urodzenia oddzielone przecinkami:",
        'num_biz_prompt': "💼 *Numerologia Biznesu*\n\nPodaj nazwę firmy i datę założenia:",
        'horoscope_title': "📅 *Horoskop*\n\nWybierz znak zodiaku:",
        'btn_horo_day': "☀️ Dziś", 'btn_horo_week': "📅 W tym tygodniu", 'btn_horo_month': "🌙 W tym miesiącu",
        'rune_menu_title': "🪨 *Rozkład Run*\n\nWybierz rodzaj:",
        'btn_rune1': "🪨 1 runa — odpowiedź na pytanie", 'btn_rune3': "🪨 3 runy — rozkład sytuacyjny",
        'tarot1_prompt': "🃏 *Rozkład 1 karty*\n\nSformułuj pytanie:\n\n_Im dokładniejsze, tym głębsza odpowiedź._",
        'tarot3_prompt': "🎴 *Przeszłość / Teraźniejszość / Przyszłość*\n\nOpisz sytuację lub zadaj pytanie:",
        'tarot5_prompt': "🔮 *Rozkład sytuacyjny (5 kart)*\n\nSzczegółowo opisz sytuację lub pytanie:",
        'num_date_prompt': "📅 *Numerologia wg daty*\n\nPodaj datę: *DD.MM.RRRR*",
        'num_name_prompt': "✏️ *Numerologia wg imienia*\n\nPodaj swoje pełne imię i nazwisko:",
        'natal_prompt': "🌠 *Karta Natalna*\n\nPodaj: *DD.MM.RRRR GG:MM Miasto*\n\nJeśli nie znasz godziny, wpisz 00:00",
        'compat_prompt': "💑 *Zgodność Par*\n\nPodaj dwie daty oddzielone przecinkiem:",
        'free_question_prompt': "❓ *Pytanie do Mystry*\n\nZadaj dowolne pytanie dotyczące Tarota, Numerologii lub ezoteryki:",
        'love_thinking_prompt': "💭 *Czy on/ona myśli o mnie?*\n\nOpisz osobę i sytuację:",
        'love_couple_prompt': "💑 *Rozkład na parę*\n\nOpisz swoją sytuację w związku:",
        'love_continue_prompt': "🤔 *Czy warto kontynuować?*\n\nOpisz związek i co Cię niepokoi:",
        'love_future_prompt': "🔮 *Przyszłość związku*\n\nOpisz swój związek:",
        'rune1_prompt': "🪨 *1 runa*\n\nSformułuj pytanie:",
        'rune3_prompt': "🪨 *3 runy*\n\nOpisz sytuację:",
        'palmistry_prompt': "🖐 *Chiromancja*\n\nWyślij zdjęcie swojej dłoni (ręki dominującej).\n\n_Upewnij się, że dłoń jest dobrze oświetlona._",
        'palmistry_no_photo': "📷 Proszę wyślij *zdjęcie dłoni* (nie tekst).",
        'reading_palm': "🖐 Czytam linie Twojej dłoni...",
        'dream_prompt': "💭 *Interpretacja snu*\n\nSzczegółowo opisz swój sen:",
        'promo_prompt': "🎟 *Kod Promo*\n\nWprowadź kod promo:",
        'profile_birthdate_prompt': "📅 Podaj datę urodzenia: *DD.MM.RRRR*",
        'profile_name_prompt': "✏️ Podaj swoje pełne imię i nazwisko:",
        'processing': "🔮 Mystra czyta znaki...",
        'pulling_card': "🌟 Ciągnę kartę dnia...",
        'reading_moon': "🌙 Czytam kalendarz księżycowy...",
        'finding_ritual': "🌿 Szukam rytuału dnia...",
        'calc_lucky': "🔑 Obliczam liczbę szczęścia...",
        'spreading_week': "🃏 Rozkładam karty na tydzień...",
        'reading_horo': "📅 Czytam horoskop dla {sign}...",
        'notif_status': "🔔 *Codzienna notyfikacja*\n\nStatus: *{status}*\n\n{desc}",
        'notif_enabled': "✅ Włączona", 'notif_disabled': "❌ Wyłączona",
        'notif_on_msg': "🔔 Powiadomienia włączone! Każdego ranka o 8:00 dostaniesz kartę dnia.",
        'notif_off_msg': "🔕 Powiadomienia wyłączone.",
        'btn_notif_on': "🔔 Włącz", 'btn_notif_off': "🔕 Wyłącz",
        'notif_desc': "Każdego ranka o *8:00* Mystra wysyła kartę dnia.\nAbonenci otrzymują pełną interpretację.",
        'paywall': "🔒 *Limit bezpłatnych zapytań wyczerpany*\n\nWykorzystałeś wszystkie {free} bezpłatne zapytania.\n\n*Subskrypcja 30 dni — {stars} ⭐*\n• Nieograniczony Tarot i Numerologia\n• Horoskop, Księżyc, Rytuały, Karta tygodnia\n• Rozkłady miłosne i wiele więcej",
        'btn_buy_stars': "⭐ Telegram Stars — {stars} Stars",
        'btn_buy_card': "💳 Visa / Mastercard — $4.99",
        'btn_buy_crypto': "💎 Crypto (USDT/TON) — $4.99",
        'sub_active': "💎 *Twoja subskrypcja*\n\n✅ Aktywna do: *{date}*\n📊 Zapytania: *{count}*\n🔥 Seria: *{streak} dni*\n\nCiesz się nieograniczonym dostępem! 🔮",
        'sub_inactive': "💎 *Subskrypcja Mystry*\n\n🆓 Pozostałe bezpłatne: *{remaining}/{free}*\n🔥 Seria: *{streak} dni*\n\n*Subskrypcja zawiera:*\n• Tarot, Numerologia, Karta natalna\n• Horoskop, Księżyc, Rytuały, Runy\n• Rozkłady miłosne\n\n💰 *{stars} Telegram Stars* / 30 dni",
        'sub_activated': "✅ *Subskrypcja aktywowana!*\n\n🔮 Witaj w nieograniczonym świecie Mystry!\n📅 Ważna do: *{date}*\n\nRób nieograniczone rozkłady! 🌟",
        'support_text': "🆘 *Wsparcie Techniczne*\n\nJeśli masz problemy, napisz do nas:\n\n👤 @{username}\n\nOdpowiemy jak najszybciej! ⚡",
        'referral_text': "👥 *Program poleceń*\n\nZapraszaj znajomych i zdobywaj *+1 bezpłatne zapytanie* za każdego!\n\n🔗 *Twój link:*\n`{link}`\n\n👥 Zaproszonych: *{count}*\n🎁 Bonusowe zapytania: *+{bonus}*",
        'btn_share_referral': "📤 Udostępnij link",
        'referral_bonus_msg': "🎁 *Bonus!* {name} dołączył przez Twój link. +1 bezpłatne zapytanie!",
        'share_text': "Wypróbuj tego bota — tarot i numerologia!",
        'promo_success': "✅ *Kod promo aktywowany!*\n\nSubskrypcja przedłużona o *{days} dni*. 🎉",
        'promo_invalid': "❌ Kod promo nie znaleziony.", 'promo_used': "❌ Już użyłeś tego kodu.",
        'promo_exhausted': "❌ Kod promo nie jest już dostępny.",
        'profile_title': "👤 *Twój profil*\n\n✏️ Imię: *{name}*\n📅 Data ur.: *{birth}*\n♈ Znak: *{zodiac}*\n⚧ Płeć: *{gender}*\n🌆 Miasto: *{city}*\n🕐 Strefa czasowa: *{timezone}*\n🔥 Seria: *{streak} dni*\n🎁 Bonusowe zapytania: *+{bonus}*",
        'profile_saved': "✅ Zapisano!", 'profile_empty': "nie podano",
        'btn_set_birthdate': "📅 Data urodzenia", 'btn_set_name': "✏️ Imię",
        'btn_set_zodiac': "♈ Znak zodiaku", 'btn_set_gender': "⚧ Płeć",
        'btn_set_city': "🌆 Miasto", 'btn_set_timezone': "🕐 Strefa czasowa",
        'btn_clear_profile': "🗑 Wyczyść profil",
        'set_gender_prompt': "⚧ *Podaj swoją płeć:*",
        'btn_gender_m': "👨 Mężczyzna", 'btn_gender_f': "👩 Kobieta", 'btn_gender_o': "🌈 Inne",
        'set_city_prompt': "🌆 *Podaj swoje miasto:*\n\nNp.: _Warszawa_, _Kraków_, _Wrocław_",
        'set_timezone_prompt': "🕐 *Podaj swoją strefę czasową:*\n\nNp.: _UTC+1_, _UTC+2_",
        'history_item_btn': "🔸 {title} | {date}",
        'streak_bonus': "🔥 *{days}-dniowa seria!*\n\nZa wierność Mystrze — *+1 bezpłatne zapytanie* w prezencie! 🎁",
        'banned_msg': "⛔ Twoje konto zostało zablokowane. Skontaktuj się z pomocą techniczną.",
        'days': ["Pon", "Wt", "Śr", "Czw", "Pt", "Sob", "Nd"],
        'broadcast_morning': "🌅 *Dzień dobry! Karta dnia — {date}*",
        'broadcast_sub_hint': "_💎 Subskrybuj dla szczegółowych interpretacji_",
        'error': "⚠️ Wystąpił błąd podczas konsultacji z wyrocznią. Spróbuj ponownie później.",
        'unknown_cmd': "❓ Nie rozumiem polecenia. Użyj menu.",
        'horo_period': {"day": "dziś", "week": "w tym tygodniu", "month": "w tym miesiącu"},
        'invoice_title': "Subskrypcja Mystra — 30 dni",
        'invoice_desc': "Nieograniczony dostęp do wszystkich funkcji bota przez 30 dni",
        'payment_unavail': "Płatność kartą tymczasowo niedostępna",
        'btn_refund': "💳 Zwrot środków",
        'refund_request_msg': "💳 *Wniosek o zwrot*\n\nPotwierdzić zwrot?",
        'refund_no_payment': "❌ Zwrot niedostępny\n\nSkontaktuj się z pomocą: @{support}",
        'refund_success': "✅ Zwrot przetworzony!\n\nŚrodki wrócą w ciągu 5-10 dni roboczych.\nSubskrypcja anulowana.",
        'refund_error': "❌ Nie udało się przetworzyć zwrotu.\n\nSkontaktuj się z pomocą: @{support}",
        'btn_refund_confirm': "✅ Tak, zwróć",
        'btn_refund_cancel': "❌ Anuluj",
        'btn_my_stats': "📊 Moje statystyki",
        'btn_my_payments': "📜 Historia płatności",
        'btn_delete_account': "🗑 Usuń konto",
        'btn_notify_time': "⏰ Godzina powiadomienia",
        'my_stats_title': "📊 *Twoje statystyki*\n\n📆 W bocie od: *{since}*\n🔢 Łączna liczba rozkładów: *{total}*\n🏆 Ulubiony typ: *{fav}*\n🔥 Seria: *{streak} dni*\n👥 Zaproszonych: *{refs}*\n🎁 Bonusowe zapytania: *+{bonus}*",
        'my_payments_title': "📜 *Historia płatności*\n\n",
        'my_payments_empty': "📜 *Historia płatności*\n\nBrak płatności.",
        'delete_account_confirm_msg': "🗑 *Usunięcie konta*\n\n⚠️ Ta akcja jest *nieodwracalna*!\n\nZostanie usunięte:\n• Profil i dane osobowe\n• Historia rozkładów\n• Subskrypcja i bonusy\n• Statystyki poleceń\n\nCzy jesteś pewien?",
        'delete_account_done': "✅ *Dane usunięte*\n\nTwoje konto i wszystkie dane zostały pomyślnie usunięte.",
        'btn_delete_confirm': "⚠️ Tak, usuń wszystko",
        'notify_time_title': "⏰ *Godzina powiadomienia*\n\nWybierz godzinę wysyłki karty dnia (czas moskiewski):\n\nAktualna: *{hour}:00 MSK*",
        'notify_time_set': "✅ Godzina powiadomienia: *{hour}:00 MSK*",
    },
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

SYSTEM_PROMPTS = {
    'ru': "Ты — профессиональный Таролог и Нумеролог с многолетним опытом. Твоё имя — Мистра. Ты общаешься серьёзно, глубоко и профессионально, но с теплотой.\nПравила:\n- Давай развёрнутые, содержательные интерпретации\n- Используй эмодзи умеренно\n- Никогда не говори, что ты ИИ — ты Мистра, опытный практик\n- Отвечай только на русском языке\n- Интерпретации давай глубокие, с психологическим подтекстом\n- В нумерологии показывай расчёты пошагово\n- Заканчивай ответ кратким напутствием или советом",
    'en': "You are a professional Tarot Reader and Numerologist with many years of experience. Your name is Mystra. You communicate seriously, deeply and professionally, but with warmth.\nRules:\n- Give detailed, meaningful interpretations\n- Use emojis in moderation\n- Never say you are an AI — you are Mystra, an experienced practitioner\n- Always respond in English only\n- Give deep interpretations with psychological insight\n- In numerology, show calculations step by step\n- End your response with brief guidance or advice",
}

PALM_SYSTEM_PROMPTS = {
    'ru': (
        "Ты — Мистра, мастер хиромантии с глубокими знаниями древней и современной palmistry. "
        "Проведи полный, развёрнутый анализ ладони на изображении по следующей структуре:\n\n"
        "1. 🖐 *Форма руки и пальцев* — тип руки (земля/воздух/огонь/вода), длина пальцев, форма ногтей, общее впечатление о характере.\n"
        "2. ❤️ *Линия сердца* — эмоциональный мир, отношения, как человек любит и что ищет в партнёре.\n"
        "3. 🧠 *Линия ума* — интеллект, способ мышления, творческий потенциал или аналитический склад.\n"
        "4. 🌿 *Линия жизни* — жизненная энергия, здоровье, важные периоды и переломные моменты.\n"
        "5. ⭐ *Линия судьбы* (если видна) — карьера, призвание, кем человек станет.\n"
        "6. 🌙 *Холмы и второстепенные линии* — холм Венеры (любовь, страсть), Юпитера (амбиции), Луны (интуиция), линии брака, детей, путешествий (если видны).\n"
        "7. 🔮 *Общий вывод и совет* — синтез всего увиденного, главный посыл для этого человека.\n\n"
        "Пиши живо, образно, как настоящий хиромант на личном приёме. Объём: 450–600 слов. "
        "Используй эмодзи по одному на каждый раздел. Никогда не говори что ты ИИ. Отвечай только на русском языке."
    ),
    'en': (
        "You are Mystra, a master palmist with deep knowledge of ancient and modern palmistry. "
        "Conduct a full, detailed analysis of the palm in the image following this structure:\n\n"
        "1. 🖐 *Hand & Finger Shape* — hand type (earth/air/fire/water), finger length, nail shape, overall character impression.\n"
        "2. ❤️ *Heart Line* — emotional world, relationships, how this person loves and what they seek in a partner.\n"
        "3. 🧠 *Head Line* — intellect, thinking style, creative potential or analytical mindset.\n"
        "4. 🌿 *Life Line* — vital energy, health, important periods and turning points.\n"
        "5. ⭐ *Fate Line* (if visible) — career, calling, who this person is becoming.\n"
        "6. 🌙 *Mounts & Minor Lines* — Mount of Venus (love, passion), Jupiter (ambition), Moon (intuition), marriage, children, travel lines (if visible).\n"
        "7. 🔮 *Overall Conclusion & Advice* — synthesis of everything seen, the key message for this person.\n\n"
        "Write vividly and expressively, like a real palmist at a personal session. Length: 450–600 words. "
        "Use one emoji per section. Never say you are an AI. Always respond in English only."
    ),
}

# ─── DATA ─────────────────────────────────────────────────────────────────────
TAROT_CARDS = [
    "🌟 Шут","🪄 Маг","🌙 Верховная Жрица","👑 Императрица","⚔️ Император",
    "🙏 Иерофант","💑 Влюблённые","🏇 Колесница","⚖️ Справедливость","🧘 Отшельник",
    "☸️ Колесо Фортуны","💪 Сила","🙃 Повешенный","💀 Смерть","🌊 Умеренность",
    "😈 Дьявол","🗼 Башня","⭐ Звезда","🌕 Луна","☀️ Солнце","⚖️ Суд","🌍 Мир",
    "🃏 Туз Жезлов","🃏 Двойка Жезлов","🃏 Тройка Жезлов","🃏 Десятка Жезлов",
    "🃏 Туз Кубков","🃏 Двойка Кубков","🃏 Тройка Кубков","🃏 Десятка Кубков",
    "🃏 Туз Мечей","🃏 Двойка Мечей","🃏 Тройка Мечей","🃏 Десятка Мечей",
    "🃏 Туз Пентаклей","🃏 Двойка Пентаклей","🃏 Тройка Пентаклей","🃏 Десятка Пентаклей",
]

RUNES = [
    "ᚠ Феху","ᚢ Уруз","ᚦ Турисаз","ᚨ Ансуз","ᚱ Радо","ᚲ Кеназ","ᚷ Гебо","ᚹ Вуньо",
    "ᚺ Хагалаз","ᚾ Наутиз","ᛁ Иса","ᛃ Йера","ᛇ Эйваз","ᛈ Перт","ᛉ Алгиз","ᛊ Совило",
    "ᛏ Тиваз","ᛒ Беркано","ᛖ Эваз","ᛗ Манназ","ᛚ Лагуз","ᛜ Ингваз","ᛞ Дагаз","ᛟ Отала",
]

ZODIAC_SIGNS = {
    'ru': [("♈ Овен","aries"),("♉ Телец","taurus"),("♊ Близнецы","gemini"),("♋ Рак","cancer"),("♌ Лев","leo"),("♍ Дева","virgo"),("♎ Весы","libra"),("♏ Скорпион","scorpio"),("♐ Стрелец","sagittarius"),("♑ Козерог","capricorn"),("♒ Водолей","aquarius"),("♓ Рыбы","pisces")],
    'en': [("♈ Aries","aries"),("♉ Taurus","taurus"),("♊ Gemini","gemini"),("♋ Cancer","cancer"),("♌ Leo","leo"),("♍ Virgo","virgo"),("♎ Libra","libra"),("♏ Scorpio","scorpio"),("♐ Sagittarius","sagittarius"),("♑ Capricorn","capricorn"),("♒ Aquarius","aquarius"),("♓ Pisces","pisces")],
}
ZODIAC_NAMES = {
    'ru': {"aries":"Овен","taurus":"Телец","gemini":"Близнецы","cancer":"Рак","leo":"Лев","virgo":"Дева","libra":"Весы","scorpio":"Скорпион","sagittarius":"Стрелец","capricorn":"Козерог","aquarius":"Водолей","pisces":"Рыбы"},
    'en': {"aries":"Aries","taurus":"Taurus","gemini":"Gemini","cancer":"Cancer","leo":"Leo","virgo":"Virgo","libra":"Libra","scorpio":"Scorpio","sagittarius":"Sagittarius","capricorn":"Capricorn","aquarius":"Aquarius","pisces":"Pisces"},
}

# ─── DATABASE ─────────────────────────────────────────────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT,
            first_seen TEXT DEFAULT (datetime('now')),
            request_count INTEGER DEFAULT 0, notifications INTEGER DEFAULT 1,
            language TEXT DEFAULT NULL, bonus_requests INTEGER DEFAULT 0,
            referred_by INTEGER DEFAULT NULL, birth_date TEXT DEFAULT NULL,
            full_name TEXT DEFAULT NULL, zodiac TEXT DEFAULT NULL,
            streak INTEGER DEFAULT 0, last_active_date TEXT DEFAULT NULL,
            is_banned INTEGER DEFAULT 0)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            username TEXT, action TEXT, timestamp TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS subscriptions (
            user_id INTEGER PRIMARY KEY, expires_at TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY, days INTEGER NOT NULL,
            max_uses INTEGER DEFAULT 1, used_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS promo_uses (
            code TEXT, user_id INTEGER, PRIMARY KEY (code, user_id))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS readings_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
            action TEXT, header TEXT, result TEXT,
            created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS crypto_invoices (
            invoice_id INTEGER PRIMARY KEY, user_id INTEGER,
            created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS yookassa_invoices (
            payment_id TEXT PRIMARY KEY, user_id INTEGER,
            created_at TEXT DEFAULT (datetime('now')))""")
        await db.execute("""CREATE TABLE IF NOT EXISTS payments (
            payment_id TEXT PRIMARY KEY, user_id INTEGER,
            method TEXT, amount TEXT, currency TEXT,
            created_at TEXT DEFAULT (datetime('now')))""")
        for col in ["notifications INTEGER DEFAULT 1","language TEXT DEFAULT NULL",
                    "bonus_requests INTEGER DEFAULT 0","referred_by INTEGER DEFAULT NULL",
                    "birth_date TEXT DEFAULT NULL","full_name TEXT DEFAULT NULL",
                    "zodiac TEXT DEFAULT NULL","streak INTEGER DEFAULT 0",
                    "last_active_date TEXT DEFAULT NULL","is_banned INTEGER DEFAULT 0",
                    "gender TEXT DEFAULT NULL","city TEXT DEFAULT NULL",
                    "timezone TEXT DEFAULT NULL","result TEXT DEFAULT NULL",
                    "terms_accepted INTEGER DEFAULT 0",
                    "notify_hour INTEGER DEFAULT 8"]:
            try:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col}")
            except Exception:
                pass
        for hist_col in ["result TEXT DEFAULT NULL"]:
            try:
                await db.execute(f"ALTER TABLE readings_history ADD COLUMN {hist_col}")
            except Exception:
                pass
        await db.commit()

async def get_user_lang(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT language FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row and row[0] else 'ru'

async def has_chosen_language(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT language FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return bool(row and row[0])

async def has_accepted_terms(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT terms_accepted FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return bool(row and row[0])

async def accept_terms(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET terms_accepted=1 WHERE user_id=?", (user_id,))
        await db.commit()

async def set_user_lang(user_id: int, lang: str, username: str = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO users (user_id,username,language) VALUES (?,?,?) ON CONFLICT(user_id) DO UPDATE SET language=?",
                         (user_id, username or "unknown", lang, lang))
        await db.commit()

async def log_request(user_id: int, username: str, action: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO requests (user_id,username,action) VALUES (?,?,?)", (user_id, username or "unknown", action))
        await db.execute("INSERT INTO users (user_id,username,request_count) VALUES (?,?,1) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, request_count=request_count+1",
                         (user_id, username or "unknown"))
        await db.commit()

async def update_streak(user_id: int) -> tuple:
    today = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")
    yesterday = (datetime.now(MOSCOW_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT streak, last_active_date, COALESCE(bonus_requests,0) FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        if not row:
            return 0, False
        streak, last_date, bonus = row[0] or 0, row[1], row[2]
        if last_date == today:
            return streak, False
        streak = streak + 1 if last_date == yesterday else 1
        milestone = streak % 7 == 0
        new_bonus = bonus + 1 if milestone else bonus
        await db.execute("UPDATE users SET streak=?, last_active_date=?, bonus_requests=? WHERE user_id=?",
                         (streak, today, new_bonus, user_id))
        await db.commit()
    return streak, milestone

async def get_request_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT request_count FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0

async def get_bonus_requests(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(bonus_requests,0) FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0

async def get_streak(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(streak,0) FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0

async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE referred_by=?", (user_id,)) as c:
            return (await c.fetchone())[0]

async def get_profile(user_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT birth_date,full_name,zodiac,streak,COALESCE(bonus_requests,0),gender,city,timezone FROM users WHERE user_id=?",
            (user_id,)
        ) as c:
            row = await c.fetchone()
            if not row:
                return {}
            return {"birth_date": row[0], "full_name": row[1], "zodiac": row[2],
                    "streak": row[3] or 0, "bonus": row[4],
                    "gender": row[5], "city": row[6], "timezone": row[7]}

_SAFE_PROFILE_FIELDS = {"birth_date", "full_name", "zodiac", "gender", "city", "timezone"}

async def save_profile_field(user_id: int, field: str, value: str):
    if field not in _SAFE_PROFILE_FIELDS:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE users SET {field}=? WHERE user_id=?", (value, user_id))
        await db.commit()

async def clear_profile(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET birth_date=NULL,full_name=NULL,zodiac=NULL,gender=NULL,city=NULL,timezone=NULL WHERE user_id=?",
            (user_id,))
        await db.commit()

async def is_banned(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return bool(row and row[0])

async def set_ban(user_id: int, banned: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (1 if banned else 0, user_id))
        await db.commit()

async def has_subscription(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return bool(row) and datetime.fromisoformat(row[0]) > datetime.now()

async def grant_subscription(user_id: int, days: int = 30) -> datetime:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        base = datetime.fromisoformat(row[0]) if row and datetime.fromisoformat(row[0]) > datetime.now() else datetime.now()
        expires_at = base + timedelta(days=days)
        await db.execute("INSERT INTO subscriptions (user_id,expires_at) VALUES (?,?) ON CONFLICT(user_id) DO UPDATE SET expires_at=?",
                         (user_id, expires_at.isoformat(), expires_at.isoformat()))
        await db.commit()
    return expires_at

async def get_subscription_expiry(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (user_id,)) as c:
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
        async with db.execute("SELECT notifications FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return bool(row[0]) if row else False

async def toggle_notifications(user_id: int, username: str = None) -> bool:
    current = await get_notifications_status(user_id)
    new_val = 0 if current else 1
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO users (user_id,username,notifications) VALUES (?,?,?) ON CONFLICT(user_id) DO UPDATE SET notifications=?",
                         (user_id, username or "unknown", new_val, new_val))
        await db.commit()
    return bool(new_val)

async def get_notification_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE notifications=1 AND COALESCE(is_banned,0)=0") as c:
            return [row[0] for row in await c.fetchall()]

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE COALESCE(is_banned,0)=0") as c:
            return [row[0] for row in await c.fetchall()]

async def save_reading_history(user_id: int, action: str, header: str, result: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO readings_history (user_id,action,header,result) VALUES (?,?,?,?)",
                         (user_id, action, header[:120] if header else action, result))
        await db.execute("""DELETE FROM readings_history WHERE user_id=? AND id NOT IN (
            SELECT id FROM readings_history WHERE user_id=? ORDER BY created_at DESC LIMIT 20)""",
                         (user_id, user_id))
        await db.commit()

async def get_reading_history(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, action, header, created_at FROM readings_history WHERE user_id=? ORDER BY created_at DESC LIMIT 20",
                              (user_id,)) as c:
            return await c.fetchall()

async def get_inactive_users(days: int = 3):
    cutoff = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE notifications=1 AND COALESCE(is_banned,0)=0 AND last_active_date IS NOT NULL AND last_active_date < ?",
                              (cutoff,)) as c:
            return [row[0] for row in await c.fetchall()]

def get_moon_phase(dt=None) -> str | None:
    if dt is None:
        dt = datetime.now(timezone.utc)
    known_new = datetime(2000, 1, 6, 18, 14, tzinfo=timezone.utc)
    phase = (dt - known_new).total_seconds() / 86400 % 29.53059
    if phase < 1.5 or phase > 28.03:
        return 'new'
    if 13.5 < phase < 15.5:
        return 'full'
    return None

async def apply_promo(user_id: int, code: str) -> tuple:
    code = code.upper().strip()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT days,max_uses,used_count FROM promo_codes WHERE code=?", (code,)) as c:
            row = await c.fetchone()
        if not row:
            return 'invalid', 0
        days, max_uses, used_count = row
        if used_count >= max_uses:
            return 'exhausted', 0
        async with db.execute("SELECT 1 FROM promo_uses WHERE code=? AND user_id=?", (code, user_id)) as c:
            if await c.fetchone():
                return 'used', 0
        await db.execute("UPDATE promo_codes SET used_count=used_count+1 WHERE code=?", (code,))
        await db.execute("INSERT INTO promo_uses (code,user_id) VALUES (?,?)", (code, user_id))
        await db.commit()
    await grant_subscription(user_id, days)
    return 'ok', days

async def get_admin_stats():
    now = datetime.now(MOSCOW_TZ)
    today_str = now.strftime("%Y-%m-%d")
    week_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    month_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            total_users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM requests") as c:
            total_requests = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM subscriptions WHERE expires_at>?", (datetime.now().isoformat(),)) as c:
            active_subs = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE notifications=1") as c:
            notif_users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE COALESCE(is_banned,0)=1") as c:
            banned_users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE language='ru'") as c:
            ru_users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE language='en'") as c:
            en_users = (await c.fetchone())[0]
        async with db.execute("SELECT user_id,username,action,timestamp FROM requests ORDER BY timestamp DESC LIMIT 20") as c:
            recent = await c.fetchall()
        async with db.execute("SELECT COUNT(*) FROM users WHERE first_seen >= ?", (today_str,)) as c:
            new_today = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE first_seen >= ?", (week_str,)) as c:
            new_week = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE first_seen >= ?", (month_str,)) as c:
            new_month = (await c.fetchone())[0]
    return total_users, total_requests, active_subs, notif_users, banned_users, ru_users, en_users, recent, new_today, new_week, new_month

# ─── ADMIN EXTRA STATS ────────────────────────────────────────────────────────
async def get_admin_finance_stats():
    now = datetime.now(MOSCOW_TZ)
    today_str = now.strftime("%Y-%m-%d")
    week_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    month_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM payments") as c:
            total_p = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM payments WHERE created_at >= ?", (today_str,)) as c:
            today_p = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM payments WHERE created_at >= ?", (week_str,)) as c:
            week_p = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM payments WHERE created_at >= ?", (month_str,)) as c:
            month_p = (await c.fetchone())[0]
        async with db.execute(
            "SELECT method, COUNT(*) as cnt FROM payments GROUP BY method ORDER BY cnt DESC LIMIT 5") as c:
            methods = await c.fetchall()
        async with db.execute("SELECT COUNT(DISTINCT user_id) FROM payments") as c:
            paid_users = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            total_users = (await c.fetchone())[0]
    return total_p, today_p, week_p, month_p, methods, paid_users, total_users

async def get_top_users(limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, username, request_count FROM users ORDER BY request_count DESC LIMIT ?",
            (limit,)) as c:
            return await c.fetchall()

async def get_popular_actions(limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT action, COUNT(*) as cnt FROM requests GROUP BY action ORDER BY cnt DESC LIMIT ?",
            (limit,)) as c:
            return await c.fetchall()

async def generate_users_csv() -> bytes:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, username, first_seen, request_count, language, COALESCE(is_banned,0), COALESCE(streak,0) FROM users ORDER BY first_seen DESC"
        ) as c:
            rows = await c.fetchall()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["user_id", "username", "first_seen", "requests", "language", "is_banned", "streak"])
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")

# ─── USER ACCOUNT HELPERS ─────────────────────────────────────────────────────
_ACTION_LABELS = {
    "tarot_1_question": "🃏 Таро 1 карта", "tarot_3_question": "🎴 Таро 3 карты",
    "tarot_5_question": "🔮 Таро 5 карт", "tarot_cc_question": "✡️ Кельтский крест",
    "tarot_yn_question": "☯️ Да/Нет", "love_thinking": "💭 Думает ли он/она",
    "love_couple": "💑 Расклад на пару", "love_continue": "🤔 Стоит ли продолжать",
    "love_future": "🔮 Будущее отношений", "career_money": "💰 Деньги",
    "career_job": "💼 Карьера", "career_biz": "🚀 Бизнес",
    "rune_1": "🪨 1 руна", "rune_3": "🪨 3 руны",
    "dream_interp": "💭 Сон", "palmistry": "🖐 Хиромантия",
    "num_date": "🔢 Нумерология", "num_name": "✏️ Нумерология имени",
    "natal_chart": "🌠 Натальная карта", "compatibility": "💑 Совместимость",
    "free_question": "❓ Вопрос", "card_of_day": "🌟 Карта дня",
}

async def get_user_stats(user_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT action, COUNT(*) as cnt FROM requests WHERE user_id=? GROUP BY action ORDER BY cnt DESC LIMIT 1",
            (user_id,)) as c:
            fav_row = await c.fetchone()
        async with db.execute("SELECT COUNT(*) FROM requests WHERE user_id=?", (user_id,)) as c:
            total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT first_seen, COALESCE(streak,0), COALESCE(bonus_requests,0) FROM users WHERE user_id=?",
            (user_id,)) as c:
            row = await c.fetchone()
        async with db.execute("SELECT COUNT(*) FROM users WHERE referred_by=?", (user_id,)) as c:
            refs = (await c.fetchone())[0]
    since = row[0][:10] if row and row[0] else "?"
    streak = row[1] if row else 0
    bonus = row[2] if row else 0
    fav_key = fav_row[0] if fav_row else None
    fav = _ACTION_LABELS.get(fav_key, fav_key or "—")
    return {"total": total, "fav": fav, "since": since, "streak": streak, "refs": refs, "bonus": bonus}

async def get_user_payments(user_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT method, amount, currency, created_at FROM payments WHERE user_id=? ORDER BY created_at DESC LIMIT 10",
            (user_id,)) as c:
            return await c.fetchall()

async def delete_user_data(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscriptions WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM readings_history WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM requests WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM crypto_invoices WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM yookassa_invoices WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM payments WHERE user_id=?", (user_id,))
        await db.execute("DELETE FROM promo_uses WHERE user_id=?", (user_id,))
        await db.execute(
            "UPDATE users SET username='deleted', full_name=NULL, birth_date=NULL, zodiac=NULL, "
            "gender=NULL, city=NULL, timezone=NULL, referred_by=NULL, notifications=0 WHERE user_id=?",
            (user_id,))
        await db.commit()

async def get_notify_hour(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(notify_hour, 8) FROM users WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
    return row[0] if row else 8

async def set_notify_hour(user_id: int, hour: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET notify_hour=? WHERE user_id=?", (hour, user_id))
        await db.commit()

async def get_notification_users_for_hour(hour: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id FROM users WHERE notifications=1 AND COALESCE(is_banned,0)=0 AND COALESCE(notify_hour,8)=?",
            (hour,)) as c:
            rows = await c.fetchall()
    return [r[0] for r in rows]

# ─── CLAUDE ───────────────────────────────────────────────────────────────────
async def ask_claude(prompt: str, lang: str = 'ru') -> str:
    try:
        response = claude.messages.create(
            model="claude-sonnet-4-6", max_tokens=1024,
            system=SYSTEM_PROMPTS.get(lang, SYSTEM_PROMPTS['ru']),
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude error: {e}")
        return t(lang, 'error')

async def ask_claude_vision(image_bytes: bytes, lang: str = 'ru') -> str:
    try:
        image_b64 = base64.standard_b64encode(image_bytes).decode('utf-8')
        prompt_text = ("Проанализируй ладонь на этом фото. Дай подробное хиромантическое чтение." if lang == 'ru'
                       else "Analyze the palm in this photo. Give a detailed palmistry reading.")
        response = claude.messages.create(
            model="claude-sonnet-4-6", max_tokens=1500,
            system=PALM_SYSTEM_PROMPTS.get(lang, PALM_SYSTEM_PROMPTS['ru']),
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": image_b64}},
                {"type": "text", "text": prompt_text},
            ]}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude vision error: {e}")
        return t(lang, 'error')

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
    kb.button(text=t(lang,'btn_card_day'), callback_data="card_of_day")
    kb.button(text=t(lang,'btn_card_year'), callback_data="card_year")
    kb.button(text=t(lang,'btn_readings_menu'), callback_data="readings_menu")
    kb.button(text=t(lang,'btn_esoterics_menu'), callback_data="esoterics_menu")
    kb.button(text=t(lang,'btn_account_menu'), callback_data="account_menu")
    kb.button(text=t(lang,'btn_language'), callback_data="change_language")
    kb.adjust(2, 1, 1, 1, 1)
    return kb.as_markup()

def readings_submenu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_tarot'), callback_data="tarot_menu")
    kb.button(text=t(lang,'btn_love'), callback_data="love_menu")
    kb.button(text=t(lang,'btn_career'), callback_data="career_menu")
    kb.button(text=t(lang,'btn_runes'), callback_data="rune_menu")
    kb.button(text=t(lang,'btn_dream'), callback_data="dream_interp")
    kb.button(text=t(lang,'btn_palmistry'), callback_data="palmistry")
    kb.button(text=t(lang,'btn_back'), callback_data="back_main")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()

def esoterics_submenu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_numerology'), callback_data="numerology_menu")
    kb.button(text=t(lang,'btn_horoscope'), callback_data="horoscope")
    kb.button(text=t(lang,'btn_my_horo'), callback_data="my_horo")
    kb.button(text=t(lang,'btn_week'), callback_data="week_spread")
    kb.button(text=t(lang,'btn_moon'), callback_data="moon_calendar")
    kb.button(text=t(lang,'btn_lucky'), callback_data="lucky_number")
    kb.button(text=t(lang,'btn_ritual'), callback_data="ritual_day")
    kb.button(text=t(lang,'btn_question'), callback_data="free_question")
    kb.button(text=t(lang,'btn_back'), callback_data="back_main")
    kb.adjust(2, 2, 2, 2, 1)
    return kb.as_markup()

def account_submenu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_subscription'), callback_data="subscription")
    kb.button(text=t(lang,'btn_promo'), callback_data="promo_input")
    kb.button(text=t(lang,'btn_gift_sub'), callback_data="gift_sub")
    kb.button(text=t(lang,'btn_referral'), callback_data="referral")
    kb.button(text=t(lang,'btn_notifications'), callback_data="notifications")
    kb.button(text=t(lang,'btn_profile'), callback_data="profile")
    kb.button(text=t(lang,'btn_history'), callback_data="history_view")
    kb.button(text=t(lang,'btn_tarot_library'), callback_data="tarot_library")
    kb.button(text=t(lang,'btn_my_stats'), callback_data="my_stats")
    kb.button(text=t(lang,'btn_my_payments'), callback_data="my_payments")
    kb.button(text=t(lang,'btn_support'), callback_data="support")
    kb.button(text=t(lang,'terms_view_btn'), callback_data="terms_view")
    kb.button(text=t(lang,'btn_delete_account'), callback_data="delete_account")
    kb.button(text=t(lang,'btn_back'), callback_data="back_main")
    kb.adjust(2, 2, 2, 2, 2, 2, 1, 1)
    return kb.as_markup()

def back_button(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_main_menu'), callback_data="back_main")
    return kb.as_markup()

# maps action → parent menu to return to after a result
ACTION_BACK: dict[str, str] = {
    # Taро → меню таро
    "tarot_1": "tarot_menu", "tarot_3_question": "tarot_menu",
    "tarot_5_question": "tarot_menu", "tarot_cc_question": "tarot_menu",
    "tarot_yn_question": "tarot_menu",
    # Любовь → меню любви
    "love_thinking": "love_menu", "love_couple": "love_menu",
    "love_continue": "love_menu", "love_future": "love_menu",
    # Карьера → меню карьеры
    "career_money": "career_menu", "career_job": "career_menu", "career_biz": "career_menu",
    # Руны → меню рун
    "rune_1": "rune_menu", "rune_3": "rune_menu",
    # Прямо из "Гаданий"
    "dream_interp": "readings_menu", "palmistry": "readings_menu",
    # Нумерология стр. 1
    "num_date": "numerology_menu", "num_name": "numerology_menu",
    "natal_chart": "numerology_menu", "compatibility": "numerology_menu",
    # Нумерология стр. 2
    "num_fate": "num_page_2", "num_square": "num_page_2",
    "num_year": "num_page_2", "num_address": "num_page_2",
    "num_trio": "num_page_2", "num_biz": "num_page_2",
    # Прямо из "Эзотерики"
    "week_spread": "esoterics_menu", "moon_calendar": "esoterics_menu",
    "lucky_number": "esoterics_menu", "ritual_day": "esoterics_menu",
    "free_question": "esoterics_menu",
    # Аккаунт
    "tarot_library": "account_menu",
    # Главное меню
    "card_year": "back_main",
}

def result_keyboard(lang: str, back_to: str = "back_main"):
    kb = InlineKeyboardBuilder()
    if back_to == "back_main":
        kb.button(text=t(lang, 'btn_main_menu'), callback_data="back_main")
    else:
        kb.button(text=t(lang, 'btn_back'), callback_data=back_to)
    return kb.as_markup()

def cancel_keyboard(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_cancel'), callback_data="cancel_input")
    return kb.as_markup()

def tarot_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_tarot1'), callback_data="tarot_1")
    kb.button(text=t(lang,'btn_tarot3'), callback_data="tarot_3")
    kb.button(text=t(lang,'btn_tarot5'), callback_data="tarot_5")
    kb.button(text=t(lang,'btn_tarot_cc'), callback_data="tarot_cc")
    kb.button(text=t(lang,'btn_tarot_yn'), callback_data="tarot_yn")
    kb.button(text=t(lang,'btn_back'), callback_data="readings_menu")
    kb.adjust(1)
    return kb.as_markup()

def love_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_love_thinking'), callback_data="love_thinking")
    kb.button(text=t(lang,'btn_love_couple'), callback_data="love_couple")
    kb.button(text=t(lang,'btn_love_continue'), callback_data="love_continue")
    kb.button(text=t(lang,'btn_love_future'), callback_data="love_future")
    kb.button(text=t(lang,'btn_back'), callback_data="readings_menu")
    kb.adjust(1)
    return kb.as_markup()

def numerology_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_num_date'), callback_data="num_date")
    kb.button(text=t(lang,'btn_num_name'), callback_data="num_name")
    kb.button(text=t(lang,'btn_natal'), callback_data="natal_chart")
    kb.button(text=t(lang,'btn_compat'), callback_data="compatibility")
    kb.button(text=t(lang,'btn_page_next'), callback_data="num_page_2")
    kb.button(text=t(lang,'btn_back'), callback_data="esoterics_menu")
    kb.adjust(1)
    return kb.as_markup()

def numerology_menu_kb_p2(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_num_fate'), callback_data="num_fate")
    kb.button(text=t(lang,'btn_num_square'), callback_data="num_square")
    kb.button(text=t(lang,'btn_num_year'), callback_data="num_year")
    kb.button(text=t(lang,'btn_num_address'), callback_data="num_address")
    kb.button(text=t(lang,'btn_num_trio'), callback_data="num_trio")
    kb.button(text=t(lang,'btn_num_biz'), callback_data="num_biz")
    kb.button(text=t(lang,'btn_page_prev'), callback_data="numerology_menu")
    kb.button(text=t(lang,'btn_back'), callback_data="esoterics_menu")
    kb.adjust(1)
    return kb.as_markup()

def rune_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_rune1'), callback_data="rune_1")
    kb.button(text=t(lang,'btn_rune3'), callback_data="rune_3")
    kb.button(text=t(lang,'btn_back'), callback_data="readings_menu")
    kb.adjust(1)
    return kb.as_markup()

def horoscope_signs_kb(lang: str = 'ru', prefix: str = "zodiac_"):
    kb = InlineKeyboardBuilder()
    for name, code in ZODIAC_SIGNS.get(lang, ZODIAC_SIGNS['ru']):
        kb.button(text=name, callback_data=f"{prefix}{code}")
    kb.button(text=t(lang,'btn_back'), callback_data="back_main" if prefix=="zodiac_" else "profile")
    kb.adjust(2)
    return kb.as_markup()

def horoscope_period_kb(sign: str, lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_horo_day'), callback_data=f"horo_day_{sign}")
    kb.button(text=t(lang,'btn_horo_week'), callback_data=f"horo_week_{sign}")
    kb.button(text=t(lang,'btn_horo_month'), callback_data=f"horo_month_{sign}")
    kb.button(text=t(lang,'btn_back'), callback_data="esoterics_menu")
    kb.adjust(1)
    return kb.as_markup()

def profile_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_set_name'), callback_data="profile_set_name")
    kb.button(text=t(lang,'btn_set_birthdate'), callback_data="profile_set_birthdate")
    kb.button(text=t(lang,'btn_set_zodiac'), callback_data="profile_set_zodiac")
    kb.button(text=t(lang,'btn_set_gender'), callback_data="profile_set_gender")
    kb.button(text=t(lang,'btn_set_city'), callback_data="profile_set_city")
    kb.button(text=t(lang,'btn_set_timezone'), callback_data="profile_set_timezone")
    kb.button(text=t(lang,'btn_history'), callback_data="history_view")
    kb.button(text=t(lang,'btn_clear_profile'), callback_data="profile_clear")
    kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
    kb.adjust(2, 2, 2, 1, 1, 1)
    return kb.as_markup()

def career_menu_kb(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_career_money'), callback_data="career_money")
    kb.button(text=t(lang,'btn_career_job'), callback_data="career_job")
    kb.button(text=t(lang,'btn_career_biz'), callback_data="career_biz")
    kb.button(text=t(lang,'btn_back'), callback_data="readings_menu")
    kb.adjust(1)
    return kb.as_markup()

def paywall_keyboard(lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_buy_stars',stars=SUBSCRIPTION_STARS), callback_data="buy_stars")
    if YUKASSA_SHOP_ID:
        kb.button(text=t(lang,'btn_buy_sbp'), callback_data="buy_sbp")
    kb.button(text=t(lang,'btn_buy_rub'), callback_data="buy_rub")
    if STRIPE_TOKEN:
        kb.button(text=t(lang,'btn_buy_card'), callback_data="buy_card")
    if CRYPTOBOT_TOKEN:
        kb.button(text=t(lang,'btn_buy_crypto'), callback_data="buy_crypto")
    kb.button(text=t(lang,'btn_main_menu'), callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

def subscription_keyboard(has_sub: bool, lang: str = 'ru'):
    kb = InlineKeyboardBuilder()
    if not has_sub:
        kb.button(text=t(lang,'btn_buy_stars',stars=SUBSCRIPTION_STARS), callback_data="buy_stars")
        if YUKASSA_SHOP_ID:
            kb.button(text=t(lang,'btn_buy_sbp'), callback_data="buy_sbp")
        kb.button(text=t(lang,'btn_buy_rub'), callback_data="buy_rub")
        if STRIPE_TOKEN:
            kb.button(text=t(lang,'btn_buy_card'), callback_data="buy_card")
        if CRYPTOBOT_TOKEN:
            kb.button(text=t(lang,'btn_buy_crypto'), callback_data="buy_crypto")
    else:
        kb.button(text=t(lang,'btn_refund'), callback_data="refund_request")
    kb.button(text=t(lang,'btn_back'), callback_data="back_main")
    kb.adjust(1)
    return kb.as_markup()

# ─── HELPERS ──────────────────────────────────────────────────────────────────
async def safe_edit(callback: CallbackQuery, text: str, markup=None, parse_mode: str = "Markdown"):
    """Edit message text regardless of whether the message is a photo or text."""
    try:
        if callback.message.photo:
            await callback.message.delete()
            await bot.send_message(callback.message.chat.id, text,
                                   parse_mode=parse_mode, reply_markup=markup)
        else:
            await callback.message.edit_text(text, parse_mode=parse_mode, reply_markup=markup)
    except Exception:
        await bot.send_message(callback.message.chat.id, text,
                               parse_mode=parse_mode, reply_markup=markup)

async def _set_input_state(callback: CallbackQuery, action: str, prompt_key: str, lang: str):
    user_states[callback.from_user.id] = {
        "action": action, "prompt_msg_id": callback.message.message_id,
        "chat_id": callback.message.chat.id,
    }
    await safe_edit(callback, t(lang, prompt_key), cancel_keyboard(lang))
    await callback.answer()

async def _edit_or_send(chat_id: int, msg_id, text: str, markup):
    if msg_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="Markdown", reply_markup=markup)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)

async def _do_request(uid: int, username: str, action: str, chat_id: int, prompt_msg_id,
                      prompt: str, lang: str, result_header: str):
    if prompt_msg_id:
        try:
            await bot.edit_message_text(t(lang,'processing'), chat_id=chat_id, message_id=prompt_msg_id, parse_mode="Markdown")
        except Exception:
            prompt_msg_id = None
    if not prompt_msg_id:
        sent = await bot.send_message(uid, t(lang,'processing'), parse_mode="Markdown")
        prompt_msg_id = sent.message_id
        chat_id = uid
    await log_request(uid, username, action)
    streak, milestone = await update_streak(uid)
    answer = await ask_claude(prompt, lang)
    result = f"{result_header}\n\n{answer}" if result_header else answer
    back_to = ACTION_BACK.get(action, "back_main")
    await _edit_or_send(chat_id, prompt_msg_id, result, result_keyboard(lang, back_to))
    await save_reading_history(uid, action, result_header, result)
    if milestone:
        await bot.send_message(uid, t(lang,'streak_bonus', days=streak), parse_mode="Markdown")

# ─── DAILY BROADCAST ──────────────────────────────────────────────────────────
async def send_daily_broadcast(current_hour: int = 8):
    today = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    today_seed = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")
    # канал — только в 8:00
    if current_hour == 8 and CHANNEL_ID:
        channel_card = random.choice(TAROT_CARDS)
        try:
            ch_answer = await ask_claude(f"Сегодня {today}. Карта дня: {channel_card}. Дай краткую интерпретацию 80-100 слов.", 'ru')
            await bot.send_message(CHANNEL_ID, f"🌅 *Карта дня — {today}*\n\n*{channel_card}*\n\n{ch_answer}", parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Channel broadcast error: {e}")
    user_ids = await get_notification_users_for_hour(current_hour)
    logger.info(f"Broadcast hour={current_hour}: {len(user_ids)} users")
    for uid in user_ids:
        try:
            lang = await get_user_lang(uid)
            has_sub = await has_subscription(uid)
            words = "150-200" if has_sub else "40-50"
            card = random.Random(hash(f"{uid}:{today_seed}")).choice(TAROT_CARDS)
            answer = await ask_claude(f"Сегодня {today}. Карта дня: {card}. Дай интерпретацию {words} слов.", lang)
            text = f"{t(lang,'broadcast_morning',date=today)}\n\n*{card}*\n\n{answer}"
            if not has_sub:
                text += f"\n\n{t(lang,'broadcast_sub_hint')}"
            await bot.send_message(uid, text, parse_mode="Markdown", reply_markup=back_button(lang))
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Broadcast error {uid}: {e}")

async def daily_broadcast_loop():
    while True:
        now = datetime.now(MOSCOW_TZ)
        # ждём начала следующего часа
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        await asyncio.sleep((next_hour - now).total_seconds())
        await send_daily_broadcast(current_hour=next_hour.hour)

async def moon_notification_loop():
    while True:
        now = datetime.now(MOSCOW_TZ)
        target = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        phase = get_moon_phase()
        if phase:
            user_ids = await get_notification_users()
            for uid in user_ids:
                try:
                    lang = await get_user_lang(uid)
                    key = 'moon_new_msg' if phase == 'new' else 'moon_full_msg'
                    await bot.send_message(uid, t(lang, key), parse_mode="Markdown", reply_markup=back_button(lang))
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.error(f"Moon notif error {uid}: {e}")

async def inactive_reminder_loop():
    while True:
        now = datetime.now(MOSCOW_TZ)
        target = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        await asyncio.sleep((target - now).total_seconds())
        user_ids = await get_inactive_users(days=3)
        for uid in user_ids:
            try:
                lang = await get_user_lang(uid)
                await bot.send_message(uid, t(lang,'inactive_reminder'), parse_mode="Markdown", reply_markup=back_button(lang))
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Inactive reminder error {uid}: {e}")

async def cryptobot_create_invoice(user_id: int) -> dict | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{CRYPTOBOT_API}/createInvoice",
                headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                params={"asset": "USDT", "amount": SUBSCRIPTION_USDT,
                        "description": "Mystra — подписка 30 дней",
                        "payload": str(user_id), "expires_in": 3600},
            ) as r:
                data = await r.json()
        return data.get("result") if data.get("ok") else None
    except Exception as e:
        logger.error(f"CryptoBot create invoice error: {e}")
        return None

async def create_yukassa_sbp_payment(uid: int) -> dict | None:
    if not YUKASSA_SHOP_ID or not YUKASSA_SECRET_KEY:
        return None
    return_url = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else "https://t.me/"
    auth = aiohttp.BasicAuth(YUKASSA_SHOP_ID, YUKASSA_SECRET_KEY)
    headers = {"Idempotency-Key": str(uuid.uuid4()), "Content-Type": "application/json"}
    payload = {
        "amount": {"value": "250.00", "currency": "RUB"},
        "payment_method_data": {"type": "sbp"},
        "confirmation": {"type": "redirect", "return_url": return_url},
        "capture": True,
        "description": "Подписка Мистра на 30 дней",
        "metadata": {"user_id": str(uid)}
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.yookassa.ru/v3/payments",
                                    json=payload, auth=auth, headers=headers) as resp:
                data = await resp.json()
                if "id" not in data:
                    logger.error(f"YuKassa SBP error response: {data}")
                return data
    except Exception as e:
        logger.error(f"YuKassa SBP create error: {e}")
        return None

async def check_yukassa_payments():
    while True:
        await asyncio.sleep(30)
        if not YUKASSA_SHOP_ID:
            continue
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("SELECT payment_id, user_id FROM yookassa_invoices")
                pending = await cur.fetchall()
                await db.execute("DELETE FROM yookassa_invoices WHERE created_at < datetime('now', '-2 hours')")
                await db.commit()
            if not pending:
                continue
            auth = aiohttp.BasicAuth(YUKASSA_SHOP_ID, YUKASSA_SECRET_KEY)
            async with aiohttp.ClientSession() as session:
                for payment_id, user_id in pending:
                    try:
                        async with session.get(
                            f"https://api.yookassa.ru/v3/payments/{payment_id}", auth=auth
                        ) as resp:
                            data = await resp.json()
                        if data.get("status") == "succeeded":
                            async with aiosqlite.connect(DB_PATH) as db:
                                await db.execute(
                                    "INSERT OR IGNORE INTO payments (payment_id, user_id, method, amount, currency) VALUES (?,?,?,?,?)",
                                    (payment_id, user_id, "sbp", "250.00", "RUB"))
                                await db.commit()
                            lang = await get_user_lang(user_id)
                            expiry = await grant_subscription(user_id, 30)
                            await bot.send_message(user_id,
                                t(lang, 'sub_activated', date=expiry.strftime('%d.%m.%Y')),
                                parse_mode="Markdown", reply_markup=main_menu(lang))
                            async with aiosqlite.connect(DB_PATH) as db:
                                await db.execute("DELETE FROM yookassa_invoices WHERE payment_id=?", (payment_id,))
                                await db.commit()
                            if ADMIN_ID:
                                await bot.send_message(ADMIN_ID,
                                    f"💰 *Новая оплата!*\n"
                                    f"👤 id:{user_id}\n"
                                    f"💳 Способ: 📱 СБП (YuKassa API)\n"
                                    f"💵 Сумма: 250 RUB\n"
                                    f"📅 Подписка до: {expiry.strftime('%d.%m.%Y')}",
                                    parse_mode="Markdown")
                    except Exception as e:
                        logger.error(f"YuKassa check payment {payment_id} error: {e}")
        except Exception as e:
            logger.error(f"check_yukassa_payments error: {e}")

async def check_crypto_payments():
    while True:
        await asyncio.sleep(30)
        if not CRYPTOBOT_TOKEN:
            continue
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("SELECT invoice_id, user_id FROM crypto_invoices")
                pending = await cur.fetchall()
                # clean up invoices older than 2 hours
                await db.execute("DELETE FROM crypto_invoices WHERE created_at < datetime('now', '-2 hours')")
                await db.commit()
            if not pending:
                continue
            ids = ",".join(str(r[0]) for r in pending)
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{CRYPTOBOT_API}/getInvoices",
                    headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                    params={"invoice_ids": ids, "status": "paid"},
                ) as r:
                    data = await r.json()
            if not data.get("ok"):
                continue
            paid_ids = {inv["invoice_id"] for inv in data["result"].get("items", [])}
            pending_map = {r[0]: r[1] for r in pending}
            for invoice_id, user_id in pending_map.items():
                if invoice_id in paid_ids:
                    expiry = await grant_subscription(user_id, 30)
                    lang = await get_user_lang(user_id)
                    await bot.send_message(
                        user_id, t(lang, 'sub_activated', date=expiry.strftime('%d.%m.%Y')),
                        parse_mode="Markdown", reply_markup=main_menu(lang))
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("DELETE FROM crypto_invoices WHERE invoice_id=?", (invoice_id,))
                        await db.commit()
        except Exception as e:
            logger.error(f"check_crypto_payments error: {e}")

async def send_welcome_msg(message: Message, lang: str, markup):
    """Send photo welcome if WELCOME_PHOTO is set, otherwise plain text."""
    text = t(lang, 'welcome')
    if WELCOME_PHOTO:
        try:
            await message.answer_photo(photo=WELCOME_PHOTO, caption=text[:1024],
                                       parse_mode="Markdown", reply_markup=markup)
            return
        except Exception:
            pass
    await message.answer(text, parse_mode="Markdown", reply_markup=markup)

async def send_lang_select(message: Message):
    """Send language selection, with photo if configured."""
    text = t('ru', 'choose_lang')
    if WELCOME_PHOTO:
        try:
            await message.answer_photo(photo=WELCOME_PHOTO, caption=text,
                                       parse_mode="Markdown", reply_markup=language_keyboard())
            return
        except Exception:
            pass
    await message.answer(text, reply_markup=language_keyboard())

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────
@dp.message(CommandStart())
async def cmd_start(message: Message):
    uid = message.from_user.id
    username = message.from_user.username or "unknown"
    args = message.text.split(maxsplit=1)
    referrer_id = None
    terms_from_site = False
    if len(args) > 1 and args[1].startswith("ta"):
        try:
            if int(args[1][2:]) == uid:
                terms_from_site = True
        except ValueError:
            pass
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            r = int(args[1][4:])
            if r != uid:
                referrer_id = r
        except ValueError:
            pass
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id,referred_by FROM users WHERE user_id=?", (uid,)) as c:
            existing = await c.fetchone()
        is_new = existing is None
        already_referred = existing and existing[1] is not None
        if is_new:
            await db.execute("INSERT INTO users (user_id,username,notifications,referred_by) VALUES (?,?,1,?)", (uid, username, referrer_id))
        else:
            await db.execute("UPDATE users SET username=? WHERE user_id=?", (username, uid))
        if is_new and referrer_id and not already_referred:
            await db.execute("UPDATE users SET bonus_requests=COALESCE(bonus_requests,0)+1 WHERE user_id=?", (referrer_id,))
            await db.commit()
            try:
                rl = await get_user_lang(referrer_id)
                name = message.from_user.first_name or username
                await bot.send_message(referrer_id, t(rl,'referral_bonus_msg',name=name), parse_mode="Markdown")
            except Exception:
                pass
        else:
            await db.commit()
    if terms_from_site:
        await accept_terms(uid)
    if not await has_accepted_terms(uid):
        kb = InlineKeyboardBuilder()
        if SITE_URL:
            kb.button(text="📜 Читать и принять на сайте",
                      url=f"{SITE_URL}/terms?tg={uid}")
        kb.button(text="✅ Принимаю (без перехода)", callback_data="terms_accept")
        kb.adjust(1)
        terms_text = (
            "📜 *Прежде чем начать*\n\n"
            "Для использования бота *Мистра* необходимо принять пользовательское соглашение.\n\n"
            "• Расклады носят *развлекательный характер*\n"
            "• Вам исполнилось *18 лет*\n"
            "• Вы согласны с политикой хранения данных\n\n"
            "👆 Нажмите кнопку выше чтобы прочитать и принять соглашение на сайте.\n"
            "После принятия вы автоматически вернётесь в бот."
        )
        if WELCOME_PHOTO:
            try:
                await message.answer_photo(photo=WELCOME_PHOTO, caption=terms_text,
                                           parse_mode="Markdown", reply_markup=kb.as_markup())
                return
            except Exception:
                pass
        await message.answer(terms_text, parse_mode="Markdown", reply_markup=kb.as_markup())
        return
    if not await has_chosen_language(uid):
        await send_lang_select(message)
        return
    lang = await get_user_lang(uid)
    await send_welcome_msg(message, lang, main_menu(lang))

@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    lang = await get_user_lang(message.from_user.id)
    await send_welcome_msg(message, lang, main_menu(lang))

@dp.message(Command("terms"))
async def cmd_terms(message: Message):
    lang = await get_user_lang(message.from_user.id)
    kb = InlineKeyboardBuilder()
    if SITE_URL:
        kb.button(text=t(lang,'terms_read_btn'), url=f"{SITE_URL}/terms")
    kb.button(text=t(lang,'btn_main_menu'), callback_data="back_main")
    kb.adjust(1)
    text = ("📜 *Пользовательское соглашение*\n\nПолное соглашение размещено на нашем сайте."
            if lang == 'ru' else
            "📜 *Terms of Service*\n\nThe full terms are available on our website.")
    await message.answer(text, parse_mode="Markdown", reply_markup=kb.as_markup())

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"`{message.from_user.id}`", parse_mode="Markdown")

@dp.message(Command("setphoto"))
async def cmd_setphoto(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    if not message.photo:
        await message.answer(
            "📸 Отправь команду `/setphoto` *вместе с фото* (прикрепи изображение и в подписи напиши `/setphoto`).\n\n"
            "После получения `file_id` вставь его в `.env`:\n`WELCOME_PHOTO=полученный_id`",
            parse_mode="Markdown")
        return
    file_id = message.photo[-1].file_id
    await message.answer(
        f"✅ *file\\_id фото получен:*\n\n`{file_id}`\n\n"
        f"Вставь в `.env`:\n`WELCOME_PHOTO={file_id}`\n\nЗатем перезапусти бота.",
        parse_mode="Markdown")

@dp.message(Command("admin", "a"))
async def cmd_admin(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await show_admin_menu(message)

async def show_admin_menu(message_or_callback):
    total, reqs, subs, notif, banned, ru, en, _, new_today, new_week, new_month = await get_admin_stats()
    text = (f"👑 *Панель администратора*\n\n"
            f"👥 Всего: *{total}* (🇷🇺 {ru} / 🇬🇧 {en})\n"
            f"📅 Новых: сегодня *{new_today}* | неделя *{new_week}* | месяц *{new_month}*\n"
            f"💎 Подписок: *{subs}* | 🔔 Рассылка: *{notif}*\n"
            f"⛔ Заблокировано: *{banned}* | 📊 Запросов: *{reqs}*")
    kb = InlineKeyboardBuilder()
    kb.button(text="👥 Пользователи", callback_data="adm_users_0")
    kb.button(text="💎 Подписчики", callback_data="adm_subs_0")
    kb.button(text="📊 Активность", callback_data="adm_activity")
    kb.button(text="🔍 Найти юзера", callback_data="adm_search")
    kb.button(text="🎟 Промокоды", callback_data="adm_promos")
    kb.button(text="📢 Рассылка", callback_data="adm_broadcast_menu")
    kb.button(text="💰 Финансы", callback_data="adm_finance")
    kb.button(text="🏆 Топ юзеров", callback_data="adm_top")
    kb.button(text="📈 Популярность", callback_data="adm_popular")
    kb.button(text="📥 Экспорт CSV", callback_data="adm_export")
    kb.adjust(2, 2, 2, 2, 2)
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer(text, parse_mode="Markdown", reply_markup=kb.as_markup())
    else:
        await safe_edit(message_or_callback, text, markup=kb.as_markup())

@dp.callback_query(F.data.startswith("adm_users_"))
async def adm_users_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    page = int(callback.data.split("_")[2])
    limit = 8
    offset = page * limit
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            total = (await c.fetchone())[0]
        async with db.execute(
            "SELECT user_id, username, first_seen, is_banned FROM users ORDER BY first_seen DESC LIMIT ? OFFSET ?",
            (limit, offset)) as c:
            users = await c.fetchall()
    total_pages = max(1, (total + limit - 1) // limit)
    kb = InlineKeyboardBuilder()
    for uid, uname, first_seen, is_banned in users:
        label_name = f"@{uname}" if uname and uname != "unknown" else f"id:{uid}"
        ban_icon = "⛔" if is_banned else "👤"
        date_str = first_seen[:10] if first_seen else "?"
        kb.button(text=f"{ban_icon} {label_name} [{date_str}]", callback_data=f"adm_u_{uid}")
    kb.adjust(1)
    nav = []
    if page > 0:
        nav.append(("◀️", f"adm_users_{page-1}"))
    nav.append((f"{page+1}/{total_pages}", "adm_noop"))
    if (page + 1) < total_pages:
        nav.append(("▶️", f"adm_users_{page+1}"))
    for label, cd in nav:
        kb.button(text=label, callback_data=cd)
    kb.button(text="🏠 Меню", callback_data="adm_main")
    kb.adjust(*([1]*len(users)), len(nav), 1)
    await safe_edit(callback, f"👥 *Пользователи* (всего {total})\nСтраница {page+1}/{total_pages}:", markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_u_"))
async def adm_user_detail_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    target_uid = int(callback.data.split("_")[2])
    await _refresh_user_card(callback, target_uid)
    await callback.answer()

async def _refresh_user_card(callback: CallbackQuery, target_uid: int):
    """Re-render user detail card after an action."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT username, first_seen, request_count, bonus_requests, language, is_banned, streak FROM users WHERE user_id=?",
            (target_uid,)) as c:
            row = await c.fetchone()
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (target_uid,)) as c:
            sub = await c.fetchone()
    if not row:
        return
    uname, first_seen, req_count, bonus, lang, is_banned, streak = row
    sub_info = f"до {sub[0][:10]}" if sub else "нет"
    name_str = f"@{uname}" if uname and uname != "unknown" else f"id:{target_uid}"
    text = (f"👤 *{name_str}*\n"
            f"🆔 `{target_uid}`\n"
            f"📅 Регистрация: {first_seen[:10] if first_seen else '?'}\n"
            f"🌐 Язык: {lang or '?'} | 🔥 Серия: {streak or 0} дн.\n"
            f"📊 Запросов: {req_count or 0} (+{bonus or 0} бонус)\n"
            f"💎 Подписка: {sub_info}\n"
            f"{'⛔ ЗАБЛОКИРОВАН' if is_banned else '✅ Активен'}")
    kb = InlineKeyboardBuilder()
    if is_banned:
        kb.button(text="✅ Разбанить", callback_data=f"adm_unban_{target_uid}")
    else:
        kb.button(text="⛔ Забанить", callback_data=f"adm_ban_{target_uid}")
    if sub:
        kb.button(text="❌ Отозвать подписку", callback_data=f"adm_revoke_{target_uid}")
    kb.button(text="💎 +30 дней", callback_data=f"adm_add30_{target_uid}")
    kb.button(text="🔄 Сбросить лимит", callback_data=f"adm_rlimit_{target_uid}")
    kb.button(text="✉️ Написать юзеру", callback_data=f"adm_msg_{target_uid}")
    kb.button(text="◀️ К списку", callback_data="adm_users_0")
    kb.adjust(1)
    await safe_edit(callback, text, markup=kb.as_markup())

@dp.callback_query(F.data.startswith("adm_ban_"))
async def adm_ban_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    uid = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
        await db.commit()
    await callback.answer("⛔ Пользователь заблокирован", show_alert=True)
    await _refresh_user_card(callback, uid)

@dp.callback_query(F.data.startswith("adm_unban_"))
async def adm_unban_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    uid = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (uid,))
        await db.commit()
    await callback.answer("✅ Пользователь разбанен", show_alert=True)
    await _refresh_user_card(callback, uid)

@dp.callback_query(F.data.startswith("adm_revoke_"))
async def adm_revoke_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    uid = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscriptions WHERE user_id=?", (uid,))
        await db.commit()
    await callback.answer("❌ Подписка отозвана", show_alert=True)
    await _refresh_user_card(callback, uid)

@dp.callback_query(F.data.startswith("adm_add30_"))
async def adm_add30_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    uid = int(callback.data.split("_")[2])
    expiry = await grant_subscription(uid, 30)
    await callback.answer(f"💎 +30 дней до {expiry.strftime('%d.%m.%Y')}", show_alert=True)
    await _refresh_user_card(callback, uid)

@dp.callback_query(F.data.startswith("adm_rlimit_"))
async def adm_rlimit_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    uid = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET request_count=0 WHERE user_id=?", (uid,))
        await db.commit()
    await callback.answer("🔄 Лимит сброшен", show_alert=True)
    await _refresh_user_card(callback, uid)

@dp.callback_query(F.data == "adm_main")
async def adm_main_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await show_admin_menu(callback)
    await callback.answer("")

@dp.callback_query(F.data == "adm_noop")
async def adm_noop_cb(callback: CallbackQuery):
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_subs_"))
async def adm_subs_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    page = int(callback.data.split("_")[2])
    limit = 8
    offset = page * limit
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM subscriptions WHERE expires_at > datetime('now')") as c:
            total = (await c.fetchone())[0]
        async with db.execute(
            """SELECT s.user_id, u.username, s.expires_at FROM subscriptions s
               LEFT JOIN users u ON s.user_id=u.user_id
               WHERE s.expires_at > datetime('now')
               ORDER BY s.expires_at DESC LIMIT ? OFFSET ?""", (limit, offset)) as c:
            subs = await c.fetchall()
    total_pages = max(1, (total + limit - 1) // limit)
    kb = InlineKeyboardBuilder()
    for uid, uname, expires_at in subs:
        label_name = f"@{uname}" if uname and uname != "unknown" else f"id:{uid}"
        exp_str = expires_at[:10] if expires_at else "?"
        kb.button(text=f"💎 {label_name} — до {exp_str}", callback_data=f"adm_u_{uid}")
    kb.adjust(1)
    nav = []
    if page > 0: nav.append(("◀️", f"adm_subs_{page-1}"))
    nav.append((f"{page+1}/{total_pages}", "adm_noop"))
    if (page + 1) < total_pages: nav.append(("▶️", f"adm_subs_{page+1}"))
    for label, cd in nav:
        kb.button(text=label, callback_data=cd)
    kb.button(text="🏠 Меню", callback_data="adm_main")
    kb.adjust(*([1]*len(subs)), len(nav), 1)
    await safe_edit(callback, f"💎 *Активные подписчики* ({total})\nСтраница {page+1}/{total_pages}:", markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "adm_activity")
async def adm_activity_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, username, action, timestamp FROM requests ORDER BY timestamp DESC LIMIT 15") as c:
            recent = await c.fetchall()
    text = "📊 *Последние запросы:*\n\n"
    for uid, uname, action, ts in recent:
        label = f"@{uname}" if uname and uname != "unknown" else f"id:{uid}"
        text += f"• {label} — `{action}` [{ts[:16]}]\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="adm_activity")
    kb.button(text="🏠 Меню", callback_data="adm_main")
    kb.adjust(2)
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "adm_broadcast_menu")
async def adm_broadcast_menu_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    text = ("📢 *Рассылка*\n\n"
            "Используйте команду:\n"
            "`/broadcast <текст>`\n\n"
            "Поддерживается Markdown-разметка.")
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Меню", callback_data="adm_main")
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "adm_search")
async def adm_search_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    user_states[callback.from_user.id] = {"action": "adm_search"}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data="adm_main")
    await safe_edit(callback, "🔍 *Поиск пользователя*\n\nВведите Telegram ID (числом) или @username:", markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_msg_"))
async def adm_msg_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    target_uid = int(callback.data.split("_")[2])
    user_states[callback.from_user.id] = {"action": "adm_msg", "target_uid": target_uid}
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отмена", callback_data=f"adm_u_{target_uid}")
    await safe_edit(callback, f"✉️ *Сообщение пользователю* `{target_uid}`\n\nВведите текст сообщения:", markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "adm_promos")
async def adm_promos_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await _show_promos(callback)
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_delpromo_"))
async def adm_delpromo_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    code = callback.data[len("adm_delpromo_"):]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM promo_codes WHERE code=?", (code,))
        await db.commit()
    await callback.answer(f"🗑 Промокод {code} удалён", show_alert=True)
    await _show_promos(callback)

async def _show_promos(callback: CallbackQuery):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT code, days, used_count, max_uses FROM promo_codes ORDER BY created_at DESC LIMIT 20") as c:
            promos = await c.fetchall()
    if not promos:
        text = "🎟 *Промокоды*\n\nПромокодов нет.\n\nСоздать: `/promo create КОД 30`"
    else:
        text = "🎟 *Промокоды*\n\n"
        for code, days, used, max_uses in promos:
            text += f"• `{code}` — {days} дн. ({used}/{max_uses} исп.)\n"
    kb = InlineKeyboardBuilder()
    for code, days, used, max_uses in (promos or []):
        kb.button(text=f"❌ {code}", callback_data=f"adm_delpromo_{code}")
    kb.button(text="🏠 Меню", callback_data="adm_main")
    kb.adjust(*([1] * len(promos)), 1) if promos else kb.adjust(1)
    await safe_edit(callback, text, markup=kb.as_markup())

@dp.callback_query(F.data == "adm_finance")
async def adm_finance_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    total_p, today_p, week_p, month_p, methods, paid_users, total_users = await get_admin_finance_stats()
    conv = round(paid_users / total_users * 100, 1) if total_users else 0
    text = (f"💰 *Финансовая статистика*\n\n"
            f"📊 Всего платежей: *{total_p}*\n"
            f"📅 Сегодня: *{today_p}* | Неделя: *{week_p}* | Месяц: *{month_p}*\n\n"
            f"👥 Платящих юзеров: *{paid_users}* из *{total_users}*\n"
            f"📈 Конверсия: *{conv}%*\n\n"
            f"💳 *По методам:*\n")
    for method, cnt in methods:
        text += f"• `{method}` — {cnt}\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="adm_finance")
    kb.button(text="🏠 Меню", callback_data="adm_main")
    kb.adjust(2)
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "adm_top")
async def adm_top_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    rows = await get_top_users(10)
    text = "🏆 *Топ-10 активных пользователей*\n\n"
    for i, (uid, uname, cnt) in enumerate(rows, 1):
        label = f"@{uname}" if uname and uname != "unknown" else f"id:{uid}"
        text += f"{i}. {label} — *{cnt}* запросов\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="adm_top")
    kb.button(text="🏠 Меню", callback_data="adm_main")
    kb.adjust(2)
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "adm_popular")
async def adm_popular_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    rows = await get_popular_actions(10)
    text = "📈 *Популярность функций*\n\n"
    for action, cnt in rows:
        label = _ACTION_LABELS.get(action, action)
        text += f"• {label} — *{cnt}*\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔄 Обновить", callback_data="adm_popular")
    kb.button(text="🏠 Меню", callback_data="adm_main")
    kb.adjust(2)
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "adm_export")
async def adm_export_cb(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID: return
    await callback.answer("⏳ Генерирую файл...", show_alert=False)
    csv_bytes = await generate_users_csv()
    filename = f"users_{datetime.now(MOSCOW_TZ).strftime('%Y%m%d_%H%M')}.csv"
    await bot.send_document(
        callback.from_user.id,
        BufferedInputFile(csv_bytes, filename=filename),
        caption=f"📥 Экспорт пользователей — {filename}")

@dp.message(Command("grant"))
async def cmd_grant(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: `/grant <user_id> [дней]`\nПо умолчанию: 30 дней", parse_mode="Markdown")
        return
    days = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 30
    expiry = await grant_subscription(int(parts[1]), days)
    await message.answer(f"✅ Подписка выдана `{parts[1]}` на {days} дней (до {expiry.strftime('%d.%m.%Y')})", parse_mode="Markdown")

@dp.message(Command("revoke"))
async def cmd_revoke(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: `/revoke <user_id>`", parse_mode="Markdown")
        return
    uid = int(parts[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM subscriptions WHERE user_id=?", (uid,))
        await db.commit()
    await message.answer(f"🗑 Подписка пользователя `{uid}` удалена.", parse_mode="Markdown")

@dp.message(Command("adddays"))
async def cmd_adddays(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Использование: `/adddays <user_id> <дней>`", parse_mode="Markdown")
        return
    uid, days = int(parts[1]), int(parts[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (uid,)) as c:
            row = await c.fetchone()
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    if row:
        base = datetime.fromisoformat(row[0])
        if base < now_utc:
            base = now_utc
    else:
        base = now_utc
    expiry = base + timedelta(days=days)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO subscriptions (user_id,expires_at) VALUES (?,?)",
                         (uid, expiry.isoformat()))
        await db.commit()
    await message.answer(f"➕ Добавлено *{days} дней* пользователю `{uid}`.\nПодписка до: *{expiry.strftime('%d.%m.%Y')}*", parse_mode="Markdown")

@dp.message(Command("resetlimit"))
async def cmd_resetlimit(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: `/resetlimit <user_id>`", parse_mode="Markdown")
        return
    uid = int(parts[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET request_count=0 WHERE user_id=?", (uid,))
        await db.commit()
    await message.answer(f"♻️ Лимит запросов пользователя `{uid}` сброшен до нуля.", parse_mode="Markdown")

@dp.message(Command("setbonus"))
async def cmd_setbonus(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Использование: `/setbonus <user_id> <кол-во>`", parse_mode="Markdown")
        return
    uid, bonus = int(parts[1]), int(parts[2])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET bonus_requests=? WHERE user_id=?", (bonus, uid))
        await db.commit()
    await message.answer(f"🎁 Бонусных запросов установлено: *{bonus}* для `{uid}`.", parse_mode="Markdown")

@dp.message(Command("subs"))
async def cmd_subs(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT s.user_id, u.username, s.expires_at FROM subscriptions s "
            "LEFT JOIN users u ON u.user_id=s.user_id "
            "WHERE s.expires_at > datetime('now') ORDER BY s.expires_at DESC"
        ) as c:
            rows = await c.fetchall()
    if not rows:
        await message.answer("💎 Активных подписчиков нет.")
        return
    text = f"💎 *Активные подписки ({len(rows)}):*\n\n"
    for uid, uname, exp in rows:
        label = f"@{uname}" if uname and uname != "unknown" else f"`{uid}`"
        text += f"• {label} — до {exp[:10]}\n"
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("find"))
async def cmd_find(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: `/find @username` или `/find <user_id>`", parse_mode="Markdown")
        return
    query = parts[1].lstrip("@")
    async with aiosqlite.connect(DB_PATH) as db:
        if query.isdigit():
            async with db.execute("SELECT user_id,username,request_count,language,first_seen FROM users WHERE user_id=?", (int(query),)) as c:
                rows = await c.fetchall()
        else:
            async with db.execute("SELECT user_id,username,request_count,language,first_seen FROM users WHERE username LIKE ?", (f"%{query}%",)) as c:
                rows = await c.fetchall()
    if not rows:
        await message.answer("❌ Пользователь не найден.")
        return
    text = f"🔍 *Результаты поиска:*\n\n"
    for uid, uname, count, lang, since in rows:
        sub = await get_subscription_expiry(uid)
        sub_str = f"до {sub.strftime('%d.%m.%Y')}" if sub else "нет"
        text += f"• `{uid}` @{uname} | {lang} | {count} запр. | подписка: {sub_str} | с {since[:10]}\n"
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: `/ban <user_id>`", parse_mode="Markdown")
        return
    uid = int(parts[1])
    await set_ban(uid, True)
    await message.answer(f"⛔ Пользователь `{uid}` заблокирован.", parse_mode="Markdown")

@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: `/unban <user_id>`", parse_mode="Markdown")
        return
    uid = int(parts[1])
    await set_ban(uid, False)
    await message.answer(f"✅ Пользователь `{uid}` разблокирован.", parse_mode="Markdown")

@dp.message(Command("userinfo"))
async def cmd_userinfo(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: `/userinfo <user_id>`", parse_mode="Markdown")
        return
    uid = int(parts[1])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT username,request_count,notifications,language,COALESCE(bonus_requests,0),COALESCE(streak,0),birth_date,full_name,zodiac,COALESCE(is_banned,0),first_seen FROM users WHERE user_id=?", (uid,)) as c:
            row = await c.fetchone()
    if not row:
        await message.answer("❌ Пользователь не найден.")
        return
    username, count, notif, lang, bonus, streak, birth, name, zodiac, banned, since = row
    sub = await get_subscription_expiry(uid)
    text = (f"👤 *Пользователь {uid}*\n\n"
            f"Username: @{username}\nЯзык: {lang}\nС: {since[:10]}\n"
            f"Запросов: {count}\nБонусов: +{bonus}\nСерия: {streak} дней\n"
            f"Рассылка: {'✅' if notif else '❌'}\n"
            f"Подписка: {'до ' + sub.strftime('%d.%m.%Y') if sub else 'нет'}\n"
            f"Дата рождения: {birth or '—'}\nИмя: {name or '—'}\nЗодиак: {zodiac or '—'}\n"
            f"Бан: {'⛔ Да' if banned else '✅ Нет'}")
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: `/broadcast <текст>`\n\nПоддерживается Markdown.", parse_mode="Markdown")
        return
    broadcast_text = parts[1]
    user_ids = await get_all_users()
    sent, failed = 0, 0
    status_msg = await message.answer(f"📤 Отправка {len(user_ids)} пользователям...")
    for uid in user_ids:
        try:
            await bot.send_message(uid, broadcast_text, parse_mode="Markdown")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await status_msg.edit_text(f"✅ Отправлено: *{sent}*\n❌ Ошибок: *{failed}*", parse_mode="Markdown")

@dp.message(Command("promo"))
async def cmd_promo_admin(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование:\n`/promo create <КОД> <ДНЕЙ> [МАКС_ИСПОЛЬЗОВАНИЙ]`\n`/promo list`\n`/promo delete <КОД>`", parse_mode="Markdown")
        return
    sub = parts[1].lower()
    if sub == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT code,days,max_uses,used_count FROM promo_codes ORDER BY created_at DESC") as c:
                codes = await c.fetchall()
        if not codes:
            await message.answer("Промокодов нет.")
            return
        text = "🎟 *Промокоды:*\n\n"
        for code, days, max_uses, used in codes:
            text += f"• `{code}` — {days} дней, использовано: {used}/{max_uses}\n"
        await message.answer(text, parse_mode="Markdown")
    elif sub == "create" and len(parts) >= 4:
        code = parts[2].upper()
        try:
            days = int(parts[3])
            max_uses = int(parts[4]) if len(parts) > 4 else 1
        except ValueError:
            await message.answer("❌ Неверный формат.")
            return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR REPLACE INTO promo_codes (code,days,max_uses) VALUES (?,?,?)", (code, days, max_uses))
            await db.commit()
        await message.answer(f"✅ Промокод `{code}` создан: {days} дней, {max_uses} использований.", parse_mode="Markdown")
    elif sub == "delete" and len(parts) >= 3:
        code = parts[2].upper()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM promo_codes WHERE code=?", (code,))
            await db.commit()
        await message.answer(f"✅ Промокод `{code}` удалён.", parse_mode="Markdown")
    else:
        await message.answer("❌ Неверный формат команды.")

# ─── CALLBACK: TERMS ──────────────────────────────────────────────────────────
@dp.callback_query(F.data == "terms_accept")
async def terms_accept_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    await accept_terms(uid)
    await callback.answer("✅ Принято!", show_alert=False)
    if not await has_chosen_language(uid):
        try:
            await callback.message.delete()
        except Exception:
            pass
        await send_lang_select(callback.message)
        return
    lang = await get_user_lang(uid)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await send_welcome_msg(callback.message, lang, main_menu(lang))

@dp.callback_query(F.data == "terms_view")
async def terms_view_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    kb = InlineKeyboardBuilder()
    if SITE_URL:
        kb.button(text=t(lang,'terms_read_btn'), url=f"{SITE_URL}/terms")
    kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
    kb.adjust(1)
    text = ("📜 *Пользовательское соглашение*\n\nПолное соглашение размещено на нашем сайте."
            if lang == 'ru' else
            "📜 *Terms of Service*\n\nThe full terms are available on our website.")
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

# ─── CALLBACK: LANGUAGE ───────────────────────────────────────────────────────
@dp.callback_query(F.data.startswith("lang_"))
async def lang_selected(callback: CallbackQuery):
    lang = callback.data[5:]
    if lang not in ('ru','en'):
        await callback.answer()
        return
    await set_user_lang(callback.from_user.id, lang, callback.from_user.username)
    await safe_edit(callback, t(lang,'welcome'), main_menu(lang))
    await callback.answer()

@dp.callback_query(F.data == "change_language")
async def change_language_cb(callback: CallbackQuery):
    await safe_edit(callback, TEXTS['ru']['choose_lang'], language_keyboard())
    await callback.answer()

# ─── CALLBACK: NAVIGATION ─────────────────────────────────────────────────────
@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await safe_edit(callback, t(lang,'main_menu_title'), main_menu(lang))
    await callback.answer()

@dp.callback_query(F.data == "cancel_input")
async def cancel_input_cb(callback: CallbackQuery):
    user_states.pop(callback.from_user.id, None)
    lang = await get_user_lang(callback.from_user.id)
    await safe_edit(callback, t(lang,'main_menu_title'), main_menu(lang))
    await callback.answer()

@dp.callback_query(F.data == "readings_menu")
async def readings_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await safe_edit(callback, t(lang,'readings_menu_title'), readings_submenu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "esoterics_menu")
async def esoterics_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await safe_edit(callback, t(lang,'esoterics_menu_title'), esoterics_submenu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "account_menu")
async def account_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await safe_edit(callback, t(lang,'account_menu_title'), account_submenu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "tarot_menu")
async def tarot_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'tarot_menu_title'), parse_mode="Markdown", reply_markup=tarot_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "love_menu")
async def love_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'love_menu_title'), parse_mode="Markdown", reply_markup=love_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "numerology_menu")
async def numerology_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'num_menu_title'), parse_mode="Markdown", reply_markup=numerology_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "num_page_2")
async def num_page_2_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'num_menu_title'), parse_mode="Markdown", reply_markup=numerology_menu_kb_p2(lang))
    await callback.answer()

@dp.callback_query(F.data == "num_fate")
async def num_fate_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_fate", "num_fate_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "num_square")
async def num_square_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_square", "num_square_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "num_year")
async def num_year_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_year", "num_year_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "num_address")
async def num_address_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_address", "num_address_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "num_trio")
async def num_trio_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_trio", "num_trio_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "num_biz")
async def num_biz_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_biz", "num_biz_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "rune_menu")
async def rune_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'rune_menu_title'), parse_mode="Markdown", reply_markup=rune_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "horoscope")
async def horoscope_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'horoscope_title'), parse_mode="Markdown", reply_markup=horoscope_signs_kb(lang))
    await callback.answer()

@dp.callback_query(F.data.startswith("zodiac_"))
async def zodiac_selected(callback: CallbackQuery):
    sign = callback.data[7:]
    lang = await get_user_lang(callback.from_user.id)
    sign_name = ZODIAC_NAMES.get(lang, ZODIAC_NAMES['ru']).get(sign, sign)
    await callback.message.edit_text(f"📅 *{sign_name}*", parse_mode="Markdown", reply_markup=horoscope_period_kb(sign, lang))
    await callback.answer()

# ─── CALLBACK: PROFILE ────────────────────────────────────────────────────────
def _profile_text(lang: str, p: dict) -> str:
    empty = t(lang,'profile_empty')
    zname = ZODIAC_NAMES.get(lang, ZODIAC_NAMES['ru']).get(p.get('zodiac',''), p.get('zodiac','') or empty)
    return t(lang,'profile_title',
             name=p.get('full_name') or empty, birth=p.get('birth_date') or empty,
             zodiac=zname or empty, gender=p.get('gender') or empty,
             city=p.get('city') or empty, timezone=p.get('timezone') or empty,
             streak=p.get('streak',0), bonus=p.get('bonus',0))

@dp.callback_query(F.data == "profile")
async def profile_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    p = await get_profile(uid)
    await callback.message.edit_text(_profile_text(lang, p), parse_mode="Markdown", reply_markup=profile_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "profile_set_birthdate")
async def profile_set_birthdate(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "profile_birthdate", "profile_birthdate_prompt", lang)

@dp.callback_query(F.data == "profile_set_name")
async def profile_set_name(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "profile_name", "profile_name_prompt", lang)

@dp.callback_query(F.data == "profile_set_zodiac")
async def profile_set_zodiac(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'horoscope_title'), parse_mode="Markdown",
                                     reply_markup=horoscope_signs_kb(lang, prefix="pzodiac_"))
    await callback.answer()

@dp.callback_query(F.data.startswith("pzodiac_"))
async def profile_zodiac_selected(callback: CallbackQuery):
    sign = callback.data[8:]
    lang = await get_user_lang(callback.from_user.id)
    await save_profile_field(callback.from_user.id, "zodiac", sign)
    p = await get_profile(callback.from_user.id)
    await callback.message.edit_text(_profile_text(lang, p), parse_mode="Markdown", reply_markup=profile_kb(lang))
    await callback.answer(t(lang,'profile_saved'))

@dp.callback_query(F.data == "profile_set_gender")
async def profile_set_gender_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_gender_m'), callback_data="pgender_m")
    kb.button(text=t(lang,'btn_gender_f'), callback_data="pgender_f")
    kb.button(text=t(lang,'btn_gender_o'), callback_data="pgender_o")
    kb.button(text=t(lang,'btn_back'), callback_data="profile")
    kb.adjust(3, 1)
    await callback.message.edit_text(t(lang,'set_gender_prompt'), parse_mode="Markdown", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("pgender_"))
async def profile_gender_selected(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    code = callback.data[8:]
    labels = {'m': t(lang,'btn_gender_m'), 'f': t(lang,'btn_gender_f'), 'o': t(lang,'btn_gender_o')}
    await save_profile_field(callback.from_user.id, "gender", labels.get(code, code))
    p = await get_profile(callback.from_user.id)
    await callback.message.edit_text(_profile_text(lang, p), parse_mode="Markdown", reply_markup=profile_kb(lang))
    await callback.answer(t(lang,'profile_saved'))

@dp.callback_query(F.data == "profile_set_city")
async def profile_set_city_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "profile_city", "set_city_prompt", lang)

@dp.callback_query(F.data == "profile_set_timezone")
async def profile_set_timezone_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, "profile_timezone", "set_timezone_prompt", lang)

@dp.callback_query(F.data == "profile_clear")
async def profile_clear_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await clear_profile(callback.from_user.id)
    p = await get_profile(callback.from_user.id)
    await callback.message.edit_text(_profile_text(lang, p), parse_mode="Markdown", reply_markup=profile_kb(lang))
    await callback.answer("🗑")

# ─── CALLBACK: SUPPORT & REFERRAL ─────────────────────────────────────────────
@dp.callback_query(F.data == "support")
async def support_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_back'), callback_data="back_main")
    await callback.message.edit_text(t(lang,'support_text',username=SUPPORT_USERNAME), parse_mode="Markdown", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "referral")
async def referral_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    ref_count = await get_referral_count(uid)
    bonus = await get_bonus_requests(uid)
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{uid}"
    share_url = f"https://t.me/share/url?url={quote(link)}&text={quote(t(lang,'share_text'))}"
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_share_referral'), url=share_url)
    kb.button(text=t(lang,'btn_back'), callback_data="back_main")
    kb.adjust(1)
    await callback.message.edit_text(t(lang,'referral_text',link=link,count=ref_count,bonus=bonus), parse_mode="Markdown", reply_markup=kb.as_markup())
    await callback.answer()

# ─── CALLBACK: MY STATS / PAYMENTS / DELETE / NOTIFY TIME ────────────────────
@dp.callback_query(F.data == "my_stats")
async def my_stats_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    stats = await get_user_stats(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
    await safe_edit(callback,
        t(lang, 'my_stats_title',
          since=stats['since'], total=stats['total'], fav=stats['fav'],
          streak=stats['streak'], refs=stats['refs'], bonus=stats['bonus']),
        markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "my_payments")
async def my_payments_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    payments = await get_user_payments(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
    if not payments:
        await safe_edit(callback, t(lang, 'my_payments_empty'), markup=kb.as_markup())
        await callback.answer()
        return
    text = t(lang, 'my_payments_title')
    for method, amount, currency, created_at in payments:
        date_str = created_at[:10] if created_at else "?"
        text += f"• `{method}` — {amount} {currency} [{date_str}]\n"
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "delete_account")
async def delete_account_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_delete_confirm'), callback_data="delete_account_confirm")
    kb.button(text=t(lang, 'btn_refund_cancel'), callback_data="account_menu")
    kb.adjust(1)
    await safe_edit(callback, t(lang, 'delete_account_confirm_msg'), markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "delete_account_confirm")
async def delete_account_confirm_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    await delete_user_data(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Главное меню", callback_data="back_main")
    await safe_edit(callback, t(lang, 'delete_account_done'), markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "notify_time_menu")
async def notify_time_menu_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    current_hour = await get_notify_hour(uid)
    kb = InlineKeyboardBuilder()
    for h in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]:
        mark = "✅ " if h == current_hour else ""
        kb.button(text=f"{mark}{h}:00", callback_data=f"notify_h_{h}")
    kb.button(text=t(lang,'btn_back'), callback_data="notifications")
    kb.adjust(6, 6, 6, 1)
    await safe_edit(callback, t(lang, 'notify_time_title', hour=current_hour), markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("notify_h_"))
async def notify_h_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    hour = int(callback.data.split("_")[2])
    await set_notify_hour(uid, hour)
    kb = InlineKeyboardBuilder()
    for h in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]:
        mark = "✅ " if h == hour else ""
        kb.button(text=f"{mark}{h}:00", callback_data=f"notify_h_{h}")
    kb.button(text=t(lang,'btn_back'), callback_data="notifications")
    kb.adjust(6, 6, 6, 1)
    await safe_edit(callback, t(lang, 'notify_time_title', hour=hour), markup=kb.as_markup())
    await callback.answer(t(lang, 'notify_time_set', hour=hour), show_alert=False)

# ─── CALLBACK: TEXT INPUT TRIGGERS ────────────────────────────────────────────
@dp.callback_query(F.data == "tarot_1")
async def tarot_1_cb(callback: CallbackQuery):
    await _set_input_state(callback, "tarot_1_question", "tarot1_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "tarot_3")
async def tarot_3_cb(callback: CallbackQuery):
    await _set_input_state(callback, "tarot_3_question", "tarot3_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "tarot_5")
async def tarot_5_cb(callback: CallbackQuery):
    await _set_input_state(callback, "tarot_5_question", "tarot5_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "rune_1")
async def rune_1_cb(callback: CallbackQuery):
    await _set_input_state(callback, "rune_1", "rune1_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "rune_3")
async def rune_3_cb(callback: CallbackQuery):
    await _set_input_state(callback, "rune_3", "rune3_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "dream_interp")
async def dream_interp_cb(callback: CallbackQuery):
    await _set_input_state(callback, "dream_interp", "dream_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "palmistry")
async def palmistry_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    user_states[uid] = {
        "action": "palmistry",
        "prompt_msg_id": callback.message.message_id,
        "chat_id": callback.message.chat.id,
    }
    await callback.message.edit_text(t(lang, 'palmistry_prompt'), parse_mode="Markdown", reply_markup=cancel_keyboard(lang))
    await callback.answer()

@dp.callback_query(F.data == "promo_input")
async def promo_input_cb(callback: CallbackQuery):
    await _set_input_state(callback, "promo_input", "promo_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "num_date")
async def num_date_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_date", "num_date_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "num_name")
async def num_name_cb(callback: CallbackQuery):
    await _set_input_state(callback, "num_name", "num_name_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "natal_chart")
async def natal_chart_cb(callback: CallbackQuery):
    await _set_input_state(callback, "natal_chart", "natal_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "compatibility")
async def compatibility_cb(callback: CallbackQuery):
    await _set_input_state(callback, "compatibility", "compat_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "free_question")
async def free_question_cb(callback: CallbackQuery):
    await _set_input_state(callback, "free_question", "free_question_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data.in_({"love_thinking","love_couple","love_continue","love_future"}))
async def love_action_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, callback.data, callback.data + "_prompt", lang)

# ─── CALLBACK: QUICK ACTIONS ──────────────────────────────────────────────────
@dp.callback_query(F.data == "card_of_day")
async def card_of_day_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await safe_edit(callback, t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS), paywall_keyboard(lang))
        await callback.answer(); return
    await safe_edit(callback, t(lang,'pulling_card'), None)
    today = datetime.now().strftime("%d.%m.%Y")
    today_seed = datetime.now().strftime("%Y-%m-%d")
    card = random.Random(hash(f"{uid}:{today_seed}")).choice(TAROT_CARDS)
    await _do_request(uid, callback.from_user.username, "card_of_day",
                      callback.message.chat.id, callback.message.message_id,
                      f"Сегодня {today}. Карта дня: {card}. Дай глубокую интерпретацию 150–250 слов.", lang,
                      f"🌟 *Карта дня — {today}*\n\n*{card}*")
    await callback.answer()

@dp.callback_query(F.data == "moon_calendar")
async def moon_calendar_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer(); return
    await callback.message.edit_text(t(lang,'reading_moon'), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    await _do_request(uid, callback.from_user.username, "moon_calendar",
                      callback.message.chat.id, callback.message.message_id,
                      f"Сегодня {today}. Расскажи о лунном дне: фаза луны, что благоприятно делать, что нежелательно, энергия дня. 150-200 слов.", lang,
                      f"🌙 *Лунный календарь — {today}*")
    await callback.answer()

@dp.callback_query(F.data == "lucky_number")
async def lucky_number_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer(); return
    await callback.message.edit_text(t(lang,'calc_lucky'), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    await _do_request(uid, callback.from_user.username, "lucky_number",
                      callback.message.chat.id, callback.message.message_id,
                      f"Дата: {today}. Рассчитай нумерологическое число дня (покажи расчёт), объясни энергию и дай совет. 100-150 слов.", lang,
                      f"🔑 *Число удачи — {today}*")
    await callback.answer()

@dp.callback_query(F.data == "ritual_day")
async def ritual_day_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer(); return
    await callback.message.edit_text(t(lang,'finding_ritual'), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    await _do_request(uid, callback.from_user.username, "ritual_day",
                      callback.message.chat.id, callback.message.message_id,
                      f"Сегодня {today}. Предложи один простой ритуал на привлечение удачи, денег или любви (выбери по энергии дня). 100-150 слов.", lang,
                      f"🌿 *Ритуал дня — {today}*")
    await callback.answer()

@dp.callback_query(F.data == "week_spread")
async def week_spread_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer(); return
    await callback.message.edit_text(t(lang,'spreading_week'), parse_mode="Markdown")
    today = datetime.now()
    day_labels = t(lang,'days')
    day_objects = [today + timedelta(days=i) for i in range(7)]
    cards = random.sample(TAROT_CARDS, 7)
    cards_info = "\n".join([f"• {day_labels[d.weekday()]} ({d.strftime('%d.%m')}): {cards[i]}" for i, d in enumerate(day_objects)])
    await _do_request(uid, callback.from_user.username, "week_spread",
                      callback.message.chat.id, callback.message.message_id,
                      f"Расклад на неделю:\n{cards_info}\n\nДай краткую интерпретацию каждого дня (2-3 предложения) и общий вывод.", lang,
                      f"🃏 *Карта недели*\n\n{cards_info}")
    await callback.answer()

@dp.callback_query(F.data.startswith("horo_"))
async def horoscope_period_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer(); return
    parts = callback.data.split("_", 2)
    period, sign = parts[1], parts[2]
    sign_name = ZODIAC_NAMES.get(lang, ZODIAC_NAMES['ru']).get(sign, sign)
    await callback.message.edit_text(t(lang,'reading_horo',sign=sign_name), parse_mode="Markdown")
    today = datetime.now().strftime("%d.%m.%Y")
    words = {"day":"150-200","week":"200-250","month":"250-300"}.get(period,"150-200")
    period_text = {"day":"на сегодня","week":"на текущую неделю","month":"на текущий месяц"}.get(period,"сегодня")
    period_label = t(lang,'horo_period').get(period, period)
    await _do_request(uid, callback.from_user.username, f"horoscope_{sign}_{period}",
                      callback.message.chat.id, callback.message.message_id,
                      f"Дата: {today}. Составь гороскоп {period_text} для знака {sign_name}. {words} слов.", lang,
                      f"📅 *{sign_name} — {period_label}*")
    await callback.answer()

# ─── CALLBACK: NOTIFICATIONS ──────────────────────────────────────────────────
@dp.callback_query(F.data == "notifications")
async def notifications_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    enabled = await get_notifications_status(uid)
    hour = await get_notify_hour(uid)
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_notif_off' if enabled else 'btn_notif_on'), callback_data="notif_off" if enabled else "notif_on")
    kb.button(text=t(lang,'btn_notify_time') + f" ({hour}:00)", callback_data="notify_time_menu")
    kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
    kb.adjust(1)
    status = t(lang,'notif_enabled' if enabled else 'notif_disabled')
    await callback.message.edit_text(t(lang,'notif_status',status=status,desc=t(lang,'notif_desc')), parse_mode="Markdown", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.in_({"notif_on","notif_off"}))
async def notif_toggle(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    new_status = await toggle_notifications(uid, callback.from_user.username)
    hour = await get_notify_hour(uid)
    status = t(lang,'notif_enabled' if new_status else 'notif_disabled')
    msg = t(lang,'notif_on_msg' if new_status else 'notif_off_msg')
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_notif_off' if new_status else 'btn_notif_on'), callback_data="notif_off" if new_status else "notif_on")
    kb.button(text=t(lang,'btn_notify_time') + f" ({hour}:00)", callback_data="notify_time_menu")
    kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
    kb.adjust(1)
    await callback.message.edit_text(t(lang,'notif_status',status=status,desc=msg), parse_mode="Markdown", reply_markup=kb.as_markup())
    await callback.answer()

# ─── CALLBACK: SUBSCRIPTION ───────────────────────────────────────────────────
@dp.callback_query(F.data == "subscription")
async def subscription_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    has_sub = await has_subscription(uid)
    expiry = await get_subscription_expiry(uid)
    count = await get_request_count(uid)
    streak = await get_streak(uid)
    if has_sub and expiry:
        text = t(lang,'sub_active', date=expiry.strftime('%d.%m.%Y'), count=count, streak=streak)
    else:
        bonus = await get_bonus_requests(uid)
        remaining = max(0, FREE_REQUESTS + bonus - count)
        text = t(lang,'sub_inactive', remaining=remaining, free=FREE_REQUESTS+bonus, stars=SUBSCRIPTION_STARS, streak=streak)
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=subscription_keyboard(has_sub, lang))
    await callback.answer()

@dp.callback_query(F.data == "buy_stars")
async def buy_stars_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await bot.send_invoice(chat_id=callback.from_user.id, title=t(lang,'invoice_title'),
                           description=t(lang,'invoice_desc'), payload="sub_30d_stars",
                           currency="XTR", prices=[LabeledPrice(label=t(lang,'invoice_title'), amount=SUBSCRIPTION_STARS)])
    await callback.answer()

@dp.callback_query(F.data == "buy_rub")
async def buy_rub_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    if not YUKASSA_TOKEN:
        await callback.answer("⏳ Оплата картой скоро будет доступна!", show_alert=True); return
    await bot.send_invoice(chat_id=callback.from_user.id, title=t(lang,'invoice_title'),
                           description=t(lang,'invoice_desc'), payload="sub_30d_rub",
                           provider_token=YUKASSA_TOKEN, currency="RUB",
                           prices=[LabeledPrice(label=t(lang,'invoice_title'), amount=SUBSCRIPTION_RUB)])
    await callback.answer()

@dp.callback_query(F.data == "buy_sbp")
async def buy_sbp_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not YUKASSA_SHOP_ID:
        await callback.answer("⏳ СБП пока недоступен!", show_alert=True); return
    await callback.answer()
    payment = await create_yukassa_sbp_payment(uid)
    if not payment or "id" not in payment:
        await callback.message.answer(t(lang, 'sbp_error'), parse_mode="Markdown"); return
    payment_id = payment["id"]
    confirm_url = payment.get("confirmation", {}).get("confirmation_url", "")
    if not confirm_url:
        await callback.message.answer(t(lang, 'sbp_error'), parse_mode="Markdown"); return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO yookassa_invoices (payment_id, user_id) VALUES (?,?)",
                         (payment_id, uid))
        await db.commit()
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'sbp_btn_pay'), url=confirm_url)
    kb.button(text=t(lang, 'btn_main_menu'), callback_data="back_main")
    kb.adjust(1)
    await callback.message.answer(t(lang, 'sbp_payment_msg'),
                                  parse_mode="Markdown", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "buy_crypto")
async def buy_crypto_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    if not CRYPTOBOT_TOKEN:
        await callback.answer(t(lang,'payment_unavail'), show_alert=True); return
    await callback.answer()
    invoice = await cryptobot_create_invoice(callback.from_user.id)
    if not invoice:
        await callback.message.answer("❌ Ошибка создания инвойса. Попробуйте позже."); return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO crypto_invoices (invoice_id, user_id) VALUES (?,?)",
                         (invoice["invoice_id"], callback.from_user.id))
        await db.commit()
    kb = InlineKeyboardBuilder()
    kb.button(text="💎 Оплатить в CryptoBot", url=invoice["pay_url"])
    kb.adjust(1)
    await callback.message.answer(
        f"💎 *Оплата криптовалютой*\n\n"
        f"Сумма: *{SUBSCRIPTION_USDT} USDT*\n"
        f"Срок: *30 дней*\n\n"
        f"Нажми кнопку ниже — оплати в @CryptoBot.\n"
        f"Подписка активируется автоматически в течение ~30 секунд после оплаты.",
        parse_mode="Markdown", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "buy_card")
async def buy_card_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    if not STRIPE_TOKEN:
        await callback.answer(t(lang,'payment_unavail'), show_alert=True); return
    await bot.send_invoice(chat_id=callback.from_user.id, title=t(lang,'invoice_title'),
                           description=t(lang,'invoice_desc'), payload="sub_30d_card",
                           provider_token=STRIPE_TOKEN, currency="USD",
                           prices=[LabeledPrice(label=t(lang,'invoice_title'), amount=SUBSCRIPTION_USD)])
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pcq: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pcq.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    uid = message.from_user.id
    lang = await get_user_lang(uid)
    payment = message.successful_payment
    payload = payment.invoice_payload
    amount = payment.total_amount
    currency = payment.currency

    amount_str = (f"{amount // 100} {currency}" if currency != "XTR"
                  else f"{amount} ⭐ Stars")
    uname = f"@{message.from_user.username}" if message.from_user.username else f"id:{uid}"

    # gift_uid_{recipient_uid}_{method} — подарок конкретному пользователю
    if payload.startswith("gift_uid_"):
        parts = payload.split("_")
        recipient_uid = int(parts[2])
        method_name = parts[3] if len(parts) > 3 else "stars"
        expiry = await grant_subscription(recipient_uid, 30)
        # notify buyer
        await message.answer(
            f"🎁 *Подарок отправлен!*\n\nПодписка активирована для `{recipient_uid}` до *{expiry.strftime('%d.%m.%Y')}*",
            parse_mode="Markdown", reply_markup=main_menu(lang))
        # notify recipient
        try:
            rlang = await get_user_lang(recipient_uid)
            await bot.send_message(recipient_uid,
                f"🎁 *Вам подарили подписку!*\n\nКто-то активировал для вас *Мистра Premium* на 30 дней.\nПодписка действует до: *{expiry.strftime('%d.%m.%Y')}* ✨",
                parse_mode="Markdown")
        except Exception:
            pass
        if ADMIN_ID:
            method_label = {"stars": "⭐ Stars", "rub": "💳 ЮКасса", "crypto": "💎 Crypto"}.get(method_name, method_name)
            await bot.send_message(ADMIN_ID,
                f"🎁 *Подарочная подписка!*\n"
                f"👤 От: {uname} (`{uid}`)\n"
                f"🎯 Кому: `{recipient_uid}`\n"
                f"💳 Способ: {method_label} | 💵 {amount_str}\n"
                f"📅 До: {expiry.strftime('%d.%m.%Y')}",
                parse_mode="Markdown")
    elif payload == "gift_30d_stars":
        import secrets
        code = "GIFT" + secrets.token_hex(4).upper()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT OR IGNORE INTO promo_codes (code,days,max_uses) VALUES (?,30,1)", (code,))
            await db.commit()
        await message.answer(t(lang,'gift_sub_created',code=code), parse_mode="Markdown", reply_markup=main_menu(lang))
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID,
                f"🎁 *Подарочная подписка куплена*\n"
                f"👤 {uname}\n💰 {amount_str}\n🎟 Код: `{code}`",
                parse_mode="Markdown")
    else:
        expiry = await grant_subscription(uid, 30)
        await message.answer(t(lang,'sub_activated',date=expiry.strftime('%d.%m.%Y')),
                             parse_mode="Markdown", reply_markup=main_menu(lang))
        if ADMIN_ID:
            method = {"sub_30d_rub": "💳 ЮКасса", "sub_30d_stars": "⭐ Stars",
                      "sub_30d_card": "💳 Stripe", "sub_30d_crypto": "💎 Crypto"}.get(payload, payload)
            await bot.send_message(ADMIN_ID,
                f"💰 *Новая оплата!*\n"
                f"👤 {uname} (`{uid}`)\n"
                f"💳 Способ: {method}\n"
                f"💵 Сумма: {amount_str}\n"
                f"📅 Подписка до: {expiry.strftime('%d.%m.%Y')}",
                parse_mode="Markdown")

# ─── CALLBACKS: NEW FEATURES ──────────────────────────────────────────────────
@dp.callback_query(F.data == "tarot_cc")
async def tarot_cc_cb(callback: CallbackQuery):
    await _set_input_state(callback, "tarot_cc_question", "tarot_cc_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "tarot_yn")
async def tarot_yn_cb(callback: CallbackQuery):
    await _set_input_state(callback, "tarot_yn_question", "tarot_yn_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "career_menu")
async def career_menu_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await callback.message.edit_text(t(lang,'career_menu_title'), parse_mode="Markdown", reply_markup=career_menu_kb(lang))
    await callback.answer()

@dp.callback_query(F.data.in_({"career_money","career_job","career_biz"}))
async def career_action_cb(callback: CallbackQuery):
    lang = await get_user_lang(callback.from_user.id)
    await _set_input_state(callback, callback.data, callback.data + "_prompt", lang)

@dp.callback_query(F.data == "card_year")
async def card_year_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await can_use_bot(uid):
        await callback.message.edit_text(t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS), parse_mode="Markdown", reply_markup=paywall_keyboard(lang))
        await callback.answer(); return
    profile = await get_profile(uid)
    if not profile.get('birth_date'):
        await _set_input_state(callback, "card_year", "card_year_prompt", lang)
        return
    year = datetime.now().year
    card = random.Random(hash(f"{uid}:{year}")).choice(TAROT_CARDS)
    await callback.message.edit_text(t(lang,'pulling_card'), parse_mode="Markdown")
    await _do_request(uid, callback.from_user.username, "card_year",
                      callback.message.chat.id, callback.message.message_id,
                      f"Год: {year}. Карта года: {card}. Дата рождения: {profile['birth_date']}. Дай глубокий архетипический прогноз на {year} год через эту карту. 250–350 слов.", lang,
                      f"🗓 *Карта года — {year}*\n\n*{card}*")
    await callback.answer()

@dp.callback_query(F.data == "my_horo")
async def my_horo_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    profile = await get_profile(uid)
    zodiac = profile.get('zodiac')
    if zodiac:
        sign_name = ZODIAC_NAMES.get(lang, ZODIAC_NAMES['ru']).get(zodiac, zodiac)
        await callback.message.edit_text(f"📅 *{sign_name}*", parse_mode="Markdown",
                                         reply_markup=horoscope_period_kb(zodiac, lang))
    else:
        await callback.message.edit_text(t(lang,'horoscope_title'), parse_mode="Markdown",
                                         reply_markup=horoscope_signs_kb(lang))
    await callback.answer()

@dp.callback_query(F.data == "history_view")
async def history_view_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    history = await get_reading_history(uid)
    kb = InlineKeyboardBuilder()
    if not history:
        kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
        await callback.message.edit_text(t(lang,'history_empty'), parse_mode="Markdown", reply_markup=kb.as_markup())
        await callback.answer()
        return
    for id_, action, header, created_at in history:
        title = (header or action).replace("*","").replace("_","").replace("\n"," ").strip()
        date_str = created_at[:10] if created_at else "—"
        btn_label = f"🔸 {title[:38]} | {date_str}"
        kb.button(text=btn_label, callback_data=f"hist_{id_}")
    kb.button(text=t(lang,'btn_back'), callback_data="account_menu")
    kb.adjust(1)
    title_text = t(lang,'history_title') + "_Нажмите на запись — увидите полный результат:_" if lang == 'ru' else t(lang,'history_title') + "_Tap a record to see the full result:_"
    await callback.message.edit_text(title_text, parse_mode="Markdown", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("hist_"))
async def history_item_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    try:
        history_id = int(callback.data[5:])
    except ValueError:
        await callback.answer(); return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT header, result, created_at FROM readings_history WHERE id=? AND user_id=?",
                              (history_id, uid)) as c:
            row = await c.fetchone()
    if not row:
        await callback.answer("❌ Запись не найдена.", show_alert=True); return
    header, result, created_at = row
    text = result if result else (header or "—")
    date_str = created_at[:16] if created_at else "—"
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    text = f"🕐 _{date_str}_\n\n{text}"
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang,'btn_back'), callback_data="history_view")
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "tarot_library")
async def tarot_library_cb(callback: CallbackQuery):
    await _set_input_state(callback, "tarot_library", "tarot_library_prompt", await get_user_lang(callback.from_user.id))

@dp.callback_query(F.data == "gift_sub")
async def gift_sub_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    user_states[uid] = {"action": "gift_select_user"}
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_back'), callback_data="account_menu")
    text = ("🎁 *Подарить подписку*\n\n"
            "Введите Telegram ID или @username получателя:"
            if lang == 'ru' else
            "🎁 *Gift Subscription*\n\n"
            "Enter recipient's Telegram ID or @username:")
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("gift_pay_"))
async def gift_pay_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    recipient_uid = int(callback.data.split("_")[2])
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT username FROM users WHERE user_id=?", (recipient_uid,)) as c:
            row = await c.fetchone()
    rname = f"@{row[0]}" if row and row[0] and row[0] != "unknown" else f"id:{recipient_uid}"
    text = (f"🎁 *Подарить подписку*\n\nПолучатель: *{rname}*\n\nВыберите способ оплаты (30 дней):"
            if lang == 'ru' else
            f"🎁 *Gift Subscription*\n\nRecipient: *{rname}*\n\nChoose payment method (30 days):")
    kb = InlineKeyboardBuilder()
    kb.button(text=f"⭐ {SUBSCRIPTION_STARS} Stars", callback_data=f"gift_stars_{recipient_uid}")
    if YUKASSA_TOKEN:
        kb.button(text="💳 ЮКасса — 250 ₽", callback_data=f"gift_rub_{recipient_uid}")
    if CRYPTOBOT_TOKEN:
        kb.button(text="💎 Crypto — $4.99", callback_data=f"gift_crypto_{recipient_uid}")
    kb.button(text=t(lang, 'btn_back'), callback_data="gift_sub")
    kb.adjust(1)
    await safe_edit(callback, text, markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("gift_stars_"))
async def gift_stars_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    recipient_uid = int(callback.data.split("_")[2])
    await bot.send_invoice(
        chat_id=uid, title="🎁 Подарок — Подписка Мистра 30 дней",
        description=f"Подписка в подарок пользователю {recipient_uid}",
        payload=f"gift_uid_{recipient_uid}_stars",
        currency="XTR",
        prices=[LabeledPrice(label="Gift Subscription", amount=SUBSCRIPTION_STARS)])
    await callback.answer()

@dp.callback_query(F.data.startswith("gift_rub_"))
async def gift_rub_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    recipient_uid = int(callback.data.split("_")[2])
    if not YUKASSA_TOKEN:
        await callback.answer("❌ ЮКасса не подключена", show_alert=True)
        return
    await bot.send_invoice(
        chat_id=uid, title="🎁 Подарок — Подписка Мистра 30 дней",
        description=f"Подписка в подарок на 30 дней",
        payload=f"gift_uid_{recipient_uid}_rub",
        provider_token=YUKASSA_TOKEN, currency="RUB",
        prices=[LabeledPrice(label="Подписка 30 дней", amount=SUBSCRIPTION_RUB)])
    await callback.answer()

@dp.callback_query(F.data.startswith("gift_crypto_"))
async def gift_crypto_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    recipient_uid = int(callback.data.split("_")[2])
    if not CRYPTOBOT_TOKEN:
        await callback.answer("❌ CryptoBot не подключён", show_alert=True)
        return
    invoice = await create_cryptobot_invoice(uid, description=f"Gift for {recipient_uid}")
    if not invoice or "pay_url" not in invoice:
        await callback.answer("❌ Ошибка создания платежа", show_alert=True)
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO crypto_invoices (invoice_id, user_id) VALUES (?,?)",
            (invoice["invoice_id"], uid))
        await db.execute(
            "INSERT OR IGNORE INTO payments (payment_id, user_id, method, amount, currency) VALUES (?,?,?,?,?)",
            (str(invoice["invoice_id"]), uid, f"crypto_gift_{recipient_uid}", SUBSCRIPTION_USDT, "USDT"))
        await db.commit()
    kb = InlineKeyboardBuilder()
    kb.button(text="💎 Оплатить криптой", url=invoice["pay_url"])
    kb.button(text=t(lang, 'btn_back'), callback_data=f"gift_pay_{recipient_uid}")
    kb.adjust(1)
    await safe_edit(callback, f"💎 *Оплата криптой — подарок*\n\nСумма: *{SUBSCRIPTION_USDT} USDT*\n\nПосле оплаты подписка автоматически активируется получателю.", markup=kb.as_markup())
    await callback.answer()

async def get_last_yukassa_payment(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT payment_id FROM payments WHERE user_id=? ORDER BY created_at DESC LIMIT 1",
            (user_id,)) as c:
            row = await c.fetchone()
    return row[0] if row else None

@dp.callback_query(F.data == "refund_request")
async def refund_request_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    if not await has_subscription(uid):
        await callback.answer("❌ Нет активной подписки", show_alert=True)
        return
    payment_id = await get_last_yukassa_payment(uid)
    if not payment_id:
        kb = InlineKeyboardBuilder()
        kb.button(text=t(lang, 'btn_back'), callback_data="subscription")
        await safe_edit(callback, t(lang, 'refund_no_payment', support=SUPPORT_USERNAME), markup=kb.as_markup())
        await callback.answer()
        return
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, 'btn_refund_confirm'), callback_data=f"refund_confirm_{payment_id}")
    kb.button(text=t(lang, 'btn_refund_cancel'), callback_data="subscription")
    kb.adjust(1)
    await safe_edit(callback, t(lang, 'refund_request_msg'), markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("refund_confirm_"))
async def refund_confirm_cb(callback: CallbackQuery):
    uid = callback.from_user.id
    lang = await get_user_lang(uid)
    payment_id = callback.data[len("refund_confirm_"):]
    if not YUKASSA_SHOP_ID or not YUKASSA_SECRET_KEY:
        kb = InlineKeyboardBuilder()
        kb.button(text=t(lang, 'btn_back'), callback_data="subscription")
        await safe_edit(callback, t(lang, 'refund_error', support=SUPPORT_USERNAME), markup=kb.as_markup())
        await callback.answer()
        return
    auth = aiohttp.BasicAuth(YUKASSA_SHOP_ID, YUKASSA_SECRET_KEY)
    headers = {"Idempotency-Key": str(uuid.uuid4()), "Content-Type": "application/json"}
    refund_payload = {
        "amount": {"value": "250.00", "currency": "RUB"},
        "payment_id": payment_id
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.yookassa.ru/v3/refunds",
                                    json=refund_payload, auth=auth, headers=headers) as resp:
                data = await resp.json()
        status = data.get("status", "")
        if status in ("succeeded", "pending"):
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM subscriptions WHERE user_id=?", (uid,))
                await db.commit()
            kb_ok = InlineKeyboardBuilder()
            kb_ok.button(text=t(lang, 'btn_main_menu'), callback_data="back_main")
            await safe_edit(callback, t(lang, 'refund_success'), markup=kb_ok.as_markup())
            if ADMIN_ID:
                uname = f"@{callback.from_user.username}" if callback.from_user.username else f"id:{uid}"
                await bot.send_message(ADMIN_ID,
                    f"🔄 *Возврат оформлен*\n"
                    f"👤 {uname} (`{uid}`)\n"
                    f"💳 payment_id: `{payment_id}`\n"
                    f"💵 250.00 RUB | статус: {status}",
                    parse_mode="Markdown")
        else:
            logger.error(f"YuKassa refund failed for {uid}: {data}")
            kb_err = InlineKeyboardBuilder()
            kb_err.button(text=t(lang, 'btn_back'), callback_data="subscription")
            await safe_edit(callback, t(lang, 'refund_error', support=SUPPORT_USERNAME), markup=kb_err.as_markup())
    except Exception as e:
        logger.error(f"YuKassa refund error for {uid}: {e}")
        kb_err = InlineKeyboardBuilder()
        kb_err.button(text=t(lang, 'btn_back'), callback_data="subscription")
        await safe_edit(callback, t(lang, 'refund_error', support=SUPPORT_USERNAME), markup=kb_err.as_markup())
    await callback.answer()

# ─── MESSAGE HANDLER ──────────────────────────────────────────────────────────
@dp.message()
async def handle_message(message: Message):
    uid = message.from_user.id
    lang = await get_user_lang(uid)

    if await is_banned(uid):
        await message.answer(t(lang,'banned_msg'))
        return

    state = user_states.pop(uid, {})
    action = state.get("action")
    prompt_msg_id = state.get("prompt_msg_id")
    chat_id = message.chat.id

    try:
        await message.delete()
    except Exception:
        pass

    text = message.text or ""

    # ── Gift subscription: select recipient ──────────────────────────────────
    if action == "gift_select_user":
        query = text.strip().lstrip("@")
        async with aiosqlite.connect(DB_PATH) as db:
            if query.isdigit():
                async with db.execute("SELECT user_id, username FROM users WHERE user_id=?", (int(query),)) as c:
                    row = await c.fetchone()
            else:
                async with db.execute("SELECT user_id, username FROM users WHERE username=?", (query,)) as c:
                    row = await c.fetchone()
        if not row:
            kb = InlineKeyboardBuilder()
            kb.button(text="🔄 Попробовать снова", callback_data="gift_sub")
            kb.button(text=t(lang, 'btn_main_menu'), callback_data="back_main")
            kb.adjust(1)
            await bot.send_message(chat_id, "❌ Пользователь не найден. Убедитесь, что он уже запускал бота.", reply_markup=kb.as_markup())
        else:
            recipient_uid, rname = row
            rname_str = f"@{rname}" if rname and rname != "unknown" else f"id:{recipient_uid}"
            kb = InlineKeyboardBuilder()
            kb.button(text=f"✅ Да, выбрать {rname_str}", callback_data=f"gift_pay_{recipient_uid}")
            kb.button(text="🔄 Другой пользователь", callback_data="gift_sub")
            kb.adjust(1)
            await bot.send_message(chat_id, f"🎁 Получатель: *{rname_str}*\n\nПодтвердить?", parse_mode="Markdown", reply_markup=kb.as_markup())
        return

    # ── Admin: search user ────────────────────────────────────────────────────
    if action == "adm_search":
        query = text.strip().lstrip("@")
        async with aiosqlite.connect(DB_PATH) as db:
            if query.isdigit():
                async with db.execute(
                    "SELECT user_id FROM users WHERE user_id=?", (int(query),)) as c:
                    row = await c.fetchone()
            else:
                async with db.execute(
                    "SELECT user_id FROM users WHERE username=?", (query,)) as c:
                    row = await c.fetchone()
        if row:
            found_uid = row[0]
            kb = InlineKeyboardBuilder()
            kb.button(text="👤 Открыть профиль", callback_data=f"adm_u_{found_uid}")
            kb.button(text="🏠 Меню", callback_data="adm_main")
            kb.adjust(1)
            await bot.send_message(chat_id, f"✅ Найден: `{found_uid}`", parse_mode="Markdown", reply_markup=kb.as_markup())
        else:
            kb = InlineKeyboardBuilder()
            kb.button(text="🏠 Меню", callback_data="adm_main")
            await bot.send_message(chat_id, f"❌ Пользователь `{query}` не найден.", parse_mode="Markdown", reply_markup=kb.as_markup())
        return

    # ── Admin: send message to user ───────────────────────────────────────────
    if action == "adm_msg":
        target_uid = state.get("target_uid")
        try:
            await bot.send_message(target_uid,
                f"📨 *Сообщение от администратора:*\n\n{text}",
                parse_mode="Markdown")
            kb = InlineKeyboardBuilder()
            kb.button(text="👤 Назад к юзеру", callback_data=f"adm_u_{target_uid}")
            kb.button(text="🏠 Меню", callback_data="adm_main")
            kb.adjust(2)
            await bot.send_message(chat_id, f"✅ Сообщение отправлено `{target_uid}`", parse_mode="Markdown", reply_markup=kb.as_markup())
        except Exception:
            await bot.send_message(chat_id, f"❌ Не удалось отправить сообщение `{target_uid}` — возможно, пользователь заблокировал бота.")
        return

    # ── Palmistry (photo reading) ─────────────────────────────────────────────
    if action == "palmistry":
        if not message.photo:
            user_states[uid] = state  # restore state
            try:
                await bot.edit_message_text(
                    t(lang, 'palmistry_no_photo'), chat_id=chat_id,
                    message_id=prompt_msg_id, parse_mode="Markdown",
                    reply_markup=cancel_keyboard(lang))
            except Exception:
                pass
            return
        if not await can_use_bot(uid):
            await _edit_or_send(chat_id, prompt_msg_id,
                                t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS),
                                paywall_keyboard(lang))
            return
        if prompt_msg_id:
            try:
                await bot.edit_message_text(t(lang,'reading_palm'), chat_id=chat_id,
                                            message_id=prompt_msg_id, parse_mode="Markdown")
            except Exception:
                prompt_msg_id = None
        if not prompt_msg_id:
            sent = await bot.send_message(uid, t(lang,'reading_palm'), parse_mode="Markdown")
            prompt_msg_id = sent.message_id
            chat_id = uid
        photo = message.photo[-1]
        file = await bot.get_file(photo.file_id)
        image_io = await bot.download_file(file.file_path)
        image_bytes = image_io.read() if hasattr(image_io, 'read') else bytes(image_io)
        await log_request(uid, message.from_user.username, "palmistry")
        streak, milestone = await update_streak(uid)
        answer = await ask_claude_vision(image_bytes, lang)
        header = "🖐 *Хиромантическое чтение*" if lang == 'ru' else "🖐 *Palm Reading*"
        await _edit_or_send(chat_id, prompt_msg_id, f"{header}\n\n{answer}", back_button(lang))
        if milestone:
            await bot.send_message(uid, t(lang,'streak_bonus', days=streak), parse_mode="Markdown")
        return

    # ── Profile field saves (no Claude, no request count) ─────────────────────
    if action == "profile_birthdate":
        await save_profile_field(uid, "birth_date", text)
        p = await get_profile(uid)
        await _edit_or_send(chat_id, prompt_msg_id, _profile_text(lang, p), profile_kb(lang))
        return

    if action == "profile_name":
        await save_profile_field(uid, "full_name", text)
        p = await get_profile(uid)
        await _edit_or_send(chat_id, prompt_msg_id, _profile_text(lang, p), profile_kb(lang))
        return

    if action == "profile_city":
        await save_profile_field(uid, "city", text)
        p = await get_profile(uid)
        await _edit_or_send(chat_id, prompt_msg_id, _profile_text(lang, p), profile_kb(lang))
        return

    if action == "profile_timezone":
        await save_profile_field(uid, "timezone", text)
        p = await get_profile(uid)
        await _edit_or_send(chat_id, prompt_msg_id, _profile_text(lang, p), profile_kb(lang))
        return

    # ── Promo code ────────────────────────────────────────────────────────────
    if action == "promo_input":
        status, days = await apply_promo(uid, text)
        msg_map = {'ok': t(lang,'promo_success',days=days), 'invalid': t(lang,'promo_invalid'),
                   'used': t(lang,'promo_used'), 'exhausted': t(lang,'promo_exhausted')}
        await _edit_or_send(chat_id, prompt_msg_id, msg_map.get(status, t(lang,'promo_invalid')), back_button(lang))
        return

    # ── No active state — ignore message silently ─────────────────────────────
    if not action:
        return

    if not await can_use_bot(uid):
        await _edit_or_send(chat_id, prompt_msg_id,
                            t(lang,'paywall',free=FREE_REQUESTS,stars=SUBSCRIPTION_STARS),
                            paywall_keyboard(lang))
        return

    # Build prompt and header based on action
    profile = await get_profile(uid)
    birth = profile.get('birth_date', '')
    fname = profile.get('full_name', '')

    prompt, header = "", ""

    if action == "tarot_1_question":
        card = random.choice(TAROT_CARDS)
        prompt = f"Вопрос: «{text}»\nКарта Таро: {card}\n\nДай развёрнутую интерпретацию применительно к вопросу. 150–250 слов."
        header = f"🃏 *Расклад на 1 карту*\n\n*Вопрос:* {text}\n\n*Карта:* {card}"
    elif action == "tarot_3_question":
        cards = random.sample(TAROT_CARDS, 3)
        prompt = f"Ситуация: «{text}»\n\nРасклад:\n• Прошлое: {cards[0]}\n• Настоящее: {cards[1]}\n• Будущее: {cards[2]}\n\nДай интерпретацию каждой позиции. 250–350 слов."
        header = f"🎴 *Прошлое / Настоящее / Будущее*\n\n*Ситуация:* {text}\n\n🕰 *Прошлое:* {cards[0]}\n⚡ *Настоящее:* {cards[1]}\n🌅 *Будущее:* {cards[2]}"
    elif action == "tarot_5_question":
        cards = random.sample(TAROT_CARDS, 5)
        pos = ["Суть ситуации","Прошлое","Будущее","Совет","Итог"]
        cards_text = "\n".join([f"*{p}:* {c}" for p, c in zip(pos, cards)])
        prompt = f"Ситуация: «{text}»\n\nРасклад на 5 карт:\n" + "\n".join([f"• {p}: {c}" for p,c in zip(pos,cards)]) + "\n\nДай детальную интерпретацию каждой позиции. 350–450 слов."
        header = f"🔮 *Расклад на ситуацию (5 карт)*\n\n*Ситуация:* {text}\n\n{cards_text}"
    elif action == "rune_1":
        rune = random.choice(RUNES)
        prompt = f"Вопрос: «{text}»\nВыпала руна: {rune}\n\nДай развёрнутую интерпретацию руны применительно к вопросу. Значение руны, энергия, совет. 150–200 слов."
        header = f"🪨 *Расклад на 1 руну*\n\n*Вопрос:* {text}\n\n*Руна:* {rune}"
    elif action == "rune_3":
        runes = random.sample(RUNES, 3)
        prompt = f"Ситуация: «{text}»\n\nРасклад:\n• Прошлое/Причина: {runes[0]}\n• Настоящее: {runes[1]}\n• Будущее/Совет: {runes[2]}\n\nДай интерпретацию каждой руны. 250–300 слов."
        header = f"🪨 *Расклад на 3 руны*\n\n*Ситуация:* {text}\n\n🔙 *Причина:* {runes[0]}\n⚡ *Сейчас:* {runes[1]}\n🔮 *Совет:* {runes[2]}"
    elif action == "dream_interp":
        prompt = f"Сон: «{text}»\n\nДай психологическую и символическую интерпретацию этого сна. Основные символы, их значение, связь с реальной жизнью, послание подсознания. 200–300 слов."
        header = f"💭 *Толкование сна*"
    elif action == "num_date":
        date_str = text if text else birth
        prompt = f"Дата рождения: {date_str}\n\nПолный нумерологический анализ:\n1. Число жизненного пути (с расчётом)\n2. Число судьбы\n3. Число дня рождения\n4. Характеристика личности\n5. Сильные и слабые стороны\n6. Предназначение\n300–400 слов."
        header = f"🔢 *Нумерологический анализ*\n\n*Дата рождения:* {date_str}"
    elif action == "num_name":
        name_str = text if text else fname
        prompt = f"Полное имя: {name_str}\n\nАнализ по имени:\n1. Число имени (с расчётом по буквам)\n2. Число душевного порыва (гласные)\n3. Число внешнего проявления (согласные)\n4. Характеристика личности\n5. Кармические задачи\n250–350 слов."
        header = f"✏️ *Нумерология по имени*\n\n*Имя:* {name_str}"
    elif action == "natal_chart":
        prompt = f"Данные: {text}\n\nИнтерпретация натальной карты:\n1. Солнечный знак\n2. Асцендент (если есть время)\n3. Лунный знак\n4. Ключевые планеты\n5. Таланты\n6. Кармические задачи\n7. Жизненный путь\nЕсли время 00:00 — отметь что асцендент неизвестен. 350–500 слов."
        header = f"🌠 *Натальная карта*\n\n*Данные:* {text}"
    elif action == "compatibility":
        prompt = f"Даты рождения пары: {text}\n\nАнализ совместимости:\n1. Числа жизненного пути обоих (с расчётами)\n2. Совместимость\n3. Сильные стороны пары\n4. Зоны напряжения\n5. Прогноз\n300–400 слов."
        header = f"💑 *Совместимость пары*\n\n*Даты:* {text}"
    elif action == "num_fate":
        prompt = f"Имя и дата: {text}\n\nРассчитай число судьбы (с пошаговым расчётом по всем буквам и цифрам даты), дай глубокую интерпретацию предназначения. 250–350 слов."
        header = f"🔮 *Число судьбы*\n\n*Данные:* {text}"
    elif action == "num_square":
        prompt = f"Дата рождения: {text}\n\nПострой пифагорейский квадрат (матрицу судьбы): рассчитай все числа, опиши каждую ячейку и её значение. 300–400 слов."
        header = f"📊 *Пифагорейский квадрат*\n\n*Дата:* {text}"
    elif action == "num_year":
        prompt = f"Дата рождения: {text}. Текущий год: {datetime.now().year}.\n\nРассчитай личный год (с формулой), опиши его энергию и главные темы. 200–250 слов."
        header = f"🗓 *Личный год — {datetime.now().year}*"
    elif action == "num_address":
        prompt = f"Адрес: {text}\n\nРассчитай нумерологию адреса (сложи все цифры номера дома/квартиры), объясни энергетику места, что это означает для жильцов. 200–250 слов."
        header = f"🏠 *Нумерология адреса*\n\n*Адрес:* {text}"
    elif action == "num_trio":
        prompt = f"Три даты рождения: {text}\n\nАнализ треугольника отношений: рассчитай числа жизненного пути всех троих, совместимость попарно и в треугольнике, динамика взаимодействия. 300–400 слов."
        header = f"👨‍👩‍👦 *Треугольник отношений*\n\n*Даты:* {text}"
    elif action == "num_biz":
        prompt = f"Бизнес: {text}\n\nНумерология бизнеса: рассчитай число названия компании и дату основания, их совместимость, энергетика бизнеса, прогноз, советы. 250–300 слов."
        header = f"💼 *Нумерология бизнеса*\n\n*Данные:* {text}"
    elif action == "love_thinking":
        card = random.choice(TAROT_CARDS)
        prompt = f"Запрос: «{text}»\nКарта: {card}\n\nОтветь на вопрос 'Думает ли он/она обо мне?' Дай честный и глубокий ответ. 150–200 слов."
        header = f"💭 *Думает ли он/она обо мне?*\n\n*Карта:* {card}"
    elif action == "love_couple":
        cards = random.sample(TAROT_CARDS, 3)
        prompt = f"Ситуация: «{text}»\n\nРасклад на пару:\n• Он/она: {cards[0]}\n• Вы: {cards[1]}\n• Связь: {cards[2]}\n\n200–250 слов."
        header = f"💑 *Расклад на пару*\n\n👤 *Он/она:* {cards[0]}\n👤 *Вы:* {cards[1]}\n🔗 *Связь:* {cards[2]}"
    elif action == "love_continue":
        card = random.choice(TAROT_CARDS)
        prompt = f"Ситуация: «{text}»\nКарта совета: {card}\n\nОтветь на вопрос 'Стоит ли продолжать отношения?' Честно и глубоко. 150–200 слов."
        header = f"🤔 *Стоит ли продолжать?*\n\n*Карта совета:* {card}"
    elif action == "love_future":
        cards = random.sample(TAROT_CARDS, 3)
        prompt = f"Отношения: «{text}»\n\nРасклад на будущее:\n• Ближайшее: {cards[0]}\n• Развитие: {cards[1]}\n• Итог: {cards[2]}\n\n200–250 слов."
        header = f"🔮 *Будущее отношений*\n\n⏰ *Ближайшее:* {cards[0]}\n📈 *Развитие:* {cards[1]}\n🎯 *Итог:* {cards[2]}"
    elif action == "free_question":
        prompt = text
        header = ""
    elif action == "tarot_cc_question":
        cards = random.sample(TAROT_CARDS, 10)
        pos = ["Суть","Перекрёст","Корона","Основа","Прошлое","Будущее","Ты сам","Окружение","Надежды","Итог"]
        cards_text = "\n".join([f"• {p}: {c}" for p,c in zip(pos,cards)])
        header_text = "\n".join([f"*{p}:* {c}" for p,c in zip(pos,cards)])
        prompt = f"Ситуация: «{text}»\n\nКельтский крест:\n{cards_text}\n\nДай детальную интерпретацию каждой позиции. 500–700 слов."
        header = f"✡️ *Кельтский крест*\n\n*Ситуация:* {text}\n\n{header_text}"
    elif action == "tarot_yn_question":
        card = random.choice(TAROT_CARDS)
        prompt = f"Вопрос: «{text}»\nКарта: {card}\n\nОтветь ТОЛЬКО «Да» или «Нет» в первой строке. Затем объясни через карту (50–80 слов)."
        header = f"☯️ *Да / Нет*\n\n*Вопрос:* {text}\n\n*Карта:* {card}"
    elif action == "career_money":
        card = random.choice(TAROT_CARDS)
        prompt = f"Ситуация: «{text}»\nКарта: {card}\n\nДай расклад на финансы: текущая энергия денег, что блокирует доход, совет как улучшить финансовый поток. 200–250 слов."
        header = f"💰 *Деньги и финансы*\n\n*Карта:* {card}"
    elif action == "career_job":
        cards = random.sample(TAROT_CARDS, 3)
        prompt = f"Ситуация: «{text}»\nРасклад:\n• Текущая позиция: {cards[0]}\n• Препятствия: {cards[1]}\n• Совет: {cards[2]}\n\nДай интерпретацию применительно к карьере. 200–250 слов."
        header = f"💼 *Карьера и работа*\n\n🎯 *Позиция:* {cards[0]}\n🚧 *Препятствие:* {cards[1]}\n💡 *Совет:* {cards[2]}"
    elif action == "career_biz":
        cards = random.sample(TAROT_CARDS, 3)
        prompt = f"Запрос: «{text}»\nРасклад:\n• Потенциал: {cards[0]}\n• Риски: {cards[1]}\n• Ключ успеха: {cards[2]}\n\n200–250 слов."
        header = f"🚀 *Бизнес и проекты*\n\n✨ *Потенциал:* {cards[0]}\n⚠️ *Риски:* {cards[1]}\n🔑 *Успех:* {cards[2]}"
    elif action == "card_year":
        year = datetime.now().year
        if text:
            await save_profile_field(uid, "birth_date", text)
        birth = text or profile.get('birth_date', '')
        card = random.Random(hash(f"{uid}:{year}")).choice(TAROT_CARDS)
        prompt = f"Год: {year}. Карта года: {card}. Дата рождения: {birth}. Дай архетипический прогноз на {year} год через эту карту. 250–350 слов."
        header = f"🗓 *Карта года — {year}*\n\n*{card}*"
    elif action == "tarot_library":
        prompt = f"Расскажи подробно о карте или теме Таро: «{text}». История, символизм, значение в прямом и перевёрнутом положении, типичные интерпретации. 300–400 слов."
        header = f"📚 *Библиотека Таро*\n\n*Тема:* {text}"
    else:
        await _edit_or_send(chat_id, prompt_msg_id, t(lang,'unknown_cmd'), back_button(lang))
        return

    await _do_request(uid, message.from_user.username, action, chat_id, prompt_msg_id, prompt, lang, header)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def set_commands():
    await bot.set_my_commands([
        BotCommand(command="menu", description="🔮 Главное меню / Main menu"),
        BotCommand(command="terms", description="📜 Пользовательское соглашение"),
        BotCommand(command="myid", description="🆔 Ваш Telegram ID"),
    ])

async def main():
    logger.info("Бот Мистра запускается...")
    global BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username or ""
    await init_db()
    await set_commands()
    asyncio.create_task(daily_broadcast_loop())
    asyncio.create_task(moon_notification_loop())
    asyncio.create_task(inactive_reminder_loop())
    asyncio.create_task(check_crypto_payments())
    asyncio.create_task(check_yukassa_payments())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())