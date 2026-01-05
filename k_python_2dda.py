import os
import asyncio
import aiohttp
import random
import json
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
import storage
from keep_alive import keep_alive
from collections import Counter

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY")

if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не задан!")
    exit(1)

if not STEAM_API_KEY:
    logger.warning("⚠️ STEAM_API_KEY не задан, некоторые функции будут ограничены")

# Инициализация
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage_obj = MemoryStorage()
dp = Dispatcher(storage=storage_obj)

# Инициализация БД
try:
    storage.init_db()
    logger.info("✅ База данных инициализирована")
except Exception as e:
    logger.error(f"❌ Ошибка инициализации БД: {e}")

# Кеши (глобальные переменные)
HEROES_CACHE = {}
ITEMS_CACHE = {}
RANK_TIER_MMR = {
    11: 10, 12: 160, 13: 310, 14: 460, 15: 610,
    21: 760, 22: 910, 23: 1060, 24: 1210, 25: 1360,
    31: 1510, 32: 1660, 33: 1810, 34: 1960, 35: 2110,
    41: 2260, 42: 2410, 43: 2560, 44: 2710, 45: 2860,
    51: 3010, 52: 3160, 53: 3310, 54: 3460, 55: 3610,
    61: 3760, 62: 3910, 63: 4060, 64: 4210, 65: 4360,
    71: 4510, 72: 4660, 73: 4810, 74: 4960, 75: 5110,
    80: 6000
}

# Состояния для FSM
class ProfileStates(StatesGroup):
    waiting_steam_url = State()
    waiting_friend_url = State()

class QuizStates(StatesGroup):
    waiting_answer = State()

# === УЛУЧШЕННЫЕ УТИЛИТЫ ===
def steam64_to_account_id(steam64: int) -> int:
    """Конвертация SteamID64 в Account ID"""
    return steam64 - 76561197960265728

async def extract_account_id_safe(steam_url: str) -> int:
    """Безопасное извлечение Account ID из Steam URL"""
    try:
        steam_url = steam_url.strip().rstrip("/")
        
        # Если это уже account_id
        if steam_url.isdigit() and len(steam_url) < 11:
            return int(steam_url)
        
        # Если это profiles/
        if "/profiles/" in steam_url:
            steam64 = int(steam_url.split("/")[-1])
            return steam64_to_account_id(steam64)
        
        # Если это id/ (vanity URL)
        elif "/id/" in steam_url:
            vanity = steam_url.split("/")[-1]
            if not STEAM_API_KEY:
                return None
                
            async with aiohttp.ClientSession() as session:
                url = f"https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/?key={STEAM_API_KEY}&vanityurl={vanity}"
                async with session.get(url, timeout=10) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data.get("response", {}).get("success") == 1:
                            steam64 = int(data["response"]["steamid"])
                            return steam64_to_account_id(steam64)
        
        # Если просто число (возможно steam64 или account_id)
        elif steam_url.isdigit():
            num = int(steam_url)
            # Если это похоже на steam64 (большое число)
            if num > 76561197960265728:
                return steam64_to_account_id(num)
            else:
                return num  # Уже account_id
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting account id: {e}")
        return None

async def get_player_data(account_id: int):
    """Получение данных игрока с обработкой ошибок"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.opendota.com/api/players/{account_id}",
                timeout=10
            ) as r:
                if r.status == 200:
                    return await r.json()
                logger.warning(f"Player API returned {r.status}")
                return None
    except Exception as e:
        logger.error(f"Error getting player {account_id}: {e}")
        return None

async def get_recent_matches(account_id: int, limit=20):
    """Получение последних матчей"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.opendota.com/api/players/{account_id}/recentMatches",
                timeout=15
            ) as r:
                if r.status == 200:
                    matches = await r.json()
                    return matches[:limit] if isinstance(matches, list) else []
                return []
    except Exception as e:
        logger.error(f"Error getting matches: {e}")
        return []

async def get_heroes_data():
    """Получение данных о героях с кешированием"""
    global HEROES_CACHE
    
    if HEROES_CACHE:
        return HEROES_CACHE
    
    try:
        # Сначала пробуем локальный файл
        with open('hero_names.json', 'r', encoding='utf-8') as f:
            HEROES_CACHE = json.load(f)
            # Конвертируем строковые ключи в int
            HEROES_CACHE = {int(k): v for k, v in HEROES_CACHE.items()}
            logger.info("✅ Герои загружены из локального файла")
            return HEROES_CACHE
    except Exception as e:
        logger.warning(f"Local heroes file not found, using API: {e}")
    
    # Если локальный файл не найден, используем API
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.opendota.com/api/constants/heroes",
                timeout=15
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    HEROES_CACHE = {int(k): v['localized_name'] for k, v in data.items()}
                    return HEROES_CACHE
    except Exception as e:
        logger.error(f"Error getting heroes: {e}")
        return {}

async def format_matches_for_display(matches):
    """Форматирование матчей для отображения"""
    if not matches:
        return "📭 Нет данных о последних матчах"
    
    heroes = await get_heroes_data()
    lines = []
    wins = 0
    total = len(matches)
    roles = []
    
    for i, m in enumerate(matches[:10], 1):  # Показываем до 10 матчей
        # Определяем победу
        is_radiant = m.get('player_slot', 0) < 128
        radiant_win = m.get('radiant_win', False)
        win = (is_radiant and radiant_win) or (not is_radiant and not radiant_win)
        
        if win: 
            wins += 1
        
        # Определяем роль
        lane = m.get('lane_role', 0)
        if lane == 1: 
            roles.append("Safe Lane")
        elif lane == 2: 
            roles.append("Mid Lane")
        elif lane == 3: 
            roles.append("Off Lane")
        elif lane == 4 or lane == 5: 
            roles.append("Support")
        
        # Форматируем детали матча
        hero_id = m.get('hero_id', 0)
        hero_name = heroes.get(hero_id, f"Герой {hero_id}")
        
        k, d, a = m.get('kills', 0), m.get('deaths', 0), m.get('assists', 0)
        
        # Рассчитываем KDA
        kda = f"{k}/{d}/{a}"
        if d > 0:
            kda_ratio = (k + a) / d
            kda += f" ({kda_ratio:.2f})"
        
        # Время матча
        duration = m.get('duration', 0)
        time_str = f"{duration // 60}:{duration % 60:02d}"
        
        # Эмодзи для исхода
        outcome = "✅" if win else "❌"
        
        # Форматируем строку
        line = f"{i}. {outcome} <b>{hero_name}</b>\n"
        line += f"   📊 KDA: {kda} | 🕒 {time_str}\n"
        
        if i < 6:  # Первые 5 матчей показываем с разделителем
            lines.append(line + "─" * 30)
        else:
            lines.append(line)
    
    # Рассчитываем винрейт
    winrate = (wins / total * 100) if total > 0 else 0
    
    # Определяем основную роль
    if roles:
        role_counter = Counter(roles)
        main_role, role_count = role_counter.most_common(1)[0]
        main_role = f"{main_role} ({role_count}/{len(roles)} игр)"
    else:
        main_role = "Не определено"
    
    # Формируем заголовок
    header = (
        f"📊 <b>Статистика за последние {total} игр:</b>\n"
        f"🔥 <b>Винрейт:</b> {winrate:.1f}% ({wins}W - {total-wins}L)\n"
        f"🎭 <b>Основная роль:</b> {main_role}\n\n"
        f"<b>Последние матчи:</b>\n"
    )
    
    return header + "\n".join(lines)

# === КЛАВИАТУРЫ ===
def get_main_keyboard():
    """Основная клавиатура"""
    builder = ReplyKeyboardBuilder()
    buttons = [
        "👤 Профиль", "📊 Анализ", "🎮 Викторина",
        "👥 Друзья", "🤝 Сравнить", "🏆 Топ игроков",
        "⚙️ Настройки", "ℹ️ Помощь"
    ]
    for btn in buttons:
        builder.button(text=btn)
    builder.adjust(2, 2, 2, 2)
    return builder.as_markup(resize_keyboard=True, selective=True)

def get_profile_keyboard():
    """Клавиатура для профиля"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data="refresh_profile")
    builder.button(text="📈 Подробная статистика", callback_data="detailed_stats")
    builder.button(text="🎮 Текущая игра", callback_data="current_match")
    builder.adjust(1)
    return builder.as_markup()

# === ОБРАБОТЧИКИ КОМАНД ===
@dp.message(Command("start"))
async def start_command(message: types.Message):
    """Обработчик команды /start"""
    welcome_text = (
        "🎮 <b>Добро пожаловать в DotaStats Bot!</b>\n\n"
        "Я помогу вам отслеживать статистику Dota 2:\n\n"
        "📊 <b>Основные функции:</b>\n"
        "• 👤 <b>Профиль</b> - ваша статистика и MMR\n"
        "• 📊 <b>Анализ</b> - сравнение с другими игроками\n"
        "• 🎮 <b>Викторина</b> - проверьте знания по Dota 2\n"
        "• 👥 <b>Друзья</b> - сравнение с друзьями\n"
        "• 🏆 <b>Топ игроков</b> - рейтинг пользователей бота\n\n"
        "📌 <b>Для начала привяжите Steam профиль:</b>\n"
        "1. Отправьте ссылку на ваш Steam профиль\n"
        "2. Или используйте команду /bind\n\n"
        "⚡ <b>Примеры ссылок:</b>\n"
        "• https://steamcommunity.com/profiles/76561198...\n"
        "• https://steamcommunity.com/id/ваш_ник"
    )
    
    await message.answer(
        welcome_text,
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

@dp.message(Command("help"))
async def help_command(message: types.Message):
    """Обработчик команды /help"""
    help_text = (
        "🆘 <b>Справка по командам:</b>\n\n"
        "👤 <b>Профиль:</b>\n"
        "• /profile - ваша статистика\n"
        "• /bind [ссылка] - привязать Steam профиль\n\n"
        "📊 <b>Статистика:</b>\n"
        "• /analyze - анализ производительности\n"
        "• /compare [ссылка] - сравнение с другим игроком\n\n"
        "👥 <b>Друзья:</b>\n"
        "• /addfriend [ссылка] - добавить друга\n"
        "• /friends - список друзей\n\n"
        "🎮 <b>Развлечения:</b>\n"
        "• /quiz - начать викторину\n"
        "• /leaderboard - таблица лидеров\n\n"
        "⚙️ <b>Настройки:</b>\n"
        "• /settings - настройки уведомлений\n"
        "• /reset - сброс данных\n\n"
        "📌 <b>Или используйте кнопки меню!</b>"
    )
    
    await message.answer(help_text, parse_mode="HTML")

@dp.message(Command("bind"))
async def bind_command(message: types.Message, state: FSMContext):
    """Привязка Steam профиля"""
    args = message.text.split()
    
    if len(args) > 1:
        # Если ссылка передана сразу в команде
        steam_url = ' '.join(args[1:])
        await process_steam_url(message, steam_url)
    else:
        # Просим прислать ссылку
        await message.answer(
            "🔗 <b>Отправьте ссылку на ваш Steam профиль:</b>\n\n"
            "<i>Примеры:</i>\n"
            "• https://steamcommunity.com/profiles/76561198...\n"
            "• https://steamcommunity.com/id/your_nickname\n"
            "• Или просто ваш Steam ID",
            parse_mode="HTML"
        )
        await state.set_state(ProfileStates.waiting_steam_url)

@dp.message(ProfileStates.waiting_steam_url)
async def process_steam_link(message: types.Message, state: FSMContext):
    """Обработка Steam ссылки из состояния"""
    steam_url = message.text
    await process_steam_url(message, steam_url)
    await state.clear()

async def process_steam_url(message: types.Message, steam_url: str):
    """Обработка Steam URL"""
    try:
        # Показываем "типинг"
        await message.answer_chat_action("typing")
        
        # Извлекаем account_id
        account_id = await extract_account_id_safe(steam_url)
        
        if not account_id:
            await message.answer(
                "❌ <b>Не удалось распознать Steam профиль.</b>\n\n"
                "Проверьте правильность ссылки и попробуйте еще раз.",
                parse_mode="HTML"
            )
            return
        
        # Получаем данные игрока
        player_data = await get_player_data(account_id)
        
        if not player_data:
            await message.answer(
                "❌ <b>Не удалось получить данные игрока.</b>\n\n"
                "Возможно, профиль скрыт или произошла ошибка API.",
                parse_mode="HTML"
            )
            return
        
        # Сохраняем в базу
        profile_name = player_data.get('profile', {}).get('personaname', 'Игрок')
        storage.bind_user(message.from_user.id, account_id)
        
        # Отправляем подтверждение
        await message.answer(
            f"✅ <b>Профиль успешно привязан!</b>\n\n"
            f"👤 <b>Игрок:</b> {profile_name}\n"
            f"🆔 <b>Account ID:</b> {account_id}\n\n"
            f"Теперь вы можете использовать все функции бота!",
            parse_mode="HTML",
            reply_markup=get_main_keyboard()
        )
        
        logger.info(f"User {message.from_user.id} bound to account {account_id}")
        
    except Exception as e:
        logger.error(f"Error processing steam URL: {e}")
        await message.answer(
            "❌ <b>Произошла ошибка при обработке профиля.</b>\n\n"
            "Попробуйте позже или свяжитесь с поддержкой.",
            parse_mode="HTML"
        )

@dp.message(F.text == "👤 Профиль")
@dp.message(Command("profile"))
async def profile_command(message: types.Message):
    """Показ профиля пользователя"""
    try:
        # Получаем account_id из базы
        account_id = storage.get_account_id(message.from_user.id)
        
        if not account_id:
            await message.answer(
                "❌ <b>Профиль не привязан.</b>\n\n"
                "Для привязки отправьте ссылку на Steam профиль или используйте команду /bind",
                parse_mode="HTML"
            )
            return
        
        # Показываем "типинг"
        await message.answer_chat_action("typing")
        
        # Получаем данные игрока
        player_data = await get_player_data(account_id)
        
        if not player_data:
            await message.answer(
                "❌ <b>Не удалось получить данные профиля.</b>\n\n"
                "Попробуйте позже или обновите привязку.",
                parse_mode="HTML"
            )
            return
        
        # Извлекаем данные
        profile = player_data.get('profile', {})
        profile_name = profile.get('personaname', 'Неизвестно')
        avatar = profile.get('avatarfull', '')
        
        # Получаем MMR
        mmr_estimate = player_data.get('mmr_estimate', {}).get('estimate', 0)
        rank_tier = player_data.get('rank_tier', 0)
        
        # Форматируем MMR
        if mmr_estimate:
            mmr_text = f"{mmr_estimate} MMR"
        elif rank_tier:
            mmr_estimate = RANK_TIER_MMR.get(rank_tier, 0)
            if mmr_estimate:
                mmr_text = f"~{mmr_estimate} MMR (ранг {rank_tier})"
            else:
                mmr_text = f"Ранг {rank_tier}"
        else:
            mmr_text = "Неизвестно"
        
        # Получаем последние матчи
        matches = await get_recent_matches(account_id, 10)
        matches_text = await format_matches_for_display(matches)
        
        # Формируем сообщение профиля
        profile_text = (
            f"👤 <b>Профиль игрока:</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Никнейм:</b> {profile_name}\n"
            f"• <b>MMR:</b> {mmr_text}\n"
            f"• <b>Account ID:</b> {account_id}\n\n"
        )
        
        # Добавляем аватар если есть
        if avatar:
            await message.answer_photo(
                photo=avatar,
                caption=profile_text,
                parse_mode="HTML",
                reply_markup=get_profile_keyboard()
            )
        else:
            await message.answer(
                profile_text,
                parse_mode="HTML",
                reply_markup=get_profile_keyboard()
            )
        
        # Отправляем статистику матчей отдельным сообщением
        await message.answer(
            matches_text,
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Error in profile command: {e}")
        await message.answer(
            "❌ <b>Произошла ошибка при получении профиля.</b>\n\n"
            "Попробуйте позже.",
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "refresh_profile")
async def refresh_profile_callback(callback: types.CallbackQuery):
    """Обновление профиля"""
    await callback.answer("🔄 Обновляем...")
    await profile_command(callback.message)

@dp.message(F.text == "📊 Анализ")
@dp.message(Command("analyze"))
async def analyze_command(message: types.Message):
    """Анализ производительности"""
    try:
        account_id = storage.get_account_id(message.from_user.id)
        
        if not account_id:
            await message.answer(
                "❌ <b>Профиль не привязан.</b>\n\n"
                "Сначала привяжите Steam профиль.",
                parse_mode="HTML"
            )
            return
        
        await message.answer_chat_action("typing")
        
        # Получаем данные benchmark
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.opendota.com/api/players/{account_id}/benchmarks",
                timeout=15
            ) as r:
                if r.status != 200:
                    await message.answer(
                        "❌ <b>Не удалось получить данные для анализа.</b>\n\n"
                        "Попробуйте позже.",
                        parse_mode="HTML"
                    )
                    return
                
                bench = await r.json()
        
        if not bench or 'error' in bench:
            await message.answer(
                "❌ <b>Нет данных для анализа.</b>\n\n"
                "Возможно, у вас недостаточно матчей.",
                parse_mode="HTML"
            )
            return
        
        # Формируем анализ
        analysis_text = "📊 <b>Анализ производительности:</b>\n"
        analysis_text += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Определяем метрики для анализа
        metrics = {
            'gold_per_min': ('💰 Золото в минуту (GPM)', 'Среднее: 450-550 GPM'),
            'xp_per_min': ('📈 Опыт в минуту (XPM)', 'Среднее: 500-600 XPM'),
            'kills_per_min': ('⚔️ Убийств в минуту', 'Среднее: 0.25-0.35'),
            'hero_damage_per_min': ('💥 Урон по героям', 'Среднее: 400-500 урона'),
            'hero_healing_per_min': ('❤️ Лечение в минуту', 'Среднее: 50-100 лечения'),
            'tower_damage': ('🏰 Урон по башням', 'Среднее: 500-1000 урона'),
            'last_hits_per_min': ('🎯 Ластхитов в минуту', 'Среднее: 4-6 ластхитов')
        }
        
        for metric_key, (metric_name, normal_range) in metrics.items():
            if metric_key in bench and bench[metric_key]:
                # Берем 95-й перцентиль (обычно это показатель игрока)
                data_points = bench[metric_key]
                if len(data_points) >= 6:  # Убедимся, что есть достаточно данных
                    # Берем предпоследний перцентиль (обычно 80-й или 90-й)
                    target_index = min(4, len(data_points) - 1)
                    percentile_data = data_points[target_index]
                    
                    percentile = percentile_data.get('percentile', 0)
                    value = percentile_data.get('value', 0)
                    
                    # Оценка производительности
                    if percentile >= 0.8:
                        emoji = "🔥"
                        rating = "Отлично"
                    elif percentile >= 0.6:
                        emoji = "👍"
                        rating = "Хорошо"
                    elif percentile >= 0.4:
                        emoji = "➖"
                        rating = "Средне"
                    elif percentile >= 0.2:
                        emoji = "⚠️"
                        rating = "Ниже среднего"
                    else:
                        emoji = "❌"
                        rating = "Плохо"
                    
                    analysis_text += (
                        f"{emoji} <b>{metric_name}</b>\n"
                        f"   Значение: {value:.1f}\n"
                        f"   Рейтинг: {rating} (лучше чем {percentile*100:.1f}% игроков)\n"
                        f"   {normal_range}\n\n"
                    )
        
        # Добавляем общую оценку
        if len(analysis_text.split('\n')) > 10:  # Если есть достаточно данных
            analysis_text += "━━━━━━━━━━━━━━━━━━━━\n"
            analysis_text += "📈 <b>Совет:</b> Сосредоточьтесь на улучшении показателей с низким рейтингом.\n"
            analysis_text += "Регулярно анализируйте свои игры для прогресса!"
        
        await message.answer(analysis_text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Error in analyze command: {e}")
        await message.answer(
            "❌ <b>Произошла ошибка при анализе.</b>\n\n"
            "Попробуйте позже.",
            parse_mode="HTML"
        )

@dp.message(F.text == "🎮 Викторина")
@dp.message(Command("quiz"))
async def quiz_menu_command(message: types.Message):
    """Меню викторины"""
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🎯 Начать викторину", callback_data="quiz_start")
    keyboard.button(text="🏆 Таблица лидеров", callback_data="quiz_leaderboard")
    keyboard.button(text="ℹ️ Правила", callback_data="quiz_rules")
    keyboard.adjust(1)
    
    await message.answer(
        "🎮 <b>Викторина по Dota 2</b>\n\n"
        "Проверьте свои знания о игре!\n\n"
        "<b>Правила:</b>\n"
        "• 10 случайных вопросов\n"
        "• +10 очков за правильный ответ\n"
        "• -5 очков за неправильный\n"
        "• Ограничение по времени: 30 секунд на вопрос",
        parse_mode="HTML",
        reply_markup=keyboard.as_markup()
    )

# ... (продолжение обработчиков)

async def main():
    """Основная функция запуска бота"""
    try:
        logger.info("🚀 Запуск DotaStats Bot...")
        
        # Запускаем keep-alive сервер
        keep_alive()
        logger.info("✅ Keep-alive сервер запущен")
        
        # Загружаем данные героев
        await get_heroes_data()
        logger.info("✅ Данные героев загружены")
        
        # Запускаем бота
        logger.info("🤖 Бот запущен и готов к работе!")
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске бота: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())