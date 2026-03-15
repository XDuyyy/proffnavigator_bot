import asyncio
import json
import logging
import math
import os
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Any, Optional

import aiosqlite

# =========================
# MATPLOTLIB (Pure OOP / No Pyplot)
# =========================
import matplotlib
matplotlib.use("Agg") # Бэкенд без GUI (для серверов)
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery, FSInputFile, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# =========================
# КОНФИГУРАЦИЯ И КОНСТАНТЫ
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "ююююю")
DB_PATH = "proforientation.db"
CHARTS_DIR = "charts"
CONFIG_PATH = "config.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# =========================
# ЗАГРУЗКА ДАННЫХ
# =========================
try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    logging.warning(f"Конфиг {CONFIG_PATH} не найден! Бот не сможет работать корректно.")
    CONFIG = {"categories": {}, "questions": [], "professions": {}}

# Основные справочники из конфига
CATEGORIES_RU: Dict[str, str] = CONFIG.get("categories", {})
PROFESSIONS: Dict[str, List[str]] = CONFIG.get("professions", {})
QUESTIONS: List[Dict[str, Any]] = CONFIG.get("questions", [])

# =========================
# СЛОВАРЬ СМЕШАННЫХ ПРОФИЛЕЙ
# =========================

MIX_NAMES_RU = {
    frozenset(["technical", "analytic"]): "Техническо-аналитический профиль (Инженерия данных, R&D)",
    frozenset(["social", "creative"]): "Социально-креативный профиль (Арт-терапия, Медиа, PR)",
    frozenset(["analytic", "creative"]): "Аналитико-креативный профиль (Архитектура, UX/UI, Геймдизайн)",
    frozenset(["technical", "social"]): "Техно-социальный профиль (IT-педагогика, MedTech)",
    frozenset(["technical", "enterprising"]): "Технологическое предпринимательство (Startup-лидер, CTO)",
    frozenset(["social", "enterprising"]): "Социальное лидерство (HR-директор, Управление проектами)",
    frozenset(["creative", "enterprising"]): "Креативное лидерство (Продюсирование, Арт-директор)",
    frozenset(["analytic", "conventional"]): "Точный учет и системный анализ (Big Data, Аудит)",
    frozenset(["creative", "conventional"]): "Креатив + Организация (Дизайн-менеджмент, Ивент-менеджер)",
    frozenset(["nature", "analytic"]): "Естественно-научный аналитик (Биоинформатика, Генетика)",
    frozenset(["nature", "technical"]): "Инженерно-природный профиль (Агрокибернетика, Экология)",
    frozenset(["nature", "creative"]): "Эко-дизайн и Ландшафтная архитектура",
    frozenset(["enterprising", "analytic"]): "Бизнес-аналитика и Стратегия (Product Manager)",
    frozenset(["social", "conventional"]): "Администрирование социальных процессов (Госслужба)",
    frozenset(["technical", "creative"]): "Промышленный дизайн и VR/AR-разработка",
}

# =========================
# СИНХРОНИЗАЦИЯ
# =========================

DB_WRITE_SEM = asyncio.Semaphore(1) 
CHART_SEM = asyncio.Semaphore(2) 
user_locks: Dict[int, asyncio.Lock] = {}

def get_lock(user_id: int) -> asyncio.Lock:
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]

# =========================
# БАЗА ДАННЫХ (SQLite)
# =========================

@asynccontextmanager
async def connect_db():
    db = await aiosqlite.connect(DB_PATH)
    try:
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.execute("PRAGMA busy_timeout=5000;")
        yield db
    finally:
        await db.close()

async def init_db():
    async with connect_db() as db:
        cursor = await db.execute("PRAGMA journal_mode=WAL;")
        mode = await cursor.fetchone()
        logging.info(f"SQLite Journal Mode: {mode[0] if mode else 'UNKNOWN'}")

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                scores_json TEXT NOT NULL,
                result_text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.commit()

async def save_result_to_db(user_id: int, username: Optional[str], scores: Dict[str, float], result_text: str):
    async with DB_WRITE_SEM:
        async with connect_db() as db:
            await db.execute(
                "INSERT INTO results (user_id, username, scores_json, result_text) VALUES (?, ?, ?, ?)",
                (user_id, username, json.dumps(scores, ensure_ascii=False), result_text),
            )
            await db.commit()

# =========================
# ГРАФИКИ (Matplotlib OOP)
# =========================

def _generate_radar_chart_sync(scores: Dict[str, float], path: str):
    categories = list(CATEGORIES_RU.keys())
    if not categories:
        return

    try:
        values = [float(scores.get(c, 0.0)) for c in categories]
    except (ValueError, TypeError):
        values = [0.0] * len(categories)

    labels = [CATEGORIES_RU[c].replace(" ", "\n") for c in categories]
    num_vars = len(categories)
    angles = [n / float(num_vars) * 2 * math.pi for n in range(num_vars)]
    angles += angles[:1]
    values += values[:1]

    # Создание директории, если нет
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    # Создание фигуры
    fig = Figure(figsize=(7, 7))
    FigureCanvas(fig)
    ax = fig.add_subplot(111, polar=True)

    # Настройка осей
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=10, color="grey")

    max_val = max(values) if max(values) > 0 else 10.0
    ax.set_rlabel_position(0)
    
    y_ticks = [max_val * 0.25, max_val * 0.5, max_val * 0.75]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([f"{y:g}" for y in y_ticks], fontsize=8, color="grey")
    ax.set_ylim(0, max_val)

    # Рисование
    ax.plot(angles, values, linewidth=2, linestyle='solid')
    ax.fill(angles, values, alpha=0.1) 
    
    ax.set_title("Профориентационный профиль", size=14, y=1.1)
    fig.savefig(path, bbox_inches='tight')

async def generate_radar_chart(scores: Dict[str, float], user_id: int) -> str:
    path = os.path.join(CHARTS_DIR, f"radar_{user_id}_{int(time.time())}.png")
    async with CHART_SEM:
        await asyncio.to_thread(_generate_radar_chart_sync, scores, path)
    return path

# =========================
# ЛОГИКА БОТА И ПОДСЧЕТА
# =========================

class TestState(StatesGroup):
    answering = State()

router = Router()

main_menu_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🚀 Начать тест")],
        [KeyboardButton(text="ℹ️ О боте"), KeyboardButton(text="❓ Помощь")],
    ],
    resize_keyboard=True
)

def yes_no_inline_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="ans:yes"),
         InlineKeyboardButton(text="Нет", callback_data="ans:no")]
    ])

def normalize_scores(raw_data: Any) -> Dict[str, float]:
    """Приводит данные из хранилища к формату float."""
    clean_scores = {k: 0.0 for k in CATEGORIES_RU.keys()}
    if isinstance(raw_data, dict):
        for k, v in raw_data.items():
            if k in clean_scores:
                try:
                    clean_scores[k] = float(v)
                except (ValueError, TypeError):
                    pass
    return clean_scores

def get_result_text(scores: Dict[str, float]) -> str:
    """Определяет победителя или смешанный профиль."""
    if not scores: return "Нет данных."
    
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    
    cat1, score1 = sorted_scores[0]
    cat2, score2 = sorted_scores[1] if len(sorted_scores) > 1 else (None, 0.0)

    result_title = ""
    result_profs = []

    is_mixed = False
    if cat2 and score2 > 0:
        if score2 >= score1 * 0.85: 
            mix_key = frozenset([cat1, cat2])
            if mix_key in MIX_NAMES_RU:
                is_mixed = True
                result_title = MIX_NAMES_RU[mix_key]
                profs1 = PROFESSIONS.get(cat1, [])
                profs2 = PROFESSIONS.get(cat2, [])
                result_profs = list(dict.fromkeys(profs1[:5] + profs2[:5])) 

    if not is_mixed:
        cat_name = CATEGORIES_RU.get(cat1, cat1)
        result_title = f"{cat_name}"
        result_profs = PROFESSIONS.get(cat1, [])

    text = f"🎯 Ваш результат: <b>{result_title}</b>\n\n"
    
    if result_profs:
        text += "💼 <b>Рекомендуемые профессии:</b>\n"
        for p in result_profs:
            text += f"• {p}\n"
    else:
        text += "<i>Список профессий для этого профиля пока не заполнен.</i>"
        
    return text

async def send_next_question(message: Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get("index")
    order = data.get("order")

    # Валидация состояния
    if not isinstance(idx, int) or not isinstance(order, list):
        await message.answer("⚠️ Ошибка состояния. Начните заново: /start", reply_markup=main_menu_kb)
        await state.clear()
        return

    if idx < 0 or idx >= len(order):
        await message.answer("⚠️ Тест завершен или сбился. /start", reply_markup=main_menu_kb)
        await state.clear()
        return

    q_idx = order[idx]
    
    # Валидация вопроса
    if not isinstance(q_idx, int) or q_idx < 0 or q_idx >= len(QUESTIONS):
        await message.answer("⚠️ Ошибка данных теста (ID). /start", reply_markup=main_menu_kb)
        await state.clear()
        return

    q = QUESTIONS[q_idx]
    q_text = q.get("text")
    
    if not isinstance(q_text, str):
        await message.answer("⚠️ Ошибка текста вопроса. /start", reply_markup=main_menu_kb)
        await state.clear()
        return

    await message.answer(
        f"Вопрос {idx + 1}/{len(order)}:\n\n{q_text}", 
        reply_markup=yes_no_inline_kb()
    )

# =========================
# ХЕНДЛЕРЫ
# =========================

@router.message(CommandStart())
@router.message(F.text == "🚀 Начать тест")
async def start_test_handler(message: Message, state: FSMContext):
    if not QUESTIONS:
        await message.answer("Ошибка: вопросы не загружены. Проверьте config.json.")
        return

    import random
    order = list(range(len(QUESTIONS)))
    random.shuffle(order)
    
    await state.clear()
    await state.set_state(TestState.answering)
    
    initial_scores = {k: 0.0 for k in CATEGORIES_RU.keys()}
    
    await state.update_data(index=0, scores=initial_scores, order=order)
    await message.answer("Начинаем тест! Отвечайте честно.", reply_markup=main_menu_kb)
    await send_next_question(message, state)

@router.message(F.text == "ℹ️ О боте")
async def about_handler(message: Message):
    await message.answer(
        "Этот бот поможет вам определить профессиональные склонности.\n"
        "Разработан в рамках школьного проекта.\n"
        "Определяет как чистые, так и смешанные типы личности."
    )

@router.message(F.text == "❓ Помощь")
async def help_handler(message: Message):
    await message.answer(
        "Нажмите кнопку «Начать тест», чтобы пройти тестирование.\n"
        "Отвечайте «Да» или «Нет» на вопросы.\n"
        "В конце вы получите результат (включая смешанные профили) и график."
    )

@router.callback_query(TestState.answering, F.data.startswith("ans:"))
async def answer_handler(call: CallbackQuery, state: FSMContext):
    async with get_lock(call.from_user.id):
        if await state.get_state() != TestState.answering:
            try:
                await call.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await call.answer("Тест завершен.")
            return

        data = await state.get_data()
        
        idx = data.get("index")
        order = data.get("order")
        scores_raw = data.get("scores")

        if not isinstance(idx, int) or not isinstance(order, list):
            await call.message.answer("⚠️ Ошибка данных. /start")
            await state.clear()
            return

        scores = normalize_scores(scores_raw)

        if idx < 0 or idx >= len(order):
            await call.message.answer("⚠️ Тест сбился. /start")
            await state.clear()
            return

        _, _, action = (call.data or "").partition(":")
        if action not in ("yes", "no"):
            await call.answer("Ошибка")
            return

        await call.answer()
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Подсчет баллов
        if action == "yes":
            q_idx = order[idx]
            if isinstance(q_idx, int) and 0 <= q_idx < len(QUESTIONS):
                q_data = QUESTIONS[q_idx]
                weights = q_data.get("weights", {})
                if isinstance(weights, dict):
                    for cat, val in weights.items():
                        if cat in scores:
                            try:
                                scores[cat] += float(val)
                            except (ValueError, TypeError):
                                pass

        idx += 1

        if idx >= len(order):
            # === ФИНАЛ ===
            result_txt = get_result_text(scores)
            
            # Сохранение в БД
            try:
                await save_result_to_db(call.from_user.id, call.from_user.username, scores, result_txt)
            except Exception as e:
                logging.error(f"DB Error: {e}")

            # График
            chart_path = None
            try:
                chart_path = await generate_radar_chart(scores, call.from_user.id)
            except Exception as e:
                logging.error(f"Chart Error: {e}")

            sent = False
            if chart_path and os.path.exists(chart_path):
                try:
                    await call.message.answer_photo(
                        photo=FSInputFile(chart_path),
                        caption=f"✅ <b>Тест завершён!</b>\n\n{result_txt}",
                        parse_mode="HTML",
                        reply_markup=main_menu_kb
                    )
                    sent = True
                except Exception:
                    pass
            
            # Если фото не отправилось
            if not sent:
                await call.message.answer(
                    f"✅ <b>Тест завершён!</b>\n\n{result_txt}",
                    parse_mode="HTML",
                    reply_markup=main_menu_kb
                )

            # Очистка
            if chart_path and os.path.exists(chart_path):
                try:
                    os.remove(chart_path)
                except OSError:
                    pass
            
            await state.clear()
        else:
            await state.update_data(index=idx, scores=scores)
            await send_next_question(call.message, state)

@router.message()
async def default_handler(message: Message):
    await message.answer("Используйте меню для навигации.", reply_markup=main_menu_kb)

# =========================
# ЗАПУСК
# =========================

async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    logging.info("Бот запущен...")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен.")