import asyncio
import logging
import sqlite3
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
    URLInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

logging.basicConfig(level=logging.INFO)

# ╔══════════════════════════════════════════╗
#   ⚙️  НАСТРОЙКИ — ИЗМЕНИТЕ ПОД СЕБЯ
# ╚══════════════════════════════════════════╝

TOKEN    = "8735847211:AAFgiY61R6jc7yx4mpKNziXyIKKFJX0z20o"
ADMIN_ID = 8677023495

# Фото-баннер: путь к файлу, URL или None
WELCOME_PHOTO = "banner.png"

SHOP_NAME    = "🌲 SHOP"
SHOP_TAGLINE = "Товары для настоящих ценителей природы!"

DB_PATH = "shop.db"   # файл базы данных (создаётся автоматически)

# ════════════════════════════════════════════

bot     = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp      = Dispatcher(storage=storage)


# ════════════════════════════════════════════
#   СОСТОЯНИЯ FSM
# ════════════════════════════════════════════

class AdminStates(StatesGroup):
    waiting_for_broadcast = State()


# ════════════════════════════════════════════
#   БАЗА ДАННЫХ (SQLite)
# ════════════════════════════════════════════

def db_connect():
    return sqlite3.connect(DB_PATH)


def db_init():
    """Создаёт таблицы при первом запуске"""
    with db_connect() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id           INTEGER PRIMARY KEY,
                username     TEXT,
                first_name   TEXT,
                last_name    TEXT,
                registered_at TEXT,
                blocked      INTEGER DEFAULT 0
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                status     TEXT DEFAULT 'waiting',
                created_at TEXT,
                rating     INTEGER DEFAULT NULL
            )
        """)
        con.commit()


def db_get_user(uid: int):
    with db_connect() as con:
        row = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    return row


def db_register_user(user: types.User):
    now_str = now()
    with db_connect() as con:
        exists = con.execute("SELECT id FROM users WHERE id=?", (user.id,)).fetchone()
        if not exists:
            con.execute(
                "INSERT INTO users VALUES (?,?,?,?,?,0)",
                (user.id, user.username, user.first_name, user.last_name, now_str)
            )
        else:
            con.execute(
                "UPDATE users SET username=?, first_name=? WHERE id=?",
                (user.username, user.first_name, user.id)
            )
        con.commit()


def db_set_blocked(uid: int, blocked: bool):
    with db_connect() as con:
        con.execute("UPDATE users SET blocked=? WHERE id=?", (int(blocked), uid))
        con.commit()


def db_is_blocked(uid: int) -> bool:
    row = db_get_user(uid)
    return bool(row[5]) if row else False


def db_add_request(user_id: int) -> int:
    with db_connect() as con:
        cur = con.execute(
            "INSERT INTO requests (user_id, status, created_at) VALUES (?,?,?)",
            (user_id, "waiting", now())
        )
        con.commit()
        return cur.lastrowid


def db_update_request_status(req_id: int, status: str):
    with db_connect() as con:
        con.execute("UPDATE requests SET status=? WHERE id=?", (status, req_id))
        con.commit()


def db_set_rating(req_id: int, rating: int):
    with db_connect() as con:
        con.execute("UPDATE requests SET rating=? WHERE id=?", (rating, req_id))
        con.commit()


def db_get_stats():
    with db_connect() as con:
        total_users    = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_requests = con.execute("SELECT COUNT(*) FROM requests").fetchone()[0]
        completed      = con.execute("SELECT COUNT(*) FROM requests WHERE status='completed'").fetchone()[0]
        rejected       = con.execute("SELECT COUNT(*) FROM requests WHERE status='rejected'").fetchone()[0]
        avg_rating_row = con.execute("SELECT AVG(rating) FROM requests WHERE rating IS NOT NULL").fetchone()
        avg_rating     = round(avg_rating_row[0], 1) if avg_rating_row[0] else "—"
    return total_users, total_requests, completed, rejected, avg_rating


def db_get_user_history(user_id: int):
    with db_connect() as con:
        rows = con.execute(
            "SELECT id, status, created_at, rating FROM requests WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (user_id,)
        ).fetchall()
    return rows


def db_get_all_users(limit=20):
    with db_connect() as con:
        rows = con.execute(
            "SELECT id, username, first_name, blocked FROM users LIMIT ?", (limit,)
        ).fetchall()
        total = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    return rows, total


def db_restore_queue() -> dict:
    """При старте восстанавливает активные/ожидающие заявки из БД в память"""
    restored = {}
    with db_connect() as con:
        # Сбрасываем зависшие заявки прошлых сессий
        con.execute(
            "UPDATE requests SET status='closed' WHERE status IN ('active','waiting')"
        )
        con.commit()
    return restored


# ════════════════════════════════════════════
#   ОЧЕРЕДЬ В ПАМЯТИ
# ════════════════════════════════════════════

# queue хранит активные/ожидающие заявки текущей сессии
# {request_id: {"user_id": int, "status": str, "created_at": str}}
queue = {}

# Словарь для хранения req_id ожидающего оценки для каждого клиента
# {user_id: req_id}
pending_ratings: dict[int, int] = {}


# ════════════════════════════════════════════
#   КЛАВИАТУРЫ
# ════════════════════════════════════════════

def kb_client_main():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬  Связаться с консультантом", callback_data="contact")],
    ])


def kb_admin_accept(request_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять",   callback_data=f"accept_{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{request_id}"),
        ],
        [
            InlineKeyboardButton(text="👤 Профиль",  callback_data=f"profile_{request_id}"),
            InlineKeyboardButton(text="📋 История",  callback_data=f"history_{request_id}"),
        ],
    ])


def kb_admin_chat(request_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data=f"profile_{request_id}"),
            InlineKeyboardButton(text="📋 История", callback_data=f"history_{request_id}"),
        ],
    ])


def kb_admin_reply():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Сделка проведена"),  KeyboardButton(text="🔚 Завершить диалог")],
            [KeyboardButton(text="👤 Профиль клиента"),   KeyboardButton(text="📋 История клиента")],
            [KeyboardButton(text="📊 Статистика"),         KeyboardButton(text="📢 Рассылка")],
        ],
        resize_keyboard=True,
    )


def kb_admin_panel():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Статистика",      callback_data="admin_stats"),
            InlineKeyboardButton(text="👥 Пользователи",    callback_data="admin_users"),
        ],
        [
            InlineKeyboardButton(text="📋 Активные заявки", callback_data="admin_active"),
            InlineKeyboardButton(text="📢 Рассылка",        callback_data="admin_broadcast"),
        ],
    ])


def kb_rating():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐",     callback_data="rate_1"),
            InlineKeyboardButton(text="⭐⭐",   callback_data="rate_2"),
            InlineKeyboardButton(text="⭐⭐⭐", callback_data="rate_3"),
        ],
        [
            InlineKeyboardButton(text="⭐⭐⭐⭐",   callback_data="rate_4"),
            InlineKeyboardButton(text="⭐⭐⭐⭐⭐", callback_data="rate_5"),
        ],
        [InlineKeyboardButton(text="Пропустить →", callback_data="rate_skip")],
    ])


# ════════════════════════════════════════════
#   ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ════════════════════════════════════════════

def now() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M")


def get_active_req():
    for req_id, data in queue.items():
        if data["status"] == "active":
            return req_id, data
    return None, None


def fmt_profile(user_id: int) -> str:
    row = db_get_user(user_id)
    if not row:
        return "❌ Пользователь не найден в базе."
    uid, username, first_name, last_name, registered_at, blocked = row
    uname     = f"@{username}" if username else "—"
    full_name = f"{first_name} {last_name or ''}".strip()
    history   = db_get_user_history(user_id)
    total     = len(history)
    completed = sum(1 for r in history if r[1] == "completed")
    rejected  = sum(1 for r in history if r[1] == "rejected")
    ratings   = [r[3] for r in history if r[3]]
    avg_r     = f"{sum(ratings)/len(ratings):.1f} ⭐" if ratings else "—"
    status    = "🚫 Заблокирован" if blocked else "✅ Активен"
    return (
        f"┌─ 👤 <b>Профиль клиента</b>\n"
        f"│\n"
        f"│  🆔  ID: <code>{user_id}</code>\n"
        f"│  🙍  Имя: <b>{full_name}</b>\n"
        f"│  📱  Telegram: {uname}\n"
        f"│  📅  Регистрация: {registered_at}\n"
        f"│\n"
        f"│  📦  Всего заявок:  <b>{total}</b>\n"
        f"│  ✅  Выполнено:     <b>{completed}</b>\n"
        f"│  ❌  Отклонено:     <b>{rejected}</b>\n"
        f"│  ⭐  Средняя оценка: <b>{avg_r}</b>\n"
        f"│\n"
        f"└─ Статус: {status}\n\n"
        f"<i>Бан: /ban {user_id}   |   Разбан: /unban {user_id}</i>"
    )


def fmt_history(user_id: int) -> str:
    rows = db_get_user_history(user_id)
    if not rows:
        return "📋 История заявок пуста."
    ico_map = {"completed": "✅", "rejected": "❌", "active": "🔄", "waiting": "⏳"}
    lines   = ["📋 <b>История заявок</b> (последние 10)\n"]
    for req_id, status, created_at, rating in rows:
        ico      = ico_map.get(status, "❓")
        star_str = f"  {'⭐' * rating}" if rating else ""
        lines.append(f"{ico} <b>Заявка #{req_id}</b> — {created_at}{star_str}")
    return "\n".join(lines)


async def send_welcome(chat_id: int, first_name: str):
    text = (
        f"<b>{SHOP_NAME}</b>  |  <i>{SHOP_TAGLINE}</i>\n\n"
        f"👋 Привет, <b>{first_name}</b>!\n\n"
        f"Добро пожаловать в наш магазин!\n"
        f"Здесь вы можете получить консультацию\n"
        f"и оформить заказ быстро и удобно.\n\n"
        f"👇 Нажмите кнопку, чтобы начать:"
    )
    markup = kb_client_main()
    if WELCOME_PHOTO:
        try:
            photo = URLInputFile(WELCOME_PHOTO) if str(WELCOME_PHOTO).startswith("http") else FSInputFile(WELCOME_PHOTO)
            await bot.send_photo(chat_id, photo=photo, caption=text, reply_markup=markup)
            return
        except Exception as e:
            logging.warning(f"Фото не отправлено: {e}")
    await bot.send_message(chat_id, text, reply_markup=markup)


# ════════════════════════════════════════════
#   КОМАНДЫ
# ════════════════════════════════════════════

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    db_register_user(message.from_user)
    user = message.from_user
    if db_is_blocked(user.id):
        await message.answer(
            "🚫 <b>Доступ ограничен</b>\n\n"
            "Ваш аккаунт заблокирован.\n"
            "Если это ошибка — свяжитесь с поддержкой."
        )
        return
    if user.id == ADMIN_ID:
        await message.answer(
            f"🛡 <b>Добро пожаловать, {user.first_name}!</b>\n\n"
            f"Панель управления {SHOP_NAME} активна.",
            reply_markup=kb_admin_reply()
        )
        await message.answer("📋 <b>Панель управления:</b>", reply_markup=kb_admin_panel())
    else:
        await send_welcome(message.chat.id, user.first_name)


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("🛡 <b>Панель администратора</b>", reply_markup=kb_admin_panel())


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("ℹ️ <b>Использование:</b> <code>/ban ID</code>")
        return
    uid = int(parts[1])
    if not db_get_user(uid):
        await message.answer(f"❌ Пользователь <code>{uid}</code> не найден.")
        return
    if db_is_blocked(uid):
        await message.answer(f"⚠️ Пользователь <code>{uid}</code> уже заблокирован.")
        return
    db_set_blocked(uid, True)
    for req_id, data in list(queue.items()):
        if data["user_id"] == uid:
            queue.pop(req_id)
    row  = db_get_user(uid)
    name = row[2] if row else "—"
    await message.answer(
        f"🚫 <b>Пользователь заблокирован</b>\n\n"
        f"👤 {name} — <code>{uid}</code>\n\n"
        f"<i>Разблокировать: /unban {uid}</i>"
    )
    try:
        await bot.send_message(uid, "🚫 <b>Ваш аккаунт заблокирован.</b>")
    except Exception:
        pass


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("ℹ️ <b>Использование:</b> <code>/unban ID</code>")
        return
    uid = int(parts[1])
    if not db_get_user(uid):
        await message.answer(f"❌ Пользователь <code>{uid}</code> не найден.")
        return
    if not db_is_blocked(uid):
        await message.answer(f"⚠️ Пользователь <code>{uid}</code> не заблокирован.")
        return
    db_set_blocked(uid, False)
    row  = db_get_user(uid)
    name = row[2] if row else "—"
    await message.answer(f"✅ <b>Пользователь разблокирован!</b>\n\n👤 {name} — <code>{uid}</code>")
    try:
        await bot.send_message(uid, "✅ <b>Вы разблокированы!</b>\nДобро пожаловать обратно! 🌲")
        await send_welcome(uid, row[2])
    except Exception:
        pass


# ════════════════════════════════════════════
#   КЛИЕНТ — СВЯЗАТЬСЯ
# ════════════════════════════════════════════

@dp.callback_query(F.data == "contact")
async def cb_contact(callback: types.CallbackQuery):
    user = callback.from_user
    db_register_user(user)

    if db_is_blocked(user.id):
        await callback.answer("🚫 Вы заблокированы.", show_alert=True)
        return
    for data in queue.values():
        if data["user_id"] == user.id and data["status"] in ("waiting", "active"):
            await callback.answer("⏳ У вас уже есть активная заявка!", show_alert=True)
            return

    req_id = db_add_request(user.id)
    queue[req_id] = {"user_id": user.id, "status": "waiting", "created_at": now()}

    await callback.message.answer(
        f"🌿 <b>Заявка #{req_id} отправлена!</b>\n\n"
        f"⏳ Ожидайте — консультант скоро подключится…\n\n"
        f"<i>Обычно отвечаем в течение нескольких минут.\n"
        f"Пожалуйста, не закрывайте чат!</i>"
    )
    await callback.answer("✅ Заявка отправлена!")

    username  = f"@{user.username}" if user.username else "—"
    full_name = f"{user.first_name} {user.last_name or ''}".strip()
    await bot.send_message(
        ADMIN_ID,
        f"🔔 <b>Новая заявка #{req_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🙍 Имя:      <b>{full_name}</b>\n"
        f"📱 Username: {username}\n"
        f"🆔 ID:       <code>{user.id}</code>\n"
        f"🕐 Время:    {queue[req_id]['created_at']}\n"
        f"━━━━━━━━━━━━━━━━━━━━",
        reply_markup=kb_admin_accept(req_id),
    )


# ════════════════════════════════════════════
#   ПРИНЯТЬ / ОТКЛОНИТЬ
# ════════════════════════════════════════════

@dp.callback_query(F.data.startswith("accept_"))
async def cb_accept(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    req_id = int(callback.data.split("_")[1])
    if req_id not in queue:
        await callback.answer("❌ Заявка уже обработана или бот был перезапущен.", show_alert=True)
        return
    queue[req_id]["status"] = "active"
    db_update_request_status(req_id, "active")
    client_id = queue[req_id]["user_id"]
    await callback.message.edit_text(
        f"✅ <b>Заявка #{req_id} принята!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Клиент: <code>{client_id}</code>\n\n"
        f"<i>Пишите — сообщения идут напрямую клиенту.</i>",
        reply_markup=None,
    )
    await bot.send_message(ADMIN_ID, "💬 <b>Чат активен!</b>", reply_markup=kb_admin_reply())
    await bot.send_message(
        client_id,
        "🎉 <b>Консультант подключился!</b>\n\n"
        "Напишите ваш вопрос — мы здесь и готовы помочь! 👇",
    )
    await callback.answer("✅ Принято!")


@dp.callback_query(F.data.startswith("reject_"))
async def cb_reject(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    req_id = int(callback.data.split("_")[1])
    if req_id not in queue:
        await callback.answer("❌ Заявка уже обработана или бот был перезапущен.", show_alert=True)
        return
    client_id = queue[req_id]["user_id"]
    db_update_request_status(req_id, "rejected")
    await callback.message.edit_text(f"❌ <b>Заявка #{req_id} отклонена.</b>", reply_markup=None)
    await bot.send_message(
        client_id,
        "😔 <b>Заявка отклонена.</b>\n\n"
        "Попробуйте обратиться немного позже — мы всегда рады помочь! 🌿",
        reply_markup=kb_client_main(),
    )
    queue.pop(req_id)
    await callback.answer()


# ════════════════════════════════════════════
#   ПРОФИЛЬ / ИСТОРИЯ (инлайн)
# ════════════════════════════════════════════

@dp.callback_query(F.data.startswith("profile_"))
async def cb_profile(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    req_id = int(callback.data.split("_")[1])
    # Ищем user_id: сначала в очереди, потом в БД
    if req_id in queue:
        user_id = queue[req_id]["user_id"]
    else:
        with db_connect() as con:
            row = con.execute("SELECT user_id FROM requests WHERE id=?", (req_id,)).fetchone()
        if not row:
            await callback.answer("❌ Заявка не найдена.", show_alert=True)
            return
        user_id = row[0]
    await callback.answer()
    await bot.send_message(ADMIN_ID, fmt_profile(user_id))


@dp.callback_query(F.data.startswith("history_"))
async def cb_history(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    req_id = int(callback.data.split("_")[1])
    # Ищем user_id: сначала в очереди, потом в БД
    if req_id in queue:
        user_id = queue[req_id]["user_id"]
    else:
        with db_connect() as con:
            row = con.execute("SELECT user_id FROM requests WHERE id=?", (req_id,)).fetchone()
        if not row:
            await callback.answer("❌ Заявка не найдена.", show_alert=True)
            return
        user_id = row[0]
    await callback.answer()
    await bot.send_message(ADMIN_ID, fmt_history(user_id))


# ════════════════════════════════════════════
#   ОЦЕНКА ДИАЛОГА
# ════════════════════════════════════════════

@dp.callback_query(F.data.startswith("rate_"))
async def cb_rate(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    req_id  = pending_ratings.get(user_id)

    if callback.data == "rate_skip":
        await callback.message.edit_text(
            "Спасибо! Если появятся вопросы — мы всегда здесь. 🌿",
            reply_markup=None,
        )
        await callback.answer()
        pending_ratings.pop(user_id, None)
        return

    rating = int(callback.data.split("_")[1])
    stars  = "⭐" * rating

    if req_id:
        db_set_rating(req_id, rating)
        pending_ratings.pop(user_id, None)

    await callback.message.edit_text(
        f"🙏 <b>Спасибо за оценку!</b>\n\n"
        f"Ваша оценка: {stars}\n\n"
        f"Будем рады видеть вас снова в {SHOP_NAME}! 🌲",
        reply_markup=None,
    )
    await callback.answer("Спасибо!")

    # Уведомляем админа
    if req_id:
        await bot.send_message(
            ADMIN_ID,
            f"⭐ <b>Новая оценка по заявке #{req_id}</b>\n\n"
            f"Клиент поставил: {stars} ({rating}/5)"
        )


# ════════════════════════════════════════════
#   ИНЛАЙН ПАНЕЛЬ АДМИНИСТРАТОРА
# ════════════════════════════════════════════

@dp.callback_query(F.data == "admin_stats")
async def cb_stats(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    total_users, total_req, completed, rejected, avg_r = db_get_stats()
    active  = sum(1 for d in queue.values() if d["status"] == "active")
    waiting = sum(1 for d in queue.values() if d["status"] == "waiting")
    await callback.answer()
    await callback.message.answer(
        f"📊 <b>Статистика {SHOP_NAME}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👥  Пользователей:        <b>{total_users}</b>\n"
        f"📦  Всего заявок:         <b>{total_req}</b>\n"
        f"✅  Выполнено:            <b>{completed}</b>\n"
        f"❌  Отклонено:            <b>{rejected}</b>\n"
        f"⭐  Средняя оценка:       <b>{avg_r}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔄  Активных диалогов:   <b>{active}</b>\n"
        f"⏳  В ожидании:          <b>{waiting}</b>"
    )


@dp.callback_query(F.data == "admin_users")
async def cb_users(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    rows, total = db_get_all_users()
    if not rows:
        await callback.answer("Пользователей пока нет.", show_alert=True)
        return
    lines = [f"👥 <b>Пользователи ({total})</b>\n"]
    for uid, username, first_name, blocked in rows:
        uname = f"@{username}" if username else "—"
        ico   = "🚫" if blocked else "✅"
        lines.append(f"{ico} <b>{first_name}</b> {uname}\n   └ <code>{uid}</code>")
    if total > 20:
        lines.append(f"\n<i>...и ещё {total - 20}</i>")
    await callback.answer()
    await callback.message.answer("\n".join(lines))


@dp.callback_query(F.data == "admin_active")
async def cb_active_requests(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    if not queue:
        await callback.answer("Активных заявок нет.", show_alert=True)
        return
    ico_map = {"waiting": "⏳", "active": "🟢"}
    lines   = [f"📋 <b>Заявки в очереди ({len(queue)})</b>\n"]
    for req_id, data in queue.items():
        ico = ico_map.get(data["status"], "❓")
        lines.append(
            f"{ico} <b>Заявка #{req_id}</b>\n"
            f"   🆔 <code>{data['user_id']}</code>  🕐 {data['created_at']}"
        )
    await callback.answer()
    await callback.message.answer("\n\n".join(lines))


@dp.callback_query(F.data == "admin_broadcast")
async def cb_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await callback.message.answer(
        "📢 <b>Рассылка</b>\n\n"
        "Введите текст — его получат все активные пользователи.\n\n"
        "<i>Для отмены: /cancel</i>"
    )
    await state.set_state(AdminStates.waiting_for_broadcast)


# ════════════════════════════════════════════
#   РАССЫЛКА (FSM)
# ════════════════════════════════════════════

@dp.message(AdminStates.waiting_for_broadcast)
async def fsm_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text and message.text.strip() == "/cancel":
        await state.clear()
        await message.answer("❌ Рассылка отменена.", reply_markup=kb_admin_reply())
        return

    with db_connect() as con:
        uids = [
            r[0] for r in
            con.execute("SELECT id FROM users WHERE blocked=0 AND id!=?", (ADMIN_ID,)).fetchall()
        ]

    sent = failed = 0
    for uid in uids:
        try:
            await bot.send_message(
                uid,
                f"📢 <b>Сообщение от {SHOP_NAME}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{message.text}"
            )
            sent += 1
        except Exception:
            failed += 1

    await state.clear()
    await message.answer(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📨 Доставлено:     <b>{sent}</b>\n"
        f"❌ Не доставлено: <b>{failed}</b>",
        reply_markup=kb_admin_reply(),
    )


# ════════════════════════════════════════════
#   ОСНОВНОЙ ХЕНДЛЕР ЧАТА
# ════════════════════════════════════════════

@dp.message()
async def chat_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    text    = message.text or ""

    # ─── АДМИНИСТРАТОР ───────────────────────
    if user_id == ADMIN_ID:

        if text == "✅ Сделка проведена":
            req_id, data = get_active_req()
            if not data:
                await message.answer("⚠️ Нет активного диалога.")
                return
            client_id = data["user_id"]
            db_update_request_status(req_id, "completed")
            queue.pop(req_id)
            await message.answer(
                f"✅ <b>Сделка #{req_id} закрыта!</b>",
                reply_markup=ReplyKeyboardRemove(),
            )
            # Сохраняем req_id для оценки клиента
            pending_ratings[client_id] = req_id
            await bot.send_message(
                client_id,
                f"🎉 <b>Сделка успешно завершена!</b>\n\n"
                f"Спасибо, что выбрали {SHOP_NAME}! 💚\n\n"
                f"⭐ Пожалуйста, оцените качество консультации:",
                reply_markup=kb_rating(),
            )
            return

        if text == "🔚 Завершить диалог":
            req_id, data = get_active_req()
            if not data:
                await message.answer("⚠️ Нет активного диалога.")
                return
            db_update_request_status(req_id, "closed")
            await bot.send_message(
                data["user_id"],
                "🔚 <b>Диалог завершён.</b>\n\nЕсли остались вопросы — обращайтесь! 🌿",
                reply_markup=kb_client_main(),
            )
            await message.answer("🔚 <b>Диалог закрыт.</b>", reply_markup=ReplyKeyboardRemove())
            queue.pop(req_id)
            return

        if text == "👤 Профиль клиента":
            req_id, data = get_active_req()
            if not data:
                await message.answer("⚠️ Нет активного диалога.")
                return
            await message.answer(fmt_profile(data["user_id"]))
            return

        if text == "📋 История клиента":
            req_id, data = get_active_req()
            if not data:
                await message.answer("⚠️ Нет активного диалога.")
                return
            await message.answer(fmt_history(data["user_id"]))
            return

        if text == "📊 Статистика":
            total_users, total_req, completed, rejected, avg_r = db_get_stats()
            active  = sum(1 for d in queue.values() if d["status"] == "active")
            waiting = sum(1 for d in queue.values() if d["status"] == "waiting")
            await message.answer(
                f"📊 <b>Статистика {SHOP_NAME}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👥  Пользователей:        <b>{total_users}</b>\n"
                f"📦  Всего заявок:         <b>{total_req}</b>\n"
                f"✅  Выполнено:            <b>{completed}</b>\n"
                f"❌  Отклонено:            <b>{rejected}</b>\n"
                f"⭐  Средняя оценка:       <b>{avg_r}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🔄  Активных диалогов:   <b>{active}</b>\n"
                f"⏳  В ожидании:          <b>{waiting}</b>"
            )
            return

        # ✅ Рассылка через кнопку — устанавливает FSM-состояние
        if text == "📢 Рассылка":
            await message.answer(
                "📢 <b>Рассылка</b>\n\n"
                "Введите текст рассылки.\n"
                "<i>Для отмены — /cancel</i>"
            )
            await state.set_state(AdminStates.waiting_for_broadcast)
            return

        # Пересылка клиенту (текст + медиа)
        req_id, data = get_active_req()
        if data:
            cid = data["user_id"]
            if message.photo:
                await bot.send_photo(cid, message.photo[-1].file_id,
                                     caption=f"💬 <b>Консультант:</b>\n{message.caption or ''}")
            elif message.document:
                await bot.send_document(cid, message.document.file_id,
                                        caption=f"💬 <b>Консультант:</b>\n{message.caption or ''}")
            elif message.sticker:
                await bot.send_sticker(cid, message.sticker.file_id)
            elif message.voice:
                await bot.send_voice(cid, message.voice.file_id)
            elif text:
                await bot.send_message(cid, f"💬 <b>Консультант:</b>\n{text}")
        else:
            await message.answer("ℹ️ Нет активного диалога. Ожидайте заявок.")
        return

    # ─── КЛИЕНТ ──────────────────────────────
    db_register_user(message.from_user)

    if db_is_blocked(user_id):
        await message.answer("🚫 <b>Доступ ограничен.</b>\nВаш аккаунт заблокирован.")
        return

    for req_id, data in queue.items():
        if data["user_id"] == user_id and data["status"] == "active":
            name = message.from_user.first_name
            # Пересылаем медиа администратору
            if message.photo:
                await bot.send_photo(ADMIN_ID, message.photo[-1].file_id,
                                     caption=f"📸 <b>{name}:</b>\n{message.caption or ''}",
                                     reply_markup=kb_admin_chat(req_id))
            elif message.document:
                await bot.send_document(ADMIN_ID, message.document.file_id,
                                        caption=f"📎 <b>{name}:</b>\n{message.caption or ''}",
                                        reply_markup=kb_admin_chat(req_id))
            elif message.sticker:
                await bot.send_sticker(ADMIN_ID, message.sticker.file_id)
            elif message.voice:
                await bot.send_voice(ADMIN_ID, message.voice.file_id,
                                     caption=f"🎤 <b>{name}</b>")
            elif text:
                await bot.send_message(ADMIN_ID, f"💬 <b>{name}:</b>\n{text}",
                                       reply_markup=kb_admin_chat(req_id))
            return

    await message.answer(
        "🌿 У вас нет активной заявки.\n\n"
        "Нажмите кнопку ниже, чтобы связаться с консультантом:",
        reply_markup=kb_client_main(),
    )


# ════════════════════════════════════════════
#   ЗАПУСК
# ════════════════════════════════════════════

async def main():
    db_init()
    db_restore_queue()   # сбрасываем зависшие заявки прошлых сессий
    logging.info("🌲 Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
