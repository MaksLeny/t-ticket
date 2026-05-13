"""
Telegram-бот для генерации билетов об оплате проезда.
Хостинг: Render.com (Web Service)

Архитектура:
  - Flask принимает Telegram Webhook и отдаёт HTML-билеты по уникальным ссылкам.
  - HTML генерируется в памяти, хранится в словаре ticket_store.
  - Билет открывается через кнопку Web App прямо в Telegram (встроенный браузер).
  - JS-таймер работает корректно т.к. страница открывается по https://, не file://.

Переменные окружения (задаются в Render Dashboard → Environment):
  BOT_TOKEN    — токен бота от @BotFather
  RENDER_URL   — публичный URL сервиса, напр. https://my-bot.onrender.com
"""

import os
import random
import string
import uuid
import logging
from datetime import datetime, timezone, timedelta

import telebot
from telebot import types
from flask import Flask, abort, request as flask_request
import psycopg2
from psycopg2.extras import RealDictCursor
import pickle

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

BOT_TOKEN     = os.environ["BOT_TOKEN"]    # задать в Render → Environment
RENDER_URL    = os.environ["RENDER_URL"]   # напр. https://my-bot.onrender.com
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template.html")
MSK           = timezone(timedelta(hours=3))
DATABASE_URL  = os.environ.get("DATABASE_URL")  # PostgreSQL URL из Render

# =============================================================================
# WHITELIST — список Telegram user_id которым разрешено пользоваться ботом.
# Добавить нового пользователя: вписать его user_id в этот set.
# Узнать свой id можно у @userinfobot в Telegram.
# =============================================================================
WHITELIST: set[int] = {
    2021457397,   # владелец бота
}

# =============================================================================
# БАЗА ДАННЫХ
# =============================================================================

def init_db():
    """Инициализация PostgreSQL таблиц."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_ticket BYTEA,
                favorites BYTEA,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Таблица билетов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                token TEXT PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                route TEXT,
                vehicle TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                expires_at DOUBLE PRECISION
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✅ PostgreSQL база инициализирована")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")


def get_db_connection():
    """Получить соединение с PostgreSQL."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def save_user_data(user_id: int, username: str, first_name: str, last_ticket, favorites):
    """Сохранить/обновить пользователя."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO users (user_id, username, first_name, last_ticket, favorites, last_activity)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_ticket = EXCLUDED.last_ticket,
                favorites = EXCLUDED.favorites,
                last_activity = CURRENT_TIMESTAMP
        """, (user_id, username, first_name, 
              psycopg2.Binary(pickle.dumps(last_ticket)) if last_ticket else None,
              psycopg2.Binary(pickle.dumps(favorites)) if favorites else None))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"💾 Сохранён юзер {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения юзера: {e}")


def load_user_data(user_id: int):
    """Загрузить данные пользователя."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT last_ticket, favorites FROM users WHERE user_id = %s",
            (user_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if row:
            return (pickle.loads(row['last_ticket']) if row['last_ticket'] else None,
                    pickle.loads(row['favorites']) if row['favorites'] else [])
        return None, []
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки юзера: {e}")
        return None, []


def save_ticket(token: str, user_id: int, route: str, vehicle: str, expires_at: float):
    """Сохранить билет в БД."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO tickets (token, user_id, route, vehicle, expires_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (token, user_id, route, vehicle, expires_at))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"🎫 Билет {token[:8]} сохранён")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения билета: {e}")


def get_user_stats(user_id: int = None):
    """Получить статистику."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if user_id:
            cursor.execute("SELECT COUNT(*) FROM tickets WHERE user_id = %s", (user_id,))
        else:
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM users")
        
        result = cursor.fetchone()['count']
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики: {e}")
        return 0


def get_all_users_info():
    """Получить инфо о всех пользователях."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT user_id, username, first_name, created_at, 
                   (SELECT COUNT(*) FROM tickets WHERE tickets.user_id = users.user_id) as ticket_count
            FROM users
            ORDER BY last_activity DESC
        """)
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"❌ Ошибка получения всех юзеров: {e}")
        return []


# Инициализируем БД при запуске
if DATABASE_URL:
    init_db()
else:
    logger.warning("⚠️ DATABASE_URL не задана, работаем без БД")

# =============================================================================
# FLASK + БОТ
# =============================================================================

flask_app = Flask(__name__)
bot       = telebot.TeleBot(BOT_TOKEN, threaded=False)

# Хранилище HTML-билетов: token → (html_bytes, expires_at)
# expires_at — Unix-timestamp (UTC) после которого запись считается устаревшей.
ticket_store: dict[str, tuple] = {}


@flask_app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = telebot.types.Update.de_json(flask_request.get_json(force=True))
    bot.process_new_updates([update])
    return "ok", 200


@flask_app.route("/ticket/<token>")
def serve_ticket(token: str):
    entry = ticket_store.get(token)
    if entry is None:
        abort(404)
    html_bytes, expires_at = entry
    # Билет просрочен — отдаём 410 Gone вместо 404 чтобы пользователь понял почему
    if datetime.now(timezone.utc).timestamp() > expires_at:
        ticket_store.pop(token, None)
        abort(410)
    return html_bytes, 200, {"Content-Type": "text/html; charset=utf-8"}


@flask_app.route("/healthz")
def health():
    now = datetime.now(timezone.utc).timestamp()
    active = sum(1 for _, (_, exp) in ticket_store.items() if now <= exp)
    total  = len(ticket_store)
    return {"status": "ok", "tickets_active": active, "tickets_total": total}, 200


# =============================================================================
# ГЕНЕРАЦИЯ HTML
# =============================================================================

def generate_ticket_serial() -> str:
    return "QR" + "".join(random.choices(string.digits, k=12))


def generate_ticket_number(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S") + "".join(random.choices(string.digits, k=3))


def build_html(route: str, vehicle: str, payment_unix: int) -> bytes:
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    payment_dt   = datetime.fromtimestamp(payment_unix, tz=MSK)
    now_utc      = datetime.now(timezone.utc)
    pay_utc      = datetime.fromtimestamp(payment_unix, tz=timezone.utc)
    elapsed_secs = max(0, int((now_utc - pay_utc).total_seconds()))
    elapsed_str  = f"{(elapsed_secs % 3600) // 60:02d}:{elapsed_secs % 60:02d}"

    html = html.replace("{{ROUTE}}",         route)
    html = html.replace("{{VEHICLE}}",       vehicle)
    html = html.replace("{{TC}}",            vehicle)
    html = html.replace("{{DATETIME}}",      payment_dt.strftime("%d.%m.%Y %H:%M"))
    html = html.replace("{{ELAPSED}}",       elapsed_str)
    html = html.replace("{{T_PAY}}",         str(payment_unix))
    html = html.replace("{{TICKET_SERIAL}}", generate_ticket_serial())
    html = html.replace("{{TICKET_NUMBER}}", generate_ticket_number(payment_dt))
    html = html.replace("{{PRICE}}",         "53")

    # Адаптивный QR-код
    html = html.replace(
        'style="height:1880px;width:1880px;',
        'style="width:100%;max-width:100vw;height:auto;display:block;',
    )

    # Заменяем содержимое <script> на рабочий таймер (через find/slice, без re)
    sc_open  = "<script>"
    sc_close = "</script>"
    si = html.find(sc_open)
    ei = html.find(sc_close, si)
    if si != -1 and ei != -1:
        timer_js = (
            "\n(function() {\n"
            "  var p = " + str(payment_unix - 23) + ";\n"
            "  function pad(n){return n<10?'0'+n:''+n;}\n"
            "  function tick(){\n"
            "    var t=Math.max(0,Math.floor(Date.now()/1000)-p);\n"
            "    var h=Math.floor(t/3600);\n"
            "    var m=Math.floor((t%3600)/60);\n"
            "    var s=t%60;\n"
            "    var txt=(h>0?pad(h)+':':'')+pad(m)+':'+pad(s);\n"
            "    document.querySelectorAll('strong').forEach(function(el){\n"
            "      if(/^\\d{2}:/.test(el.textContent.trim())){el.textContent=txt;}\n"
            "    });\n"
            "  }\n"
            "  tick();setInterval(tick,1000);\n"
            "})();\n"
        )
        html = html[:si + len(sc_open)] + timer_js + html[ei:]

    return html.encode("utf-8")



def is_allowed(user_id: int) -> bool:
    """Возвращает True если пользователь есть в whitelist."""
    return user_id in WHITELIST


def validate_route(route: str) -> bool:
    """Валидация маршрута: 1-10 символов, буквы/цифры/кириллица."""
    if not (1 <= len(route) <= 10):
        return False
    # Разрешены: буквы (любые), цифры, символ '-' и 'А'-'Я'
    return all(c.isalnum() or c in '-' for c in route)


def validate_vehicle(vehicle: str) -> bool:
    """Валидация номера ТС: 1-10 символов, буквы/цифры."""
    if not (1 <= len(vehicle) <= 10):
        return False
    return all(c.isalnum() for c in vehicle)


def check_access(message: types.Message) -> bool:
    """
    Проверяет доступ. Если пользователя нет в whitelist —
    отправляет отказ и возвращает False. Используется в начале каждого хендлера.
    """
    if not is_allowed(message.from_user.id):
        bot.send_message(
            message.chat.id,
            "⛔ У вас нет доступа к этому боту.",
        )
        return False
    return True

# =============================================================================
# ХРАНИЛИЩЕ СОСТОЯНИЙ
# =============================================================================

user_data: dict[int, dict] = {}


def get_user(uid: int) -> dict:
    if uid not in user_data:
        # Пробуем загрузить из БД
        last_ticket, favorites = load_user_data(uid)
        user_data[uid] = {
            "last":         last_ticket,
            "favorites":    favorites,
            "state":        None,
            "payment_unix": None,
        }
    return user_data[uid]


# =============================================================================
# КЛАВИАТУРЫ
# =============================================================================

def main_keyboard() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(types.KeyboardButton("🎫 Новый билет"))
    kb.row(
        types.KeyboardButton("🔁 Повторить последний"),
        types.KeyboardButton("⭐ Избранное"),
    )
    kb.row(
        types.KeyboardButton("📋 Справка"),
        types.KeyboardButton("ℹ️ О боте"),
    )
    return kb


def ticket_keyboard(token: str, route: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(
        "🎫 Открыть билет",
        web_app=types.WebAppInfo(url=f"{RENDER_URL}/ticket/{token}"),
    ))
    kb.add(types.InlineKeyboardButton(
        "⭐ В избранное",
        callback_data=f"add_fav:{route}",
    ))
    return kb


def favorites_keyboard(favorites: list, edit_mode: bool = False) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    for i, route in enumerate(favorites):
        if edit_mode:
            kb.add(types.InlineKeyboardButton(
                text=f"❌ №{route}",
                callback_data=f"remove_fav:{i}",
            ))
        else:
            kb.add(types.InlineKeyboardButton(
                text=f"№{route}",
                callback_data=f"fav:{i}",
            ))
    if edit_mode:
        kb.add(types.InlineKeyboardButton("◀️ Назад", callback_data="fav:back"))
    else:
        kb.add(types.InlineKeyboardButton("✏️ Редактировать", callback_data="fav:edit"))
        kb.add(types.InlineKeyboardButton("❌ Закрыть", callback_data="fav:close"))
    return kb



def _cleanup_expired_tickets() -> int:
    """
    Удаляет из ticket_store все записи у которых истёк срок хранения.
    Вызывается при каждой генерации нового билета — O(n) по числу записей,
    но n мало (1 запись на пользователя в час → не более нескольких тысяч).
    Возвращает количество удалённых записей.
    """
    now = datetime.now(timezone.utc).timestamp()
    expired = [t for t, (_, exp) in ticket_store.items() if now > exp]
    for t in expired:
        ticket_store.pop(t, None)
    return len(expired)

# =============================================================================
# ЯДРО: ГЕНЕРАЦИЯ И ОТПРАВКА БИЛЕТА
# =============================================================================

def _send_ticket(
    message: types.Message,
    route: str,
    vehicle: str,
    msg_date_override: int | None = None,
    is_new_ticket: bool = False,
):
    user = get_user(message.chat.id)

    if msg_date_override is not None:
        payment_unix = msg_date_override
        user["payment_unix"] = payment_unix
    elif user["payment_unix"] is not None and not is_new_ticket:
        payment_unix = user["payment_unix"]
    else:
        payment_unix = message.date
        user["payment_unix"] = payment_unix

    user["last"] = (route, vehicle)

    try:
        html_bytes = build_html(route, vehicle, payment_unix)
    except FileNotFoundError:
        bot.send_message(message.chat.id, "❌ Файл template.html не найден.")
        logger.error("❌ template.html не найден")
        return

    # Чистим устаревшие билеты перед добавлением нового
    _cleanup_expired_tickets()

    token = uuid.uuid4().hex
    # Билет живёт 1 час (3600 секунд) с момента генерации
    expires_at = datetime.now(timezone.utc).timestamp() + 3600
    ticket_store[token] = (html_bytes, expires_at)
    
    # Сохраняем в БД
    save_ticket(token, message.from_user.id, route, vehicle, expires_at)
    save_user_data(
        message.from_user.id, 
        message.from_user.username or "unknown",
        message.from_user.first_name or "unknown",
        user["last"],
        user["favorites"]
    )

    bot.send_message(
        message.chat.id,
        f"🎫 *Билет готов!*\nМаршрут: №{route} · ТС: {vehicle}",
        parse_mode="Markdown",
        reply_markup=ticket_keyboard(token, route),
    )
    
    logger.info(f"🎫 Билет создан для юзера {message.from_user.id}: {route} {vehicle}")


# =============================================================================
# ОБРАБОТЧИКИ БОТА
# =============================================================================

@bot.message_handler(commands=["start"])
def handle_start(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id)
        
        # Сохраняем пользователя в БД при первом обращении
        save_user_data(
            message.from_user.id,
            message.from_user.username or "unknown",
            message.from_user.first_name or "unknown",
            user["last"],
            user["favorites"]
        )
        logger.info(f"👤 Новый пользователь (или возврат): {message.from_user.id}")
        
        welcome_text = (
            "👋 *Привет, добро пожаловать!*\n\n"
            "Я помогаю быстро генерировать уведомления об оплате проезда.\n\n"
            "💡 *Что я умею:*\n"
            "• Создавать билеты за секунду\n"
            "• Сохранять избранные маршруты\n"
            "• Повторно использовать последний билет\n\n"
            "Выбери действие ниже или используй /help для подробной справки."
        )
        bot.send_message(
            message.chat.id,
            welcome_text,
            parse_mode="Markdown",
            reply_markup=main_keyboard(),
        )
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_start: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка инициализации бота.")
        except:
            pass


@bot.message_handler(commands=["help"])
def handle_help(message: types.Message):
    try:
        if not check_access(message): return
        help_text = (
            "📚 *Справка по боту*\n\n"
            "*Основные команды:*\n"
            "🎫 *Новый билет* — создать билет (нужны маршрут и номер ТС)\n"
            "🔁 *Повторить последний* — быстро создать билет с теми же данными\n"
            "⭐ *Избранное* — управление сохранённными маршрутами\n\n"
            "*Примеры использования:*\n"
            "• Введи: `10А 1140`\n"
            "• После генерации жми «⭐ В избранное» для сохранения\n"
            "• В избранном можно отредактировать номер ТС\n\n"
            "*Технические детали:*\n"
            "• Билет действует 1 час с момента создания\n"
            "• Используй /status для информации о текущем билете\n"
        )
        
        # Для админа добавляем команду /admin
        if list(WHITELIST)[0] == message.from_user.id:
            help_text += "\n*Команды администратора:*\n/admin — открыть админ-панель"
        
        bot.send_message(
            message.chat.id,
            help_text,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_help: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при выводе справки.")
        except:
            pass


@bot.message_handler(commands=["admin"])
def handle_admin(message: types.Message):
    try:
        # Только для владельца
        if not is_allowed(message.from_user.id) or list(WHITELIST)[0] != message.from_user.id:
            bot.send_message(message.chat.id, "⛔ Эта команда доступна только администратору.")
            logger.warning(f"❌ Попытка доступа к /admin от {message.from_user.id}")
            return
        
        # Получаем статистику
        total_users = len(get_all_users_info())
        total_tickets = get_user_stats()
        
        admin_text = (
            "📊 *АДМИН ПАНЕЛЬ*\n\n"
            f"👥 *Всего пользователей:* {total_users}\n"
            f"🎫 *Всего билетов:* {total_tickets}\n\n"
            "*Последние пользователи:*\n"
        )
        
        users = get_all_users_info()[:10]  # Последние 10 юзеров
        if users:
            for user_id, username, first_name, created_at, ticket_count in users:
                username_str = f"@{username}" if username else "unknown"
                admin_text += f"\n• ID: `{user_id}`\n  Имя: {first_name} {username_str}\n  Билетов: {ticket_count}"
        else:
            admin_text += "\n(Нет пользователей)"
        
        bot.send_message(
            message.chat.id,
            admin_text,
            parse_mode="Markdown",
        )
        logger.info(f"📊 Админ {message.from_user.id} открыл панель")
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_admin: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при загрузке админ-панели.")
        except:
            pass


@bot.message_handler(commands=["status"])
def handle_status(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id)
        
        status_lines = ["📊 *Ваша информация:*\n"]
        
        if user["last"]:
            route, vehicle = user["last"]
            status_lines.append(f"🚌 Последний билет: №{route} · ТС {vehicle}")
        else:
            status_lines.append("🚌 Последний билет: не создан")
        
        if user["favorites"]:
            status_lines.append(f"⭐ Избранные маршруты: {', '.join(f'№{r}' for r in user['favorites'])}")
        else:
            status_lines.append("⭐ Избранные маршруты: нет")
        
        active_count = sum(1 for _, (_, exp) in ticket_store.items() 
                           if datetime.now(timezone.utc).timestamp() <= exp)
        status_lines.append(f"🎫 Активные билеты в системе: {active_count}")
        
        bot.send_message(
            message.chat.id,
            "\n".join(status_lines),
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"❌ Ошибка в handle_status: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при получении статуса.")
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "🎫 Новый билет")
def handle_new_ticket(message: types.Message):
    try:
        if not check_access(message): return
        get_user(message.from_user.id)["state"] = "awaiting_input"
        bot.send_message(
            message.chat.id,
            "Введи *маршрут* и *номер ТС* через пробел.\n\nПример: `10А 1140`",
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"❌ Ошибка в handle_new_ticket: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка инициализации.")
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "🔁 Повторить последний")
def handle_repeat_last(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id)
        if not user["last"]:
            bot.send_message(message.chat.id, "⚠️ Нет данных о последнем билете. Сначала создай новый.")
            return
        route, vehicle = user["last"]
        _send_ticket(message, route, vehicle)
    except Exception as e:
        print(f"❌ Ошибка в handle_repeat_last: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при повторении билета.")
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "⭐ Избранное")
def handle_favorites(message: types.Message):
    if not check_access(message): return
    user = get_user(message.from_user.id)
    if not user["favorites"]:
        bot.send_message(
            message.chat.id,
            "⭐ Список избранного пуст.\n\nПосле генерации нажми «⭐ В избранное».",
        )
        return
    bot.send_message(
        message.chat.id,
        f"⭐ *Избранные маршруты ({len(user['favorites'])}) шт.:*\n\nНажми на маршрут или выбери редактирование.",
        parse_mode="Markdown",
        reply_markup=favorites_keyboard(user["favorites"], edit_mode=False),
    )


@bot.message_handler(func=lambda m: m.text == "📋 Справка")
def handle_help_button(message: types.Message):
    try:
        if not check_access(message): return
        handle_help(message)
    except Exception as e:
        print(f"❌ Ошибка в handle_help_button: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при открытии справки.")
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "ℹ️ О боте")
def handle_about(message: types.Message):
    try:
        if not check_access(message): return
        about_text = (
            "ℹ️ *О боте*\n\n"
            "🤖 *Telegram Ticket Bot*\n"
            "Простой и быстрый инструмент для генерации уведомлений об оплате проезда.\n\n"
            "✨ *Возможности:*\n"
            "• Генерация QR-кодов и билетов за одну секунду\n"
            "• Сохранение избранных маршрутов\n"
            "• Автоматический таймер с момента оплаты\n"
            "• Безопасное хранилище (1 час)\n\n"
            "🔐 *Безопасность:*\n"
            "• Доступ только авторизованным пользователям\n"
            "• Билеты автоматически удаляются через час\n\n"
            "Введи /help для подробной справки."
        )
        bot.send_message(
            message.chat.id,
            about_text,
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"❌ Ошибка в handle_about: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при открытии информации о боте.")
        except:
            pass


@bot.message_handler(func=lambda m: get_user(m.from_user.id).get("state") == "awaiting_input")
def handle_input(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id)
        parts = message.text.strip().split()
        if len(parts) != 2:
            bot.send_message(
                message.chat.id,
                "❌ Неверный формат. Введи ровно два значения через пробел.\nПример: `10А 1140`",
                parse_mode="Markdown",
            )
            return
        
        route, vehicle = parts[0].upper(), parts[1].upper()
        
        # Валидация
        if not validate_route(route):
            bot.send_message(
                message.chat.id,
                "❌ Маршрут некорректен. Используй 1-10 символов (буквы/цифры/-).\nПример: `10А` или `5-З`",
                parse_mode="Markdown",
            )
            logger.warning(f"❌ Невалидный маршрут от {message.from_user.id}: {route}")
            return
        
        if not validate_vehicle(vehicle):
            bot.send_message(
                message.chat.id,
                "❌ Номер ТС некорректен. Используй 1-10 символов (буквы/цифры).\nПример: `1140`",
                parse_mode="Markdown",
            )
            logger.warning(f"❌ Невалидный ТС от {message.from_user.id}: {vehicle}")
            return
        
        user["state"] = None
        _send_ticket(message, route, vehicle, is_new_ticket=True)
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_input: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка обработки. Попробуй ещё раз.")
        except:
            pass


@bot.message_handler(
    func=lambda m: str(get_user(m.from_user.id).get("state", "")).startswith("awaiting_vehicle:")
)
def handle_vehicle_input(message: types.Message):
    try:
        if not check_access(message): return
        user  = get_user(message.from_user.id)
        route = user["state"].split(":", 1)[1]
        user["state"] = None
        _send_ticket(
            message, route, message.text.strip(),
            msg_date_override=int(datetime.now(timezone.utc).timestamp()),
        )
    except Exception as e:
        print(f"❌ Ошибка в handle_vehicle_input: {e}")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка обработки. Попробуй ещё раз.")
        except:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("fav:"))
def handle_fav_callback(call: types.CallbackQuery):
    try:
        user    = get_user(call.from_user.id)
        payload = call.data[4:]

        if payload == "close":
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot.answer_callback_query(call.id)
            return
        
        if payload == "edit":
            bot.edit_message_text(
                f"⭐ *Редактирование избранного ({len(user['favorites'])}) шт.:*\n\nНажми на маршрут для удаления:",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="Markdown",
                reply_markup=favorites_keyboard(user["favorites"], edit_mode=True),
            )
            bot.answer_callback_query(call.id)
            return
        
        if payload == "back":
            bot.edit_message_text(
                f"⭐ *Избранные маршруты ({len(user['favorites'])}) шт.:*\n\nНажми на маршрут или выбери редактирование.",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="Markdown",
                reply_markup=favorites_keyboard(user["favorites"], edit_mode=False),
            )
            bot.answer_callback_query(call.id)
            return

        idx = int(payload)
        if idx >= len(user["favorites"]):
            bot.answer_callback_query(call.id, "⚠️ Запись устарела.")
            return

        route = user["favorites"][idx]
        bot.answer_callback_query(call.id)
        bot.delete_message(call.message.chat.id, call.message.message_id)
        user["state"] = f"awaiting_vehicle:{route}"
        bot.send_message(
            call.message.chat.id,
            f"Маршрут №*{route}* выбран.\nВведи номер ТС:",
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"❌ Ошибка в handle_fav_callback: {e}")
        try:
            bot.answer_callback_query(call.id, "⚠️ Ошибка обработки. Попробуй ещё раз.")
        except:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("remove_fav:"))
def handle_remove_fav_callback(call: types.CallbackQuery):
    try:
        user = get_user(call.from_user.id)
        idx = int(call.data[11:])
        if idx >= len(user["favorites"]):
            bot.answer_callback_query(call.id, "⚠️ Запись устарела.")
            return
        
        removed_route = user["favorites"].pop(idx)
        bot.answer_callback_query(call.id, f"✅ Маршрут №{removed_route} удалён из избранного!")
        
        if not user["favorites"]:
            bot.edit_message_text(
                "⭐ Список избранного пуст.\n\nПосле генерации нажми «⭐ В избранное».",
                call.message.chat.id,
                call.message.message_id,
            )
        else:
            bot.edit_message_text(
                f"⭐ *Редактирование избранного ({len(user['favorites'])}) шт.:*\n\nНажми на маршрут для удаления:",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="Markdown",
                reply_markup=favorites_keyboard(user["favorites"], edit_mode=True),
            )
    except (ValueError, IndexError):
        print(f"❌ Ошибка парсинга индекса в handle_remove_fav_callback")
        try:
            bot.answer_callback_query(call.id, "⚠️ Ошибка при удалении.")
        except:
            pass
    except Exception as e:
        print(f"❌ Ошибка в handle_remove_fav_callback: {e}")
        try:
            bot.answer_callback_query(call.id, "⚠️ Ошибка обработки.")
        except:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("add_fav:"))
def handle_add_fav_callback(call: types.CallbackQuery):
    try:
        user  = get_user(call.from_user.id)
        route = call.data[8:]

        if route in user["favorites"]:
            bot.answer_callback_query(call.id, "⭐ Уже в избранном!")
            return

        user["favorites"].append(route)
        bot.answer_callback_query(call.id, f"✅ Маршрут №{route} добавлен в избранное!")

        try:
            bot.edit_message_reply_markup(
                call.message.chat.id,
                call.message.message_id,
                reply_markup=types.InlineKeyboardMarkup(),
            )
        except Exception:
            pass
    except Exception as e:
        print(f"❌ Ошибка в handle_add_fav_callback: {e}")
        try:
            bot.answer_callback_query(call.id, "⚠️ Ошибка добавления в избранное.")
        except:
            pass


# =============================================================================
# ЗАПУСК
# =============================================================================

def setup_webhook():
    webhook_url = f"{RENDER_URL}/webhook/{BOT_TOKEN}"
    bot.remove_webhook()
    bot.set_webhook(url=webhook_url)
    print(f"✅ Webhook установлен: {webhook_url}")


# Вызываем при импорте — gunicorn не запускает __main__
setup_webhook()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Сервер запущен на порту {port}")
    flask_app.run(host="0.0.0.0", port=port)
