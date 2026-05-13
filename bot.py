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
import json
import logging
import threading
import random
import string
import uuid
from datetime import datetime, timezone, timedelta

import telebot
from telebot import types
from flask import Flask, abort, request as flask_request

# =============================================================================
# ЛОГИРОВАНИЕ
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ticket-bot")

# =============================================================================
# КОНФИГУРАЦИЯ — проверка переменных окружения при старте
# =============================================================================
def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        log.critical("Переменная окружения '%s' не задана. Задай в Render → Environment.", name)
        raise SystemExit(1)
    return value

BOT_TOKEN     = _require_env("BOT_TOKEN")
RENDER_URL    = _require_env("RENDER_URL")
# GitHub API — нужны для персистентного whitelist.
# GITHUB_TOKEN  — Personal Access Token (classic), scope: repo
# GITHUB_OWNER  — твой GitHub username (например: maksleny)
# GITHUB_REPO   — название репозитория (например: t-ticket)
# GITHUB_BRANCH — ветка (обычно: main)
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
GITHUB_OWNER  = os.environ.get("GITHUB_OWNER", "")
GITHUB_REPO   = os.environ.get("GITHUB_REPO", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template.html")
MSK           = timezone(timedelta(hours=3))

log.info("Конфигурация загружена. RENDER_URL=%s", RENDER_URL)

# =============================================================================
# ДОСТУП
# ADMIN_IDS — полный доступ: бот + команда /admin.
# USER_IDS  — только доступ к боту и созданию билетов.
# Узнать Telegram ID можно у @userinfobot.
# =============================================================================
ADMIN_IDS: set[int] = {
    2021457397,   # владелец бота
}
USER_IDS: set[int] = {
    6176826288,   # Савелий
}

# =============================================================================
# ДИНАМИЧЕСКИЙ WHITELIST — персистентный через GitHub
#
# Как это работает:
#   /allow <id>  →  бот добавляет id в USER_IDS прямо в bot.py на GitHub
#                   через GitHub Contents API (PUT /contents/bot.py).
#   /deny  <id>  →  бот удаляет id из USER_IDS в bot.py на GitHub.
#   Render автоматически передёплоивает сервис при пуше в репо
#   (если настроен Auto-Deploy) — изменения вступают в силу сразу.
#   Даже без авто-деплоя изменения переживают рестарт: при следующем
#   деплое Render возьмёт актуальный bot.py с GitHub.
#
# Переменные окружения (задать в Render Dashboard):
#   GITHUB_TOKEN  — Personal Access Token, scope: repo
#   GITHUB_OWNER  — твой GitHub username
#   GITHUB_REPO   — название репозитория
#   GITHUB_BRANCH — ветка (обычно main)
# =============================================================================

dynamic_allowed_users: set[int] = set()   # runtime-кэш (актуален до рестарта)


def _github_get_bot_py() -> tuple[str, str]:
    """
    Читает bot.py из GitHub репозитория.
    Возвращает (содержимое_файла, sha) — sha нужен для последующего PUT.
    Бросает RuntimeError если GitHub API недоступен или токен не задан.
    """
    import http.client, base64, urllib.parse
    if not all([GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO]):
        raise RuntimeError(
            "GitHub не настроен. Задай GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO "
            "в переменных окружения Render."
        )
    path = f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/bot.py"
    if GITHUB_BRANCH != "main":
        path += f"?ref={urllib.parse.quote(GITHUB_BRANCH)}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
        "User-Agent":    "TelegramTicketBot/4.0",
    }
    conn = http.client.HTTPSConnection("api.github.com")
    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        body = resp.read()
    finally:
        conn.close()
    if resp.status != 200:
        raise RuntimeError(f"GitHub GET bot.py → HTTP {resp.status}: {body[:200].decode('utf-8','replace')}")
    import json as _json
    data    = _json.loads(body)
    content = base64.b64decode(data["content"]).decode("utf-8")
    sha     = data["sha"]
    return content, sha


def _github_put_bot_py(content: str, sha: str, commit_msg: str) -> None:
    """
    Пушит обновлённый bot.py в GitHub репозиторий.
    content — полное содержимое файла (строка).
    sha     — текущий SHA файла (получен из _github_get_bot_py).
    """
    import http.client, base64, json as _json, urllib.parse
    path = f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/bot.py"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
        "Content-Type":  "application/json",
        "User-Agent":    "TelegramTicketBot/4.0",
    }
    body_data = _json.dumps({
        "message": commit_msg,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "sha":     sha,
        "branch":  GITHUB_BRANCH,
    }, ensure_ascii=True).encode("utf-8")
    conn = http.client.HTTPSConnection("api.github.com")
    try:
        conn.request("PUT", path, body=body_data,
                     headers={**headers, "Content-Length": str(len(body_data))})
        resp = conn.getresponse()
        body = resp.read()
    finally:
        conn.close()
    if resp.status not in (200, 201):
        raise RuntimeError(f"GitHub PUT bot.py → HTTP {resp.status}: {body[:200].decode('utf-8','replace')}")


def _whitelist_add(user_id: int) -> None:
    """
    Добавляет user_id в USER_IDS в bot.py на GitHub.
    Алгоритм:
      1. Читаем bot.py с GitHub.
      2. Находим строку USER_IDS: set[int] = { ... } и вставляем новый id.
      3. Пушим изменённый файл обратно.
      4. Обновляем runtime-кэш dynamic_allowed_users.
    """
    import re
    content, sha = _github_get_bot_py()

    # Ищем блок USER_IDS = { ... }  (может занимать несколько строк)
    pattern = r'(USER_IDS\s*:\s*set\[int\]\s*=\s*\{)([^}]*)(\})'
    m = re.search(pattern, content, re.DOTALL)
    if not m:
        raise RuntimeError("Не найден блок USER_IDS в bot.py. Проверь формат файла.")

    block_inner = m.group(2)  # всё что внутри { ... }

    # Проверяем, нет ли уже такого id
    if re.search(rf'\b{user_id}\b', block_inner):
        dynamic_allowed_users.add(user_id)
        return  # уже есть

    # Добавляем новую строку перед закрывающей скобкой
    new_line = f"\n    {user_id},"
    new_block = m.group(1) + block_inner.rstrip() + new_line + "\n" + m.group(3)
    content   = content[:m.start()] + new_block + content[m.end():]

    _github_put_bot_py(content, sha, f"whitelist: add {user_id}")
    dynamic_allowed_users.add(user_id)
    log.info("whitelist: добавлен %s", user_id)


def _whitelist_remove(user_id: int) -> None:
    """
    Удаляет user_id из USER_IDS в bot.py на GitHub.
    """
    import re
    content, sha = _github_get_bot_py()

    pattern = r'(USER_IDS\s*:\s*set\[int\]\s*=\s*\{)([^}]*)(\})'
    m = re.search(pattern, content, re.DOTALL)
    if not m:
        raise RuntimeError("Не найден блок USER_IDS в bot.py.")

    block_inner = m.group(2)

    if not re.search(rf'\b{user_id}\b', block_inner):
        dynamic_allowed_users.discard(user_id)
        return  # и так нет

    # Удаляем строку с этим id (с опциональным комментарием)
    block_inner = re.sub(
        rf'[ \t]*{user_id}[,]?[ \t]*(#[^\n]*)?\n?',
        '',
        block_inner,
    )
    new_block = m.group(1) + block_inner + m.group(3)
    content   = content[:m.start()] + new_block + content[m.end():]

    _github_put_bot_py(content, sha, f"whitelist: remove {user_id}")
    dynamic_allowed_users.discard(user_id)
    log.info("whitelist: удалён %s", user_id)




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



def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def is_allowed(user_id: int) -> bool:
    return user_id in ADMIN_IDS or user_id in USER_IDS or user_id in dynamic_allowed_users

def check_access(message: types.Message) -> bool:
    if not is_allowed(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ У вас нет доступа к этому боту.")
        return False
    return True

def check_admin(message: types.Message) -> bool:
    if not is_admin(message.from_user.id):
        bot.send_message(message.chat.id, "⛔ Эта команда доступна только администратору.")
        return False
    return True


def notify_admins_about_unauthorized_start(message: types.Message) -> None:
    username = message.from_user.username or "—"
    name = f"{message.from_user.first_name or '—'} {message.from_user.last_name or ''}".strip()
    text = (
        "⚠️ Внимание! Неавторизованный пользователь попытался запустить бота:\n"
        f"user_id: `{message.from_user.id}`\n"
        f"username: @{username}\n"
        f"имя: {name}\n"
        f"сообщение: `{message.text or '—'}`"
    )
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(admin_id, text, parse_mode="Markdown")
        except Exception as e:
            log.warning("Не удалось уведомить админа %s: %s", admin_id, e)

# =============================================================================
# ХРАНИЛИЩЕ СОСТОЯНИЙ
# =============================================================================

user_data: dict[int, dict] = {}

# Лог событий — последние 500 действий
event_log: list[dict] = []
MAX_LOG = 500

def log_event(user, action: str) -> None:
    event_log.append({
        "time":     datetime.now(MSK),
        "user_id":  user.id,
        "username": user.username or "—",
        "name":     f"{user.first_name or ''} {user.last_name or ''}".strip() or "—",
        "action":   action,
    })
    if len(event_log) > MAX_LOG:
        event_log.pop(0)


def get_user(uid: int, tg_user=None) -> dict:
    if uid not in user_data:
        user_data[uid] = {
            "last":          None,
            "favorites":     [],
            "state":         None,
            "payment_unix":  None,
            "username":      "—",
            "name":          "—",
            "tickets_count": 0,
            "first_seen":    datetime.now(MSK).strftime("%d.%m.%Y %H:%M"),
        }
    if tg_user is not None:
        user_data[uid]["username"] = tg_user.username or "—"
        user_data[uid]["name"] = (
            f"{tg_user.first_name or ''} {tg_user.last_name or ''}".strip() or "—"
        )
    return user_data[uid]

def reset_state(uid: int) -> None:
    get_user(uid)["state"] = None


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
    user = get_user(message.chat.id, message.from_user)

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
        return

    # Чистим устаревшие билеты перед добавлением нового
    _cleanup_expired_tickets()

    token = uuid.uuid4().hex
    # Билет живёт 1 час (3600 секунд) с момента генерации
    expires_at = datetime.now(timezone.utc).timestamp() + 3600
    ticket_store[token] = (html_bytes, expires_at)

    log_event(message.from_user, f"билет №{route} ТС {vehicle}")
    user["tickets_count"] = user.get("tickets_count", 0) + 1

    bot.send_message(
        message.chat.id,
        f"🎫 *Билет готов!*\nМаршрут: №{route} · ТС: {vehicle}",
        parse_mode="Markdown",
        reply_markup=ticket_keyboard(token, route),
    )


# =============================================================================
# ОБРАБОТЧИКИ БОТА
# =============================================================================

@bot.message_handler(commands=["start"])
def handle_start(message: types.Message):
    try:
        if not is_allowed(message.from_user.id):
            notify_admins_about_unauthorized_start(message)
            bot.send_message(message.chat.id, "⛔ У вас нет доступа к этому боту.")
            return

        reset_state(message.from_user.id)
        get_user(message.from_user.id, message.from_user)
        log_event(message.from_user, "/start")
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
        log.exception("Ошибка в handle_start")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка инициализации бота.")
        except:
            pass


@bot.message_handler(commands=["help"])
def handle_help(message: types.Message):
    try:
        if not check_access(message): return
        reset_state(message.from_user.id)
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
        bot.send_message(
            message.chat.id,
            help_text,
            parse_mode="Markdown",
        )
    except Exception as e:
        log.exception("Ошибка в handle_help")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при выводе справки.")
        except:
            pass


@bot.message_handler(commands=["status"])
def handle_status(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id, message.from_user)
        
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
        log.exception("Ошибка в handle_status")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при получении статуса.")
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "🎫 Новый билет")
def handle_new_ticket(message: types.Message):
    try:
        if not check_access(message): return
        get_user(message.from_user.id, message.from_user)["state"] = "awaiting_input"
        bot.send_message(
            message.chat.id,
            "Введи *маршрут* и *номер ТС* через пробел.\n\nПример: `10А 1140`",
            parse_mode="Markdown",
        )
    except Exception as e:
        log.exception("Ошибка в handle_new_ticket")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка инициализации.")
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "🔁 Повторить последний")
def handle_repeat_last(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id, message.from_user)
        if not user["last"]:
            bot.send_message(message.chat.id, "⚠️ Нет данных о последнем билете. Сначала создай новый.")
            return
        route, vehicle = user["last"]
        _send_ticket(message, route, vehicle)
    except Exception as e:
        log.exception("Ошибка в handle_repeat_last")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при повторении билета.")
        except:
            pass


@bot.message_handler(func=lambda m: m.text == "⭐ Избранное")
def handle_favorites(message: types.Message):
    if not check_access(message): return
    user = get_user(message.from_user.id, message.from_user)
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
        log.exception("Ошибка в handle_help_button")
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
            "🤖 *Транспорт Плюс*\n"
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
        log.exception("Ошибка в handle_about")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при открытии информации о боте.")
        except:
            pass


@bot.message_handler(func=lambda m: get_user(m.from_user.id, m.from_user).get("state") == "awaiting_input")
def handle_input(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id, message.from_user)
        parts = message.text.strip().split()
        if len(parts) != 2:
            bot.send_message(
                message.chat.id,
                "❌ Неверный формат. Введи ровно два значения через пробел.\nПример: `10А 1140`",
                parse_mode="Markdown",
            )
            return
        user["state"] = None
        route, vehicle = parts[0].upper(), parts[1]
        _send_ticket(message, route, vehicle, is_new_ticket=True)
    except Exception as e:
        log.exception("Ошибка в handle_input")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка обработки. Попробуй ещё раз.")
        except:
            pass


@bot.message_handler(
    func=lambda m: str(get_user(m.from_user.id, m.from_user).get("state", "")).startswith("awaiting_vehicle:")
)
def handle_vehicle_input(message: types.Message):
    try:
        if not check_access(message): return
        user  = get_user(message.from_user.id, message.from_user)
        route = user["state"].split(":", 1)[1]
        user["state"] = None
        _send_ticket(
            message, route, message.text.strip(),
            msg_date_override=int(datetime.now(timezone.utc).timestamp()),
        )
    except Exception as e:
        log.exception("Ошибка в handle_vehicle_input")
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
        log.exception("Ошибка в handle_fav_callback")
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
        log.exception("Ошибка парсинга индекса в handle_remove_fav_callback")
        try:
            bot.answer_callback_query(call.id, "⚠️ Ошибка при удалении.")
        except:
            pass
    except Exception as e:
        log.exception("Ошибка в handle_remove_fav_callback")
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
        log.exception("Ошибка в handle_add_fav_callback")
        try:
            bot.answer_callback_query(call.id, "⚠️ Ошибка добавления в избранное.")
        except:
            pass


# =============================================================================
# KEEP-ALIVE — не даём Render усыпить сервис (бесплатный план, 15 мин таймаут)
# =============================================================================
def _keepalive_loop():
    import time, urllib.request
    url = f"{RENDER_URL}/healthz"
    while True:
        time.sleep(600)
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                log.info("Keep-alive ping: %s", resp.status)
        except Exception as e:
            log.warning("Keep-alive ping failed: %s", e)

threading.Thread(target=_keepalive_loop, daemon=True, name="keepalive").start()


@bot.message_handler(commands=["cancel"])
def handle_cancel(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id, message.from_user)
        if user["state"] is None:
            bot.send_message(message.chat.id, "Нет активного действия для отмены.")
            return
        reset_state(message.from_user.id)
        bot.send_message(message.chat.id, "❌ Действие отменено.", reply_markup=main_keyboard())
    except Exception as e:
        log.exception("Ошибка в handle_cancel")


@bot.message_handler(commands=["admin"])
def handle_admin(message: types.Message):
    try:
        if not check_admin(message): return
        now       = datetime.now(timezone.utc).timestamp()
        today_msk = datetime.now(MSK).date()

        active_tickets = sum(1 for _, (_, exp) in ticket_store.items() if now <= exp)
        total_users    = len(user_data)
        tickets_today  = sum(
            1 for e in event_log
            if e["time"].date() == today_msk and "билет" in e["action"]
        )
        users_today = len({
            e["user_id"] for e in event_log if e["time"].date() == today_msk
        })

        last_events = event_log[-5:][::-1]
        events_text = "\n".join(
            f"  `{e['time'].strftime('%H:%M')}` @{e['username']} ({e['user_id']}) — {e['action']}"
            for e in last_events
        ) or "  нет событий"

        users_text = "\n".join(
            f"  {'👑' if uid in ADMIN_IDS else '👤'} `{uid}` @{d['username']} {d['name']} "
            f"| билетов: {d['tickets_count']} | с {d['first_seen']}"
            for uid, d in user_data.items()
        ) or "  нет пользователей"

        text = (
            "🛠 *Админ-панель*\n\n"
            f"👥 Всего пользователей: *{total_users}*\n"
            f"🎫 Билетов за сегодня: *{tickets_today}*\n"
            f"🟢 Активных билетов сейчас: *{active_tickets}*\n"
            f"📅 Активных пользователей сегодня: *{users_today}*\n\n"
            f"*Последние действия:*\n{events_text}\n\n"
            f"*Все пользователи:*\n{users_text}"
        )
        bot.send_message(message.chat.id, text, parse_mode="Markdown")
        log.info("Админ-панель: user_id=%s", message.from_user.id)
    except Exception as e:
        log.exception("Ошибка в handle_admin")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при загрузке панели.")
        except Exception:
            pass


@bot.message_handler(commands=["ha"])
def handle_help_admin(message: types.Message):
    """Справка по командам администратора."""
    try:
        if not check_admin(message): return
        text = (
            "🛠 *Команды администратора:*\n\n"
            "/admin — *Админ-панель*\n"
            "  Статистика, логи, список пользователей\n\n"
            "/allow <user_id> — *Добавить пользователя*\n"
            "  Пример: `/allow 123456789`\n\n"
            "/deny <user_id> — *Удалить пользователя*\n"
            "  Пример: `/deny 123456789`\n\n"
            "/allowed — *Список всех разрешённых пользователей*\n\n"
            "/ha — *Эта справка*"
        )
        bot.send_message(message.chat.id, text, parse_mode="Markdown")
    except Exception as e:
        log.exception("Ошибка в handle_help_admin")


@bot.message_handler(commands=["allow"])
def handle_allow(message: types.Message):
    """Добавляет пользователя в список разрешённых."""
    try:
        if not check_admin(message): return
        
        args = message.text.split()
        if len(args) != 2:
            bot.send_message(message.chat.id, "❌ Формат: `/allow <user_id>`\nПример: `/allow 123456789`", parse_mode="Markdown")
            return
        
        try:
            user_id = int(args[1])
        except ValueError:
            bot.send_message(message.chat.id, "❌ user_id должен быть числом.", parse_mode="Markdown")
            return
        
        if user_id in ADMIN_IDS:
            bot.send_message(message.chat.id, f"⚠️ Пользователь `{user_id}` уже админ.", parse_mode="Markdown")
            return
        
        if user_id in dynamic_allowed_users or user_id in USER_IDS:
            bot.send_message(message.chat.id, f"⚠️ Пользователь `{user_id}` уже в списке разрешённых.", parse_mode="Markdown")
            return

        wait = bot.send_message(message.chat.id, "⏳ Обновляю whitelist на GitHub...")
        try:
            _whitelist_add(user_id)
        except RuntimeError as e:
            bot.edit_message_text(f"❌ Ошибка GitHub:\n{e}", message.chat.id, wait.message_id)
            return
        log_event(message.from_user, f"разрешил доступ {user_id}")
        bot.edit_message_text(
            f"✅ Пользователь `{user_id}` добавлен.\n\n"
            f"Изменение сохранено в bot.py на GitHub и переживёт любой рестарт Render.\n"
            f"Если включён Auto-Deploy — сервис передеплоится автоматически.",
            message.chat.id, wait.message_id, parse_mode="Markdown",
        )
        log.info("Добавлен пользователь %s админом %s", user_id, message.from_user.id)
    except Exception as e:
        log.exception("Ошибка в handle_allow")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при добавлении пользователя.")
        except Exception:
            pass


@bot.message_handler(commands=["deny"])
def handle_deny(message: types.Message):
    """Удаляет пользователя из списка разрешённых."""
    try:
        if not check_admin(message): return
        
        args = message.text.split()
        if len(args) != 2:
            bot.send_message(message.chat.id, "❌ Формат: `/deny <user_id>`\nПример: `/deny 123456789`", parse_mode="Markdown")
            return
        
        try:
            user_id = int(args[1])
        except ValueError:
            bot.send_message(message.chat.id, "❌ user_id должен быть числом.", parse_mode="Markdown")
            return
        
        if user_id in ADMIN_IDS:
            bot.send_message(message.chat.id, "❌ Нельзя удалить админа.", parse_mode="Markdown")
            return
        
        if user_id not in dynamic_allowed_users and user_id not in USER_IDS:
            bot.send_message(message.chat.id, f"⚠️ Пользователь `{user_id}` не в USER_IDS.", parse_mode="Markdown")
            return

        wait = bot.send_message(message.chat.id, "⏳ Обновляю whitelist на GitHub...")
        try:
            _whitelist_remove(user_id)
        except RuntimeError as e:
            bot.edit_message_text(f"❌ Ошибка GitHub:\n{e}", message.chat.id, wait.message_id)
            return
        log_event(message.from_user, f"запретил доступ {user_id}")
        bot.edit_message_text(
            f"✅ Пользователь `{user_id}` удалён.\n\n"
            f"Изменение сохранено в bot.py на GitHub.",
            message.chat.id, wait.message_id, parse_mode="Markdown",
        )
        log.info("Удалён пользователь %s админом %s", user_id, message.from_user.id)
    except Exception as e:
        log.exception("Ошибка в handle_deny")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при удалении пользователя.")
        except Exception:
            pass


@bot.message_handler(commands=["allowed"])
def handle_allowed(message: types.Message):
    """Показывает список всех разрешённых пользователей."""
    try:
        if not check_admin(message): return
        
        total_admin = len(ADMIN_IDS)
        total_static = len(USER_IDS)
        total_dynamic = len(dynamic_allowed_users)
        total = total_admin + total_static + total_dynamic
        
        lines = [
            "📋 *Список разрешённых пользователей:*\n",
            f"👑 *Администраторы ({total_admin}):*"
        ]
        for uid in sorted(ADMIN_IDS):
            name = user_data.get(uid, {}).get("name", "—")
            lines.append(f"  `{uid}` {name}")
        
        lines.append(f"\n👤 *Статические пользователи ({total_static}):*")
        for uid in sorted(USER_IDS):
            name = user_data.get(uid, {}).get("name", "—")
            lines.append(f"  `{uid}` {name}")
        
        lines.append(f"\n🆕 *Динамические пользователи ({total_dynamic}):*")
        if dynamic_allowed_users:
            for uid in sorted(dynamic_allowed_users):
                name = user_data.get(uid, {}).get("name", "—")
                lines.append(f"  `{uid}` {name}")
        else:
            lines.append("  нет")
        
        lines.append(f"\n📊 *Итого: {total} пользователей*")
        
        bot.send_message(message.chat.id, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        log.exception("Ошибка в handle_allowed")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при загрузке списка.")
        except Exception:
            pass


# =============================================================================
# ЗАПУСК
# =============================================================================

def setup_webhook():
    webhook_url = f"{RENDER_URL}/webhook/{BOT_TOKEN}"
    bot.remove_webhook()
    bot.set_webhook(url=webhook_url)
    log.info("Webhook установлен: %s", webhook_url)


# Вызываем при импорте — gunicorn не запускает __main__
setup_webhook()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    log.info("Сервер запущен на порту %s", port)
    flask_app.run(host="0.0.0.0", port=port)
