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

BOT_TOKEN      = _require_env("BOT_TOKEN")
RENDER_URL     = _require_env("RENDER_URL")
# Telegram Mini App — Direct Link запуск.
# Задай в Render → Environment (или поменяй здесь если не меняются).
BOT_USERNAME   = os.environ.get("BOT_USERNAME",   "ticket_murmansk_bot")
APP_SHORT_NAME = os.environ.get("APP_SHORT_NAME", "ticket_murman")
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template.html")

# GitHub API — для персистентного user_data и whitelist.
# Получить токен: GitHub → Settings → Developer settings
#   → Personal access tokens → Tokens (classic) → scope: repo
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
GITHUB_OWNER  = os.environ.get("GITHUB_OWNER", "")
GITHUB_REPO   = os.environ.get("GITHUB_REPO", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
# Путь к файлу данных пользователей в репозитории
USERDATA_FILE = "user_data.json"
MSK           = timezone(timedelta(hours=3))
START_TIME    = datetime.now(timezone.utc)   # Момент запуска — для uptime в /admin
# Секрет для верификации Telegram webhook-запросов.
# Telegram добавляет заголовок X-Telegram-Bot-Api-Secret-Token к каждому POST.
# Мы проверяем его в /webhook/ — запросы без токена получают 403.
import hashlib as _hs
WEBHOOK_SECRET = _hs.sha256(BOT_TOKEN.encode()).hexdigest()[:32]

log.info("Конфигурация загружена. RENDER_URL=%s", RENDER_URL)

# =============================================================================
# ХРАНИЛИЩЕ СОСТОЯНИЙ — объявляем здесь, до первого использования в _load_user_data
# =============================================================================
user_data: dict[int, dict] = {}
MAX_LOG = 500

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
    1970426555,
}

# =============================================================================
# GITHUB API — чтение/запись файлов в репозиторий
# Используется для персистентного user_data и динамического whitelist
# =============================================================================

dynamic_allowed_users: set[int] = set()


def _gh_available() -> bool:
    """True если все переменные GitHub заданы."""
    return bool(GITHUB_TOKEN and GITHUB_OWNER and GITHUB_REPO)


def _gh_get_raw(path: str, _retries: int = 3, _delay: float = 3.0) -> tuple[str | None, str | None]:
    """
    Читает файл из репозитория как текст.
    Возвращает (содержимое, sha) или (None, None) при ошибке/404.
    При сетевых ошибках повторяет до _retries раз с паузой _delay секунд.
    """
    import http.client, base64 as _b64, urllib.parse as _up, time as _time
    if not _gh_available():
        return None, None
    encoded  = _up.quote(path, safe="/")
    url_path = "/repos/" + GITHUB_OWNER + "/" + GITHUB_REPO + "/contents/" + encoded
    if GITHUB_BRANCH != "main":
        url_path += "?ref=" + _up.quote(GITHUB_BRANCH)
    headers = {
        "Authorization": "token " + GITHUB_TOKEN,
        "Accept":        "application/vnd.github+json",
        "User-Agent":    "TelegramTicketBot/4.0",
    }
    last_exc = None
    for attempt in range(1, _retries + 1):
        conn = http.client.HTTPSConnection("api.github.com", timeout=10)
        try:
            conn.request("GET", url_path, headers=headers)
            resp = conn.getresponse()
            body = resp.read()
        except Exception as exc:
            last_exc = exc
            log.warning("GitHub GET %s — попытка %d/%d, ошибка: %s", path, attempt, _retries, exc)
            conn.close()
            if attempt < _retries:
                _time.sleep(_delay)
            continue
        finally:
            conn.close()
        if resp.status == 404:
            return None, None
        if resp.status == 200:
            meta = json.loads(body)
            return _b64.b64decode(meta["content"]).decode("utf-8"), meta["sha"]
        # 5xx — серверная ошибка GitHub, имеет смысл повторить
        if resp.status >= 500 and attempt < _retries:
            log.warning("GitHub GET %s -> HTTP %s — попытка %d/%d, повтор через %.0f сек",
                        path, resp.status, attempt, _retries, _delay)
            _time.sleep(_delay)
            continue
        log.warning("GitHub GET %s -> HTTP %s", path, resp.status)
        return None, None
    log.error("GitHub GET %s — все %d попытки исчерпаны. Последняя ошибка: %s", path, _retries, last_exc)
    return None, None


def _gh_put_raw(path: str, text_content: str, sha: str | None, commit_msg: str,
                _retries: int = 3, _delay: float = 3.0) -> bool:
    """
    Записывает текстовый файл в репозиторий.
    sha=None — создание нового файла.
    Возвращает True при успехе.
    При сетевых ошибках и 5xx повторяет до _retries раз с паузой _delay секунд.
    """
    import http.client, base64 as _b64, urllib.parse as _up, time as _time
    if not _gh_available():
        return False
    encoded   = _up.quote(path, safe="/")
    url_path  = "/repos/" + GITHUB_OWNER + "/" + GITHUB_REPO + "/contents/" + encoded
    raw_bytes = text_content.encode("utf-8")
    body_obj: dict = {
        "message": commit_msg,
        "content": _b64.b64encode(raw_bytes).decode("ascii"),
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        body_obj["sha"] = sha
    body_data = json.dumps(body_obj, ensure_ascii=True).encode("utf-8")
    headers = {
        "Authorization":  "token " + GITHUB_TOKEN,
        "Accept":         "application/vnd.github+json",
        "Content-Type":   "application/json",
        "Content-Length": str(len(body_data)),
        "User-Agent":     "TelegramTicketBot/4.0",
    }
    last_exc = None
    for attempt in range(1, _retries + 1):
        conn = http.client.HTTPSConnection("api.github.com", timeout=15)
        try:
            conn.request("PUT", url_path, body=body_data, headers=headers)
            resp = conn.getresponse()
            resp.read()
        except Exception as exc:
            last_exc = exc
            log.warning("GitHub PUT %s — попытка %d/%d, ошибка: %s", path, attempt, _retries, exc)
            conn.close()
            if attempt < _retries:
                _time.sleep(_delay)
            continue
        finally:
            conn.close()
        if resp.status in (200, 201):
            return True
        if resp.status >= 500 and attempt < _retries:
            log.warning("GitHub PUT %s -> HTTP %s — попытка %d/%d, повтор через %.0f сек",
                        path, resp.status, attempt, _retries, _delay)
            _time.sleep(_delay)
            continue
        log.warning("GitHub PUT %s -> HTTP %s", path, resp.status)
        return False
    log.error("GitHub PUT %s — все %d попытки исчерпаны. Последняя ошибка: %s", path, _retries, last_exc)
    return False


# =============================================================================
# ПЕРСИСТЕНТНЫЙ user_data
#
# Сохраняемые поля (user_data.json на GitHub):
#   favorites, tickets_count, first_seen, username, name
#
# Сессионные поля (только в памяти, не сохраняются):
#   state, payment_unix, last
#
# Схема работы:
#   Старт → _load_user_data() → читаем user_data.json с GitHub в память
#   Изменение → _save_user_data_async() → пишем в GitHub в фоновом потоке
# =============================================================================

_SAVE_FIELDS = {"favorites", "tickets_count", "first_seen", "username", "name", "added_at", "transport"}
_save_lock      = threading.Lock()
_user_data_lock = threading.Lock()  # защита от concurrent writes в user_data


def _default_user() -> dict:
    return {
        "last":          None,
        "favorites":     [],
        "state":         None,
        "payment_unix":  None,
        "username":      "—",
        "name":          "—",
        "tickets_count": 0,
        "first_seen":    datetime.now(MSK).strftime("%d.%m.%Y %H:%M"),
        "added_at":      datetime.now(MSK).strftime("%d.%m.%Y"),
        "transport":     "bus",
    }


def _load_user_data() -> None:
    """
    Загружает user_data из user_data.json на GitHub при старте сервиса.
    Если файла нет — начинаем с пустого словаря (первый запуск).
    Если GitHub не настроен — данные живут только в памяти (предупреждение в логах).
    """
    if not _gh_available():
        missing = [v for v in ("GITHUB_TOKEN","GITHUB_OWNER","GITHUB_REPO") if not os.environ.get(v)]
        log.warning(
            "GitHub не настроен — user_data НЕ будет персистентным! "
            "Не заданы переменные: %s", ", ".join(missing)
        )
        return
    log.info(
        "GitHub настроен: owner=%s repo=%s branch=%s — загружаю user_data...",
        GITHUB_OWNER, GITHUB_REPO, GITHUB_BRANCH,
    )
    raw, _ = _gh_get_raw(USERDATA_FILE)
    if raw is None:
        log.info("user_data.json не найден на GitHub — чистый старт")
        return
    try:
        stored = json.loads(raw)
    except Exception as e:
        log.warning("Ошибка парсинга user_data.json: %s", e)
        return
    count = 0
    for uid_str, fields in stored.items():
        uid = int(uid_str)
        if uid not in user_data:
            user_data[uid] = _default_user()
        for k in _SAVE_FIELDS:
            if k in fields:
                user_data[uid][k] = fields[k]
        count += 1
    log.info("Загружено %d пользователей из GitHub", count)


def _do_save_user_data() -> bool:
    """
    Синхронно сохраняет user_data в user_data.json на GitHub.
    Возвращает True при успехе.
    Вызывается с _save_lock уже захваченным.
    """
    _, sha = _gh_get_raw(USERDATA_FILE)
    # Снимок под блокировкой — защита от "dictionary changed size during iteration"
    with _user_data_lock:
        snapshot = {
            str(uid): {k: d[k] for k in _SAVE_FIELDS if k in d}
            for uid, d in user_data.items()
        }
    ts = datetime.now(MSK).strftime("%d.%m.%Y %H:%M")
    ok = _gh_put_raw(
        USERDATA_FILE,
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        sha,
        "user_data: update " + ts,
    )
    if ok:
        log.info("user_data сохранён на GitHub (%d пользователей)", len(snapshot))
    else:
        log.warning("Не удалось сохранить user_data на GitHub")
    return ok


def _save_user_data_sync() -> bool:
    """
    Блокирующее сохранение — используется для критичных событий
    (создание билета, изменение избранного).
    Выполняется в текущем потоке, возвращает True при успехе.
    Если GitHub не настроен — сразу возвращает False (без ошибки).
    """
    if not _gh_available():
        return False
    with _save_lock:
        return _do_save_user_data()


def _save_user_data_async() -> None:
    """
    Неблокирующее сохранение — запускает _save_user_data_sync в отдельном
    НЕ-daemon потоке. Не-daemon важно: gunicorn дожидается завершения
    таких потоков перед остановкой воркера, поэтому запись успевает дойти
    до GitHub даже при плановом рестарте.
    """
    def _worker():
        _save_user_data_sync()
    threading.Thread(target=_worker, daemon=False, name="save-userdata").start()


# =============================================================================
# ДИНАМИЧЕСКИЙ WHITELIST — патчит USER_IDS в bot.py на GitHub
# =============================================================================

def _whitelist_add(user_id: int) -> None:
    """
    Добавляет user_id в блок USER_IDS: set[int] = { ... } в bot.py на GitHub.
    После пуша Render передеплоится (если включён Auto-Deploy).
    """
    import re
    content, sha = _gh_get_raw("bot.py")
    if content is None:
        raise RuntimeError("Не удалось прочитать bot.py с GitHub.")
    pattern = r'(USER_IDS\s*:\s*set\[int\]\s*=\s*\{)([^}]*)(\})'
    m = re.search(pattern, content, re.DOTALL)
    if not m:
        raise RuntimeError("Блок USER_IDS не найден в bot.py.")
    if re.search(r'\b' + str(user_id) + r'\b', m.group(2)):
        dynamic_allowed_users.add(user_id)
        return
    new_inner = m.group(2).rstrip() + "\n    " + str(user_id) + ",\n"
    content   = content[:m.start()] + m.group(1) + new_inner + m.group(3) + content[m.end():]
    if not _gh_put_raw("bot.py", content, sha, "whitelist: add " + str(user_id)):
        raise RuntimeError("Не удалось записать bot.py на GitHub.")
    dynamic_allowed_users.add(user_id)


def _whitelist_remove(user_id: int) -> None:
    """Удаляет user_id из USER_IDS в bot.py на GitHub."""
    import re
    content, sha = _gh_get_raw("bot.py")
    if content is None:
        raise RuntimeError("Не удалось прочитать bot.py с GitHub.")
    pattern = r'(USER_IDS\s*:\s*set\[int\]\s*=\s*\{)([^}]*)(\})'
    m = re.search(pattern, content, re.DOTALL)
    if not m:
        raise RuntimeError("Блок USER_IDS не найден в bot.py.")
    inner = re.sub(r'[ \t]*' + str(user_id) + r'[,]?[ \t]*(#[^\n]*)?\n?', '', m.group(2))
    content = content[:m.start()] + m.group(1) + inner + m.group(3) + content[m.end():]
    if not _gh_put_raw("bot.py", content, sha, "whitelist: remove " + str(user_id)):
        raise RuntimeError("Не удалось записать bot.py на GitHub.")
    dynamic_allowed_users.discard(user_id)


# =============================================================================
# FLASK + БОТ
# =============================================================================

flask_app = Flask(__name__)
bot       = telebot.TeleBot(BOT_TOKEN, threaded=False)

# Время жизни билета — 2 часа с момента генерации
TICKET_TTL = 7200  # секунд

# Хранилище HTML-билетов: token → (html_bytes, expires_at)
# expires_at — Unix-timestamp (UTC) после которого запись считается устаревшей.
ticket_store: dict[str, tuple] = {}
_ticket_store_lock = threading.Lock()

# Rate limiting: user_id → list[timestamp] (последние генерации за скользящий час)
RATE_LIMIT_MAX  = 10   # максимум билетов в час на пользователя
rate_limit_store: dict[int, list[float]] = {}
_rate_limit_lock = threading.Lock()


@flask_app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    # Проверяем секретный токен — Telegram добавляет его к каждому запросу.
    # Запросы без верного токена (случайные боты, сканеры) получают 403.
    incoming = flask_request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if incoming != WEBHOOK_SECRET:
        log.warning("Webhook: неверный secret от %s — отклонён", flask_request.remote_addr)
        abort(403)
    data = flask_request.get_json(force=True, silent=True)
    if data is None:
        log.warning("Webhook: пустое или невалидное тело запроса от %s", flask_request.remote_addr)
        abort(400)
    update = telebot.types.Update.de_json(data)
    bot.process_new_updates([update])
    return "ok", 200


# Корневой маршрут — отдаёт template.html как оболочку Mini App.
# Telegram открывает именно "/" когда пользователь нажимает Direct Link.
# start_param (token) передаётся через tg.initDataUnsafe.start_param в JS.
@flask_app.route("/")
def serve_index():
    try:
        html = _get_template()
        return html, 200, {"Content-Type": "text/html; charset=utf-8"}
    except FileNotFoundError:
        abort(404)


# /ticket/<token> — отдаёт готовый HTML билета (без SDK) для fetch() из JS.
# Используется только внутренним fetch в template.html — не открывается напрямую.
@flask_app.route("/ticket/<token>")
def serve_ticket(token: str):
    with _ticket_store_lock:
        entry = ticket_store.get(token)
    if entry is None:
        abort(404)
    html_bytes, expires_at = entry
    # Билет просрочен — 410 Gone
    if datetime.now(timezone.utc).timestamp() > expires_at:
        with _ticket_store_lock:
            ticket_store.pop(token, None)
        abort(410)
    # Добавляем CORS-заголовок — fetch из того же origin, но на всякий случай
    return html_bytes, 200, {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
    }


@flask_app.route("/healthz")
def health():
    now = datetime.now(timezone.utc).timestamp()
    with _ticket_store_lock:
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


def normalize_input(raw: str) -> tuple[str, str] | None:
    """
    Нормализует ввод пользователя: "10а 1140", "10 А 1140", "10А  1140" → ("10А", "1140")
    Возвращает (route, vehicle) или None если формат не распознан.
    Логика:
      • убираем лишние пробелы
      • склеиваем цифровую часть маршрута с буквенным суффиксом если они разбиты пробелом
        (напр. "10 А 1140" → токены ["10","А","1140"] → маршрут "10А", ТС "1140")
      • маршрут приводим к верхнему регистру
    """
    import re as _re
    tokens = raw.strip().split()
    if not tokens:
        return None

    # Защита от слишком длинного ввода
    if len(raw) > 50:
        return None

    # Случай 1: ровно 2 токена — стандартный ввод "10А 1140"
    if len(tokens) == 2:
        route_raw, vehicle = tokens
        route = route_raw.upper()
        # Базовая валидация: маршрут содержит цифры, ТС не длиннее 20 символов
        if not _re.search(r'\d', route) or len(vehicle) > 20 or len(route) > 10:
            return None
        return route, vehicle

    # Случай 2: 3 токена — маршрут разбит: "10 А 1140" или "10А 11 40" (опечатка в ТС)
    if len(tokens) == 3:
        # Если первый токен — цифры, второй — буква(ы), третий — номер ТС
        if _re.fullmatch(r"\d+", tokens[0]) and _re.fullmatch(r"[A-Za-zА-Яа-яЁё]+", tokens[1]):
            route = (tokens[0] + tokens[1]).upper()
            vehicle = tokens[2]
            return route, vehicle
        # Если первый токен — маршрут (цифры+буквы), второй и третий — части ТС
        if _re.fullmatch(r"[0-9]+[A-Za-zА-Яа-яЁё]*", tokens[0]):
            route = tokens[0].upper()
            vehicle = tokens[1] + tokens[2]
            return route, vehicle

    return None


_template_cache: str | None = None
_template_cache_lock = threading.Lock()


def _get_template() -> str:
    """Читает template.html с диска один раз, затем отдаёт из кеша."""
    global _template_cache
    with _template_cache_lock:
        if _template_cache is None:
            with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
                _template_cache = f.read()
        return _template_cache


def build_html(route: str, vehicle: str, payment_unix: int,
               transport: str = "bus") -> bytes:
    """
    Генерирует HTML билета.
    transport: "bus" → «Автобус», "troll" → «Троллейбус»

    Таймер: data-pay-unix атрибут добавляется к <body>.
    extractPaymentUnix() в шаблоне находит его способом 1 (regex).
    build_html НЕ трогает <script> — шаблон сам делает tg.expand().
    Формат таймера: ММ:СС, при >59:59 автоматически ЧЧ:ММ:СС.
    """
    import re as _re
    html = _get_template()

    # payment_unix может быть int/float (timestamp) или datetime
    if isinstance(payment_unix, datetime):
        orig_ts = int(payment_unix.timestamp())
    else:
        orig_ts = int(payment_unix)

    payment_dt   = datetime.fromtimestamp(orig_ts, tz=MSK)
    now_utc      = datetime.now(timezone.utc)
    pay_utc      = datetime.fromtimestamp(orig_ts, tz=timezone.utc)
    elapsed_secs = max(0, int((now_utc - pay_utc).total_seconds()))
    elapsed_str  = f"{(elapsed_secs % 3600) // 60:02d}:{elapsed_secs % 60:02d}"

    # ── FIX ТАЙМЕРА: data-pay-unix в <body> ──────────────────────────────────
    # extractPaymentUnix() в шаблоне ищет data-pay-unix="XXXXXXXXXX" способом 1.
    # Это надёжнее чем "var p = ..." который перезаписывался раньше.
    body_match = _re.search(r"<body([^>]*)>", html)
    if body_match:
        old_body = body_match.group(0)
        # Смещение старта таймера: показываем, что оплата была чуть раньше.
        # Это делает таймер видимым не с ~00:03, а с желаемых ~00:23.
        START_TIMER_OFFSET = 23
        display_ts = max(0, orig_ts - START_TIMER_OFFSET)
        if "data-pay-unix" not in old_body:
            new_body = old_body.rstrip(">") + f' data-pay-unix="{display_ts}">'
        else:
            new_body = _re.sub(r'data-pay-unix="[^"]*"', f'data-pay-unix="{display_ts}"', old_body)
        html = html.replace(old_body, new_body, 1)

    # ── Тип транспорта ────────────────────────────────────────────────────────
    transport_label = "Троллейбус" if transport == "troll" else "Автобус"
    html = html.replace(" Автобус: №{{ROUTE}} ", f" {transport_label}: №{{ROUTE}} ")

    # Поддерживаем несколько форматов плейсхолдеров, например:
    #   {{ROUTE}}, {route}, {ROUTE} и т.п. — в шаблонах могут встречаться
    # одинарные фигурные скобки и разный регистр.
    html = _re.sub(r'(?:\{\{\s*ROUTE\s*\}\}|\{\s*ROUTE\s*\})', route, html, flags=_re.IGNORECASE)
    html = _re.sub(r'(?:\{\{\s*VEHICLE\s*\}\}|\{\s*VEHICLE\s*\}|\{\s*TC\s*\})', vehicle, html, flags=_re.IGNORECASE)
    html = _re.sub(r'(?:\{\{\s*DATETIME\s*\}\}|\{\s*DATETIME\s*\})', payment_dt.strftime("%d.%m.%Y %H:%M"), html, flags=_re.IGNORECASE)
    html = _re.sub(r'(?:\{\{\s*ELAPSED\s*\}\}|\{\s*ELAPSED\s*\})', elapsed_str, html, flags=_re.IGNORECASE)
    html = _re.sub(r'(?:\{\{\s*TICKET_SERIAL\s*\}\}|\{\s*TICKET_SERIAL\s*\})', generate_ticket_serial(), html, flags=_re.IGNORECASE)
    html = _re.sub(r'(?:\{\{\s*TICKET_NUMBER\s*\}\}|\{\s*TICKET_NUMBER\s*\})', generate_ticket_number(payment_dt), html, flags=_re.IGNORECASE)
    html = _re.sub(r'(?:\{\{\s*T_PAY\s*\}\}|\{\s*T_PAY\s*\})', str(orig_ts), html, flags=_re.IGNORECASE)
    html = _re.sub(r'(?:\{\{\s*PRICE\s*\}\}|\{\s*PRICE\s*\})', "53", html, flags=_re.IGNORECASE)

    # ── Адаптивный QR-код ─────────────────────────────────────────────────────
    html = html.replace(
        'style="height:1880px;width:1880px;',
        'style="width:100%;max-width:100vw;height:auto;display:block;',
    )

    # <script> НЕ трогаем — шаблон сам управляет fullscreen и таймером

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

import collections as _collections

# Лог событий — последние 500 действий (deque O(1) вместо list.pop(0) O(n))
event_log: _collections.deque = _collections.deque(maxlen=MAX_LOG)
_event_log_lock = threading.Lock()

def log_event(user, action: str) -> None:
    with _event_log_lock:
        event_log.append({
            "time":     datetime.now(MSK),
            "user_id":  user.id,
            "username": user.username or "—",
            "name":     f"{user.first_name or ''} {user.last_name or ''}".strip() or "—",
            "action":   action,
        })


def get_user(uid: int, tg_user=None) -> dict:
    with _user_data_lock:
        if uid not in user_data:
            user_data[uid] = _default_user()
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
    """
    Кнопка использует Direct Link формат:
      https://t.me/<bot>/<app>?startapp=<token>
    Именно этот формат активирует:
      • полноэкранный режим (fullscreen) по умолчанию
      • кнопку "Перейти в бота" в меню ⋮
      • нативное поведение Mini App а не просто WebView
    """
    kb = types.InlineKeyboardMarkup(row_width=1)
    direct_link = f"https://t.me/{BOT_USERNAME}/{APP_SHORT_NAME}?startapp={token}"
    kb.add(types.InlineKeyboardButton(
        "🎫 Открыть билет",
        url=direct_link,
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



def _format_uptime() -> str:
    """Возвращает строку вида 'Работает 3 дня 14 часов 22 минуты'."""
    delta   = datetime.now(timezone.utc) - START_TIME
    total_s = int(delta.total_seconds())
    days    = total_s // 86400
    hours   = (total_s % 86400) // 3600
    minutes = (total_s % 3600) // 60

    def _plural_ru(n: int, one: str, few: str, many: str) -> str:
        if 11 <= n % 100 <= 14:
            return many
        r = n % 10
        if r == 1:  return one
        if 2 <= r <= 4: return few
        return many

    parts = []
    if days:
        parts.append(f"{days} {_plural_ru(days, 'день', 'дня', 'дней')}")
    if hours:
        parts.append(f"{hours} {_plural_ru(hours, 'час', 'часа', 'часов')}")
    if minutes or not parts:
        parts.append(f"{minutes} {_plural_ru(minutes, 'минута', 'минуты', 'минут')}")
    return "Работает " + " ".join(parts)


def _check_rate_limit(user_id: int) -> tuple[bool, int]:
    """
    Проверяет лимит генерации билетов.
    Возвращает (allowed: bool, remaining: int).
    Использует скользящее окно 1 час.
    """
    now = datetime.now(timezone.utc).timestamp()
    window_start = now - 3600
    with _rate_limit_lock:
        timestamps = rate_limit_store.get(user_id, [])
        # Оставляем только события за последний час
        timestamps = [t for t in timestamps if t > window_start]
        rate_limit_store[user_id] = timestamps
        if len(timestamps) >= RATE_LIMIT_MAX:
            # Когда освободится следующий слот
            oldest = min(timestamps)
            wait_secs = int(oldest + 3600 - now) + 1
            return False, wait_secs
        return True, RATE_LIMIT_MAX - len(timestamps)


def _register_rate_limit(user_id: int) -> None:
    """Регистрирует факт генерации билета для rate limiting."""
    now = datetime.now(timezone.utc).timestamp()
    with _rate_limit_lock:
        if user_id not in rate_limit_store:
            rate_limit_store[user_id] = []
        rate_limit_store[user_id].append(now)


def _cleanup_expired_tickets() -> int:
    """
    Удаляет из ticket_store все записи у которых истёк срок хранения.
    Вызывается при каждой генерации нового билета — O(n) по числу записей,
    но n мало (1 запись на пользователя в час → не более нескольких тысяч).
    Возвращает количество удалённых записей.
    """
    now = datetime.now(timezone.utc).timestamp()
    with _ticket_store_lock:
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
    transport: str = "bus",
):
    user = get_user(message.chat.id, message.from_user)

    # ── Rate limiting ──────────────────────────────────────────────────────────
    if not is_admin(message.from_user.id):
        allowed, info = _check_rate_limit(message.from_user.id)
        if not allowed:
            wait_min = (info + 59) // 60
            bot.send_message(
                message.chat.id,
                f"⛔ *Лимит превышен!*\n\n"
                f"Максимум {RATE_LIMIT_MAX} билетов в час.\n"
                f"Следующий доступен через ~{wait_min} мин.",
                parse_mode="Markdown",
            )
            return
    # ──────────────────────────────────────────────────────────────────────────

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
        html_bytes = build_html(route, vehicle, payment_unix, transport=transport)
    except FileNotFoundError:
        bot.send_message(message.chat.id, "❌ Файл template.html не найден.")
        return

    # Чистим устаревшие билеты перед добавлением нового
    _cleanup_expired_tickets()

    token = uuid.uuid4().hex
    # Билет живёт TICKET_TTL секунд с момента генерации
    expires_at = datetime.now(timezone.utc).timestamp() + TICKET_TTL
    with _ticket_store_lock:
        ticket_store[token] = (html_bytes, expires_at)

    log_event(message.from_user, f"билет №{route} ТС {vehicle}")
    user["tickets_count"] = user.get("tickets_count", 0) + 1
    # Регистрируем для rate limiting
    _register_rate_limit(message.from_user.id)
    # Синхронное сохранение — генерация билета критична, нельзя терять данные
    _save_user_data_sync()

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
        
        with _ticket_store_lock:
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
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("🚌 Автобус",    callback_data="transport:bus"),
            types.InlineKeyboardButton("🚎 Троллейбус", callback_data="transport:troll"),
        )
        bot.send_message(message.chat.id, "Выбери тип транспорта:", reply_markup=kb)
    except Exception as e:
        log.exception("Ошибка в handle_new_ticket")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка инициализации.")
        except: pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("transport:"))
def handle_transport_choice(call: types.CallbackQuery):
    try:
        if not is_allowed(call.from_user.id):
            bot.answer_callback_query(call.id, "⛔ Нет доступа.")
            return
        transport = call.data[10:]
        label = "🚌 Автобус" if transport == "bus" else "🚎 Троллейбус"
        user = get_user(call.from_user.id, call.from_user)
        user["state"]     = "awaiting_input"
        user["transport"] = transport
        bot.answer_callback_query(call.id)
        text = label + " выбран.\n\nВведи *маршрут* и *номер ТС* через пробел.\n\nПример: `10А 1140`"
        bot.edit_message_text(
            text,
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown",
        )
    except Exception as e:
        log.exception("Ошибка в handle_transport_choice")
        try:
            bot.answer_callback_query(call.id, "⚠️ Ошибка.")
        except: pass


@bot.message_handler(func=lambda m: m.text == "🔁 Повторить последний")
def handle_repeat_last(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id, message.from_user)
        if not user["last"]:
            bot.send_message(message.chat.id, "⚠️ Нет данных о последнем билете. Сначала создай новый.")
            return
        route, vehicle = user["last"]
        transport = user.get("transport", "bus")
        _send_ticket(message, route, vehicle, transport=transport)
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


@bot.message_handler(func=lambda m: m.text and not m.text.startswith("/") and user_data.get(m.from_user.id, {}).get("state") == "awaiting_input")
def handle_input(message: types.Message):
    try:
        if not check_access(message): return
        user = get_user(message.from_user.id, message.from_user)
        parsed = normalize_input(message.text)
        if parsed is None:
            bot.send_message(
                message.chat.id,
                "❌ Неверный формат. Введи маршрут и номер ТС через пробел.\nПример: `10А 1140`",
                parse_mode="Markdown",
            )
            return
        route, vehicle = parsed
        # Если ввод был в нестандартном формате — показываем что поняли
        normalized_text = f"{route} {vehicle}"
        original_text   = message.text.strip()
        user["state"] = None
        if normalized_text.replace(" ", "").lower() != original_text.replace(" ", "").lower():
            bot.send_message(
                message.chat.id,
                f"✏️ Автокоррекция: `{original_text}` → `{normalized_text}`",
                parse_mode="Markdown",
            )
        transport = user.get("transport", "bus")
        _send_ticket(message, route, vehicle, is_new_ticket=True, transport=transport)
    except Exception as e:
        log.exception("Ошибка в handle_input")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка обработки. Попробуй ещё раз.")
        except:
            pass


@bot.message_handler(
    func=lambda m: m.text and not m.text.startswith("/") and str(user_data.get(m.from_user.id, {}).get("state", "")).startswith("awaiting_vehicle:")
)
def handle_vehicle_input(message: types.Message):
    try:
        if not check_access(message): return
        user  = get_user(message.from_user.id, message.from_user)
        route = user["state"].split(":", 1)[1]
        vehicle = message.text.strip()
        if not vehicle:
            bot.send_message(message.chat.id, "❌ Номер ТС не может быть пустым. Введи номер ТС:")
            return
        if len(vehicle) > 20:
            bot.send_message(message.chat.id, "❌ Номер ТС слишком длинный (макс. 20 символов). Введи номер ТС:")
            return
        user["state"] = None
        transport = user.get("transport", "bus")
        _send_ticket(
            message, route, vehicle,
            msg_date_override=int(datetime.now(timezone.utc).timestamp()),
            transport=transport,
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
            if not user["favorites"]:
                bot.edit_message_text(
                    "⭐ Список избранного пуст.\n\nПосле генерации нажми «⭐ В избранное».",
                    call.message.chat.id, call.message.message_id,
                )
            else:
                bot.edit_message_text(
                    f"⭐ *Избранные маршруты ({len(user['favorites'])}) шт.:*\n\nНажми на маршрут или выбери редактирование.",
                    call.message.chat.id, call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=favorites_keyboard(user["favorites"], edit_mode=False),
                )
            bot.answer_callback_query(call.id)
            return

        try:
            idx = int(payload)
        except ValueError:
            bot.answer_callback_query(call.id, "⚠️ Неверный формат данных.")
            return
        if idx >= len(user["favorites"]) or idx < 0:
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
        _save_user_data_async()
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

        FAV_LIMIT = 5
        if len(user["favorites"]) >= FAV_LIMIT:
            bot.answer_callback_query(
                call.id,
                f"⛔ Лимит избранного: максимум {FAV_LIMIT} маршрутов. Удали лишний и попробуй снова.",
                show_alert=True,
            )
            return

        user["favorites"].append(route)
        _save_user_data_async()
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

        with _ticket_store_lock:
            active_tickets = sum(1 for _, (_, exp) in ticket_store.items() if now <= exp)
        with _event_log_lock:
            log_snapshot = list(event_log)
        with _user_data_lock:
            user_data_snapshot = dict(user_data)
        total_users    = len(user_data_snapshot)
        tickets_today  = sum(
            1 for e in log_snapshot
            if e["time"].date() == today_msk and "билет" in e["action"]
        )
        users_today = len({
            e["user_id"] for e in log_snapshot if e["time"].date() == today_msk
        })

        last_events = log_snapshot[-5:][::-1]
        events_text = "\n".join(
            f"  `{e['time'].strftime('%H:%M')}` @{e['username']} ({e['user_id']}) — {e['action']}"
            for e in last_events
        ) or "  нет событий"

        users_text = "\n".join(
            f"  {'👑' if uid in ADMIN_IDS else '👤'} `{uid}` @{d['username']} {d['name']} "
            f"| билетов: {d['tickets_count']} | с {d['first_seen']}"
            for uid, d in user_data_snapshot.items()
        ) or "  нет пользователей"

        # Telegram лимит — 4096 символов на сообщение. Обрезаем users_text если нужно.
        MAX_MSG = 4096
        header = (
            "🛠 *Админ-панель*\n\n"
            f"⏱ *{_format_uptime()}*\n\n"
            f"👥 Всего пользователей: *{total_users}*\n"
            f"🎫 Билетов за сегодня: *{tickets_today}*\n"
            f"🟢 Активных билетов сейчас: *{active_tickets}*\n"
            f"📅 Активных пользователей сегодня: *{users_today}*\n\n"
            f"*Последние действия:*\n{events_text}\n\n"
            f"*Все пользователи:*\n"
        )
        available = MAX_MSG - len(header) - 10
        if len(users_text) > available:
            users_text = users_text[:available] + "\n  …(обрезано)"
        text = header + users_text
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
        lines = [
            "🛠 Команды администратора:",
            "",
            "📊 Панель и статистика:",
            "/admin — панель, статистика, uptime, логи",
            "/allowed — список всех разрешённых пользователей",
            "/ha — эта справка",
            "",
            "👥 Управление доступом:",
            "/allow <user_id> — добавить пользователя",
            "  Пример: /allow 123456789",
            "/deny <user_id> — удалить пользователя",
            "  Пример: /deny 123456789",
            "",
            "📣 Рассылка:",
            "/broadcast <текст> — отправить всем пользователям",
            "  Пример: /broadcast Плановые работы с 22:00",
            "",
            "💾 Данные:",
            "/datasync — синхронизация user_data на GitHub",
            "",
            "---------------------",
            "👤 Команды пользователя:",
            "",
            "/start — запустить бота",
            "/help — справка по боту",
            "/status — последний билет и избранное",
            "/cancel — отменить текущее действие",
        ]
        bot.send_message(message.chat.id, "\n".join(lines))
    except Exception as e:
        log.exception("Ошибка в handle_help_admin")
        try:
            bot.send_message(message.chat.id, "Ошибка при отправке справки: " + str(e))
        except Exception:
            pass


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

        if user_id in USER_IDS or user_id in dynamic_allowed_users:
            bot.send_message(message.chat.id, f"⚠️ Пользователь `{user_id}` уже разрешён.", parse_mode="Markdown")
            return

        wait = bot.send_message(message.chat.id, "⏳ Обновляю whitelist на GitHub...")
        try:
            _whitelist_add(user_id)
        except RuntimeError as e:
            try:
                bot.delete_message(message.chat.id, wait.message_id)
            except Exception:
                pass
            bot.send_message(message.chat.id, f"❌ Ошибка GitHub:\n{e}")
            return

        # Записываем дату добавления
        u = get_user(user_id)
        u["added_at"] = datetime.now(MSK).strftime("%d.%m.%Y")

        log_event(message.from_user, f"разрешил доступ {user_id}")
        log.info("Добавлен пользователь %s админом %s", user_id, message.from_user.id)

        # Уведомляем нового пользователя
        try:
            bot.send_message(
                user_id,
                "✅ *Вам открыт доступ к боту!*\n\n"
                "Нажми /start чтобы начать работу.\n\n"
                "🎫 Ты сможешь быстро генерировать билеты об оплате проезда.",
                parse_mode="Markdown",
            )
            notified = "Пользователь уведомлён ✅"
        except Exception:
            notified = "Уведомить не удалось — пользователь ещё не запускал бота"

        try:
            bot.delete_message(message.chat.id, wait.message_id)
        except Exception:
            pass

        bot.send_message(
            message.chat.id,
            f"✅ Пользователь `{user_id}` добавлен.\n"
            f"📁 Сохранено в bot.py на GitHub\n"
            f"📩 {notified}",
            parse_mode="Markdown",
        )
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
            bot.send_message(message.chat.id, f"⚠️ Пользователь `{user_id}` не найден.", parse_mode="Markdown")
            return
        wait = bot.send_message(message.chat.id, "⏳ Обновляю whitelist на GitHub...")
        try:
            _whitelist_remove(user_id)
        except RuntimeError as e:
            bot.edit_message_text(f"❌ Ошибка GitHub:\n{e}", message.chat.id, wait.message_id)
            return
        log_event(message.from_user, f"запретил доступ {user_id}")
        bot.edit_message_text(
            f"✅ Пользователь `{user_id}` удалён. Сохранено в bot.py на GitHub.",
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

        def fmt(uid: int, icon: str) -> str:
            d       = user_data.get(uid, {})
            uname   = f"@{d.get('username','—')}" if d.get('username','—') != '—' else 'без username'
            name    = d.get('name', '—')
            added   = d.get('added_at', '—')
            tickets = d.get('tickets_count', 0)
            return (
                f"{icon} {uname} · {name}\n"
                f"    🆔 `{uid}` · 📅 с {added} · 🎫 {tickets} шт."
            )

        lines = ["📋 *Список разрешённых пользователей*\n"]

        lines.append(f"👑 *Администраторы — {len(ADMIN_IDS)} чел.:*")
        for uid in sorted(ADMIN_IDS):
            lines.append(fmt(uid, "👑"))

        lines.append(f"\n👤 *Пользователи (код) — {len(USER_IDS)} чел.:*")
        if USER_IDS:
            for uid in sorted(USER_IDS):
                lines.append(fmt(uid, "👤"))
        else:
            lines.append("  нет")

        lines.append(f"\n🆕 *Добавлены через /allow — {len(dynamic_allowed_users)} чел.:*")
        if dynamic_allowed_users:
            for uid in sorted(dynamic_allowed_users):
                lines.append(fmt(uid, "🆕"))
        else:
            lines.append("  нет")

        total = len(ADMIN_IDS) + len(USER_IDS) + len(dynamic_allowed_users)
        lines.append(f"\n📊 *Итого: {total} пользователей*")

        bot.send_message(message.chat.id, "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        log.exception("Ошибка в handle_allowed")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при загрузке списка.")
        except Exception:
            pass


@bot.message_handler(commands=["datasync"])
def handle_datasync(message: types.Message):
    """Принудительно сохраняет user_data на GitHub. Только для администраторов."""
    try:
        if not check_admin(message): return

        if not _gh_available():
            bot.send_message(
                message.chat.id,
                "❌ GitHub не настроен.\n\n"
                "Задай переменные в Render Dashboard:\n"
                "`GITHUB_TOKEN`, `GITHUB_OWNER`, `GITHUB_REPO`",
                parse_mode="Markdown",
            )
            return

        wait = bot.send_message(message.chat.id, "⏳ Сохраняю user_data на GitHub...")
        ok = _save_user_data_sync()

        try:
            bot.delete_message(message.chat.id, wait.message_id)
        except Exception:
            pass

        if ok:
            ts = datetime.now(MSK).strftime("%d.%m.%Y %H:%M:%S")
            bot.send_message(
                message.chat.id,
                f"✅ *Синхронизация выполнена*\n\n"
                f"📁 Файл `user_data.json` обновлён на GitHub\n"
                f"👥 Пользователей сохранено: *{len(user_data)}*\n"
                f"🕐 Время: {ts}",
                parse_mode="Markdown",
            )
            log.info("datasync: сохранено %d пользователей", len(user_data))
        else:
            bot.send_message(
                message.chat.id,
                "❌ *Не удалось сохранить данные*\n\n"
                "Проверь:\n"
                "• Логи Render на наличие ошибок\n"
                "• Права токена GitHub (scope: `repo`)\n"
                "• Правильность `GITHUB_OWNER` и `GITHUB_REPO`",
                parse_mode="Markdown",
            )
    except Exception as e:
        log.exception("Ошибка в handle_datasync")
        try:
            bot.send_message(message.chat.id, f"⚠️ Ошибка: {e}")
        except Exception:
            pass


@bot.message_handler(commands=["broadcast"])
def handle_broadcast(message: types.Message):
    """Рассылает сообщение всем пользователям. Только для администраторов."""
    try:
        if not check_admin(message): return

        # Извлекаем текст после команды
        parts = message.text.split(None, 1)
        if len(parts) < 2 or not parts[1].strip():
            bot.send_message(
                message.chat.id,
                "📣 *Broadcast — рассылка всем пользователям*\n\n"
                "Формат: `/broadcast <текст>`\n\n"
                "Пример:\n"
                "`/broadcast 🔧 Плановые работы сегодня с 22:00 до 23:00. Бот будет недоступен.`",
                parse_mode="Markdown",
            )
            return

        text_to_send = parts[1].strip()
        broadcast_text = (
            f"📣 *Сообщение от администратора:*\n\n{text_to_send}"
        )

        # Собираем всех получателей (все кроме отправляющего админа)
        all_ids: set[int] = set()
        all_ids.update(ADMIN_IDS)
        all_ids.update(USER_IDS)
        all_ids.update(dynamic_allowed_users)
        all_ids.discard(message.from_user.id)   # себе не слать

        wait = bot.send_message(
            message.chat.id,
            f"⏳ Рассылаю {len(all_ids)} пользователям...",
        )

        # Рассылку выполняем в отдельном потоке — не блокируем webhook worker
        def _do_broadcast(ids: set, text: str, chat_id: int, wait_msg_id: int, sender_id: int):
            ok_count   = 0
            fail_count = 0
            for uid in ids:
                try:
                    bot.send_message(uid, text, parse_mode="Markdown")
                    ok_count += 1
                except Exception as exc:
                    log.warning("broadcast: не удалось отправить %s: %s", uid, exc)
                    fail_count += 1

            log.info("Broadcast от %s: всего=%d, ОК=%d, ошибок=%d",
                     sender_id, len(ids), ok_count, fail_count)

            try:
                bot.delete_message(chat_id, wait_msg_id)
            except Exception:
                pass

            result_icon = "✅" if fail_count == 0 else "⚠️"
            try:
                bot.send_message(
                    chat_id,
                    f"{result_icon} *Рассылка завершена*\n\n"
                    f"✅ Доставлено: *{ok_count}*\n"
                    f"❌ Не доставлено: *{fail_count}*\n\n"
                    f"💬 Текст:\n_{text_to_send}_",
                    parse_mode="Markdown",
                )
            except Exception:
                pass

        log_event(message.from_user, f"broadcast: {len(all_ids)} получателей")
        threading.Thread(
            target=_do_broadcast,
            args=(all_ids, broadcast_text, message.chat.id, wait.message_id, message.from_user.id),
            daemon=False,
            name="broadcast",
        ).start()
    except Exception as e:
        log.exception("Ошибка в handle_broadcast")
        try:
            bot.send_message(message.chat.id, "⚠️ Ошибка при рассылке.")
        except Exception:
            pass


# =============================================================================
# ЗАПУСК
# =============================================================================

def setup_webhook():
    webhook_url = f"{RENDER_URL}/webhook/{BOT_TOKEN}"
    bot.remove_webhook()
    # Передаём secret_token — Telegram будет добавлять его в заголовок каждого запроса
    bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET)
    log.info("Webhook установлен: %s (secret_token задан)", webhook_url)


# Загружаем user_data из GitHub при старте
_load_user_data()

# Вызываем при импорте — gunicorn не запускает __main__
setup_webhook()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    log.info("Сервер запущен на порту %s", port)
    flask_app.run(host="0.0.0.0", port=port)
