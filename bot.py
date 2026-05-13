"""
Образовательный проект: Автоматизация формирования билетов через Telegram-бота.
Дисциплина: Информационные технологии.

Архитектура v3 — GitHub Pages как хостинг:
    1. Python генерирует HTML с реальными данными (маршрут, ТС, время).
    2. Файл публикуется в GitHub-репозиторий через GitHub REST API (PUT /contents).
    3. GitHub Pages автоматически раздаёт файл по публичному URL.
    4. Бот отправляет пользователю ссылку — страница открывается в браузере,
       JS работает полноценно, таймер тикает в реальном времени.
"""

import telebot
import random
import string
import base64
import json
from datetime import datetime, timezone, timedelta
from telebot import types

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

BOT_TOKEN = "8784348539:AAHU40WKbpF0c4RYZXwKwgGHz27OKJfd_CU"

# GitHub Personal Access Token (classic).
# Как получить:
#   GitHub -> Settings -> Developer settings -> Personal access tokens -> Tokens (classic)
#   -> Generate new token -> отметь scope "repo" -> Generate -> скопируй сюда.
GITHUB_TOKEN = "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"

GITHUB_OWNER  = "maksleny"       # твой GitHub username
GITHUB_REPO   = "t-ticket"       # название репозитория
GITHUB_FOLDER = "tickets"        # папка в репо (создастся автоматически)
PAGES_BASE_URL = f"https://{GITHUB_OWNER}.github.io/{GITHUB_REPO}"

TEMPLATE_PATH = "template.html"
MSK = timezone(timedelta(hours=3))

# =============================================================================
# ТРАНСЛИТЕРАЦИЯ
# Все строки которые идут через HTTP (имя файла, URL, commit message)
# должны быть ASCII. Кириллицу в маршруте транслитерируем.
# =============================================================================
_TRANSLIT = {
    'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'Yo','Ж':'Zh',
    'З':'Z','И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O',
    'П':'P','Р':'R','С':'S','Т':'T','У':'U','Ф':'F','Х':'Kh','Ц':'Ts',
    'Ч':'Ch','Ш':'Sh','Щ':'Shch','Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'Yu','Я':'Ya',
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh',
    'з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o',
    'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts',
    'ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
}

def to_ascii_route(route: str) -> str:
    """Транслитерирует маршрут в ASCII для использования в URL и имени файла."""
    return ''.join(_TRANSLIT.get(c, c) for c in route)


# =============================================================================
# ХРАНИЛИЩЕ СОСТОЯНИЙ
# =============================================================================
user_data: dict[int, dict] = {}
bot = telebot.TeleBot(BOT_TOKEN)


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def get_user(user_id: int) -> dict:
    if user_id not in user_data:
        user_data[user_id] = {
            "last":         None,
            "favorites":    [],
            "state":        None,
            "payment_unix": None,
            "last_gh_path": None,
        }
    return user_data[user_id]


def generate_ticket_serial() -> str:
    return "QR" + "".join(random.choices(string.digits, k=12))


def generate_ticket_number(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S") + "".join(random.choices(string.digits, k=3))


def format_payment_datetime(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y %H:%M")


def load_template() -> str:
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        return f.read()


def build_html(route: str, vehicle: str, payment_unix: int) -> str:
    """
    Заполняет шаблон данными и возвращает готовый HTML.

    Маркеры в template.html:
    {{ROUTE}}         - Номер маршрута
    {{VEHICLE}}       - Бортовой номер ТС
    {{DATETIME}}      - Дата/время оплаты (дд.мм.гггг чч:мм)
    {{ELAPSED}}       - Начальное ММ:СС (показывается до загрузки JS)
    {{PAYMENT_UNIX}}  - Unix-timestamp оплаты (для JS-таймера)
    {{TICKET_SERIAL}} - Серия билета (QR + 12 цифр)
    {{TICKET_NUMBER}} - Номер билета (дата + 3 цифры)
    {{PRICE}}         - Стоимость (53)

    Таймер:
        Страница открывается по https:// (GitHub Pages) — JS работает
        без ограничений (в отличие от file://). Вшиваем {{PAYMENT_UNIX}}
        прямо в <script>: JS каждую секунду считает Date.now() - paymentUnix
        и обновляет DOM. Точность абсолютная — без накопленной ошибки.
    """
    html = load_template()
    payment_dt = datetime.fromtimestamp(payment_unix, tz=MSK)

    now_utc      = datetime.now(timezone.utc)
    pay_utc      = datetime.fromtimestamp(payment_unix, tz=timezone.utc)
    elapsed_secs = max(0, int((now_utc - pay_utc).total_seconds()))
    elapsed_str  = f"{(elapsed_secs % 3600) // 60:02d}:{elapsed_secs % 60:02d}"

    html = html.replace("{{ROUTE}}",          route)
    html = html.replace("{{VEHICLE}}",        vehicle)
    html = html.replace("{{DATETIME}}",       format_payment_datetime(payment_dt))
    html = html.replace("{{ELAPSED}}",        elapsed_str)
    html = html.replace("{{PAYMENT_UNIX}}",   str(payment_unix))
    html = html.replace("{{TICKET_SERIAL}}",  generate_ticket_serial())
    html = html.replace("{{TICKET_NUMBER}}",  generate_ticket_number(payment_dt))
    html = html.replace("{{PRICE}}",          "53")

    # ПАТЧ: QR на всю ширину экрана
    # SingleFile хардкодит style="height:1880px;width:1880px;..."
    # Перебиваем на адаптивный width:100% / height:auto
    html = html.replace(
        'style="height:1880px;width:1880px;',
        'style="width:100%;max-width:100vw;height:auto;display:block;',
    )

    # Добавляем id к <strong> вокруг таймера — JS найдёт его по getElementById
    html = html.replace(
        f'<strong _ngcontent-ng-c2869113626>{elapsed_str}</strong>',
        f'<strong _ngcontent-ng-c2869113626 id="elapsed-timer">{elapsed_str}</strong>',
    )

    # JS-таймер: считает разницу между Date.now() и payment_unix каждую секунду.
    # Работает только на https:// — именно поэтому нужен GitHub Pages.
    timer_script = f"""
<script>
(function() {{
  var paymentUnix = {payment_unix};  // Unix-timestamp оплаты (UTC, секунды)

  function formatElapsed(totalSecs) {{
    var h = Math.floor(totalSecs / 3600);
    var m = Math.floor((totalSecs % 3600) / 60);
    var s = totalSecs % 60;
    function pad(n) {{ return n < 10 ? '0' + n : '' + n; }}
    // Показываем часы только когда они появились (как в оригинале: 49:35:43)
    return (h > 0 ? pad(h) + ':' : '') + pad(m) + ':' + pad(s);
  }}

  function tick() {{
    var nowSecs = Math.floor(Date.now() / 1000);
    var elapsed = Math.max(0, nowSecs - paymentUnix);
    var el = document.getElementById('elapsed-timer');
    if (el) el.textContent = formatElapsed(elapsed);
  }}

  tick();                   // обновить сразу при загрузке
  setInterval(tick, 1000);  // затем каждую секунду
}})();
</script>
"""
    html += timer_script
    return html


# =============================================================================
# GITHUB API
# =============================================================================

def github_put_file(file_path: str, content_bytes: bytes, commit_message: str) -> str:
    """
    Создаёт или обновляет файл через GitHub Contents API.
    Использует ssl + socket напрямую — без http.client, который
    ломается на не-ASCII символах при encode('latin-1') в заголовках.
    """
    import ssl
    import socket
    import urllib.parse

    encoded_path = urllib.parse.quote(file_path, safe="/")
    api_path = f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{encoded_path}"

    # Шаг 1: GET — получаем sha если файл уже существует
    sha = _github_request("GET", api_path, body=None)
    sha_value = None
    if sha is not None and sha.get("sha"):
        sha_value = sha["sha"]

    # Шаг 2: PUT — пушим файл
    payload = {
        "message": commit_message,
        "content": base64.b64encode(content_bytes).decode("ascii"),
    }
    if sha_value:
        payload["sha"] = sha_value

    result = _github_request("PUT", api_path, body=payload)
    if result is None:
        raise Exception("GitHub вернул пустой ответ")

    return f"{PAGES_BASE_URL}/{file_path}"


def _github_request(method: str, path: str, body: dict | None) -> dict | None:
    """
    Делает HTTPS-запрос к api.github.com через ssl.SSLContext + socket.
    Все строки кодируются в UTF-8 вручную — никакого latin-1.
    """
    import ssl
    import socket

    # Тело запроса
    body_bytes = b""
    if body is not None:
        body_bytes = json.dumps(body, ensure_ascii=True).encode("utf-8")

    # Заголовки собираем как байты вручную — минуем putheader() и latin-1
    token_bytes   = GITHUB_TOKEN.encode("utf-8")
    headers_lines = (
        f"{method} {path} HTTP/1.1\r\n"
        f"Host: api.github.com\r\n"
        f"User-Agent: TelegramTicketBot/4.0\r\n"
        f"Accept: application/vnd.github+json\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body_bytes)}\r\n"
    ).encode("utf-8")

    auth_line = b"Authorization: token " + token_bytes + b"\r\n"
    end_line  = b"\r\n"

    request_bytes = headers_lines + auth_line + end_line + body_bytes

    # TLS-соединение
    ctx = ssl.create_default_context()
    with socket.create_connection(("api.github.com", 443), timeout=30) as sock:
        with ctx.wrap_socket(sock, server_hostname="api.github.com") as tls:
            tls.sendall(request_bytes)

            # Читаем ответ
            response = b""
            while True:
                chunk = tls.recv(4096)
                if not chunk:
                    break
                response += chunk

    # Парсим HTTP-ответ
    header_end = response.index(b"\r\n\r\n")
    header_part = response[:header_end].decode("utf-8", errors="replace")
    body_part   = response[header_end + 4:]

    status_line = header_part.split("\r\n")[0]
    status_code = int(status_line.split()[1])

    # Chunked transfer encoding — собираем тело
    if b"Transfer-Encoding: chunked" in response[:header_end]:
        body_part = _decode_chunked(body_part)

    if status_code == 404 and method == "GET":
        return None  # файл не существует — это нормально

    if status_code not in (200, 201):
        raise Exception(
            f"GitHub {method} вернул HTTP {status_code}:\n"
            f"{body_part[:400].decode('utf-8', errors='replace')}"
        )

    return json.loads(body_part)


def _decode_chunked(data: bytes) -> bytes:
    """Разбирает chunked transfer encoding."""
    result = b""
    while data:
        crlf = data.index(b"\r\n")
        chunk_size = int(data[:crlf], 16)
        if chunk_size == 0:
            break
        result += data[crlf + 2 : crlf + 2 + chunk_size]
        data = data[crlf + 2 + chunk_size + 2:]
    return result


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
    return kb


def favorites_keyboard(favorites: list) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    for i, route in enumerate(favorites):
        kb.add(types.InlineKeyboardButton(
            text=f"№{route}",
            callback_data=f"fav:{i}",
        ))
    kb.add(types.InlineKeyboardButton("❌ Закрыть", callback_data="fav:close"))
    return kb


def ticket_keyboard(route: str, vehicle: str, url: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🌐 Открыть билет", url=url),
        types.InlineKeyboardButton("⭐ В избранное",   callback_data=f"add_fav:{route}"),
    )
    return kb


# =============================================================================
# ОБРАБОТЧИКИ
# =============================================================================

@bot.message_handler(commands=["start", "help"])
def handle_start(message: types.Message):
    get_user(message.from_user.id)
    bot.send_message(
        message.chat.id,
        "👋 Привет! Я генерирую уведомления об оплате проезда.\n\nВыбери действие:",
        reply_markup=main_keyboard(),
    )


@bot.message_handler(func=lambda m: m.text == "🎫 Новый билет")
def handle_new_ticket(message: types.Message):
    get_user(message.from_user.id)["state"] = "awaiting_input"
    bot.send_message(
        message.chat.id,
        "Введи *маршрут* и *номер ТС* через пробел.\n\nПример: `10А 1140`",
        parse_mode="Markdown",
    )


@bot.message_handler(func=lambda m: m.text == "🔁 Повторить последний")
def handle_repeat_last(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["last"]:
        bot.send_message(message.chat.id, "⚠️ Нет данных о последнем билете. Сначала создай новый.")
        return
    route, vehicle = user["last"]
    _send_ticket(message, route, vehicle)


@bot.message_handler(func=lambda m: m.text == "⭐ Избранное")
def handle_favorites(message: types.Message):
    user = get_user(message.from_user.id)
    if not user["favorites"]:
        bot.send_message(
            message.chat.id,
            "⭐ Список избранного пуст.\n\nПосле генерации нажми «⭐ В избранное».",
        )
        return
    bot.send_message(
        message.chat.id,
        "⭐ *Избранные маршруты:*\nВведи номер ТС после выбора маршрута:",
        parse_mode="Markdown",
        reply_markup=favorites_keyboard(user["favorites"]),
    )


@bot.message_handler(func=lambda m: get_user(m.from_user.id).get("state") == "awaiting_input")
def handle_input(message: types.Message):
    user = get_user(message.from_user.id)
    user["state"] = None

    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.send_message(
            message.chat.id,
            "❌ Неверный формат. Введи ровно два значения через пробел.\nПример: `10А 1140`",
            parse_mode="Markdown",
        )
        user["state"] = "awaiting_input"
        return

    route, vehicle = parts[0].upper(), parts[1]
    _send_ticket(message, route, vehicle, is_new_ticket=True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("fav:"))
def handle_fav_callback(call: types.CallbackQuery):
    user    = get_user(call.from_user.id)
    payload = call.data[4:]

    if payload == "close":
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id)
        return

    idx = int(payload)
    if idx >= len(user["favorites"]):
        bot.answer_callback_query(call.id, "⚠️ Запись устарела.")
        return

    route = user["favorites"][idx]
    bot.answer_callback_query(call.id)
    bot.delete_message(call.message.chat.id, call.message.message_id)

    # Запрашиваем номер ТС для выбранного маршрута
    user["state"] = f"awaiting_vehicle:{route}"
    bot.send_message(
        call.message.chat.id,
        f"Маршрут №*{route}* выбран.\nВведи номер ТС:",
        parse_mode="Markdown",
    )


@bot.message_handler(func=lambda m: str(get_user(m.from_user.id).get("state", "")).startswith("awaiting_vehicle:"))
def handle_vehicle_input(message: types.Message):
    user  = get_user(message.from_user.id)
    route = user["state"].split(":", 1)[1]
    user["state"] = None
    vehicle = message.text.strip()
    _send_ticket(
        message, route, vehicle,
        msg_date_override=int(datetime.now(timezone.utc).timestamp()),
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("add_fav:"))
def handle_add_fav_callback(call: types.CallbackQuery):
    user    = get_user(call.from_user.id)
    # payload теперь только маршрут (без ТС)
    route = call.data[8:]

    if route in user["favorites"]:
        bot.answer_callback_query(call.id, "⭐ Уже в избранном!")
        return

    user["favorites"].append(route)
    bot.answer_callback_query(call.id, f"✅ Маршрут №{route} добавлен в избранное!")

    # Убираем кнопку «В избранное», оставляем только «Открыть»
    gh_path = user.get("last_gh_path")
    if gh_path:
        new_kb = types.InlineKeyboardMarkup()
        new_kb.add(types.InlineKeyboardButton(
            "🌐 Открыть билет", url=f"{PAGES_BASE_URL}/{gh_path}"
        ))
        try:
            bot.edit_message_reply_markup(
                call.message.chat.id, call.message.message_id, reply_markup=new_kb
            )
        except Exception:
            pass


# =============================================================================
# ЯДРО: ГЕНЕРАЦИЯ, ПУБЛИКАЦИЯ, ОТПРАВКА ССЫЛКИ
# =============================================================================

import threading

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
        html_bytes = build_html(route, vehicle, payment_unix).encode("utf-8")
    except FileNotFoundError:
        bot.send_message(message.chat.id, "❌ Файл template.html не найден рядом с bot.py.")
        return

    route_ascii = to_ascii_route(route)
    gh_path     = f"{GITHUB_FOLDER}/ticket_{route_ascii}_{vehicle}.html"
    user["last_gh_path"] = gh_path
    pages_url   = f"{PAGES_BASE_URL}/{gh_path}"

    # Отправляем ссылку СРАЗУ — не ждём пока GitHub примет файл
    bot.send_message(
        message.chat.id,
        f"🎫 *Билет готов!*\n"
        f"Маршрут: №{route} · ТС: {vehicle}\n\n"
        f"🔗 {pages_url}\n\n"
        f"_Открывай через 15–20 секунд — GitHub Pages\n"
        f"применяет изменения не мгновенно._",
        parse_mode="Markdown",
        reply_markup=ticket_keyboard(route, vehicle, pages_url),
    )

    # Пушим файл в фоне — бот не зависает пока идёт запрос к GitHub
    def push_in_background():
        try:
            github_put_file(
                file_path      = gh_path,
                content_bytes  = html_bytes,
                commit_message = f"ticket: {route_ascii} / {vehicle}",
            )
        except Exception as e:
            try:
                bot.send_message(
                    message.chat.id,
                    f"⚠️ Не удалось опубликовать билет:\n{e}",
                )
            except Exception:
                pass

    threading.Thread(target=push_in_background, daemon=True).start()

# =============================================================================
# ЗАПУСК
# =============================================================================

if __name__ == "__main__":
    if "ТВОЙ_GITHUB_TOKEN" in GITHUB_TOKEN:
        print("=" * 60)
        print("ВНИМАНИЕ: GITHUB_TOKEN не заполнен!")
        print("Получи токен:")
        print("  GitHub -> Settings -> Developer settings")
        print("  -> Personal access tokens -> Tokens (classic)")
        print("  -> Generate new token -> scope: repo -> Generate")
        print("=" * 60)
        print()
    print("Бот запущен. Нажми Ctrl+C для остановки.")
    bot.infinity_polling()