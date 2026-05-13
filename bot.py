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
from datetime import datetime, timezone, timedelta

import telebot
from telebot import types
from flask import Flask, abort, request as flask_request

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

BOT_TOKEN     = os.environ["BOT_TOKEN"]    # задать в Render → Environment
RENDER_URL    = os.environ["RENDER_URL"]   # напр. https://my-bot.onrender.com
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template.html")
MSK           = timezone(timedelta(hours=3))

# =============================================================================
# FLASK + БОТ
# =============================================================================

flask_app = Flask(__name__)
bot       = telebot.TeleBot(BOT_TOKEN, threaded=False)

# Хранилище HTML-билетов: token → html_bytes
ticket_store: dict[str, bytes] = {}


@flask_app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    update = telebot.types.Update.de_json(flask_request.get_json(force=True))
    bot.process_new_updates([update])
    return "ok", 200


@flask_app.route("/ticket/<token>")
def serve_ticket(token: str):
    html = ticket_store.get(token)
    if html is None:
        abort(404)
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}


@flask_app.route("/healthz")
def health():
    return "ok", 200


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

    # Заменяем встроенный скрипт шаблона на наш рабочий таймер
    # (оригинальный скрипт ищет {{T_PAY}} который уже заменён — таймер не работал)
    timer_script_inline = f"""
    // Живой таймер — считает секунды с момента оплаты
    (function() {{
      var paymentUnix = {payment_unix};
      function pad(n) {{ return n < 10 ? '0' + n : '' + n; }}
      function tick() {{
        var total = Math.max(0, Math.floor(Date.now() / 1000) - paymentUnix);
        var h = Math.floor(total / 3600);
        var m = Math.floor((total % 3600) / 60);
        var s = total % 60;
        var text = (h > 0 ? pad(h) + ':' : '') + pad(m) + ':' + pad(s);
        // Ищем все <strong> и обновляем тот у которого текст похож на таймер MM:SS
        var strongs = document.querySelectorAll('strong');
        strongs.forEach(function(el) {{
          if (/^\d{{2}}:\d{{2}}/.test(el.textContent.trim())) {{
            el.textContent = text;
          }}
        }});
      }}
      tick();
      setInterval(tick, 1000);
    }})();
    """
    # Находим существующий <script> в шаблоне и заменяем его содержимое
    import re as _re
    html = _re.sub(r'(<script[^>]*>).*?(</script>)', r'' + timer_script_inline + r'', html, count=1, flags=_re.DOTALL)

    # Адаптивный QR-код (SingleFile хардкодит 1880×1880px)
    html = html.replace(
        'style="height:1880px;width:1880px;',
        'style="width:100%;max-width:100vw;height:auto;display:block;',
    )


    return html.encode("utf-8")


# =============================================================================
# ХРАНИЛИЩЕ СОСТОЯНИЙ
# =============================================================================

user_data: dict[int, dict] = {}


def get_user(uid: int) -> dict:
    if uid not in user_data:
        user_data[uid] = {
            "last":         None,   # (route, vehicle)
            "favorites":    [],     # [route, ...]
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


def favorites_keyboard(favorites: list) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    for i, route in enumerate(favorites):
        kb.add(types.InlineKeyboardButton(
            text=f"№{route}",
            callback_data=f"fav:{i}",
        ))
    kb.add(types.InlineKeyboardButton("❌ Закрыть", callback_data="fav:close"))
    return kb


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
        return

    token = uuid.uuid4().hex
    ticket_store[token] = html_bytes

    bot.send_message(
        message.chat.id,
        f"🎫 *Билет готов!*\nМаршрут: №{route} · ТС: {vehicle}",
        parse_mode="Markdown",
        reply_markup=ticket_keyboard(token, route),
    )


# =============================================================================
# ОБРАБОТЧИКИ БОТА
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
        "⭐ *Избранные маршруты:*",
        parse_mode="Markdown",
        reply_markup=favorites_keyboard(user["favorites"]),
    )


@bot.message_handler(func=lambda m: get_user(m.from_user.id).get("state") == "awaiting_input")
def handle_input(message: types.Message):
    user = get_user(message.from_user.id)
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


@bot.message_handler(
    func=lambda m: str(get_user(m.from_user.id).get("state", "")).startswith("awaiting_vehicle:")
)
def handle_vehicle_input(message: types.Message):
    user  = get_user(message.from_user.id)
    route = user["state"].split(":", 1)[1]
    user["state"] = None
    _send_ticket(
        message, route, message.text.strip(),
        msg_date_override=int(datetime.now(timezone.utc).timestamp()),
    )


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
    user["state"] = f"awaiting_vehicle:{route}"
    bot.send_message(
        call.message.chat.id,
        f"Маршрут №*{route}* выбран.\nВведи номер ТС:",
        parse_mode="Markdown",
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("add_fav:"))
def handle_add_fav_callback(call: types.CallbackQuery):
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
