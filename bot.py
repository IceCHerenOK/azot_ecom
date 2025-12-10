import logging
import requests
from collections import defaultdict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
)
from datetime import date, timedelta, datetime, timezone

from gs_client import get_cost_by_article, get_unit_economy_by_article


# ================== КОНФИГ ==================

# ⛔ СЮДА вставь СВОЙ токен от BotFather
BOT_TOKEN = "8501880752:AAEsRyrrOS4q5XKu7LgmgLw4RQvDVpQD6mA"

# ⛔ СЮДА вставь СВОЙ Ozon Client ID и API Key
OZON_CLIENT_ID = "108356"
OZON_API_KEY = "69052570-41ab-4595-baae-e2fa26ad6cd6"

OZON_API_URL = "https://api-seller.ozon.ru"
OZON_HEADERS = {
    "Client-Id": OZON_CLIENT_ID,
    "Api-Key": OZON_API_KEY,
    "Content-Type": "application/json",
}

# состояния для чатов
STATE_OZON = "waiting_for_ozon_artikul"
STATE_WB = "waiting_for_wb_artikul"

user_state: dict[int, str] = {}

# чат, в который слать уведомления о новых FBS заказах
ADMIN_CHAT_ID: int | None = None

# уже увиденные FBS-постинги (для уведомлений)
KNOWN_FBS_POSTINGS: set[str] = set()


# ================== ЛОГИ ==================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ================== МЕНЮ ==================

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📊 Аналитика Ozon", callback_data="ozon_analytics")],
        [InlineKeyboardButton("📊 Аналитика WB", callback_data="wb_analytics")],
        [InlineKeyboardButton("📈 Отчёт по заказам Ozon", callback_data="ozon_orders_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_orders_period_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("Сегодня", callback_data="ozon_orders_1"),
            InlineKeyboardButton("3 дня", callback_data="ozon_orders_3"),
        ],
        [
            InlineKeyboardButton("7 дней", callback_data="ozon_orders_7"),
            InlineKeyboardButton("30 дней", callback_data="ozon_orders_30"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


async def show_main_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Я бот-агент для маркетплейсов.\n\n"
        "Сейчас могу:\n"
        "• Показать аналитику по отдельным товарам Ozon\n"
        "• Пример аналитики по WB\n"
        "• Сделать отчёт по заказам Ozon за период (по артикулам)\n\n"
        "Выбери действие:"
    )
    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=get_main_menu_keyboard(),
    )


# ================== АНАЛИТИКА ПО SKU ==================

def get_ozon_sales_for_sku(sku: int, days: int = 14) -> dict:
    """
    Аналитика продаж по SKU через /v1/analytics/data.
    Возвращаем:
        ok: bool
        revenue: выручка
        ordered_units: штучки
    """
    date_to = date.today()
    date_from = date_to - timedelta(days=days)

    url = f"{OZON_API_URL}/v1/analytics/data"
    payload = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "metrics": ["revenue", "ordered_units"],
        "dimension": ["sku"],
        "filters": [],
        "sort": [{"key": "ordered_units", "order": "DESC"}],
        "limit": 1000,
        "offset": 0,
    }

    try:
        resp = requests.post(url, headers=OZON_HEADERS, json=payload, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Ozon API error (analytics): {e}")
        return {"ok": False, "message": "Аналитика недоступна (ошибка /v1/analytics/data)"}

    data = resp.json()
    result = data.get("result", {})
    rows = result.get("data", []) or []

    if not rows:
        return {"ok": True, "revenue": 0.0, "ordered_units": 0}

    sku_str = str(sku)
    total_revenue = 0.0
    total_units = 0

    for row in rows:
        dims = row.get("dimensions", []) or []
        if not dims:
            continue
        if dims[0].get("id") != sku_str:
            continue

        metrics = row.get("metrics", []) or []
        if len(metrics) >= 2:
            total_revenue += metrics[0]
            total_units += metrics[1]

    return {"ok": True, "revenue": total_revenue, "ordered_units": total_units}


def get_ozon_stats(artikul: str) -> dict:
    """
    Базовая инфа по товару в Ozon:
      - цена
      - остаток
      - продажи за 14 дней
      - закупка из Google Sheets
    """
    artikul = artikul.strip()

    if artikul.isdigit():
        payload_info = {
            "offer_id": [],
            "product_id": [],
            "sku": [int(artikul)],
        }
    else:
        payload_info = {
            "offer_id": [artikul],
            "product_id": [],
            "sku": [],
        }

    url_info = f"{OZON_API_URL}/v3/product/info/list"

    try:
        resp = requests.post(url_info, headers=OZON_HEADERS, json=payload_info, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Ozon API error (info): {e}")
        return {"error": f"Ошибка при запросе к Ozon API: {e}"}

    data = resp.json()
    items = data.get("items", []) or []

    if not items:
        return {
            "error": (
                f"Товар с идентификатором '{artikul}' не найден в Ozon.\n"
                "Проверь, что товар есть в этом кабинете продавца и что ты вводишь "
                "либо offer_id (артикул продавца), либо SKU."
            )
        }

    item = items[0]

    # Цена
    price_str = item.get("price") or "0"
    try:
        price = float(price_str.replace(",", "."))
    except (ValueError, AttributeError):
        price = 0.0

    # Остатки
    stocks_info = item.get("stocks") or {}
    stocks_list = stocks_info.get("stocks", []) or []
    stock_qty = sum(st.get("present", 0) for st in stocks_list)

    # Продажи за 14 дней
    sku_value = item.get("sku")
    orders = "позже подключим"
    sales_qty = "позже подключим"
    revenue = "позже подключим"

    if sku_value:
        analytics = get_ozon_sales_for_sku(sku_value, days=14)
        if analytics.get("ok"):
            revenue = round(analytics.get("revenue", 0.0), 2)
            units = int(analytics.get("ordered_units", 0))
            orders = units
            sales_qty = units
        else:
            msg = analytics.get("message", "нет данных по /v1/analytics/data")
            orders = msg
            sales_qty = msg
            revenue = msg

    # Закупка из Google Sheets
    offer_id_for_sheet = item.get("offer_id") or artikul
    cost = get_cost_by_article(offer_id_for_sheet)
    if cost is None:
        purchase_price = "нет в таблице"
    else:
        purchase_price = cost

    return {
        "orders": orders,
        "sales_qty": sales_qty,
        "revenue": revenue,
        "sell_price": price,
        "purchase_price": purchase_price,
        "stock_qty": stock_qty,
    }


def fake_wb_stats(artikul: str) -> dict:
    """Пока заглушка для WB."""
    return {
        "orders": 52,
        "sales_qty": 47,
        "revenue": 52000,
        "sell_price": 1100,
        "purchase_price": 700,
        "stock_qty": 210,
    }


def format_ozon_stats(artikul: str, s: dict) -> str:
    if "error" in s:
        return f"<b>Ozon · {artikul}</b>\n\n❌ {s['error']}"

    purchase = s["purchase_price"]
    if isinstance(purchase, (int, float)):
        purchase_str = f"{purchase:.1f} ₽"
    else:
        purchase_str = str(purchase)

    return (
        f"<b>Ozon · {artikul}</b>\n"
        f"Заказы: {s['orders']}\n"
        f"Продажи (шт): {s['sales_qty']}\n"
        f"Выручка: {s['revenue']}\n"
        f"Средняя цена продажи: {s['sell_price']} ₽\n"
        f"Закупка: {purchase_str}\n"
        f"Остаток: {s['stock_qty']} шт\n"
    )


def format_wb_stats(artikul: str, s: dict) -> str:
    return (
        f"<b>Wildberries · {artikul}</b>\n"
        f"Заказы: {s['orders']}\n"
        f"Продажи (шт): {s['sales_qty']}\n"
        f"Выручка: {s['revenue']} ₽\n"
        f"Средняя цена продажи: {s['sell_price']} ₽\n"
        f"Закупка: {s['purchase_price']}\n"
        f"Остаток: {s['stock_qty']} шт\n"
    )


# ================== УВЕДОМЛЕНИЯ О НОВЫХ FBS ==================

def fetch_new_fbs_postings(hours_back: int = 1):
    """
    Новые FBS-постинги в статусе awaiting_packaging за последние hours_back часов.
    Используем /v3/posting/fbs/unfulfilled/list.
    """
    global KNOWN_FBS_POSTINGS

    url = f"{OZON_API_URL}/v3/posting/fbs/unfulfilled/list"

    now = datetime.now(timezone.utc)
    cutoff_to = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    cutoff_from = (now - timedelta(hours=hours_back)).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")

    payload = {
        "dir": "asc",
        "filter": {
            "status": "awaiting_packaging",
            "cutoff_from": cutoff_from,
            "cutoff_to": cutoff_to,
        },
        "limit": 100,
        "offset": 0,
    }

    try:
        resp = requests.post(url, headers=OZON_HEADERS, json=payload, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Ozon API error (FBS unfulfilled): {e}")
        return []

    data = resp.json()
    postings = data.get("result", {}).get("postings", []) or []

    new_postings = []
    for p in postings:
        posting_number = p.get("posting_number")
        if not posting_number:
            continue
        if posting_number in KNOWN_FBS_POSTINGS:
            continue
        KNOWN_FBS_POSTINGS.add(posting_number)
        new_postings.append(p)

    return new_postings


def format_fbs_notification(posting: dict) -> str:
    posting_number = posting.get("posting_number", "—")
    order_number = posting.get("order_number", "—")
    status = posting.get("status", "—")

    products = posting.get("products", []) or []

    lines = []
    for prod in products:
        name = prod.get("name") or prod.get("offer_id") or "Товар"
        offer_id = prod.get("offer_id", "")
        qty = prod.get("quantity", 0)
        line = f"• {name}"
        if offer_id:
            line += f" ({offer_id})"
        line += f" — {qty} шт"
        lines.append(line)

    products_block = "\n".join(lines) if lines else "Без списка товаров"

    text = (
        f"🆕 Новый заказ FBS\n\n"
        f"<b>Posting:</b> {posting_number}\n"
        f"<b>Order:</b> {order_number}\n"
        f"<b>Статус:</b> {status}\n\n"
        f"<b>Товары:</b>\n{products_block}"
    )
    return text


async def check_fbs_orders_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Периодический джоб: ищет новые FBS-заказы и шлёт владельцу бота.
    """
    global ADMIN_CHAT_ID

    if not ADMIN_CHAT_ID:
        return

    new_postings = fetch_new_fbs_postings(hours_back=1)
    if not new_postings:
        return

    for posting in new_postings:
        msg = format_fbs_notification(posting)
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=msg,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Failed to send FBS notification: {e}")


# ================== ОТЧЁТ ПО ЗАКАЗАМ ЗА ПЕРИОД ==================

def fetch_fbs_orders_grouped(days: int):
    """
    Забираем FBS-заказы за последние days дней через /v3/posting/fbs/list
    и группируем по offer_id:
      data[offer_id] = { name, qty }
    """
    url = f"{OZON_API_URL}/v3/posting/fbs/list"

    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).isoformat(timespec="seconds").replace("+00:00", "Z")
    to = now.isoformat(timespec="seconds").replace("+00:00", "Z")

    payload = {
        "dir": "ASC",
        "limit": 1000,
        "offset": 0,
        "with": {
            "analytics_data": False,
            "financial_data": False,
        },
        "filter": {
            "since": since,
            "to": to,
            "status": "",  # все статусы
            "delivery_method_id": [],
            "warehouse_id": [],
        },
    }

    try:
        resp = requests.post(url, headers=OZON_HEADERS, json=payload, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Ozon API error (FBS list): {e}")
        return {"ok": False, "error": f"Ошибка при запросе заказов: {e}"}

    data = resp.json()
    postings = data.get("result", {}).get("postings", []) or []

    grouped = defaultdict(lambda: {"name": "", "qty": 0})

    for p in postings:
        products = p.get("products", []) or []

        for prod in products:
            offer_id = prod.get("offer_id")
            if not offer_id:
                continue

            name = prod.get("name") or offer_id
            qty = prod.get("quantity", 0)

            g = grouped[offer_id]
            if not g["name"]:
                g["name"] = name
            g["qty"] += qty

    return {"ok": True, "data": grouped}


def format_orders_report(days: int, grouped_data: dict) -> str:
    """
    grouped_data — словарь из fetch_fbs_orders_grouped()["data"].

    Берём из юнит-экономики:
      sell_price  — цена продажи (руб/шт)
      commission  — комиссия (руб/шт, столбец L)
      logistics   — логистика полная (руб/шт, столбец O)
      storage     — хранение (руб/шт, столбец Q)
      extra       — доп.расходы (руб/шт, столбец R)
      cost        — себестоимость (руб/шт, столбец D)

    EBITDA за шт. = sell_price - (commission + logistics + storage + extra + cost)
    """
    if not grouped_data:
        return f"За последние {days} дн. заказов Ozon не найдено."

    lines = [f"📊 Отчёт по заказам Ozon за последние {days} дн.:\n"]

    total_revenue = 0.0
    total_cost = 0.0
    total_commission = 0.0
    total_logistics = 0.0
    total_storage = 0.0
    total_extra = 0.0
    total_ebitda = 0.0

    for offer_id, info in grouped_data.items():
        name = info["name"]
        qty = info["qty"]

        ue = get_unit_economy_by_article(offer_id)
        if ue is None:
            # если строки нет в таблице — просто показываем qty
            lines.append(
                f"<b>{offer_id}</b> · {name}\n"
                f"  Кол-во: {qty} шт\n"
                f"  ⚠ Нет строки в юнит-экономике\n"
            )
            continue

        sell_price = ue.get("sell_price") or 0.0
        commission_per_unit = ue.get("commission") or 0.0  # руб/шт из столбца L
        logistics = ue.get("logistics") or 0.0
        storage = ue.get("storage") or 0.0
        extra = ue.get("extra") or 0.0
        cost = ue.get("cost") or 0.0

        # выручка
        revenue = sell_price * qty

        # расходы по статьям
        commission_total = commission_per_unit * qty
        logistics_total = logistics * qty
        storage_total = storage * qty
        extra_total = extra * qty
        cost_total = cost * qty

        # EBITDA
        ebitda_unit = sell_price - (commission_per_unit + logistics + storage + extra + cost)
        ebitda_total = ebitda_unit * qty

        total_revenue += revenue
        total_cost += cost_total
        total_commission += commission_total
        total_logistics += logistics_total
        total_storage += storage_total
        total_extra += extra_total
        total_ebitda += ebitda_total

        lines.append(
            f"<b>{offer_id}</b> · {name}\n"
            f"  Кол-во: {qty} шт\n"
            f"  Цена продажи: {sell_price:.2f} ₽\n"
            f"  Выручка: {revenue:.2f} ₽\n"
            f"  Комиссия: {commission_per_unit:.2f} ₽/шт → {commission_total:.2f} ₽\n"
            f"  Логистика: {logistics:.2f} ₽/шт → {logistics_total:.2f} ₽\n"
            f"  Хранение: {storage:.2f} ₽/шт → {storage_total:.2f} ₽\n"
            f"  Доп. расходы: {extra:.2f} ₽/шт → {extra_total:.2f} ₽\n"
            f"  Себестоимость: {cost:.2f} ₽/шт → {cost_total:.2f} ₽\n"
            f"  EBITDA за шт.: {ebitda_unit:.2f} ₽\n"
            f"  EBITDA всего: {ebitda_total:.2f} ₽\n"
        )

    lines.append(
        "\n<b>Итого по отчёту:</b>\n"
        f"Выручка: {total_revenue:.2f} ₽\n"
        f"Себестоимость: {total_cost:.2f} ₽\n"
        f"Комиссия: {total_commission:.2f} ₽\n"
        f"Логистика: {total_logistics:.2f} ₽\n"
        f"Хранение: {total_storage:.2f} ₽\n"
        f"Доп. расходы: {total_extra:.2f} ₽\n"
        f"<b>EBITDA (до налогов): {total_ebitda:.2f} ₽</b>"
    )

    return "\n".join(lines)


# =============== ХЕЛПЕР ДЛЯ ДЛИННЫХ СООБЩЕНИЙ ===============

async def send_long_html_message(
    chat_id: int,
    text: str,
    context: ContextTypes.DEFAULT_TYPE,
    max_len: int = 3500,
):
    """
    Режет большое HTML-сообщение на части по абзацам (\n\n)
    и шлёт их по очереди, чтобы не ловить 'Message is too long'.
    """
    paragraphs = text.split("\n\n")
    buf = ""

    for p in paragraphs:
        if buf:
            candidate = buf + "\n\n" + p
        else:
            candidate = p

        if len(candidate) > max_len:
            # отправляем накопленное и начинаем новый буфер
            await context.bot.send_message(
                chat_id=chat_id,
                text=buf,
                parse_mode="HTML",
            )
            buf = p
        else:
            buf = candidate

    if buf:
        await context.bot.send_message(
            chat_id=chat_id,
            text=buf,
            parse_mode="HTML",
        )


# ================== HANDLERS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start — запоминаем ADMIN_CHAT_ID и показываем главное меню.
    """
    global ADMIN_CHAT_ID

    user = update.effective_user
    chat_id = update.effective_chat.id

    ADMIN_CHAT_ID = chat_id  # этот чат будет получать пуши по FBS

    user_state.pop(chat_id, None)

    text = (
        f"Привет, {user.first_name}!\n\n"
        "Я бот-агент для маркетплейсов.\n"
        "Сейчас могу:\n"
        "• Показать аналитику по отдельным товарам Ozon\n"
        "• Пример аналитики по WB\n"
        "• Сделать отчёт по заказам Ozon за период (по артикулам)\n\n"
        "Выбери действие:"
    )

    if update.message:
        await update.message.reply_text(text, reply_markup=get_main_menu_keyboard())
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=get_main_menu_keyboard(),
        )


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /menu — просто выводит главное меню.
    """
    chat_id = update.effective_chat.id
    user_state.pop(chat_id, None)
    await show_main_menu(chat_id, context)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id
    data = query.data

    if data == "ozon_analytics":
        user_state[chat_id] = STATE_OZON
        await query.message.reply_text(
            "Режим: Ozon.\n"
            "Введи артикул <b>Ozon</b> (offer_id или SKU).\n"
            "Чтобы вернуться в меню — напиши <code>меню</code> или команду /menu.",
            parse_mode="HTML",
        )

    elif data == "wb_analytics":
        user_state[chat_id] = STATE_WB
        await query.message.reply_text(
            "Режим: Wildberries.\n"
            "Введи артикул <b>WB</b>.\n"
            "Чтобы вернуться в меню — напиши <code>меню</code> или команду /menu.",
            parse_mode="HTML",
        )

    elif data == "ozon_orders_menu":
        await query.message.reply_text(
            "Выбери период для отчёта по заказам Ozon:",
            reply_markup=get_orders_period_keyboard(),
        )

    elif data.startswith("ozon_orders_"):
        days = int(data.split("_")[-1])

        result = fetch_fbs_orders_grouped(days)
        if not result["ok"]:
            await query.message.reply_text(
                f"Ошибка при получении заказов: {result['error']}"
            )
            return

        report_text = format_orders_report(days, result["data"])
        # тут используем безопасную отправку длинного текста
        await send_long_html_message(chat_id, report_text, context)


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    text = update.message.text.strip()

    # глобальная команда "меню" текстом
    if text.lower() in ("меню", "menu", "главное меню"):
        user_state.pop(chat_id, None)
        await show_main_menu(chat_id, context)
        return

    state = user_state.get(chat_id)

    if state == STATE_OZON:
        stats = get_ozon_stats(text)
        msg = format_ozon_stats(text, stats)
        await update.message.reply_text(msg, parse_mode="HTML")
        await update.message.reply_text(
            "Можешь ввести следующий артикул Ozon.\n"
            "Или напиши <code>меню</code> или /menu для возврата в главное меню.",
            parse_mode="HTML",
        )

    elif state == STATE_WB:
        stats = fake_wb_stats(text)
        msg = format_wb_stats(text, stats)
        await update.message.reply_text(msg, parse_mode="HTML")
        await update.message.reply_text(
            "Можешь ввести следующий артикул WB.\n"
            "Или напиши <code>меню</code> или /menu для возврата в главное меню.",
            parse_mode="HTML",
        )

    else:
        await update.message.reply_text(
            "Пока я понимаю только команды через меню.\n"
            "Нажми /start или /menu и выбери действие.",
        )


# ================== MAIN ==================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu_command))

    # кнопки
    app.add_handler(CallbackQueryHandler(button_handler))

    # обычный текст
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # 🔔 Периодическая проверка FBS заказов (каждые 60 сек, первый запуск через 10 сек)
    if app.job_queue is not None:
        app.job_queue.run_repeating(
            check_fbs_orders_job,
            interval=60,
            first=10,
        )
    else:
        logger.warning("JobQueue не инициализировался — уведомления FBS работать не будут.")

    app.run_polling()


if __name__ == "__main__":
    main()
