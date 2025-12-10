import gspread
from google.oauth2.service_account import Credentials

# ID твоей таблицы
SPREADSHEET_ID = "1Gbf7CCWVn2Lwi3O3PTzEqJBC5a2AMPKCgQYWF16MqV0"
SHEET_NAME = "Юнит экономика оз"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _open_sheet():
    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=SCOPES,
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(SHEET_NAME)


def _to_number(value: str) -> float:
    """
    Преобразуем строку вида 'р.1 234,56', '1 234,56', '25%' и т.п.
    в float. Пустое или мусор -> 0.0
    """
    if value is None:
        return 0.0
    s = str(value)
    s = s.replace("р.", "").replace("₽", "")
    s = s.replace(" ", "").replace("\u00a0", "")
    s = s.replace(",", ".")
    s = s.replace("%", "")
    s = s.strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def get_unit_economy_by_article(article_code: str) -> dict | None:
    """
    Возвращает параметры юнит-экономики по артикулу продавца:

    {
        "cost": себестоимость, руб/шт (D)
        "sell_price": цена продажи, руб/шт (I)
        "commission": комиссия, руб/шт (L)
        "logistics": логистика полная, руб/шт (O)
        "storage": хранение за ед (60 дней), руб/шт (Q)
        "extra": доп расходы, руб/шт (R)
    }

    Если артикул не найден — None.
    """
    ws = _open_sheet()

    header = ws.row_values(1)
    idx = {name: i for i, name in enumerate(header)}

    col_article = idx.get("Артикул")
    if col_article is None:
        return None

    all_rows = ws.get_all_values()[1:]  # без заголовка

    for row in all_rows:
        if len(row) <= col_article:
            continue
        if row[col_article].strip() != article_code:
            continue

        cost = _to_number(row[idx.get("Себестоимость", -1)]) if "Себестоимость" in idx else 0.0
        sell_price = _to_number(row[idx.get("Цена продажи", -1)]) if "Цена продажи" in idx else 0.0

        # 👇 комиссия в руб/шт — как ты просил, берём из столбца L "Комиссия"
        commission = 0.0
        if "Комиссия" in idx:
            commission = _to_number(row[idx["Комиссия"]])
        else:
            # на всякий случай: если переименуешь, можно подстраховаться
            pass

        logistics = _to_number(row[idx.get("Логистика полная", -1)]) if "Логистика полная" in idx else 0.0
        storage = _to_number(row[idx.get("Хранение за ед (60 дней)", -1)]) if "Хранение за ед (60 дней)" in idx else 0.0
        extra = _to_number(row[idx.get("Доп расходы", -1)]) if "Доп расходы" in idx else 0.0

        return {
            "cost": cost,
            "sell_price": sell_price,
            "commission": commission,
            "logistics": logistics,
            "storage": storage,
            "extra": extra,
        }

    return None


def get_cost_by_article(article_code: str):
    """
    Старый интерфейс, который использует аналитика по одному артикулу.
    """
    econ = get_unit_economy_by_article(article_code)
    if not econ:
        return None
    return econ["cost"]
