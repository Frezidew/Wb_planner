"""
  collect - забирает данные из WB API (все кабинеты) и складывает в PostgreSQL (каждый день).
  report  - считает спрос, запас в днях, дефицит/излишек, перемещения и заказ на месяц
            и пишет ВСЁ В ОДНУ СВОДНУЮ ТАБЛИЦУ (лист «Сводка») + лист «Состояние».
  all     - collect + report.
  init    - только создать таблицы в базе.

Запуск:  python wb_planner.py all
"""
import os
import sys
import math
import time
import argparse
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

# НАСТРОЙКИ - ТАБЛИЦА И ЛИСТЫ

SPREADSHEET_ID = "*********"
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")

# Листы, куда пишем результат (если листа нет - создастся сам).
SHEET_MASTER = "Сводка"
SHEET_STATUS = "Состояние"

# Лист с настройками, которые скрипт читает ПЕРЕД запуском (создаётся сам при первом запуске).
SHEET_SETTINGS = "Настройки анализа"
# (название в столбце A, имя переменной в скрипте, значение по умолчанию, минимум, максимум, пояснение)
# Чтобы вынести в таблицу ещё какой-то параметр - добавь сюда строку.
SETTINGS_SPEC = [
    ("Окно анализа спроса, дней", "DEMAND_WINDOW_DAYS", 30, 1, 365,
     "За сколько последних дней считать спрос (например 14 или 30). Меняй значение в столбце B."),
]

USE_SHEET_DATA = True
SOURCE_SHEET_NAME = "Свод на сегодня  (копия)"
SRC_COL_SIZE = 2           # B - ключ строки: размер
SRC_COL_ARTICLE = 3        # C - ключ строки: артикул WB
SRC_COL_FROM_CLIENT = 17   # Q - "От клиента" (возвраты, едут на склад)
SRC_COL_RETURNS = 18       # R - "Вернётся после невыкупа"
SRC_COL_BUYOUT = 23        # W - "% выкупа за месяц"
SRC_COL_EXTRA_S = 19       # S - идёт в расчёт столбца "Уже есть и едет" в сводке
SRC_COL_EXTRA_V = 22       # V - идёт в расчёт столбца "Уже есть и едет" в сводке
# Остатки ФБС по регионам (J-O). Название региона слева должно совпадать с BUCKETS.
SRC_FBS_COLS: Dict[str, int] = {
    "ЦФО": 10,           # J
    "ЮФО": 11,           # K
    "Казань": 12,        # L (на листе подписан "Нижнекамск")
    "Екатеринбург": 13,  # M
    "Новосибирск": 14,   # N
    "Красноярск": 15,    # O
}
USE_SHEET_FBS = True       # True - остатки ФБС берём из J-O, а не запрашиваем у WB по каждому складу

# ============================================================
# НАСТРОЙКИ - РЕГИОНЫ И СКЛАДЫ
# ============================================================

BUCKETS = ["ЦФО", "ЮФО", "Казань", "Екатеринбург", "Новосибирск", "Красноярск"]
OTHER_BUCKET = "Прочее"   # сюда попадает всё, что не удалось отнести к региону

# Склады продавца (ФБС) по регионам - id складов из личного кабинета WB
FBS_WAREHOUSES: Dict[str, List[int]] = {
    "ЦФО": [, ],
    "ЮФО": [],
    "Казань": [],
    "Екатеринбург": [],
    "Новосибирск": [],
    "Красноярск": [],
}

# НАСТРОЙКИ - КАБИНЕТЫ WB

ACCOUNTS: List[dict] = [
    {"key": "main", "title": "Кабинет 1", "token_env": "WB_TOKEN_1",
     "only_nm_ids": None, "fbs_warehouses": FBS_WAREHOUSES},
    {"key": "second", "title": "Кабинет 2", "token_env": "WB_TOKEN_2",
     "only_nm_ids": [], "fbs_warehouses": {}},
]

FBO_WAREHOUSE_KEYWORDS: Dict[str, str] = {
    "коледино": "ЦФО", "подольск": "ЦФО", "электросталь": "ЦФО", "белые столбы": "ЦФО",
    "тула": "ЦФО", "рязань": "ЦФО", "владимир": "ЦФО", "воронеж": "ЦФО", "котовск": "ЦФО",
    "чехов": "ЦФО", "санкт-петербург": "ЦФО", "шушары": "ЦФО", "обухово": "ЦФО",
    "краснодар": "ЮФО", "невинномысск": "ЮФО", "волгоград": "ЮФО", "ростов": "ЮФО",
    "саратов": "ЮФО", "астрахань": "ЮФО", "минеральные воды": "ЮФО",
    "казань": "Казань", "самара": "Казань", "уфа": "Казань", "нижний новгород": "Казань",
    "екатеринбург": "Екатеринбург", "тюмень": "Екатеринбург", "челябинск": "Екатеринбург",
    "новосибирск": "Новосибирск", "омск": "Новосибирск", "кемерово": "Новосибирск",
    "красноярск": "Красноярск", "иркутск": "Красноярск", "хабаровск": "Красноярск",
    "владивосток": "Красноярск",
}


# Сначала проверяется название региона (область/край), потом федеральный округ.
REGION_TO_BUCKET_OVERRIDES: Dict[str, str] = {
    "красноярск": "Красноярск", "хакас": "Красноярск", "тыва": "Красноярск",
    "иркутск": "Красноярск", "бурят": "Красноярск", "забайкаль": "Красноярск",
}
DISTRICT_TO_BUCKET: Dict[str, str] = {
    "центральн": "ЦФО", "северо-запад": "ЦФО",
    "южн": "ЮФО", "северо-кавказ": "ЮФО",
    "приволж": "Казань",
    "уральск": "Екатеринбург",
    "сибирск": "Новосибирск",
    "дальневосточ": "Красноярск",
}

# НАСТРОЙКИ - РАСЧЁТ

DEMAND_WINDOW_DAYS = 30     # за сколько последних дней считаем спрос (перед запуском берётся из листа «Настройки»)
TARGET_COVER_DAYS = 30      # нормальный запас в днях
DEFICIT_DAYS = 14           # запаса меньше этого - дефицит
SURPLUS_DAYS = 60           # запаса больше этого - излишек
DONOR_KEEP_DAYS = 30        # сколько дней запаса обязаны оставить на складе, откуда забираем
MIN_TRANSFER_QTY = 5        # перемещения меньше этого не предлагаем

ORDER_HORIZON_DAYS = 30     # на сколько дней вперёд считаем заказ (следующий месяц)
SAFETY_DAYS = 14            # запас на непредвиденное, в днях спроса
LEAD_TIME_DAYS = 0          # срок производства/доставки, в днях (если знаешь - впиши)
USE_BUYOUT_IN_ORDER = True  # переводить заказы в ожидаемые выкупы по % выкупа
BUYOUT_MATURE_FROM_DAYS = 75   # % выкупа считаем по заказам, сделанным 75..45 дней назад
BUYOUT_MATURE_TO_DAYS = 45     # (они уже успели выкупиться или вернуться)
BUYOUT_MIN_ORDERS = 10         # меньше заказов - % выкупа не считаем (берём 100%)

INITIAL_BACKFILL_DAYS = 90  # при первом запуске столько дней истории пробуем забрать

# Порядок размеров в сводке (всё, чего нет в списке, идёт после)
SIZE_ORDER = ["XS", "S", "M", "L", "XL", "XXL", "3XL", "4XL"]

# Эти артикулы продавца полностью исключаются из расчётов и из сводки
# (сравнение точное, без учёта регистра; проверяется и артикул продавца, и название карточки).
EXCLUDE_ARTICLES: List[str] = ["Кл_бамбук_шорты", "Кл_бамбук_штаны", "Кл_белый_в_полоску"]

# Если у одного размера несколько баркодов: "max" - берём максимум, "sum" - складываем
FBS_MULTI_BARCODE_MODE = "max"

COLLECT_TRANSIT = False     #
TEST_NM_IDS: List[int] = []
TEST_MODE = False           # True и список выше пустой - берём просто первые TEST_LIMIT артикулов
TEST_LIMIT = 5
DRY_RUN = False             # True - в таблицу ничего не пишем

STAT_GAP = 2.0
MIN_REQUEST_GAP = 1.2
FALLBACK_SLEEP = 2.0

# ТОКЕНЫ И БАЗА

load_dotenv()
for _acct in ACCOUNTS:
    _acct["token"] = os.getenv(_acct["token_env"])
if not ACCOUNTS[0]["token"]:
    raise RuntimeError(f"Не найден {ACCOUNTS[0]['token_env']} в .env")
ACTIVE_ACCOUNTS = [a for a in ACCOUNTS if a["token"]]
for _acct in ACCOUNTS:
    if not _acct["token"]:
        print(f"⚠️ Не найден {_acct['token_env']} в .env - «{_acct['title']}» пропускаем.")

# Кабинет, с которым скрипт работает в данный момент (переключается через use_account)
CURRENT: Dict[str, dict] = {"account": ACTIVE_ACCOUNTS[0]}


def use_account(acct: dict):
    CURRENT["account"] = acct


def wb_headers() -> Dict[str, str]:
    return {"Authorization": CURRENT["account"]["token"], "Content-Type": "application/json"}


def allowed_nm_ids() -> Optional[set]:
    """Список артикулов текущего кабинета, если он ограничен (иначе None = все)."""
    lst = CURRENT["account"].get("only_nm_ids")
    return {int(x) for x in lst} if lst else None


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/wb_planner")

GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

CARDS_LIST_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
ORDERS_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
SALES_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
WB_WAREHOUSES_STOCK_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses"
SIZES_REPORT_URL = "https://seller-analytics-api.wildberries.ru/api/v2/stocks-report/products/sizes"
FBS_STOCKS_URL = "https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}"
FBS_BATCH_SIZE = 500


@contextmanager
def conn_ctx():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS skus (
    nm_id BIGINT, size TEXT, chrt_id BIGINT, name TEXT,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (nm_id, size)
);
ALTER TABLE skus ADD COLUMN IF NOT EXISTS vendor_code TEXT;
ALTER TABLE skus ADD COLUMN IF NOT EXISTS account TEXT;
CREATE TABLE IF NOT EXISTS barcodes (
    barcode TEXT PRIMARY KEY, nm_id BIGINT, size TEXT
);
CREATE TABLE IF NOT EXISTS stock_snapshot (
    snap_date DATE, nm_id BIGINT, size TEXT, bucket TEXT, stock_type TEXT, qty INT,
    PRIMARY KEY (snap_date, nm_id, size, bucket, stock_type)
);
CREATE TABLE IF NOT EXISTS orders (
    srid TEXT PRIMARY KEY, order_date DATE, nm_id BIGINT, size TEXT, bucket TEXT,
    warehouse TEXT, region TEXT, is_cancel BOOLEAN, last_change TIMESTAMP
);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS account TEXT;
CREATE INDEX IF NOT EXISTS ix_orders_date ON orders (order_date);
CREATE TABLE IF NOT EXISTS sales (
    sale_id TEXT PRIMARY KEY, srid TEXT, sale_date DATE, nm_id BIGINT, size TEXT,
    is_return BOOLEAN, last_change TIMESTAMP
);
ALTER TABLE sales ADD COLUMN IF NOT EXISTS account TEXT;
CREATE INDEX IF NOT EXISTS ix_sales_srid ON sales (srid);
CREATE TABLE IF NOT EXISTS transit_snapshot (
    snap_date DATE, nm_id BIGINT, size TEXT, to_client INT, from_client INT,
    PRIMARY KEY (snap_date, nm_id, size)
);
CREATE TABLE IF NOT EXISTS recommendation (
    calc_date DATE, kind TEXT, nm_id BIGINT, size TEXT, from_bucket TEXT, to_bucket TEXT,
    qty INT, priority TEXT, comment TEXT
);
CREATE TABLE IF NOT EXISTS order_plan (
    calc_date DATE, nm_id BIGINT, size TEXT, stock_total INT, from_client INT,
    orders_window INT, buyout_rate REAL, net_demand_per_day REAL, order_qty INT,
    PRIMARY KEY (calc_date, nm_id, size)
);
CREATE TABLE IF NOT EXISTS sheet_snapshot (
    snap_date DATE, nm_id BIGINT, size TEXT, pipeline_total INT, from_client INT, buyout_rate REAL,
    PRIMARY KEY (snap_date, nm_id, size)
);
ALTER TABLE sheet_snapshot ADD COLUMN IF NOT EXISTS returns_expected INT;
ALTER TABLE sheet_snapshot ADD COLUMN IF NOT EXISTS extra_s INT;
ALTER TABLE sheet_snapshot ADD COLUMN IF NOT EXISTS extra_v INT;
CREATE TABLE IF NOT EXISTS run_log (
    id SERIAL PRIMARY KEY, ts TIMESTAMPTZ DEFAULT now(),
    step TEXT, status TEXT, rows_cnt INT, message TEXT
);
"""


def init_db():
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    print("База готова.")


def log_step(step: str, status: str, rows: int = 0, message: str = ""):
    print(f"[{status}] {step}: {rows}" + (f" | {message}" if message else ""))
    try:
        with conn_ctx() as c, c.cursor() as cur:
            cur.execute(
                "INSERT INTO run_log (step, status, rows_cnt, message) VALUES (%s,%s,%s,%s)",
                (step, status, int(rows), (message or "")[:1000]),
            )
    except Exception as e:
        print(f"   (не удалось записать лог: {e})")


def run_step(name: str, fn):
    try:
        rows, msg = fn()
        log_step(name, "OK", rows, msg)
    except Exception as e:
        log_step(name, "ОШИБКА", 0, str(e))


def query_df(conn, sql: str, params=None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ============================================================
# УТИЛИТЫ
# ============================================================

SIZE_RANGE_MAP = {
    '42-44': 'S', '44-46': 'M', '46-48': 'L', '48-50': 'XL', '50-52': 'XXL', '52-54': '3XL',
}


def normalize_size(val) -> str:
    if val is None:
        return "0"
    s = str(val).strip().upper()
    if not s:
        return "0"
    for dash in ['–', '—', '−', '―', '‒']:
        s = s.replace(dash, '-')
    s = s.replace(' -', '-').replace('- ', '-')
    if s in SIZE_RANGE_MAP:
        return SIZE_RANGE_MAP[s]
    size_map = {
        '2XL': 'XXL', 'XXXL': '3XL', 'XXXXL': '4XL',
        '01': 'XS', '02': 'S', '03': 'M', '04': 'L',
        '05': 'XL', '06': 'XXL', '07': '3XL',
    }
    return size_map.get(s, s)


def parse_dt(raw) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "")).replace(tzinfo=None)
    except Exception:
        return None


def bucket_by_region(district: str, region: str) -> str:
    r = (region or "").lower()
    d = (district or "").lower()
    for key, bucket in REGION_TO_BUCKET_OVERRIDES.items():
        if key in r:
            return bucket
    for key, bucket in DISTRICT_TO_BUCKET.items():
        if key in d:
            return bucket
    return OTHER_BUCKET


def bucket_by_wb_warehouse(name: str) -> str:
    n = (name or "").lower()
    for key, bucket in FBO_WAREHOUSE_KEYWORDS.items():
        if key in n:
            return bucket
    return OTHER_BUCKET


def wb_request(method: str, url: str, params=None, json_body=None, retries: int = 6,
               base_wait: float = 15.0) -> requests.Response:
    """Запрос к WB (токеном текущего кабинета) с повтором при 429 и сетевых сбоях.
    Ошибки 4xx/5xx выбрасывают исключение."""
    for attempt in range(retries):
        try:
            resp = requests.request(method, url, headers=wb_headers(), params=params,
                                    json=json_body, timeout=120)
        except requests.exceptions.RequestException as e:
            print(f"     [сеть] {e}. Попытка {attempt + 1}/{retries}")
            time.sleep(5)
            continue
        if resp.status_code == 429:
            try:
                wait = float(resp.headers.get("X-Ratelimit-Retry"))
            except (TypeError, ValueError):
                wait = base_wait * (2 ** attempt)
            print(f"     [429] Лимит запросов, ждём {wait:.0f} сек... ({attempt + 1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code} {url}: {resp.text[:300]}")
        return resp
    raise RuntimeError(f"Не удалось получить ответ после {retries} попыток: {url}")


def pace(resp: requests.Response):
    remaining = resp.headers.get("X-Ratelimit-Remaining")
    try:
        remaining = int(remaining)
    except (TypeError, ValueError):
        time.sleep(FALLBACK_SLEEP)
        return
    if remaining <= 0:
        try:
            time.sleep(float(resp.headers.get("X-Ratelimit-Reset")))
        except (TypeError, ValueError):
            time.sleep(FALLBACK_SLEEP)
    else:
        time.sleep(MIN_REQUEST_GAP)


def get_nm_ids(all_accounts: bool = False) -> List[int]:
    with conn_ctx() as c, c.cursor() as cur:
        if all_accounts:
            cur.execute("SELECT DISTINCT nm_id FROM skus ORDER BY nm_id")
        else:
            cur.execute("SELECT DISTINCT nm_id FROM skus WHERE COALESCE(account, 'main') = %s ORDER BY nm_id",
                        (CURRENT["account"]["key"],))
        ids = [r[0] for r in cur.fetchall()]
    if TEST_NM_IDS:
        wanted = {int(x) for x in TEST_NM_IDS}
        if all_accounts:
            missing = wanted - set(ids)
            if missing:
                print(f"⚠Этих артикулов нет среди карточек: {sorted(missing)}")
        ids = [i for i in ids if i in wanted]
        if all_accounts:
            print(f"ТЕСТ: работаем только по артикулам {ids}")
    elif TEST_MODE:
        ids = ids[:TEST_LIMIT]
        if all_accounts:
            print(f"ТЕСТ: работаем только по первым {TEST_LIMIT} артикулам {ids}")
    return ids

# СБОР ДАННЫХ: КАРТОЧКИ

def collect_cards() -> Tuple[int, str]:
    acct = CURRENT["account"]
    allowed = allowed_nm_ids()
    # (nm_id, размер) -> (chrt_id, название, артикул продавца)
    skus: Dict[Tuple[int, str], Tuple[Optional[int], str, str]] = {}
    barcodes: Dict[str, Tuple[int, str]] = {}
    collisions = 0
    cursor = {"limit": 100}

    while True:
        payload = {"settings": {"cursor": cursor, "filter": {"withPhoto": -1}}}
        data = wb_request("POST", CARDS_LIST_URL, json_body=payload).json()
        for card in data.get("cards", []):
            nm_id = card.get("nmID")
            if not nm_id:
                continue
            if allowed is not None and int(nm_id) not in allowed:
                continue   # кабинет ограничен списком артикулов
            title = card.get("title") or card.get("vendorCode") or ""
            vendor = (card.get("vendorCode") or "").strip()   # артикул продавца
            for size in card.get("sizes", []):
                tech = normalize_size(size.get("techSize"))
                key = (int(nm_id), tech)
                chrt = size.get("chrtID")
                chrt = int(chrt) if chrt else None
                if key in skus and skus[key][0] and chrt and skus[key][0] != chrt:
                    collisions += 1   # два разных размера карточки стали одним после нормализации
                skus[key] = (chrt, title, vendor)
                for bc in size.get("skus", []) or []:
                    barcodes[str(bc)] = key
        new_cursor = data.get("cursor", {})
        if new_cursor.get("total", 0) < cursor.get("limit", 100):
            break
        cursor = {"limit": 100, "updatedAt": new_cursor.get("updatedAt"), "nmID": new_cursor.get("nmID")}
        time.sleep(0.6)

    sku_rows = [(nm, sz, chrt, name, vendor, acct["key"]) for (nm, sz), (chrt, name, vendor) in skus.items()]
    bc_rows = [(bc, nm, sz) for bc, (nm, sz) in barcodes.items()]
    with conn_ctx() as c, c.cursor() as cur:
        execute_values(cur, """
            INSERT INTO skus (nm_id, size, chrt_id, name, vendor_code, account) VALUES %s
            ON CONFLICT (nm_id, size) DO UPDATE
            SET chrt_id = EXCLUDED.chrt_id, name = EXCLUDED.name,
                vendor_code = EXCLUDED.vendor_code, account = EXCLUDED.account, updated_at = now()""",
                       sku_rows, page_size=2000)
        execute_values(cur, """
            INSERT INTO barcodes (barcode, nm_id, size) VALUES %s
            ON CONFLICT (barcode) DO UPDATE SET nm_id = EXCLUDED.nm_id, size = EXCLUDED.size""",
                       bc_rows, page_size=5000)
    no_vendor = sum(1 for v in skus.values() if not v[2])
    msg = f"размеров {len(sku_rows)}, баркодов {len(bc_rows)}"
    if allowed is not None:
        not_found = sorted(allowed - {k[0] for k in skus})
        if not_found:
            msg += f";  артикулы не найдены в этом кабинете (проверь токен): {not_found}"
    if no_vendor:
        msg += f"; без артикула продавца: {no_vendor}"
    if collisions:
        msg += f"; {collisions} размеров слились после нормализации (проверь SIZE_RANGE_MAP)"
    return len(sku_rows), msg

# СБОР ДАННЫХ: ЗАКАЗЫ И ПРОДАЖИ

def load_pages(url: str, start_dt: datetime, label: str, max_iter: int = 200) -> list:
    all_data, last, cur_dt = [], None, start_dt
    for _ in range(max_iter):
        resp = wb_request("GET", url, params={"dateFrom": cur_dt.strftime("%Y-%m-%dT%H:%M:%S")}, base_wait=30)
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        all_data.extend(data)
        dts = [d for d in (parse_dt(x.get("lastChangeDate")) for x in data) if d]
        if not dts:
            break
        mx = max(dts)
        if last is not None and mx <= last:
            break
        last, cur_dt = mx, mx
        print(f"     Загружено {label}: {len(all_data)} (курсор: {mx})")
        time.sleep(STAT_GAP)
    return all_data


def _start_from(table: str) -> datetime:
    """С какого момента докачивать данные - отдельно по каждому кабинету."""
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute(f"SELECT max(last_change) FROM {table} WHERE COALESCE(account, 'main') = %s",
                    (CURRENT["account"]["key"],))
        last = cur.fetchone()[0]
    if last:
        return last - timedelta(days=1)
    return datetime.combine(date.today() - timedelta(days=INITIAL_BACKFILL_DAYS), datetime.min.time())


def collect_orders() -> Tuple[int, str]:
    acct_key = CURRENT["account"]["key"]
    allowed = allowed_nm_ids()
    start = _start_from("orders")
    data = load_pages(ORDERS_URL, start, "заказов")
    rows: Dict[str, tuple] = {}
    for it in data:
        srid = it.get("srid")
        dt = parse_dt(it.get("date"))
        if not srid or not dt or not it.get("nmId"):
            continue
        if allowed is not None and int(it["nmId"]) not in allowed:
            continue
        rows[srid] = (
            srid, dt.date(), int(it["nmId"]), normalize_size(it.get("techSize")),
            bucket_by_region(it.get("oblastOkrugName"), it.get("regionName")),
            it.get("warehouseName"), it.get("regionName"), bool(it.get("isCancel")),
            parse_dt(it.get("lastChangeDate")), acct_key,
        )
    if rows:
        with conn_ctx() as c, c.cursor() as cur:
            execute_values(cur, """
                INSERT INTO orders (srid, order_date, nm_id, size, bucket, warehouse, region, is_cancel, last_change, account)
                VALUES %s ON CONFLICT (srid) DO UPDATE SET
                    order_date = EXCLUDED.order_date, nm_id = EXCLUDED.nm_id, size = EXCLUDED.size,
                    bucket = EXCLUDED.bucket, warehouse = EXCLUDED.warehouse, region = EXCLUDED.region,
                    is_cancel = EXCLUDED.is_cancel, last_change = EXCLUDED.last_change,
                    account = EXCLUDED.account""",
                           list(rows.values()), page_size=2000)
    return len(rows), f"с {start:%Y-%m-%d %H:%M}"


def collect_sales() -> Tuple[int, str]:
    acct_key = CURRENT["account"]["key"]
    allowed = allowed_nm_ids()
    start = _start_from("sales")
    data = load_pages(SALES_URL, start, "продаж")
    rows: Dict[str, tuple] = {}
    for it in data:
        sale_id = it.get("saleID")
        dt = parse_dt(it.get("date"))
        if not sale_id or not dt or not it.get("nmId"):
            continue
        if allowed is not None and int(it["nmId"]) not in allowed:
            continue
        rows[str(sale_id)] = (
            str(sale_id), it.get("srid"), dt.date(), int(it["nmId"]), normalize_size(it.get("techSize")),
            str(sale_id).upper().startswith("R"), parse_dt(it.get("lastChangeDate")), acct_key,
        )
    if rows:
        with conn_ctx() as c, c.cursor() as cur:
            execute_values(cur, """
                INSERT INTO sales (sale_id, srid, sale_date, nm_id, size, is_return, last_change, account)
                VALUES %s ON CONFLICT (sale_id) DO UPDATE SET
                    srid = EXCLUDED.srid, sale_date = EXCLUDED.sale_date, nm_id = EXCLUDED.nm_id,
                    size = EXCLUDED.size, is_return = EXCLUDED.is_return, last_change = EXCLUDED.last_change,
                    account = EXCLUDED.account""",
                           list(rows.values()), page_size=2000)
    return len(rows), f"с {start:%Y-%m-%d %H:%M}"

# СБОР ДАННЫХ: ОСТАТКИ

def collect_fbo() -> Tuple[int, str]:
    nm_ids = get_nm_ids()
    if not nm_ids:
        raise RuntimeError("Нет артикулов этого кабинета в базе (сначала должен отработать шаг 'карточки').")
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute("SELECT chrt_id, nm_id, size FROM skus WHERE chrt_id IS NOT NULL")
        chrt_map = {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    totals: Dict[Tuple[int, str, str], int] = {}
    unmapped_wh: Dict[str, int] = {}
    unknown_chrt = 0

    for i in range(0, len(nm_ids), 1000):
        chunk = nm_ids[i:i + 1000]
        offset, limit = 0, 250000
        while True:
            payload = {"nmIds": chunk, "limit": limit, "offset": offset}
            resp = wb_request("POST", WB_WAREHOUSES_STOCK_URL, json_body=payload)
            items = resp.json().get("data", {}).get("items", []) or []
            for it in items:
                wh_name = it.get("warehouseName") or ""
                low = wh_name.lower()
                if "в пути" in low or "всего" in low:   # служебные строки, не склады
                    continue
                chrt = it.get("chrtId")
                if chrt is None or int(chrt) not in chrt_map:
                    unknown_chrt += 1
                    continue
                nm, size = chrt_map[int(chrt)]
                bucket = bucket_by_wb_warehouse(wh_name)
                if bucket == OTHER_BUCKET:
                    unmapped_wh[wh_name or "(без названия)"] = unmapped_wh.get(wh_name or "(без названия)", 0) + 1
                qty = int(it.get("quantity", 0) or 0)
                totals[(nm, size, bucket)] = totals.get((nm, size, bucket), 0) + qty
            if len(items) < limit:
                break
            offset += limit
        pace(resp)

    today = date.today()
    rows = [(today, nm, size, bucket, "FBO", qty) for (nm, size, bucket), qty in totals.items() if qty > 0]
    with conn_ctx() as c, c.cursor() as cur:
        # удаляем только артикулы текущего кабинета, чтобы не стереть данные другого
        cur.execute("DELETE FROM stock_snapshot WHERE snap_date=%s AND stock_type='FBO' AND nm_id = ANY(%s)",
                    (today, [int(x) for x in nm_ids]))
        if rows:
            execute_values(cur, "INSERT INTO stock_snapshot (snap_date,nm_id,size,bucket,stock_type,qty) VALUES %s",
                           rows, page_size=5000)
    msg = f"позиций {len(rows)}"
    if unmapped_wh:
        msg += "; склады без региона (-> Прочее): " + ", ".join(sorted(unmapped_wh))
    if unknown_chrt:
        msg += f"; строк с неизвестным chrtId: {unknown_chrt}"
    return len(rows), msg


def get_stocks_for_warehouse(warehouse_id: int, barcodes: List[str]) -> Dict[str, int]:
    url = FBS_STOCKS_URL.format(warehouse_id=warehouse_id)
    result: Dict[str, int] = {}
    for i in range(0, len(barcodes), FBS_BATCH_SIZE):
        batch = barcodes[i:i + FBS_BATCH_SIZE]
        resp = wb_request("POST", url, json_body={"skus": batch}, base_wait=15)
        for item in resp.json().get("stocks", []):
            result[str(item["sku"])] = result.get(str(item["sku"]), 0) + int(item["amount"])
        time.sleep(0.3)
    return result


def collect_fbs() -> Tuple[int, str]:
    if USE_SHEET_DATA and USE_SHEET_FBS:
        return 0, "пропущено: остатки ФБС берутся из таблицы (столбцы J-O)"
    wh_map = CURRENT["account"].get("fbs_warehouses") or {}
    if not wh_map:
        return 0, "пропущено: для кабинета не заданы склады ФБС (fbs_warehouses в ACCOUNTS)"
    nm_list = get_nm_ids()
    nm_ids = set(nm_list)
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute("SELECT barcode, nm_id, size FROM barcodes")
        bc_map = {r[0]: (r[1], r[2]) for r in cur.fetchall() if r[1] in nm_ids}
    if not bc_map:
        raise RuntimeError("Нет баркодов в базе (сначала должен отработать шаг 'карточки').")
    all_barcodes = list(bc_map.keys())

    totals: Dict[Tuple[int, str, str], int] = {}
    mismatch = 0
    failed_wh = []

    for bucket, wh_ids in wh_map.items():
        for wh in wh_ids:
            print(f"   ФБС склад {wh} ({bucket})...")
            try:
                stocks = get_stocks_for_warehouse(wh, all_barcodes)
            except Exception as e:
                failed_wh.append(f"{wh}: {str(e)[:120]}")
                continue
            per_size: Dict[Tuple[int, str], List[int]] = {}
            for bc, amount in stocks.items():
                if bc in bc_map:
                    per_size.setdefault(bc_map[bc], []).append(amount)
            for (nm, size), amounts in per_size.items():
                if len(set(amounts)) > 1:
                    mismatch += 1
                qty = max(amounts) if FBS_MULTI_BARCODE_MODE == "max" else sum(amounts)
                totals[(nm, size, bucket)] = totals.get((nm, size, bucket), 0) + qty

    today = date.today()
    rows = [(today, nm, size, bucket, "FBS", qty) for (nm, size, bucket), qty in totals.items() if qty > 0]
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute("DELETE FROM stock_snapshot WHERE snap_date=%s AND stock_type='FBS' AND nm_id = ANY(%s)",
                    (today, [int(x) for x in nm_list]))
        if rows:
            execute_values(cur, "INSERT INTO stock_snapshot (snap_date,nm_id,size,bucket,stock_type,qty) VALUES %s",
                           rows, page_size=5000)
    msg = f"позиций {len(rows)}; размеров, где у баркодов разные остатки: {mismatch} (режим {FBS_MULTI_BARCODE_MODE})"
    if failed_wh:
        msg += "; склады с ошибкой: " + " | ".join(failed_wh)
    if failed_wh and not rows:
        raise RuntimeError(msg)
    return len(rows), msg


def collect_transit() -> Tuple[int, str]:
    if not COLLECT_TRANSIT:
        return 0, "отключено (COLLECT_TRANSIT=False)"
    nm_ids = get_nm_ids()
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")
    result: Dict[Tuple[int, str], List[int]] = {}
    errors = []

    for idx, nm_id in enumerate(nm_ids, 1):
        sys.stdout.write(f"\r транзит: {idx}/{len(nm_ids)}   ")
        sys.stdout.flush()
        for stock_type in ("wb", "mp"):
            payload = {
                "nmID": nm_id,
                "currentPeriod": {"start": today_str, "end": today_str},
                "stockType": stock_type,
                "orderBy": {"field": "avgOrders", "mode": "asc"},
                "includeOffice": True,
            }
            try:
                resp = wb_request("POST", SIZES_REPORT_URL, json_body=payload, base_wait=5)
            except Exception as e:
                errors.append(f"{nm_id}: {str(e)[:80]}")
                continue
            for s in resp.json().get("data", {}).get("sizes", []) or []:
                metrics = s.get("metrics", {}) or {}
                key = (nm_id, normalize_size(s.get("name")))
                acc = result.setdefault(key, [0, 0])
                acc[0] += int(metrics.get("toClientCount", 0) or 0)
                acc[1] += int(metrics.get("fromClientCount", 0) or 0)
            pace(resp)
    print()

    rows = [(today, nm, size, v[0], v[1]) for (nm, size), v in result.items()]
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute("DELETE FROM transit_snapshot WHERE snap_date=%s AND nm_id = ANY(%s)",
                    (today, [int(x) for x in nm_ids]))
        if rows:
            execute_values(cur, "INSERT INTO transit_snapshot (snap_date,nm_id,size,to_client,from_client) VALUES %s",
                           rows, page_size=5000)
    msg = f"позиций {len(rows)}"
    if errors:
        msg += f"; ошибок {len(errors)}, например: {errors[0]}"
    return len(rows), msg


def parse_num(raw) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).replace("\u00a0", "").replace(" ", "").replace(",", ".").strip()
    if not s or s.startswith("#"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_percent_cell(raw) -> Optional[float]:
    """'34%' -> 0.34, '0,34' -> 0.34, '#N/A' / пусто -> None."""
    if raw is None:
        return None
    s = str(raw).replace("\u00a0", "").replace(" ", "").strip()
    if not s or s.startswith("#"):
        return None
    had_percent = "%" in s
    s = s.replace("%", "").replace(",", ".")
    try:
        v = float(s)
    except ValueError:
        return None
    return v / 100.0 if (had_percent or v > 1.0) else v


def read_source_sheet(values: List[List[str]]) -> Dict[Tuple[int, str], dict]:
    """
    Разбирает лист-источник. Данные берём только из J-R и W.
    B и C используются лишь чтобы понять, к какому товару и размеру относится строка:
    строка товара (артикул WB в C), под ней строки размеров (размер в B).
    """
    def cell(row, col):
        return row[col - 1] if len(row) >= col else ""

    def to_int(v):
        return int(v) if v is not None else None

    out: Dict[Tuple[int, str], dict] = {}
    cur_nm = None
    for row in values:
        b = cell(row, SRC_COL_SIZE).strip()
        c = cell(row, SRC_COL_ARTICLE).strip()
        digits = "".join(ch for ch in c if ch.isdigit())
        if digits:
            cur_nm = int(digits)
            continue
        if cur_nm and b:
            size = normalize_size(b)
            out[(cur_nm, size)] = {
                "from_client": to_int(parse_num(cell(row, SRC_COL_FROM_CLIENT))),
                "returns": to_int(parse_num(cell(row, SRC_COL_RETURNS))),
                "extra_s": to_int(parse_num(cell(row, SRC_COL_EXTRA_S))),
                "extra_v": to_int(parse_num(cell(row, SRC_COL_EXTRA_V))),
                "buyout": parse_percent_cell(cell(row, SRC_COL_BUYOUT)),
                "fbs": {bucket: int(parse_num(cell(row, col)) or 0) for bucket, col in SRC_FBS_COLS.items()},
            }
    return out


def collect_sheet_data() -> Tuple[int, str]:
    if not USE_SHEET_DATA:
        return 0, "отключено (USE_SHEET_DATA=False)"
    ws = get_spreadsheet().worksheet(SOURCE_SHEET_NAME)
    parsed = read_source_sheet(ws.get_all_values())
    if not parsed:
        raise RuntimeError(f"На листе «{SOURCE_SHEET_NAME}» не найдено строк размеров (проверь номера столбцов).")
    today = date.today()
    rows = [(today, nm, size, v["from_client"], v["buyout"], v["returns"], v["extra_s"], v["extra_v"])
            for (nm, size), v in parsed.items()]
    fbs_rows = [(today, nm, size, bucket, "FBS", qty)
                for (nm, size), v in parsed.items() for bucket, qty in v["fbs"].items() if qty > 0]
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute("DELETE FROM sheet_snapshot WHERE snap_date=%s", (today,))
        execute_values(cur, """INSERT INTO sheet_snapshot
            (snap_date, nm_id, size, from_client, buyout_rate, returns_expected, extra_s, extra_v)
            VALUES %s""", rows, page_size=5000)
        if USE_SHEET_FBS:
            cur.execute("DELETE FROM stock_snapshot WHERE snap_date=%s AND stock_type='FBS'", (today,))
            if fbs_rows:
                execute_values(cur, """INSERT INTO stock_snapshot (snap_date,nm_id,size,bucket,stock_type,qty)
                    VALUES %s ON CONFLICT (snap_date,nm_id,size,bucket,stock_type) DO UPDATE SET qty = EXCLUDED.qty""",
                               fbs_rows, page_size=5000)
    no_bo = sum(1 for v in parsed.values() if v["buyout"] is None)
    msg = f"строк-размеров {len(rows)}; без % выкупа: {no_bo}"
    if USE_SHEET_FBS:
        msg += f"; ФБС из таблицы: {len(fbs_rows)} позиций"
    return len(rows), msg


def collect_all():
    print("\n=== СБОР ДАННЫХ ===")
    # Таблица-источник общая для всех кабинетов - читаем один раз
    run_step("таблица (источник)", collect_sheet_data)
    for acct in ACTIVE_ACCOUNTS:
        use_account(acct)
        t = acct["title"]
        print(f"\n--- {t} ---")
        run_step(f"карточки ({t})", collect_cards)
        run_step(f"заказы ({t})", collect_orders)
        run_step(f"продажи ({t})", collect_sales)
        run_step(f"остатки ФБО ({t})", collect_fbo)
        run_step(f"остатки ФБС ({t})", collect_fbs)
        run_step(f"транзит ({t})", collect_transit)
    use_account(ACTIVE_ACCOUNTS[0])

# РАСЧЁТЫ (чистые функции: на вход таблицы, на выход таблицы)

def build_coverage(stock_df: pd.DataFrame, demand_df: pd.DataFrame, eff_days: float) -> pd.DataFrame:
    """Остаток, спрос и запас в днях по каждому 'товар x размер x регион'."""
    keys = ["nm_id", "size", "bucket"]
    if stock_df.empty:
        piv = pd.DataFrame(columns=keys + ["FBO", "FBS"])
    else:
        piv = stock_df.pivot_table(index=keys, columns="stock_type", values="qty",
                                   aggfunc="sum", fill_value=0).reset_index()
        piv.columns.name = None
    for c in ("FBO", "FBS"):
        if c not in piv.columns:
            piv[c] = 0
    if demand_df.empty:
        demand_df = pd.DataFrame(columns=keys + ["orders"])

    piv["nm_id"] = piv["nm_id"].astype("int64")
    demand_df = demand_df.copy()
    demand_df["nm_id"] = demand_df["nm_id"].astype("int64")

    cov = piv.merge(demand_df, on=keys, how="outer")
    for c in ("FBO", "FBS", "orders"):
        cov[c] = cov[c].fillna(0).astype(int)
    cov["stock"] = cov["FBO"] + cov["FBS"]
    cov["demand_pd"] = cov["orders"] / max(eff_days, 1)
    with np.errstate(divide="ignore", invalid="ignore"):
        cov["cover_days"] = np.where(cov["demand_pd"] > 0, cov["stock"] / cov["demand_pd"], np.inf)
    cov["status"] = np.select(
        [cov["demand_pd"] <= 0, cov["cover_days"] < DEFICIT_DAYS, cov["cover_days"] > SURPLUS_DAYS],
        ["нет спроса", "дефицит", "излишек"], default="норма")

    # ABC по объёму заказов товара+размера (A - примерно 80% заказов, B - следующие 15%)
    sku = cov.groupby(["nm_id", "size"], as_index=False)["orders"].sum().sort_values("orders", ascending=False)
    total = sku["orders"].sum()
    if total > 0:
        prev = (sku["orders"].cumsum() - sku["orders"]) / total
        sku["abc"] = np.where(sku["orders"] <= 0, "C", np.where(prev < 0.8, "A", np.where(prev < 0.95, "B", "C")))
    else:
        sku["abc"] = "C"
    cov = cov.merge(sku[["nm_id", "size", "abc"]], on=["nm_id", "size"], how="left")
    return cov.sort_values(["nm_id", "size", "bucket"]).reset_index(drop=True)


def build_transfers(cov: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Возвращает:
      moves  - перемещения между своими складами (ФБС): откуда -> куда -> сколько
      supply - куда направлять следующие поставки на склады WB (ФБО) и куда не отправлять
    """
    moves, supply = [], []
    work = cov[cov["bucket"] != OTHER_BUCKET]

    for (nm, size), g in work.groupby(["nm_id", "size"]):
        abc = g["abc"].iloc[0]

        need_by: Dict[str, int] = {}
        cover_by: Dict[str, float] = {}
        for r in g.itertuples():
            if r.demand_pd > 0 and r.cover_days < DEFICIT_DAYS:
                need = math.ceil(TARGET_COVER_DAYS * r.demand_pd - r.stock)
                if need > 0:
                    need_by[r.bucket] = need
                    cover_by[r.bucket] = r.cover_days

        donors: Dict[str, int] = {}
        for r in g.itertuples():
            if r.status in ("излишек", "нет спроса") and r.FBS > 0:
                avail = int(min(r.FBS, math.floor(r.stock - DONOR_KEEP_DAYS * r.demand_pd)))
                if avail > 0:
                    donors[r.bucket] = avail

        for to_b in sorted(need_by, key=lambda b: -need_by[b]):
            need = need_by[to_b]
            for from_b in sorted(donors, key=lambda b: -donors[b]):
                if need <= 0:
                    break
                q = min(need, donors[from_b])
                if q < MIN_TRANSFER_QTY:
                    continue
                moves.append({
                    "nm_id": nm, "size": size, "src": from_b, "to": to_b, "qty": int(q), "abc": abc,
                    "comment": f"в «{to_b}» запас {cover_by[to_b]:.0f} дн., норма {TARGET_COVER_DAYS}",
                })
                donors[from_b] -= q
                need -= q
            need_by[to_b] = need

        remaining = {b: n for b, n in need_by.items() if n > 0}
        total_need = sum(remaining.values())
        for b, n in remaining.items():
            supply.append({
                "nm_id": nm, "size": size, "kind": "Направить поставку (ФБО)", "bucket": b, "qty": int(n),
                "share": n / total_need if total_need else 0.0, "abc": abc,
                "comment": f"запас {cover_by[b]:.0f} дн., не хватает до нормы",
            })
        for r in g.itertuples():
            if r.status == "излишек":
                supply.append({
                    "nm_id": nm, "size": size, "kind": "Не отправлять", "bucket": r.bucket, "qty": 0,
                    "share": 0.0, "abc": abc, "comment": f"излишек: запас {r.cover_days:.0f} дн.",
                })

    moves_df = pd.DataFrame(moves, columns=["nm_id", "size", "src", "to", "qty", "abc", "comment"])
    supply_df = pd.DataFrame(supply, columns=["nm_id", "size", "kind", "bucket", "qty", "share", "abc", "comment"])
    order = {"A": 0, "B": 1, "C": 2}
    if not moves_df.empty:
        moves_df = moves_df.assign(_o=moves_df["abc"].map(order)).sort_values(["_o", "qty"], ascending=[True, False]).drop(columns="_o")
    if not supply_df.empty:
        supply_df = supply_df.assign(_o=supply_df["abc"].map(order), _k=(supply_df["kind"] != "Направить поставку (ФБО)").astype(int)) \
            .sort_values(["_o", "_k", "qty"], ascending=[True, True, False]).drop(columns=["_o", "_k"])
    return moves_df, supply_df


def split_qty(qty: int, shares: Dict[str, float]) -> Dict[str, int]:
    """Раскладывает число по регионам пропорционально долям, сумма сохраняется."""
    if qty <= 0 or not shares:
        return {}
    raw = {b: qty * s for b, s in shares.items()}
    out = {b: int(math.floor(v)) for b, v in raw.items()}
    diff = qty - sum(out.values())
    for b in sorted(raw, key=lambda k: raw[k] - out[k], reverse=True)[:diff]:
        out[b] += 1
    return out


def build_order_plan(cov: pd.DataFrame, transit_df: pd.DataFrame, buyout_df: pd.DataFrame,
                     eff_days: float, sheet_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Ориентировочный заказ на следующий месяц по среднему спросу.
    Вычитаем всё, что уже есть и скоро вернётся на склад: остаток (ФБО из API + ФБС),
    "От клиента" (Q) и "Вернётся после невыкупа" (R). % выкупа берём из таблицы (W).
    """
    keys = ["nm_id", "size"]
    g = cov.groupby(keys, as_index=False).agg(stock_total=("stock", "sum"), orders_window=("orders", "sum"))
    g["demand_pd"] = g["orders_window"] / max(eff_days, 1)

    if transit_df is not None and not transit_df.empty:
        g = g.merge(transit_df[keys + ["from_client"]].rename(columns={"from_client": "fc_api"}), on=keys, how="left")
    else:
        g["fc_api"] = np.nan
    if sheet_df is not None and not sheet_df.empty:
        g = g.merge(sheet_df[keys + ["from_client_sheet", "returns_sheet", "buyout_sheet",
                                     "extra_s_sheet", "extra_v_sheet"]], on=keys, how="left")
    else:
        for col in ("from_client_sheet", "returns_sheet", "buyout_sheet", "extra_s_sheet", "extra_v_sheet"):
            g[col] = np.nan
    g["from_client"] = g["from_client_sheet"].combine_first(g["fc_api"]).fillna(0).astype(int)

    # % выкупа: таблица -> расчёт по заказам -> 100%
    g["rate_calc"] = np.nan
    if USE_BUYOUT_IN_ORDER and buyout_df is not None and not buyout_df.empty:
        b = buyout_df.copy()
        b["rate_calc"] = (b["bought"] / b["ordered"]).clip(0, 1)
        b = b[b["ordered"] >= BUYOUT_MIN_ORDERS][keys + ["rate_calc"]]
        g = g.drop(columns="rate_calc").merge(b, on=keys, how="left")
    use_sheet = g["buyout_sheet"].notna() & (g["buyout_sheet"] > 0)
    use_calc = ~use_sheet & g["rate_calc"].notna()
    g["buyout_rate"] = np.where(use_sheet, g["buyout_sheet"], np.where(use_calc, g["rate_calc"], 1.0)).clip(0, 1)
    g["buyout_src"] = np.where(use_sheet, "таблица", np.where(use_calc, "расчёт по заказам", "нет данных (взято 100%)"))
    g["net_pd"] = g["demand_pd"] * g["buyout_rate"]

    # сколько всего уже есть и едет
    g["returns_exp"] = g["returns_sheet"].fillna(0).astype(int)
    g["extra_s"] = g["extra_s_sheet"].fillna(0).astype(int)
    g["extra_v"] = g["extra_v_sheet"].fillna(0).astype(int)
    g["available"] = (g["stock_total"] + g["from_client"] + g["returns_exp"]
                      + g["extra_s"] + g["extra_v"]).astype(int)
    g["avail_src"] = np.where(g["buyout_sheet"].notna() | g["returns_sheet"].notna(),
                              "остаток + таблица (Q, R, S, V)", "остаток (в таблице позиции нет)")

    horizon = LEAD_TIME_DAYS + ORDER_HORIZON_DAYS + SAFETY_DAYS
    g["need_raw"] = g["net_pd"] * horizon - g["available"]
    g["order_qty"] = np.maximum(0, np.ceil(g["need_raw"])).astype(int)

    # раскладка заказа по регионам пропорционально доле заказов
    split_cols: Dict[Tuple[int, str], Dict[str, int]] = {}
    qty_map = {(r.nm_id, r.size): int(r.order_qty) for r in g.itertuples()}
    for (nm, size), sub in cov.groupby(keys):
        tot = sub["orders"].sum()
        qty = qty_map.get((nm, size), 0)
        if tot > 0 and qty > 0:
            shares = {r.bucket: r.orders / tot for r in sub.itertuples() if r.orders > 0}
            split_cols[(nm, size)] = split_qty(qty, shares)
    for b_ in BUCKETS + [OTHER_BUCKET]:
        g[f"split_{b_}"] = [split_cols.get((r.nm_id, r.size), {}).get(b_, 0) for r in g.itertuples()]
    return g.sort_values(["order_qty"], ascending=False).reset_index(drop=True)

# ЕДИНАЯ СВОДНАЯ ТАБЛИЦА

STATUS_LABEL = {
    "дефицит": "дефицит",
    "норма": "норма",
    "излишек": "излишек",
    "нет спроса": "нет спроса",
}


def _size_key(s: str):
    return (SIZE_ORDER.index(s) if s in SIZE_ORDER else len(SIZE_ORDER), str(s))


def build_master(cov: pd.DataFrame, moves: pd.DataFrame, supply: pd.DataFrame, plan: pd.DataFrame,
                 vendor_map: Dict, name_map: Dict, eff_days: float) -> List[list]:
    """
    Одна строка = артикул продавца x размер x регион.
    В строке сразу: остаток, спрос, запас, статус, что привезти / отдать / поставить и сколько заказать.
    """
    all_b = BUCKETS + [OTHER_BUCKET]

    cov_idx = {(r.nm_id, r.size, r.bucket): r for r in cov.itertuples()}
    plan_idx = {(r.nm_id, r.size): r for r in plan.itertuples()}
    sku_tot = cov.groupby(["nm_id", "size"])["orders"].sum().to_dict()
    abc_map = cov.drop_duplicates(["nm_id", "size"]).set_index(["nm_id", "size"])["abc"].to_dict()

    # место в регионе по числу заказов
    place_map: Dict[Tuple, int] = {}
    pos = cov[cov["orders"] > 0].copy()
    if not pos.empty:
        pos["place"] = pos.groupby("bucket")["orders"].rank(method="min", ascending=False).astype(int)
        place_map = {(r.nm_id, r.size, r.bucket): int(r.place) for r in pos.itertuples()}

    # перемещения между своими складами (ФБС)
    incoming: Dict[Tuple, List[Tuple[str, int]]] = {}
    outgoing: Dict[Tuple, List[Tuple[str, int]]] = {}
    for r in moves.itertuples():
        incoming.setdefault((r.nm_id, r.size, r.to), []).append((r.src, int(r.qty)))
        outgoing.setdefault((r.nm_id, r.size, r.src), []).append((r.to, int(r.qty)))
    # поставки на ФБО и запреты
    fbo_supply: Dict[Tuple, int] = {}
    no_ship = set()
    for r in supply.itertuples():
        key = (r.nm_id, r.size, r.bucket)
        if r.kind == "Не отправлять":
            no_ship.add(key)
        else:
            fbo_supply[key] = fbo_supply.get(key, 0) + int(r.qty)

    skus = sorted(
        {(nm, size) for (nm, size, _b) in cov_idx},
        key=lambda k: (str(vendor_map.get(k[0]) or k[0]), _size_key(k[1]), k[0]),
    )

    header = ["Артикул продавца", "Артикул WB", "Размер", "Класс", "Регион",
              "ФБО (склад WB)", "ФБС (свой склад)", "Остаток всего",
              f"Заказов за {eff_days} дн.", "Спрос в день", "Запас в днях", "Статус",
              "Не хватает до нормы, шт",
              "ПРИВЕЗТИ сюда (ФБС), шт", "Откуда привезти",
              "ОТДАТЬ отсюда (ФБС), шт", "Куда отдать",
              "ПОСТАВКА на ФБО сюда, шт",
              "ЧТО ДЕЛАТЬ",
              "Уже есть и едет по размеру (остаток + столбцы Q, R, S, V)",
              "% выкупа",
              "Место в регионе по заказам", "Доля региона в заказах размера, %"]
    rows: List[list] = [header]

    for nm, size in skus:
        p = plan_idx.get((nm, size))
        available = int(p.available) if p is not None else ""
        buyout = p.buyout_rate * 100 if p is not None else ""
        vendor = vendor_map.get(nm) or str(nm)
        tot = sku_tot.get((nm, size), 0)

        for b in all_b:
            r = cov_idx.get((nm, size, b))
            if r is None and b == OTHER_BUCKET:
                continue   # Прочее показываем, только если там что-то есть
            key = (nm, size, b)
            if r is None:
                fbo = fbs = stock = orders = 0
                demand_pd = 0.0
                status = "нет спроса"
            else:
                fbo, fbs, stock, orders = int(r.FBO), int(r.FBS), int(r.stock), int(r.orders)
                demand_pd, status = float(r.demand_pd), r.status
            cover = (stock / demand_pd) if demand_pd > 0 else "∞"
            need_norm = max(0, math.ceil(TARGET_COVER_DAYS * demand_pd - stock)) if demand_pd > 0 else 0

            inc = incoming.get(key, [])
            out = outgoing.get(key, [])
            inc_qty = sum(q for _, q in inc)
            out_qty = sum(q for _, q in out)
            sup_qty = fbo_supply.get(key, 0)

            actions = []
            if inc:
                actions.append("Привезти: " + ", ".join(f"{q} шт из {s}" for s, q in inc))
            if sup_qty:
                actions.append(f"Поставка на ФБО: {sup_qty} шт")
            if out:
                actions.append("Отдать: " + ", ".join(f"{q} шт в {t}" for t, q in out))
            if key in no_ship and not out:
                actions.append("Не отправлять (излишек)")

            rows.append([
                vendor, nm, size, abc_map.get((nm, size), "C"), b,
                fbo, fbs, stock,
                orders, demand_pd, cover, STATUS_LABEL.get(status, status),
                need_norm,
                inc_qty or "", ", ".join(f"{s}: {q}" for s, q in inc),
                out_qty or "", ", ".join(f"{t}: {q}" for t, q in out),
                sup_qty or "",
                " | ".join(actions),
                available,
                buyout,
                place_map.get(key, ""), (100.0 * orders / tot) if tot else "",
            ])
    return rows


# ВЫВОД В GOOGLE SHEETS

def clean(v):
    if v is None:
        return ""
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        if np.isnan(v) or np.isinf(v):
            return ""
        return round(float(v), 2)
    if isinstance(v, (datetime, date)):
        return v.strftime("%Y-%m-%d %H:%M") if isinstance(v, datetime) else v.strftime("%Y-%m-%d")
    return v


def get_spreadsheet():
    if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
        raise RuntimeError(f"Не найден файл ключа сервисного аккаунта: {GOOGLE_SERVICE_ACCOUNT_FILE}")
    creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=GOOGLE_SCOPES)
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID)


def load_settings():
    """
    Читает лист «Настройки» и подставляет значения в переменные скрипта (сейчас - окно анализа спроса).
    Если листа нет - создаёт его со значениями по умолчанию. Если что-то не так с таблицей или
    значением - остаются значения из начала скрипта, запуск не падает.
    """
    print("\n=== НАСТРОЙКИ ИЗ ТАБЛИЦЫ ===")
    try:
        sh = get_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_SETTINGS)
        except gspread.WorksheetNotFound:
            ws = None
        if ws is None:
            if DRY_RUN:
                print(f"   листа «{SHEET_SETTINGS}» нет, DRY_RUN=True - не создаю, беру значения по умолчанию.")
                return
            ws = sh.add_worksheet(title=SHEET_SETTINGS, rows=50, cols=3)
            rows = [["Параметр", "Значение", "Пояснение"]]
            rows += [[label, globals()[var], note] for label, var, _d, _lo, _hi, note in SETTINGS_SPEC]
            ws.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")
            try:
                ws.format("A1:C1", {"textFormat": {"bold": True},
                                    "backgroundColor": {"red": 0.88, "green": 0.92, "blue": 0.98}})
                ws.columns_auto_resize(0, 2)
            except Exception:
                pass
            print(f"   создан лист «{SHEET_SETTINGS}» со значениями по умолчанию.")
            for label, var, *_ in SETTINGS_SPEC:
                print(f"   {label}: {globals()[var]}")
            return
        values = ws.get_all_values()
    except Exception as e:
        print(f" не удалось прочитать лист «{SHEET_SETTINGS}»: {e}. Беру значения из скрипта.")
        return

    table = {row[0].strip().lower(): row[1] for row in values[1:] if len(row) >= 2 and row[0].strip()}
    applied = []
    for label, var, _default, lo, hi, note in SETTINGS_SPEC:
        raw = table.get(label.lower())
        if raw is None:
            # параметра на листе нет (например, добавили в скрипт позже) - дописываем строку
            if not DRY_RUN:
                try:
                    ws.append_row([label, globals()[var], note], value_input_option="USER_ENTERED")
                except Exception:
                    pass
            print(f"   {label}: в таблице не найдено, использую {globals()[var]}")
            continue
        val = parse_num(raw)
        if val is None or int(val) != val or not (lo <= val <= hi):
            print(f"  {label}: значение «{raw}» не подходит (нужно целое от {lo} до {hi}), "
                  f"использую {globals()[var]}")
            continue
        globals()[var] = int(val)
        print(f"   {label}: {int(val)}")
        applied.append(f"{label} = {int(val)}")
    log_step("настройки", "OK", len(applied), "; ".join(applied))


def write_sheet(sh, title: str, rows: List[list]):
    rows = [[clean(v) for v in r] for r in rows]
    print(f"   лист «{title}»: {len(rows) - 1} строк")
    if DRY_RUN:
        return
    ncols = max(len(r) for r in rows)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=max(len(rows) + 20, 100), cols=max(ncols + 2, 10))
    ws.clear()
    if ws.row_count < len(rows) + 5 or ws.col_count < ncols:
        ws.resize(rows=max(len(rows) + 5, ws.row_count), cols=max(ncols, ws.col_count))
    CHUNK = 5000
    for i in range(0, len(rows), CHUNK):
        ws.update(range_name=f"A{i + 1}", values=rows[i:i + CHUNK], value_input_option="USER_ENTERED")
        time.sleep(1.0)


def polish_master(sh, title: str, ncols: int):
    """Закрепляем шапку и первые столбцы, включаем фильтр. Если не вышло - не страшно."""
    if DRY_RUN or sh is None:
        return
    try:
        ws = sh.worksheet(title)
        ws.freeze(rows=1, cols=5)
        last_col = gspread.utils.rowcol_to_a1(1, ncols)
        ws.format(f"A1:{last_col}", {"textFormat": {"bold": True}, "wrapStrategy": "WRAP",
                                     "backgroundColor": {"red": 0.88, "green": 0.92, "blue": 0.98}})
        ws.set_basic_filter()
    except Exception as e:
        print(f"   (оформление листа «{title}» не применилось: {e})")


# ============================================================
# ОТЧЁТ
# ============================================================

def report():
    print("\n=== РАСЧЁТ И ВЫВОД В ТАБЛИЦУ ===")
    print(f"   окно анализа спроса: {DEMAND_WINDOW_DAYS} дн.")
    today = date.today()
    with conn_ctx() as c:
        snap = query_df(c, "SELECT max(snap_date) AS d FROM stock_snapshot")["d"].iloc[0]
        if snap is None or pd.isna(snap):
            raise RuntimeError("В базе нет снимков остатков. Сначала запусти сбор данных: python wb_planner.py collect")
        stock_df = query_df(c, "SELECT nm_id,size,bucket,stock_type,qty FROM stock_snapshot WHERE snap_date=%s", (snap,))
        first_order = query_df(c, "SELECT min(order_date) AS d FROM orders")["d"].iloc[0]
        win_start = today - timedelta(days=DEMAND_WINDOW_DAYS - 1)
        demand_df = query_df(c, """SELECT nm_id,size,bucket,COUNT(*)::int AS orders FROM orders
                                   WHERE order_date >= %s AND order_date <= %s GROUP BY 1,2,3""", (win_start, today))
        names = query_df(c, "SELECT nm_id, max(name) AS name, max(vendor_code) AS vendor_code FROM skus GROUP BY nm_id")
        transit_df = query_df(c, """SELECT nm_id,size,to_client,from_client FROM transit_snapshot
                                    WHERE snap_date=(SELECT max(snap_date) FROM transit_snapshot)""")
        buyout_df = query_df(c, """
            SELECT o.nm_id, o.size, COUNT(*)::int AS ordered,
                   COUNT(*) FILTER (WHERE s.srid IS NOT NULL)::int AS bought
            FROM orders o
            LEFT JOIN (SELECT DISTINCT srid FROM sales
                       WHERE NOT is_return AND srid NOT IN (SELECT srid FROM sales WHERE is_return AND srid IS NOT NULL)) s
                   ON s.srid = o.srid
            WHERE o.order_date BETWEEN %s AND %s GROUP BY 1,2""",
                             (today - timedelta(days=BUYOUT_MATURE_FROM_DAYS), today - timedelta(days=BUYOUT_MATURE_TO_DAYS)))
        sheet_df = query_df(c, """SELECT nm_id, size, from_client AS from_client_sheet, returns_expected AS returns_sheet,
                                  buyout_rate AS buyout_sheet,
                                  extra_s AS extra_s_sheet, extra_v AS extra_v_sheet FROM sheet_snapshot
                                  WHERE snap_date = (SELECT max(snap_date) FROM sheet_snapshot)""")
        status_log = query_df(c, "SELECT ts, step, status, rows_cnt, message FROM run_log ORDER BY id DESC LIMIT 40")

    if TEST_MODE or TEST_NM_IDS:
        ids = set(get_nm_ids(all_accounts=True))
        stock_df = stock_df[stock_df["nm_id"].isin(ids)]
        demand_df = demand_df[demand_df["nm_id"].isin(ids)]
        sheet_df = sheet_df[sheet_df["nm_id"].isin(ids)]

    excl = {a.strip().lower() for a in EXCLUDE_ARTICLES if a and a.strip()}
    if excl:
        vc = names["vendor_code"].fillna("").str.strip().str.lower()
        nmn = names["name"].fillna("").str.strip().str.lower()
        excl_ids = set(names.loc[vc.isin(excl) | nmn.isin(excl), "nm_id"])
        found = set(vc[vc.isin(excl)]) | set(nmn[nmn.isin(excl)])
        not_found = sorted(excl - found)
        if not_found:
            print(f"Исключаемые артикулы не найдены среди карточек: {not_found}")
        print(f"Исключено карточек: {len(excl_ids)}")
        stock_df = stock_df[~stock_df["nm_id"].isin(excl_ids)]
        demand_df = demand_df[~demand_df["nm_id"].isin(excl_ids)]
        sheet_df = sheet_df[~sheet_df["nm_id"].isin(excl_ids)]

    if first_order is None or pd.isna(first_order):
        eff_days = DEMAND_WINDOW_DAYS
    else:
        eff_days = max(1, min(DEMAND_WINDOW_DAYS, (today - first_order).days + 1))
    if eff_days < DEMAND_WINDOW_DAYS:
        print(f"В базе заказов только за {eff_days} дн., спрос считается по ним.")
    if snap != today:
        print(f" Последний снимок остатков за {snap}, не за сегодня.")

    name_map = dict(zip(names["nm_id"], names["name"]))
    vendor_map = dict(zip(names["nm_id"], names["vendor_code"]))
    cov = build_coverage(stock_df, demand_df, eff_days)
    moves, supply = build_transfers(cov)
    plan = build_order_plan(cov, transit_df, buyout_df, eff_days, sheet_df)

    # --- сохраняем рекомендации и план заказа в базу (история) ---
    with conn_ctx() as c, c.cursor() as cur:
        cur.execute("DELETE FROM recommendation WHERE calc_date=%s", (today,))
        rec_rows = [(today, "Перемещение ФБС", int(r.nm_id), r.size, r.src, r.to, int(r.qty), r.abc, r.comment)
                    for r in moves.itertuples()]
        rec_rows += [(today, r.kind, int(r.nm_id), r.size, None, r.bucket, int(r.qty), r.abc, r.comment)
                     for r in supply.itertuples()]
        if rec_rows:
            execute_values(cur, """INSERT INTO recommendation
                (calc_date,kind,nm_id,size,from_bucket,to_bucket,qty,priority,comment) VALUES %s""", rec_rows)
        cur.execute("DELETE FROM order_plan WHERE calc_date=%s", (today,))
        plan_rows = [(today, int(r.nm_id), r.size, int(r.stock_total), int(r.from_client), int(r.orders_window),
                      float(r.buyout_rate), float(r.net_pd), int(r.order_qty)) for r in plan.itertuples()]
        if plan_rows:
            execute_values(cur, """INSERT INTO order_plan
                (calc_date,nm_id,size,stock_total,from_client,orders_window,buyout_rate,net_demand_per_day,order_qty)
                VALUES %s""", plan_rows)

    sh = None if DRY_RUN else get_spreadsheet()
    if DRY_RUN:
        print("DRY_RUN=True - в таблицу ничего не пишем.")

    # --- Сводка: всё в одном листе ---
    master_rows = build_master(cov, moves, supply, plan, vendor_map, name_map, eff_days)
    write_sheet(sh, SHEET_MASTER, master_rows)
    polish_master(sh, SHEET_MASTER, len(master_rows[0]))

    # --- Состояние ---
    st = [["Последнее обновление", datetime.now().strftime("%Y-%m-%d %H:%M")],
          ["Снимок остатков за", str(snap)], ["Дней истории заказов в расчёте", eff_days],
          ["Окно спроса, дн. (из листа «Настройки»)", DEMAND_WINDOW_DAYS], ["Норма запаса, дн.", TARGET_COVER_DAYS],
          ["Дефицит ниже, дн.", DEFICIT_DAYS], ["Излишек выше, дн.", SURPLUS_DAYS],
          ["Заказ: горизонт + запас + срок поставки, дн.", ORDER_HORIZON_DAYS + SAFETY_DAYS + LEAD_TIME_DAYS],
          [], ["Время", "Шаг", "Статус", "Строк", "Сообщение"]]
    for r in status_log.itertuples():
        st.append([r.ts.strftime("%Y-%m-%d %H:%M"), r.step, r.status, r.rows_cnt, r.message])
    write_sheet(sh, SHEET_STATUS, st)
    print("Готово.")


# ============================================================
# MAIN
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="WB Planner")
    ap.add_argument("command", choices=["init", "collect", "report", "all"])
    args = ap.parse_args()

    init_db()
    if args.command != "init":
        load_settings()   # окно анализа и пр. берём из листа «Настройки» перед запуском
    if args.command in ("collect", "all"):
        collect_all()
    if args.command in ("report", "all"):
        report()


if __name__ == "__main__":
    main()