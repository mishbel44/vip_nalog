import os
import re
import json
import logging
from typing import Optional
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, filters, CallbackQueryHandler
)
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from fsid_getter import get_fsid
import warnings

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB")
SECOND_SHEET_ID = os.getenv("SECOND_SHEET_ID")
SECOND_SHEET_TAB = os.getenv("SECOND_SHEET_TAB") or "2024"  # дефолт для второй таблицы
GOOGLE_APPLICATION_CREDENTIALS = "credent.json"
METABASE_URL = os.getenv("METABASE_URL")
MB_USER = os.getenv("MB_USER")
MB_PASSWORD = os.getenv("MB_PASSWORD")
METABASE_CARD_URL = os.getenv("METABASE_CARD_URL")
METABASE_CARD_NAME = os.getenv("METABASE_CARD_NAME")
METABASE_PARAM_NAME = "BusinessKey"
VERIFY_SSL = True
LOGIN = os.getenv("LOGIN")
USER_ID = os.getenv("USER_ID")
BACKOFFICE_API_URL = os.getenv("BACKOFFICE_API_URL")
METABASE_DATE_PARAM_NAME  = os.getenv("METABASE_DATE_PARAM_NAME") or "date"
METABASE_DATE_SEP = os.getenv("METABASE_DATE_SEP", "-")

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def simple_warning_filter(message, category, filename, lineno, file=None, line=None):
    if "per_message=False" in str(message):
        print("Ошибка per_* settings")
    else:
        # остальные предупреждения выводим как есть
        warnings.showwarning_orig(message, category, filename, lineno, file, line)

# сохраняем оригинальный обработчик и подменяем
warnings.showwarning_orig = warnings.showwarning
warnings.showwarning = simple_warning_filter

# ------------------------ GOOGLE SHEETS ------------------------

gmt3 = timezone(timedelta(hours=3))
now = datetime.now(gmt3)
today_str = now.strftime("%d.%m.%Y")

# --- выбрать нужный лист по году для основной таблицы ---
def _resolve_sheet_tab(year: Optional[str]) -> str:
    """
    Возвращает имя листа для выбранного года для ОСНОВНОЙ таблицы.
    По умолчанию — значение из переменной окружения SHEET_TAB (оно же используется для 2024).
    """
    if year == "2023":
        return "НДФЛ 23 год"
    if year == "2022":
        return "НДФЛ 22год+вопросы"
    return SHEET_TAB  # default / 2024

# --- выбрать вкладку во второй таблице по году ---
def _resolve_second_sheet_tab(year: Optional[str]) -> str:
    """
    Возвращает вкладку для ВТОРОЙ таблицы по выбранному году.
    По умолчанию — '2024'. Для 2023 → '2023', для 2022 → '2022'.
    """
    if year == "2023":
        return "2023"
    if year == "2022":
        return "2022"
    return "2024"

def _get_gs_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        GOOGLE_APPLICATION_CREDENTIALS, scope
    )
    return gspread.authorize(creds)

def _read_cells(cell_b: str, cell_k: str, sheet_tab: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    gc = _get_gs_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_tab or SHEET_TAB)
    val_b = ws.acell(cell_b).value
    val_k = ws.acell(cell_k).value
    return val_b, val_k

def _write_row_values(row: int,
                      client_status_str: str,
                      nalog: float,
                      saldo: float,
                      percent_comp: float,
                      result_str: str,
                      *,
                      sheet_tab: Optional[str] = None):
    """
    Записывает в текущую (ОСНОВНУЮ) таблицу значения:
      A — today_str
      C — client_status_str
      D — nalog
      E — saldo
      F — "менее 9%" если percent_comp <= 9.99 иначе "более 10%"
      G — result_str
    Столбец B не трогаем.
    """
    gc = _get_gs_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_tab or SHEET_TAB)

    percent_label = "менее 9%" if (percent_comp is not None and float(percent_comp) <= 9.99) else "более 10%"

    data = [
        {"range": f"A{row}", "values": [[today_str]]},
        {"range": f"C{row}", "values": [[client_status_str or ""]]},
        {"range": f"D{row}", "values": [[nalog]]},
        {"range": f"E{row}", "values": [[saldo]]},
        {"range": f"F{row}", "values": [[percent_label]]},
        {"range": f"G{row}", "values": [[result_str or ""]]},
    ]
    ws.batch_update(data)

def _write_second_table_row(
    *,
    account_value: str,
    client_status_str: str,
    nalog: float,
    turnover: float,
    GGR: float,
    sum_dep: float,
    saldo: float,
    percent_comp: float,
    result_str: str,
    second_sheet_tab: Optional[str] = None
):
    """
    Записывает ОДНУ строку в ВТОРУЮ таблицу в порядке столбцов:
    A - account_value
    B - client_status_str
    C - nalog
    D - turnover
    E - GGR
    F - sum_dep
    G - saldo
    H - percent_comp
    I - result_str
    """
    if not SECOND_SHEET_ID:
        raise RuntimeError("Не задан SECOND_SHEET_ID")
    gc = _get_gs_client()
    sh = gc.open_by_key(SECOND_SHEET_ID)
    ws = sh.worksheet(second_sheet_tab or SECOND_SHEET_TAB or "2024")

    row_values = [
        account_value or "",
        client_status_str or "",
        float(nalog) if nalog is not None else "",
        float(turnover) if turnover is not None else "",
        float(GGR) if GGR is not None else "",
        float(sum_dep) if sum_dep is not None else "",
        float(saldo) if saldo is not None else "",
        float(percent_comp) if percent_comp is not None else "",
        result_str or "",
    ]
    # добавляем в конец
    ws.append_row(row_values, value_input_option="USER_ENTERED")

def _parse_row_index(cell_ref: str) -> Optional[int]:
    m = re.match(r"^\s*[A-Za-z]+(\d+)\s*$", cell_ref)
    return int(m.group(1)) if m else None

def _safe_get(dct, *keys, default=None):
    cur = dct or {}
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _find_in_list_by_class(items: list, class_name: str):
    for it in items or []:
        if isinstance(it, dict) and it.get("class") == class_name:
            return it
    return None

def _to_gmt_plus3_from_unix(ts_raw) -> str | None:
    """Принимает unix (секунды или миллисекунды). Возвращает дату 'дд.мм.гггг' в GMT+03."""
    if ts_raw is None or ts_raw == "" or ts_raw == "-":
        return None
    try:
        ts = float(ts_raw)
    except Exception:
        return None
    if ts > 10_000_000_000:
        ts = ts / 1000.0
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    dt_gmt3 = dt_utc + timedelta(hours=3)
    return dt_gmt3.strftime("%d.%m.%Y")

# ---- helpers for Metabase rows aggregation ----
def _sum_field(rows: list[dict], key: str) -> float:
    total = 0.0
    for r in rows or []:
        val = r.get(key)
        try:
            if val is None or val == "":
                continue
            total += float(val)
        except Exception:
            continue
    return total

# ---- period → metabase date helpers ----
def _format_date_ddmmyyyy_to_iso(s: str, sep: str = "-") -> Optional[str]:
    m = re.match(r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*$", s)
    if not m:
        return None
    d, mo, y = m.groups()
    return f"{y}{sep}{mo}{sep}{d}"

def _period_to_metabase_date(period: str, sep: str = "-") -> Optional[str]:
    m = re.match(r"^\s*(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})\s*$", period or "")
    if not m:
        return None
    start_iso = _format_date_ddmmyyyy_to_iso(m.group(1), sep)
    end_iso   = _format_date_ddmmyyyy_to_iso(m.group(2), sep)
    return f"{start_iso}~{end_iso}" if start_iso and end_iso else None

# ------------------------ BACKOFFICE (inline) ------------------------
def _post_client_information(client_id: str) -> dict:
    payload = {
        "clientId": client_id,
        "login": LOGIN,
        "fsid": get_fsid(),
        "userId": USER_ID,
        "userLang": "ru",
    }
    r = session.post(BACKOFFICE_API_URL, json=payload, timeout=30, verify=VERIFY_SSL)
    if r.status_code // 100 != 2:
        raise RuntimeError(f"backoffice {r.status_code}: {_json_or_text(r)}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError("Ответ backoffice не JSON")

def _find_key_recursive(obj, key: str):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = _find_key_recursive(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = _find_key_recursive(it, key)
            if found is not None:
                return found
    return None

def _extract_cupis_level_from_bo(bo: dict) -> Optional[str]:
    raw = None
    raw = _safe_get(bo, "response", "cupisIdentLevel")
    if raw is None:
        raw = bo.get("cupisIdentLevel")
    if raw is None:
        raw = _find_key_recursive(bo, "cupisIdentLevel")

    if raw is None:
        return None

    try:
        lvl = int(raw)
    except Exception:
        try:
            lvl = int(str(raw).strip())
        except Exception:
            return None

    mapping = {3: "Продвинутая", 2: "Базовая"}
    return mapping.get(lvl, None)

# ------------------------ METABASE (inline) ------------------------

session = requests.Session()
session.headers.update({"User-Agent": "mb-bot/1.0"})

_card_id_re = re.compile(r"/question/(\d+)")

def _json_or_text(r: requests.Response):
    ctype = r.headers.get("Content-Type", "")
    if "application/json" in (ctype or "").lower():
        try:
            return r.json()
        except Exception:
            pass
    return r.text[:1000]

def _strip_quotes(val: str) -> str:
    if not isinstance(val, str) or len(val) < 2:
        return val
    if (val[0] == val[-1] == "'") or (val[0] == val[-1] == '"'):
        return val[1:-1]
    return val

def _is_number_type(t: str | None) -> bool:
    t = (t or "").lower()
    return any(k in t for k in ["number", "integer", "id"])

def _coerce_value(val: str, param_type: str | None):
    if _is_number_type(param_type):
        if isinstance(val, str) and val.isdigit():
            return int(val)
        try:
            return float(val)
        except Exception:
            return val
    return val

def mb_login():
    if not METABASE_URL or not MB_USER or not MB_PASSWORD:
        raise RuntimeError("Не заданы METABASE_URL/MB_USER/MB_PASSWORD")
    r = session.post(
        f"{METABASE_URL}/api/session",
        json={"username": MB_USER, "password": MB_PASSWORD},
        timeout=20,
        verify=VERIFY_SSL,
        allow_redirects=False,
    )
    if r.status_code in (301, 302):
        raise RuntimeError(f"Редирект при логине на {r.headers.get('Location')} — проверь BASE URL.")
    if r.status_code == 401:
        raise RuntimeError("401 Unauthorized — проверь логин/пароль.")
    if r.status_code // 100 != 2:
        raise RuntimeError(f"Ошибка {r.status_code} при логине: {_json_or_text(r)}")
    token = r.json().get("id")
    if not token:
        raise RuntimeError(f"Логин без токена: {_json_or_text(r)}")
    session.headers.update({"X-Metabase-Session": token})

def get_card_id_from_url(url: str) -> int:
    m = _card_id_re.search(url)
    if not m:
        raise ValueError("В URL нет числового ID после /question/…")
    return int(m.group(1))

def parse_params_from_url(url: str) -> dict:
    from urllib.parse import urlparse, parse_qs
    q = urlparse(url).query
    if not q:
        return {}
    out = {}
    for k, vals in parse_qs(q, keep_blank_values=True).items():
        if not vals:
            continue
        out[k] = _strip_quotes(vals[0])
    return out

def find_card_id_by_name(name: str) -> int:
    r = session.get(f"{METABASE_URL}/api/search", params={"query": name}, timeout=20, verify=VERIFY_SSL)
    if r.status_code // 100 != 2:
        raise RuntimeError(f"/api/search {r.status_code}: {_json_or_text(r)}")
    raw = r.json()
    items = raw if isinstance(raw, list) else raw.get("data", [])
    name_l = (name or "").lower()
    cards = [it for it in items if (it.get("model") == "card" and name_l in (it.get("name", "").lower()))]
    if not cards:
        raise RuntimeError(f"Карточка с именем, включающим '{name}', не найдена.")
    return cards[0]["id"]

def get_card_details(card_id: int) -> dict:
    r = session.get(f"{METABASE_URL}/api/card/{card_id}", timeout=20, verify=VERIFY_SSL)
    if r.status_code // 100 != 2:
        raise RuntimeError(f"/api/card/{card_id} {r.status_code}: {_json_or_text(r)}")
    return r.json()

def inspect_card_parameters(card: dict):
    out = []
    dsq = card.get("dataset_query") or {}
    native = dsq.get("native") or {}
    ttags = (native.get("template-tags") or {}) if isinstance(native, dict) else {}
    for nm, meta in ttags.items():
        out.append({"name": nm, "kind": "template", "target": ["variable", ["template-tag", nm]], "type": meta.get("type")})

    name_to_target = {}
    param_type_by_name = {}
    for m in card.get("parameter_mappings") or []:
        target = m.get("target")
        pid = m.get("parameter_id")
        for p in (dsq.get("parameters") or []):
            if p.get("id") == pid:
                nm = p.get("name")
                if nm:
                    name_to_target[nm] = target
                    param_type_by_name[nm] = p.get("type")
    for p in (card.get("parameters") or []):
        nm = p.get("name")
        if nm and p.get("target"):
            name_to_target[nm] = p.get("target")
            param_type_by_name[nm] = p.get("type") or param_type_by_name.get(nm)

    for nm, tgt in name_to_target.items():
        out.append({"name": nm, "kind": "field", "target": tgt, "type": param_type_by_name.get(nm)})

    seen = set()
    uniq = []
    for it in out:
        if it["name"] in seen:
            continue
        seen.add(it["name"])
        uniq.append(it)
    return uniq

def pick_params_for_value(params, desired_name: str):
    if not params:
        return []
    dl = desired_name.lower().strip()
    exact = [p for p in params if p["name"].lower() == dl]
    if exact:
        return exact
    synonyms = [
        "business", "key", "client", "account", "acc", "bk", "userid", "user_id",
        "номер", "счет", "счёт", "клиент"
    ]
    similar = []
    for p in params:
        name_l = p["name"].lower()
        if any(s in name_l for s in synonyms):
            similar.append(p)
    if similar:
        return similar
    if len(params) == 1:
        return params
    return []

def build_param_entry(param_meta: dict, raw_value: str):
    name = param_meta.get("name") or ""
    name_l = name.lower()
    t = (param_meta.get("type") or "").lower()
    date_name_l = (METABASE_DATE_PARAM_NAME or "").lower()

    # --- DATE OVERRIDE (match browser request) ---
    if name_l == date_name_l:
        raw = str(raw_value or "").strip()
        if "~" not in raw and raw:
            raw = f"{raw}~{raw}"
        raw = raw.replace(".", "-")
        return {
            "type": "date/range",
            "target": ["dimension", ["template-tag", name]],
            "value": raw
        }

    def _is_number_type_local(tt: str | None) -> bool:
        tt = (tt or "").lower()
        return any(k in tt for k in ["number", "integer", "id"])

    def _coerce_value_local(val: str, param_type: str | None):
        if _is_number_type_local(param_type):
            if isinstance(val, str) and val.isdigit():
                return int(val)
            try:
                return float(val)
            except Exception:
                return val
        return val

    coerced = _coerce_value_local(raw_value, param_meta.get("type"))
    if t == "dimension":
        t = "category" if isinstance(coerced, str) else "number"
    return {"type": t or ("category" if isinstance(coerced, str) else "number"),
            "target": param_meta["target"],
            "value": coerced}

def run_card_json(card_id: int, account_value: str, url_params: dict | None = None):
    card = get_card_details(card_id)
    params_meta = inspect_card_parameters(card)

    chosen = pick_params_for_value(params_meta, METABASE_PARAM_NAME)
    if not chosen:
        names = [p["name"] for p in params_meta]
        raise RuntimeError(f"Не нашёл подходящий параметр для '{METABASE_PARAM_NAME}'. В карточке есть: {names}")

    payload_parameters = []
    for k, v in (url_params or {}).items():
        match = [p for p in params_meta if p["name"].lower() == k.lower()]
        match.sort(key=lambda pm: 0 if pm.get("kind") == "template" else 1)
        for pm in match:
            payload_parameters.append(build_param_entry(pm, str(v)))

    chosen_targets = {json.dumps(pm["target"], ensure_ascii=False) for pm in chosen}
    payload_parameters = [p for p in payload_parameters if json.dumps(p.get("target"), ensure_ascii=False) not in chosen_targets]
    for pm in chosen:
        payload_parameters.append(build_param_entry(pm, account_value))

    body = {"parameters": payload_parameters}
    from urllib.parse import quote
    qs_parameters = quote(json.dumps(payload_parameters, ensure_ascii=False))
    url = f"{METABASE_URL}/api/card/{card_id}/query/json?parameters={qs_parameters}"
    r = session.post(url, json=body, timeout=60, verify=VERIFY_SSL)
    if r.status_code // 100 != 2:
        raise RuntimeError(f"/api/card/{card_id}/query/json {r.status_code}: {_json_or_text(r)}")
    return r.json()

# ------------------------ STATUS MAPPING & PICKING (БО) ------------------------

_STATUS_MAP = {
    ("8", "803"):  {"name": "Станд-л",         "kind": "main"},
    ("8", "814"):  {"name": "Пари-л",          "kind": "main"},
    ("8", "807"):  {"name": "ВИП",             "kind": "main"},
    ("8", "809"):  {"name": "Кандидат в Вип",  "kind": "main"},
    ("8", "808"):  {"name": "СуперВип",        "kind": "main"},
    ("25", "2502"):{"name": "Скорум-л",        "kind": "secondary"},
}

_FIRST_GROUP = {"станд-л", "пари-л"}
_SECOND_GROUP = {"супервип", "кандидат в вип", "вип"}
_SECONDARY_GROUP = {"скорум-л"}

def _normalize_code(v) -> str:
    s = "" if v is None else str(v)
    return s.strip()

def _pick_latest_statuses(grade_ratings: list[dict]) -> tuple[Optional[str], Optional[str]]:
    latest_main = None
    latest_secondary = None

    for item in grade_ratings or []:
        obj = (item or {}).get("object") or {}
        gt = _normalize_code(obj.get("gradeType"))
        mst = _normalize_code(obj.get("manualSubType"))
        key = (gt, mst)
        meta = _STATUS_MAP.get(key)
        if not meta:
            continue

        t_raw = obj.get("time")
        try:
            t = float(t_raw) if t_raw is not None else -1.0
        except Exception:
            t = -1.0
        if t > 10_000_000_000:
            t = t / 1000.0

        if meta["kind"] == "main":
            if latest_main is None or t > latest_main[0]:
                latest_main = (t, meta["name"])
        else:
            if latest_secondary is None or t > latest_secondary[0]:
                latest_secondary = (t, meta["name"])

    return (latest_main[1] if latest_main else None,
            latest_secondary[1] if latest_secondary else None)

def format_client_status(grade_ratings: list[dict]) -> str:
    main_s, sec_s = _pick_latest_statuses(grade_ratings)
    if main_s and sec_s:
        return f"{main_s}, {sec_s}"
    if main_s:
        return main_s
    if sec_s:
        return sec_s
    return "Нет статуса"

def pick_reward_group(grade_ratings: list[dict]) -> str:
    main_s, sec_s = _pick_latest_statuses(grade_ratings)
    main_l = (main_s or "").strip().lower()
    sec_l  = (sec_s or "").strip().lower()

    if main_l in _SECOND_GROUP:
        return "2"
    if main_l in _FIRST_GROUP:
        return "1"
    if main_l == "" and sec_l in _SECONDARY_GROUP:
        return "1"
    if sec_l in _SECONDARY_GROUP:
        return "1"
    return "—"

# ------------------------ NEW: RESULT LOGIC ------------------------

def compute_result(reward_group: str, percent_comp: float, nalog: float) -> str:
    try:
        pc = float(percent_comp)
        n  = float(nalog)
    except Exception:
        return "—"

    if reward_group == "1":
        if pc <= 9.99:
            return "Сразу компенсируем"
        return "Сразу компенсируем" if n <= 10000 else "Нужна справка"

    if reward_group == "2":
        if pc <= 9.99:
            return "Сразу компенсируем"
        return "Сразу компенсируем" if n <= 50000 else "Нужна справка"

    return "—"

# ------------------------ TELEGRAM HANDLERS ------------------------

WAIT_FOR_YEAR, WAIT_FOR_CELL, WAIT_FOR_CONFIRM = range(3)

PERIODS = {
    "2022": "01.03.2022 - 31.12.2022",
    "2023": "01.01.2023 - 31.12.2023",
    "2024": "01.01.2024 - 31.12.2024",
}

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Привет! Напиши /nalog, чтобы рассчитать по ячейке.")

async def nalog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("2022", callback_data="year:2022"),
            InlineKeyboardButton("2023", callback_data="year:2023"),
            InlineKeyboardButton("2024", callback_data="year:2024"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выберите год:", reply_markup=reply_markup)
    return WAIT_FOR_YEAR

async def year_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # callback_data вида "year:2024"
    data = query.data
    year = data.split(":", 1)[1] if ":" in data else data
    context.user_data["year"] = year
    context.user_data["period"] = PERIODS.get(year, "—")

    # установить SECOND_SHEET_TAB для второй таблицы
    context.user_data["second_sheet_tab"] = _resolve_second_sheet_tab(year)

    await query.edit_message_text(
        f"Год выбран: {year}\nТеперь введите ссылку на ячейку (например, A21):"
    )
    return WAIT_FOR_CELL

async def cell_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cell_ref = (update.message.text or "").strip()
    row = _parse_row_index(cell_ref)
    if not row:
        await update.message.reply_text("Не понял номер строки. Пример: A21")
        return WAIT_FOR_CELL

    processing_msg = await update.message.reply_text("Обработка...")

    try:
        cell_b = f"B{row}"
        cell_k = f"K{row}"

        # определить лист по выбранному году (основная таблица)
        selected_year = context.user_data.get("year")
        sheet_tab = _resolve_sheet_tab(selected_year)

        # 1) читаем Google Sheets
        account_value, sum_value = _read_cells(cell_b, cell_k, sheet_tab)
        account_value = account_value or ""
        sum_value = sum_value or ""  # сумма выигрыша

        # 2) тянем из Метабазы
        mb_rows: list[dict] = []
        mb_error: str | None = None
        if account_value:
            try:
                initial_params = {}
                if METABASE_CARD_URL:
                    card_id = get_card_id_from_url(METABASE_CARD_URL)
                    initial_params = parse_params_from_url(METABASE_CARD_URL) or {}
                elif METABASE_CARD_NAME:
                    mb_login()
                    card_id = find_card_id_by_name(METABASE_CARD_NAME)
                else:
                    raise RuntimeError("Не указан METABASE_CARD_URL или METABASE_CARD_NAME")

                period = context.user_data.get("period", "—")
                date_range = _period_to_metabase_date(period, sep=METABASE_DATE_SEP)
                if date_range:
                    initial_params[METABASE_DATE_PARAM_NAME] = date_range

                if "X-Metabase-Session" not in session.headers:
                    mb_login()

                rows = run_card_json(card_id, account_value, url_params=initial_params)
                if isinstance(rows, list) and rows:
                    mb_rows = rows
            except Exception as e:
                logger.exception("Metabase fetch failed")
                mb_error = f"Ошибка Метабазы: {e.__class__.__name__}: {e}"

        # 2.5) backoffice
        LAD = None
        STATUS = []
        cupis_lvl = None
        if account_value:
            try:
                bo = _post_client_information(account_value)
                items = _safe_get(bo, "response", "list", default=[])
                visit = _find_in_list_by_class(items, "Fon.Client.Visit")
                online_date_unix = _safe_get(visit or {}, "object", "onlineDate")
                LAD = _to_gmt_plus3_from_unix(online_date_unix)

                grades = _find_in_list_by_class(items, "Fon.Antifraud.ClientGrades")
                STATUS = _safe_get(grades or {}, "object", "gradeRatings", default=[]) or []

                cupis_lvl = _extract_cupis_level_from_bo(bo)
            except Exception:
                logger.exception("Backoffice fetch failed")
                LAD = None
                STATUS = []
                cupis_lvl = None

        # 3) расчёт
        lad_str = LAD or "—"

        turnover_sport  = _sum_field(mb_rows, "Turnover_Sport")
        turnover_scorum = _sum_field(mb_rows, "Turnover_Scorum")
        turnover = round(turnover_sport + turnover_scorum, 2)

        saldo = round(_sum_field(mb_rows, "GGR_dep_wds"), 2)

        ggr_sport  = _sum_field(mb_rows, "GGR_Sport")
        ggr_scorum = _sum_field(mb_rows, "GGR_Scorum")
        GGR = round(ggr_sport + ggr_scorum, 2)

        sum_dep = round(_sum_field(mb_rows, "Deposits"), 2)

        try:
            nalog_base = float((sum_value or "0").replace(",", "."))
        except Exception:
            nalog_base = 0.0
        nalog = round(nalog_base * 0.13, 2)

        percent_comp = round((nalog / saldo) * 100, 2) if saldo not in (0, 0.0) else 0.0

        if not cupis_lvl:
            latest_row = mb_rows[-1] if mb_rows else {}
            cupis_lvl_fallback = latest_row.get("ECupisIdentLevelCaption", "—")
            if isinstance(cupis_lvl_fallback, str) and cupis_lvl_fallback.startswith("Уровень идентификации в ЦУПИС: "):
                cupis_lvl_fallback = cupis_lvl_fallback.replace("Уровень идентификации в ЦУПИС: ", "", 1).strip()
            cupis_lvl = cupis_lvl_fallback or "—"

        period = context.user_data.get("period", "—")
        meta_note = f"\n(метабаза: {mb_error})" if mb_error else ""

        client_status_str = format_client_status(STATUS)
        reward_group = pick_reward_group(STATUS)
        result_str = compute_result(reward_group, percent_comp, nalog)

        # запись в основную таблицу
        try:
            _write_row_values(
                row=row,
                client_status_str=client_status_str,
                nalog=nalog,
                saldo=saldo,
                percent_comp=percent_comp,
                result_str=result_str,
                sheet_tab=sheet_tab,
            )
        except Exception:
            logger.exception("Failed to write values to Google Sheets")

        text = (
            f"Дата - <code>{today_str}</code>\n"
            f"Номер счета - <code>{account_value}</code>\n"
            f"Статус - <code>{client_status_str}</code>\n"
            f"Оборот - <code>{turnover}</code>\n"
            f"Сальдо - <code>{saldo}</code>\n"
            f"ГГР - <code>{GGR}</code>\n"
            f"Сумма депозитов - <code>{sum_dep}</code>\n"
            f"Сумма выигрыша - <code>{sum_value}</code>\n"
            f"Размер налога - <code>{nalog}</code>\n"
            f"Процент - <code>{'менее 9%' if (percent_comp is not None and float(percent_comp) <= 9.99) else 'более 10%'}</code>\n"
            f"Результат - <code>{result_str}</code>\n"
            f"Цупис - <code>{cupis_lvl}</code>\n"
            f"LAD - <code>{lad_str}</code>\n"
            # f"Период - {period}"
            # f"{meta_note}"
        )

        # Сохраним всё нужное для потенциальной записи во вторую таблицу
        context.user_data["second_payload"] = {
            "account_value": account_value,
            "client_status_str": client_status_str,
            "nalog": nalog,
            "turnover": turnover,
            "GGR": GGR,
            "sum_dep": sum_dep,
            "saldo": saldo,
            "percent_comp": percent_comp,
            "result_str": result_str,
        }
        context.user_data["last_message_text"] = text
        context.user_data["last_message_id"] = processing_msg.message_id
        context.user_data["last_chat_id"] = processing_msg.chat_id

        # Если "Сразу компенсируем" — показываем кнопки Да/Нет
        if result_str == "Сразу компенсируем":
            keyboard = [
                [
                    InlineKeyboardButton("Пердать АФ", callback_data="second:yes"),
                    InlineKeyboardButton("Не передавать", callback_data="second:no"),
                ]
            ]
            await processing_msg.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))
            return WAIT_FOR_CONFIRM
        else:
            # без кнопок просто выводим текст
            await processing_msg.edit_text(text, parse_mode="HTML")
            return ConversationHandler.END

    except Exception as e:
        logger.exception("Error while processing /nalog")
        await processing_msg.edit_text(f"Ошибка: {e}")
        return ConversationHandler.END

async def confirm_second_table(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий 'Да'/'Нет' для записи во вторую таблицу."""
    query = update.callback_query
    await query.answer()
    data = query.data  # "second:yes" | "second:no"

    # убираем кнопки под сообщением: просто редактируем разметку
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    payload = context.user_data.get("second_payload") or {}
    base_text = context.user_data.get("last_message_text") or ""

    if data == "second:yes":
        try:
            second_tab = context.user_data.get("second_sheet_tab") or _resolve_second_sheet_tab(context.user_data.get("year"))
            _write_second_table_row(
                account_value=payload.get("account_value", ""),
                client_status_str=payload.get("client_status_str", ""),
                nalog=payload.get("nalog", 0.0),
                turnover=payload.get("turnover", 0.0),
                GGR=payload.get("GGR", 0.0),
                sum_dep=payload.get("sum_dep", 0.0),
                saldo=payload.get("saldo", 0.0),
                percent_comp=payload.get("percent_comp", 0.0),
                result_str=payload.get("result_str", ""),
                second_sheet_tab=second_tab,
            )
            # Обновим текст сообщением-подтверждением (кнопок уже нет)
            try:
                await query.edit_message_text(f"{base_text}\n\n✅ Передано в таблицу АФ (лист «{second_tab}»)", parse_mode="HTML")
            except Exception:
                pass
        except Exception as e:
            logger.exception("Failed to write to SECOND sheet")
            try:
                await query.edit_message_text(f"{base_text}\n\n⚠️ Не удалось записать в таблицу АФ: {e}", parse_mode="HTML")
            except Exception:
                pass
    else:
        # Пользователь отказался — просто отметим это
        try:
            await query.edit_message_text(f"{base_text}\n⛔ Передача в таблицу АФ отменена", parse_mode="HTML")
        except Exception:
            pass

    # Очистим временные данные
    context.user_data.pop("second_payload", None)
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END

def main():
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN == "PASTE_TELEGRAM_TOKEN":
        raise SystemExit("Заполни TELEGRAM_TOKEN (переменная окружения или константа в файле)")
    if not SHEET_ID or SHEET_ID == "PASTE_SHEET_ID":
        raise SystemExit("Заполни SHEET_ID (ID таблицы между /d/ и /edit)")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("nalog", nalog_cmd)],
        states={
            WAIT_FOR_YEAR: [CallbackQueryHandler(year_chosen, pattern=r"^year:\d{4}$")],
            WAIT_FOR_CELL: [MessageHandler(filters.TEXT & ~filters.COMMAND, cell_input)],
            WAIT_FOR_CONFIRM: [CallbackQueryHandler(confirm_second_table, pattern=r"^second:(yes|no)$")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(conv)

    app.run_polling()

if __name__ == "__main__":
    main()