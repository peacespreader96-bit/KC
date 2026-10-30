#!/usr/bin/env python3
"""
KaramChand OSINT Telegram Bot · Production Edition
FastAPI + DuckDB (httpfs/HuggingFace) + Long Polling + SQLite members
"""

import io
import json
import logging
import os
import re
import sqlite3
import threading
import time
import html as html_lib
import queue as _queue_mod
import urllib.parse
import uuid
from collections import deque
from datetime import datetime, timezone, timedelta

import duckdb
import requests
from fastapi import FastAPI
from fastapi import Query as FQ
from fastapi.responses import JSONResponse

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
TOKEN       = os.getenv("BOT_TOKEN",   "8723126732:AAHVFOySgFaWN4_4InqtKrvkgN15crbPi0w")
ADMIN_ID    = int(os.getenv("ADMIN_ID",    "5505679330"))
HF_TOKEN    = os.getenv("HF_TOKEN",    "hf_YOUR_READ_TOKEN_HERE") # 🔴 PASTE YOUR HF TOKEN HERE
BOT_LINK    = os.getenv("BOT_LINK",    "https://t.me/Karam_Chand_bot")
DEV_LINK    = os.getenv("DEV_LINK",    "https://t.me/ansafrozkhan")
DEV_HANDLE  = os.getenv("DEV_HANDLE",  "@ansafrozkhan")
BOT_NAME    = os.getenv("BOT_NAME",    "KaramChand")
UPI_ID      = os.getenv("UPI_ID",      "admin@upi") # 🔴 CHANGE TO YOUR UPI ID
BASE_HF     = "hf://buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/"
BASE_URL    = f"https://api.telegram.org/bot{TOKEN}"

FORCE_CHANNELS: list[str] = []  # Example: ["@Channel1", "@Channel2"]
FREE_DAILY_LIMIT = 3
SPIN_INTERVAL = "daily"  # daily | alternate | weekly
SPIN_WINNER_LIMIT = 5
SPIN_JACKPOT = 64
SPIN_WINS = {64: "premium", 43: 5, 22: 3, 1: 1}

PLANS: dict[str, dict] = {
    "day":   {"id": "day",   "name": "1 Day Pass",   "days": 1,  "amount": 29},
    "week":  {"id": "week",  "name": "7 Days Pass",  "days": 7,  "amount": 79},
    "month": {"id": "month", "name": "30 Days Pass", "days": 30, "amount": 150},
}

SUPPORTED_LANGS = ["en"]

SEARCH_COOLDOWN    = 0
MSG_COOLDOWN       = 1
MAX_QUEUE          = 15
SESSION_TTL        = 1800
QUEUE_BROADCAST_IV = 5
GC_INTERVAL        = 300
PARQUET_PARTS      = 7
_STAGE_GAP         = 0.45

_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
class _RingHandler(logging.Handler):
    def __init__(self, maxlen: int = 400) -> None:
        super().__init__()
        self._buf: deque[str] = deque(maxlen=maxlen)
        self._lk = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        with self._lk:
            self._buf.append(self.format(record))

    def tail(self, n: int = 150) -> list[str]:
        with self._lk:
            return list(self._buf)[-n:]

_FMT  = "%(asctime)s [%(levelname)-5s] %(message)s"
_ring = _RingHandler(400)
_ring.setFormatter(logging.Formatter(_FMT, "%H:%M:%S"))
logging.basicConfig(level=logging.INFO, format=_FMT, datefmt="%H:%M:%S")
logging.getLogger().addHandler(_ring)
log = logging.getLogger("bot")

app = FastAPI(title="KaramChand OSINT", docs_url=None, redoc_url=None)

# ═══════════════════════════════════════════════════════════════════════════════
#  DUCKDB
# ═══════════════════════════════════════════════════════════════════════════════
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

log.info("⏳ Initialising DuckDB connection …")
db = duckdb.connect(config={"custom_user_agent": _UA})

db.execute("SET memory_limit='200MB'; SET threads=1; SET http_timeout=10000;")
db.execute("INSTALL httpfs; LOAD httpfs;")

if HF_TOKEN and HF_TOKEN != "hf_YOUR_READ_TOKEN_HERE":
    db.execute(f"CREATE SECRET (TYPE HUGGINGFACE, TOKEN '{HF_TOKEN}');")

urls = [f"{BASE_HF}idx_phone.{i}.parquet" for i in range(PARQUET_PARTS)]
url_list = "[" + ", ".join(f"'{u}'" for u in urls) + "]"
db.execute(f"CREATE VIEW people_phone AS SELECT * FROM read_parquet({url_list})")

_db_lock = threading.Lock()
log.info("✅ DuckDB ready")

def _to_dicts(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    cols = [d[0] for d in conn.description]
    return [
        {c: (None if str(v) == "nan" else v) for c, v in zip(cols, row)}
        for row in conn.fetchall()
    ]

def db_search(number: str) -> list[dict]:
    """Single DB connection protected by a lock for thread safety."""
    sql = "SELECT * FROM people_phone WHERE phoneNumber = ? LIMIT 5"
    with _db_lock:
        conn = db.execute(sql, [number])
        return _to_dicts(conn)

# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED STATE
# ═══════════════════════════════════════════════════════════════════════════════
_sl = threading.RLock()

user_data:     dict[int, dict] = {}
user_sessions: dict[int, dict] = {}
active_users:  set[int]        = set()
global_stats:  dict[str, int]  = {"searches": 0, "results": 0}

flow: dict[int, dict] = {}
support_routes: dict[int, int] = {}
seen_day: dict[int, str] = {}

# ═══════════════════════════════════════════════════════════════════════════════
#  SQLITE  ·  users.db
# ═══════════════════════════════════════════════════════════════════════════════
_sql_lock = threading.Lock()
_sql = sqlite3.connect(_DB_PATH, check_same_thread=False)
_sql.row_factory = sqlite3.Row

def _init_sqlite() -> None:
    with _sql_lock:
        _sql.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA busy_timeout=3000;
            CREATE TABLE IF NOT EXISTS users (
                chat_id          INTEGER PRIMARY KEY,
                name             TEXT,
                username         TEXT,
                is_premium       INTEGER DEFAULT 0,
                premium_until    REAL DEFAULT 0,
                total_searches   INTEGER DEFAULT 0,
                daily_searches   INTEGER DEFAULT 0,
                last_search_date TEXT,
                referral_count   INTEGER DEFAULT 0,
                referred_by      INTEGER,
                last_spin_date   TEXT,
                history          TEXT
            );
            CREATE TABLE IF NOT EXISTS payments (
                id         TEXT PRIMARY KEY,
                chat_id    INTEGER,
                plan_id    TEXT,
                plan_name  TEXT,
                amount     INTEGER,
                days       INTEGER,
                utr        TEXT,
                status     TEXT,
                created_at REAL,
                settled_at REAL
            );
            CREATE TABLE IF NOT EXISTS kv (
                k TEXT PRIMARY KEY,
                v TEXT
            );
            """
        )
        _sql.commit()

_init_sqlite()
log.info("✅ SQLite ready · %s", _DB_PATH)

def _blank_user(cid: int, name: str | None, username: str | None) -> dict:
    return {
        "id":              cid,
        "name":            name or "User",
        "username":        username or "",
        "searches":        0,
        "results":         0,
        "last_search":     0.0,
        "last_msg":        0.0,
        "lang":            "en",
        "is_premium":      False,
        "premium_until":   0.0,
        "history":         [],
        "daily_searches":  0,
        "last_search_date": "",
        "referral_count":  0,
        "referred_by":     None,
        "last_spin_date":  "",
    }

def _from_row(row: sqlite3.Row) -> dict:
    try:
        history = json.loads(row["history"] or "[]")
        if not isinstance(history, list):
            history = []
    except Exception:
        history = []
    referred = row["referred_by"]
    return {
        "id":              int(row["chat_id"]),
        "name":            row["name"] or "User",
        "username":        row["username"] or "",
        "searches":        int(row["total_searches"] or 0),
        "results":         0,
        "last_search":     0.0,
        "last_msg":        0.0,
        "lang":            "en",
        "is_premium":      bool(row["is_premium"]),
        "premium_until":   float(row["premium_until"] or 0),
        "history":         history,
        "daily_searches":  int(row["daily_searches"] or 0),
        "last_search_date": row["last_search_date"] or "",
        "referral_count":  int(row["referral_count"] or 0),
        "referred_by":     int(referred) if referred not in (None, "") else None,
        "last_spin_date":  row["last_spin_date"] or "",
    }

def _save_user(u: dict) -> None:
    history = json.dumps(u.get("history") or [], ensure_ascii=False)
    referred = u.get("referred_by")
    with _sql_lock:
        _sql.execute(
            """
            INSERT INTO users (
                chat_id, name, username, is_premium, premium_until, total_searches,
                daily_searches, last_search_date, referral_count, referred_by,
                last_spin_date, history
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                name=excluded.name,
                username=excluded.username,
                is_premium=excluded.is_premium,
                premium_until=excluded.premium_until,
                total_searches=excluded.total_searches,
                daily_searches=excluded.daily_searches,
                last_search_date=excluded.last_search_date,
                referral_count=excluded.referral_count,
                referred_by=excluded.referred_by,
                last_spin_date=excluded.last_spin_date,
                history=excluded.history
            """,
            (
                int(u["id"]),
                u.get("name") or "User",
                u.get("username") or "",
                1 if u.get("is_premium") else 0,
                float(u.get("premium_until") or 0),
                int(u.get("searches") or 0),
                int(u.get("daily_searches") or 0),
                u.get("last_search_date") or "",
                int(u.get("referral_count") or 0),
                int(referred) if referred else None,
                u.get("last_spin_date") or "",
                history,
            ),
        )
        _sql.commit()

def _load_user(cid: int) -> dict | None:
    with _sql_lock:
        row = _sql.execute("SELECT * FROM users WHERE chat_id = ?", (cid,)).fetchone()
    return _from_row(row) if row else None

def _kv_get(key: str, default: str = "") -> str:
    with _sql_lock:
        row = _sql.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
    return row["v"] if row else default

def _kv_set(key: str, value: str) -> None:
    with _sql_lock:
        _sql.execute(
            "INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )
        _sql.commit()

def _expire_premium(u: dict) -> bool:
    until = float(u.get("premium_until") or 0)
    if u.get("is_premium") and until <= time.time():
        u["is_premium"] = False
        return True
    return False

def _get_ud(cid: int, name: str | None = None, username: str | None = None) -> dict:
    """Load a member from SQLite into memory, then keep the row current."""
    dirty = False
    u = user_data.get(cid)
    if u is None:
        u = _load_user(cid)
        if u is None:
            u = _blank_user(cid, name, username)
            user_data[cid] = u
            _save_user(u)
        else:
            user_data[cid] = u
    if name and name != u.get("name"):
        u["name"] = name
        dirty = True
    if username and username != u.get("username"):
        u["username"] = username
        dirty = True
    if _expire_premium(u):
        dirty = True
    if dirty:
        _save_user(u)
    return u

def _persist_loop() -> None:
    """The worker increments in-memory search totals. Flush them without touching the worker."""
    while True:
        time.sleep(3)
        with _sl:
            snap = [(int(u["id"]), int(u.get("searches") or 0)) for u in user_data.values()]
        for cid, searches in snap:
            with _sql_lock:
                _sql.execute(
                    "UPDATE users SET total_searches = ? WHERE chat_id = ? AND COALESCE(total_searches, 0) < ?",
                    (searches, cid, searches),
                )
                _sql.commit()

def _remember(cid: int, query: str, found: int) -> None:
    u = user_data.get(cid)
    if not u:
        return
    hist = list(u.get("history") or [])
    hist.append({"query": query, "at": ts_str(), "found": found})
    u["history"] = hist[-20:]
    try:
        _save_user(u)
    except Exception:
        log.exception("history save failed")

# ═══════════════════════════════════════════════════════════════════════════════
#  QUEUE STATE
# ═══════════════════════════════════════════════════════════════════════════════
_ql           = threading.Lock()
_search_q     = _queue_mod.Queue()
_queue_order: list[dict] = []

def _q_pos(cid: int) -> int:
    for i, item in enumerate(_queue_order):
        if item["chat_id"] == cid:
            return i + 1
    return 0

# ═══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM API
# ═══════════════════════════════════════════════════════════════════════════════
def tg(method: str, payload: dict, *, retries: int = 2) -> dict:
    url = f"{BASE_URL}/{method}"
    for attempt in range(retries + 1):
        try:
            data = requests.post(url, json=payload, timeout=10).json()
            if not data.get("ok"):
                desc = data.get("description", "")
                if "message is not modified" not in desc:
                    log.warning("TG/%s → %s", method, desc)
            return data
        except requests.RequestException as exc:
            if attempt == retries:
                log.error("TG/%s failed (%d retries): %s", method, retries, exc)
                return {}
            time.sleep(0.4 * (attempt + 1))
    return {}

def send_msg(cid: int, text: str, kb=None) -> int | None:
    p: dict = {
        "chat_id": cid, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }
    if kb: p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendMessage", p).get("result", {}).get("message_id")

def edit_msg(cid: int, mid: int | None, text: str, kb=None) -> None:
    if not mid: return
    p: dict = {
        "chat_id": cid, "message_id": mid, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }
    if kb: p["reply_markup"] = {"inline_keyboard": kb}
    tg("editMessageText", p)

def answer_cb(cb_id: str, text: str | None = None, alert: bool = False) -> None:
    p: dict = {"callback_query_id": cb_id}
    if text:
        p["text"] = text[:180]
        p["show_alert"] = alert
    tg("answerCallbackQuery", p)

def send_html_file(cid: int, filename: str, content: str, caption: str) -> None:
    try:
        requests.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
            files={"document": (filename, io.BytesIO(content.encode("utf-8")), "text/html")},
            timeout=25,
        )
    except Exception:
        log.exception("HTML document failed for %s", cid)

# ═══════════════════════════════════════════════════════════════════════════════
#  UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
IST = timezone(timedelta(hours=5, minutes=30))
_e  = html_lib.escape
_D  = f"<i>Desk · {html_lib.escape(DEV_HANDLE)}</i>"

def now_ist() -> datetime:
    return datetime.now(IST)

def ts_str() -> str:
    return now_ist().strftime("%d-%m-%Y %H:%M:%S IST")

def fmt_until(ts: float) -> str:
    return datetime.fromtimestamp(ts, IST).strftime("%d %b %Y, %H:%M IST")

def elapsed_label(secs: float) -> str:
    if secs < 0.5: return "⚡ Blazing fast (<0.5s)"
    if secs < 1.5: return "🚀 Lightning (~1s)"
    if secs < 5:   return f"⚡ Fast ({secs:.1f}s)"
    if secs < 15:  return f"🔥 {secs:.1f}s"
    return f"⏱ {secs:.1f}s"

def sv(v) -> str:
    s = str(v).strip() if v is not None else ""
    return "" if s.lower() == "nan" else s

def q_bar(pos: int, cap: int = 10) -> str:
    """Neon queue bar. Position 1 is full. Position 6 of a 10-step scale is half."""
    pct = 100 if pos <= 1 else max(10, 100 - (pos - 1) * (100 // cap))
    filled = int(round(cap * pct / 100))
    filled = max(0, min(cap, filled))
    return f"[{'█' * filled}{'░' * (cap - filled)}] {pct}%"

def clean_addr(addr) -> str:
    if not addr: return ""
    return re.sub(r"\s+", " ", re.sub(r"!+", ", ", str(addr))).strip(", ")

def fmt_td(secs: int) -> str:
    secs = max(0, int(secs))
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def _today() -> str:
    return now_ist().strftime("%Y-%m-%d")

def _daily_used(u: dict) -> int:
    if u.get("last_search_date") != _today():
        return 0
    return int(u.get("daily_searches") or 0)

def _bonus_today(cid: int) -> int:
    try:
        return int(_kv_get(f"bonus:{cid}:{_today()}", "0") or "0")
    except ValueError:
        return 0

def _add_bonus(cid: int, n: int) -> int:
    key = f"bonus:{cid}:{_today()}"
    total = _bonus_today(cid) + n
    _kv_set(key, str(total))
    return total

def _premium_active(u: dict) -> bool:
    if _expire_premium(u):
        _save_user(u)
    return bool(u.get("is_premium")) and float(u.get("premium_until") or 0) > time.time()

def _extend_premium(u: dict, days: int) -> float:
    now = time.time()
    base = float(u.get("premium_until") or 0)
    if not (u.get("is_premium") and base > now):
        base = now
    u["is_premium"] = True
    u["premium_until"] = base + days * 86400
    return float(u["premium_until"])

def _window_id(now: datetime | None = None) -> str:
    now = now or now_ist()
    if SPIN_INTERVAL == "weekly":
        year, week, _ = now.isocalendar()
        return f"{year}-W{week:02d}"
    if SPIN_INTERVAL == "alternate":
        epoch = datetime(2026, 1, 1, tzinfo=IST).date()
        return f"alt-{(now.date() - epoch).days // 2}"
    return now.strftime("%Y-%m-%d")

def _spin_due(u: dict) -> bool:
    return (u.get("last_spin_date") or "") != _window_id()

def _next_window_dt() -> datetime:
    now = now_ist()
    if SPIN_INTERVAL == "weekly":
        days = (7 - now.weekday()) % 7 or 7
        return (now + timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    if SPIN_INTERVAL == "alternate":
        epoch = datetime(2026, 1, 1, tzinfo=IST).date()
        add = 2 if (now.date() - epoch).days % 2 == 0 else 1
        return (now + timedelta(days=add)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

def _countdown() -> str:
    secs = int((_next_window_dt() - now_ist()).total_seconds())
    return fmt_td(max(secs, 0))

def _jackpots_left() -> int:
    try:
        used = int(_kv_get(f"spinwin:{_window_id()}", "0") or "0")
    except ValueError:
        used = 0
    return max(0, SPIN_WINNER_LIMIT - used)

def _take_jackpot() -> bool:
    key = f"spinwin:{_window_id()}"
    try:
        used = int(_kv_get(key, "0") or "0")
    except ValueError:
        used = 0
    if used >= SPIN_WINNER_LIMIT:
        return False
    _kv_set(key, str(used + 1))
    return True

def _clear_flow(cid: int) -> None:
    flow.pop(cid, None)

def _member_of(cid: int, channel: str) -> bool:
    data = tg("getChatMember", {"chat_id": channel, "user_id": cid})
    result = data.get("result") or {}
    status = result.get("status", "")
    if status in ("creator", "administrator", "member"):
        return True
    return status == "restricted" and bool(result.get("is_member"))

def _needs_join(cid: int) -> bool:
    if cid == ADMIN_ID or not FORCE_CHANNELS:
        return False
    return any(not _member_of(cid, ch) for ch in FORCE_CHANNELS)

# ═══════════════════════════════════════════════════════════════════════════════
#  UI DICTIONARY & KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════
T = {
    "en": {
        "welcome": "This is your private lookup desk.",
        "search_prompt": "Send a 10-digit mobile number. Nothing else is needed.",
        "not_found": "No records found.",
        "invalid": "Send a 10-digit mobile number — digits only, no spaces.",
    }
}

def get_text(cid: int, key: str) -> str:
    lang = user_data.get(cid, {}).get("lang", "en")
    return T.get(lang, T["en"]).get(key, "")

def dashboard_kb():
    return [
        [{"text": "👤 My Profile", "callback_data": "profile"},
         {"text": "👑 Premium", "callback_data": "premium"}],
        [{"text": "📜 History", "callback_data": "history"},
         {"text": "🆘 Support", "callback_data": "support"}],
        [{"text": "🔍 Start Search", "callback_data": "search"}],
    ]

def main_kb():
    return dashboard_kb()

def back_kb():
    return [[{"text": "Back", "callback_data": "dashboard"}]]

def pricing_kb():
    rows = [
        [{"text": f"{p['name']} · ₹{p['amount']}", "callback_data": f"plan:{pid}"}]
        for pid, p in PLANS.items()
    ]
    rows.append([{"text": "Back", "callback_data": "dashboard"}])
    return rows

def result_kb(page: int, total: int):
    nav = []
    if page > 0: nav.append({"text": "◀️ Prev", "callback_data": f"pg:{page - 1}"})
    if total > 1: nav.append({"text": f"📄 {page + 1}/{total}", "callback_data": "noop"})
    if page < total - 1: nav.append({"text": "Next ▶️", "callback_data": f"pg:{page + 1}"})
    rows = [nav] if nav else []
    rows.append([{"text": "📄 HTML Report", "callback_data": f"html:{page}"}])
    rows.append([{"text": "Back", "callback_data": "dashboard"}])
    return rows

def force_kb():
    rows = []
    for ch in FORCE_CHANNELS:
        handle = ch[1:] if str(ch).startswith("@") else None
        if handle:
            rows.append([{"text": f"Join {ch}", "url": f"https://t.me/{handle}"}])
        else:
            rows.append([{"text": f"Join {ch}", "callback_data": "noop"}])
    rows.append([{"text": "Verify ✅", "callback_data": "verify"}])
    return rows

def broadcast_kb():
    return [[
        {"text": "All Users", "callback_data": "cast:all"},
        {"text": "Premium Only", "callback_data": "cast:prem"},
    ]]

# ═══════════════════════════════════════════════════════════════════════════════
#  MESSAGE TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════
def msg_dashboard(u: dict) -> str:
    name = _e(u.get("name") or "there")
    if _premium_active(u):
        until = fmt_until(float(u.get("premium_until") or 0))
        return (
            f"👑 <b>Welcome back, {name}.</b>\n\n"
            "Your Premium desk is open. Lookups are unlimited, and the queue keeps a place for you.\n\n"
            f"Membership through <b>{_e(until)}</b>.\n\n"
            f"{_D}"
        )
    used = _daily_used(u)
    allowance = FREE_DAILY_LIMIT + _bonus_today(int(u["id"]))
    left = max(0, allowance - used)
    extra = ""
    if _bonus_today(int(u["id"])):
        extra = " Today's extra pulls from the spin are included.\n\n"
    return (
        f"<b>Welcome, {name}.</b>\n\n"
        "This is your private lookup desk. Send a 10-digit mobile number, or choose a destination below.\n\n"
        f"Complimentary lookups left today · <b>{left}</b> of {allowance}.\n"
        f"{extra}\n{_D}"
    )

def msg_profile(u: dict) -> str:
    gold = _premium_active(u)
    used = _daily_used(u)
    bonus = _bonus_today(int(u["id"]))
    if gold or int(u["id"]) == ADMIN_ID:
        allowance = "Unlimited"
    else:
        allowance = f"{max(0, FREE_DAILY_LIMIT + bonus - used)} left today"
    if gold:
        status = f"👑 Premium · until {_e(fmt_until(float(u.get('premium_until') or 0)))}"
    elif int(u["id"]) == ADMIN_ID:
        status = "Operator · unlimited lookups"
    else:
        status = "Complimentary"
    handle = f"@{_e(u['username'])}\n" if u.get("username") else ""
    link = f"{BOT_LINK}?start=ref_{u['id']}"
    return (
        f"{'👑 ' if gold else ''}<b>{_e(u.get('name') or 'Member')}</b>\n"
        f"{handle}"
        f"ID · <code>{u['id']}</code>\n"
        f"Status · {status}\n\n"
        f"Lookups · <b>{int(u.get('searches') or 0)}</b>\n"
        f"Today · <b>{_e(str(allowance))}</b>\n"
        f"Invitations · <b>{int(u.get('referral_count') or 0)}</b>\n\n"
        "Five people who join with your link earn you a day of Premium.\n"
        f"<code>{_e(link)}</code>\n\n"
        f"{_D}"
    )

def msg_history(u: dict) -> str:
    hist = list(u.get("history") or [])[-10:]
    if not hist:
        return f"<b>History</b>\n\nNothing filed yet. A lookup will keep its place here.\n\n{_D}"
    lines = ["<b>Recent lookups</b>", ""]
    for item in reversed(hist):
        found = item.get("found")
        if found == 0:
            tag = "no match"
        elif found == 1:
            tag = "1 match"
        elif isinstance(found, int):
            tag = f"{found} matches"
        else:
            tag = "filed"
        lines.append(
            f"• <code>{_e(str(item.get('query', '')))}</code> · {_e(str(item.get('at', '')))} · {tag}"
        )
    lines += ["", _D]
    return "\n".join(lines)

def msg_pricing(u: dict) -> str:
    if _premium_active(u):
        head = (
            f"👑 <b>Premium is active</b> through {_e(fmt_until(float(u.get('premium_until') or 0)))}.\n\n"
            "Extend it whenever you like. Payment is manual UPI — a person confirms it."
        )
    else:
        head = (
            "<b>Premium membership</b>\n\n"
            "Unlimited lookups, a gold desk, and a calmer place in the queue. "
            "You pay by UPI. An operator confirms the reference, then your memo arrives."
        )
    return f"{head}\n\nChoose a term.\n\n{_D}"

def msg_support_prompt() -> str:
    return (
        "<b>Support</b>\n\n"
        "Write the next message as you would to a person. It goes to the operator, "
        "and their reply comes back here.\n\n"
        f"{_D}"
    )

def msg_force() -> str:
    lines = [
        "<b>One step before the desk opens</b>",
        "",
        "Join the channels below, then tap verify. We check membership before letting a lookup through.",
        "",
    ]
    for ch in FORCE_CHANNELS:
        lines.append(f"• {_e(str(ch))}")
    lines += ["", _D]
    return "\n".join(lines)

def msg_stats(u: dict) -> str:
    return msg_profile(u)

def msg_help() -> str:
    return (
        f"ℹ️ <b>How the desk works</b>\n\n"
        "Send a 10-digit mobile number. Complimentary members have "
        f"{FREE_DAILY_LIMIT} lookups a day. Premium lifts the cap.\n\n"
        "Contacts and identity numbers arrive hidden. Tap them to reveal.\n\n"
        f"{_D}"
    )

def msg_queue(pos: int, query: str) -> str:
    bar = q_bar(pos)
    if pos == 1:
        return (
            f"<b>You're next.</b>\n"
            f"{bar}\n"
            f"🔎 <code>{_e(query)}</code>\n"
            "The desk is opening your file."
        )
    return (
        f"<b>Queue position {pos}</b>\n"
        f"{bar}\n"
        f"🔎 <code>{_e(query)}</code>\n"
        f"Estimated wait · ~{pos * 3}s"
    )

def msg_searching(query: str) -> str:
    return f"🔍 Scanning databases...\n🔎 <code>{_e(query)}</code>"

def msg_stage(query: str, label: str) -> str:
    return f"{label}\n🔎 <code>{_e(query)}</code>"

def msg_no_result(query: str) -> str:
    return (
        f"<b>No record matches</b>\n"
        f"🔎 <code>{_e(query)}</code>\n\n"
        "Nothing in the directory carries that number.\n\n"
        f"{_D}"
    )

def msg_limit(u: dict) -> str:
    return (
        "<b>Today's complimentary lookups are used.</b>\n\n"
        f"Free members have {FREE_DAILY_LIMIT} a day. Premium removes the cap — "
        "the plans sit under Premium, and a person confirms the UPI reference.\n\n"
        f"Lookups on file · <b>{int(u.get('searches') or 0)}</b>\n\n"
        f"{_D}"
    )

def msg_spin(u: dict, pending: str | None) -> str:
    left = _jackpots_left()
    held = ""
    if pending:
        held = f"\n\nYour number <code>{_e(pending)}</code> is held. It runs after this pull."
    return (
        "🎰 <b>Lucky spin</b>\n\n"
        f"Next window · <b>{_e(_countdown())}</b>\n"
        f"Cadence · {SPIN_INTERVAL}\n"
        f"Premium jackpots left this window · <b>{left}</b> of {SPIN_WINNER_LIMIT}\n\n"
        "One pull per window. Three sevens — the slot landing on 64 — grants a day of Premium, "
        "while the house still has a jackpot to give. Smaller alignments add lookups for today."
        f"{held}\n\n{_D}"
    )

def msg_welcome(name: str) -> str:
    return msg_dashboard({"id": 0, "name": name, "is_premium": False, "premium_until": 0, "daily_searches": 0, "last_search_date": "", "searches": 0})

# ═══════════════════════════════════════════════════════════════════════════════
#  RESULT CARD
# ═══════════════════════════════════════════════════════════════════════════════
def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"

def format_card(row: dict, query: str, elapsed: float | None = None) -> str:
    name        = sv(row.get("name"))
    fathersName = sv(row.get("fathersName"))
    phoneNumber = sv(row.get("phoneNumber"))
    otherNumber = sv(row.get("otherNumber"))
    aadharNumber= sv(row.get("aadharNumber"))
    address     = clean_addr(row.get("address"))
    state       = sv(row.get("state"))
    pincode     = sv(row.get("pincode"))
    district    = sv(row.get("district"))
    town        = sv(row.get("town"))

    lines = ["<b>Directory note</b>", "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄", f"🔎 <code>{_e(query)}</code>", ""]
    if name:        lines.append(f"👤 <b>Name</b> ➡️ {_e(name)}")
    if fathersName: lines.append(f"👨 <b>Father</b> ➡️ {_e(fathersName)}")
    if phoneNumber: lines.append(f"📱 <b>Contact</b> ➡️ {_spoiler(phoneNumber)}")
    if otherNumber: lines.append(f"📞 <b>Alt</b> ➡️ {_spoiler(otherNumber)}")
    if aadharNumber:lines.append(f"🪪 <b>ID</b> ➡️ {_spoiler(aadharNumber)}")
    if address:     lines.append(f"🏠 <b>Address</b>\n<blockquote expandable>{_e(address)}</blockquote>")
    loc = ", ".join(x for x in [town, district, state] if x)
    if loc:         lines.append(f"📍 <b>Location</b> ➡️ {_e(loc)}")
    if pincode:     lines.append(f"📮 <b>Zip</b> ➡️ <code>{_e(pincode)}</code>")

    lines += ["", f"🕰 {ts_str()}"]
    if elapsed is not None:
        lines.append(f"⚡ Speed: {elapsed_label(elapsed)}")
        if elapsed < 1.0:
            lines.append("🏎️ BLAZING FAST")
    lines.append(_D)
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
#  HTML  ·  reports and membership memos share one press style
# ═══════════════════════════════════════════════════════════════════════════════
_HTML_CSS = """
:root { --ink:#16130f; --paper:#f7f1e6; --gold:#8a6d3b; --line:#e0d3bf; --mute:#6e6458; }
* { box-sizing: border-box; }
body { margin:0; background:#e7dcc6; color:var(--ink); font: 16px/1.55 Georgia, "Times New Roman", serif; }
article { max-width: 680px; margin: 36px auto; background: var(--paper); padding: 48px 44px 40px; }
.kicker { margin:0; font: 600 11px/1.4 "Gill Sans", "Trebuchet MS", sans-serif; letter-spacing: .22em; text-transform: uppercase; color: var(--gold); }
h1 { font-weight: 500; font-size: 36px; line-height: 1.15; margin: 10px 0 0; }
.rule { height: 1px; border: 0; margin: 22px 0; background: linear-gradient(90deg, #c6a15b, transparent); }
.rows { display: grid; grid-template-columns: 168px 1fr; gap: 12px 18px; }
.k { font: 600 11px/1.4 "Gill Sans", "Trebuchet MS", sans-serif; letter-spacing: .08em; text-transform: uppercase; color: var(--mute); padding-top: 4px; }
.v { margin:0; }
.memo { margin-top: 28px; padding: 16px 0 0; border-top: 1px solid var(--line); }
.memo b { font: 600 11px/1.4 "Gill Sans", "Trebuchet MS", sans-serif; letter-spacing: .18em; text-transform: uppercase; color: var(--gold); }
.memo p { margin: 8px 0 0; font: 14px/1.55 "Gill Sans", "Trebuchet MS", sans-serif; color: #3a332b; }
footer { margin-top: 28px; font: 12px/1.4 "Gill Sans", "Trebuchet MS", sans-serif; color: var(--mute); }
"""

def _html_page(title: str, kicker: str, pairs: list[tuple[str, str]], memo: str) -> str:
    cells = []
    for label, value in pairs:
        if not value:
            continue
        cells.append(f"<div class='k'>{_e(label)}</div><p class='v'>{value}</p>")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{_e(title)}</title>
<style>{_HTML_CSS}</style>
</head>
<body>
<article>
  <p class="kicker">{_e(kicker)}</p>
  <h1>{_e(title)}</h1>
  <hr class="rule"/>
  <section class="rows">
    {''.join(cells)}
  </section>
  <section class="memo">
    <b>Memo</b>
    <p>{memo}</p>
  </section>
  <footer>{_e(BOT_NAME)} · Private desk · {_e(ts_str())}</footer>
</article>
</body>
</html>
"""

def generate_html(row: dict, query: str) -> str:
    address = clean_addr(row.get("address"))
    loc = ", ".join(x for x in [sv(row.get("town")), sv(row.get("district")), sv(row.get("state"))] if x)
    pairs = [
        ("Query", f"<code>{_e(query)}</code>"),
        ("Name", _e(sv(row.get("name")))),
        ("Father", _e(sv(row.get("fathersName")))),
        ("Contact", _e(sv(row.get("phoneNumber")))),
        ("Alternate", _e(sv(row.get("otherNumber")))),
        ("Identity", _e(sv(row.get("aadharNumber")))),
        ("Address", _e(address)),
        ("Location", _e(loc)),
        ("Postal code", _e(sv(row.get("pincode")))),
    ]
    memo = (
        f"{_e(BOT_NAME)} prepared this note from the directory on file. "
        "It is a private record of one lookup. Keep it, or destroy it — it is yours."
    )
    title = sv(row.get("name")) or f"Note for {query}"
    return _html_page(title, f"{BOT_NAME} · Directory note", pairs, memo)

def generate_receipt_html(user: dict, plan: dict, when: str, expiry: str, utr: str = "") -> str:
    pairs = [
        ("Member", _e(user.get("name") or "Member")),
        ("Plan bought", _e(plan["name"])),
        ("Amount paid", f"₹{_e(str(plan['amount']))}"),
        ("Date and time", _e(when)),
        ("Expiry", _e(expiry)),
        ("Lookups to date", _e(str(int(user.get("searches") or 0)))),
        ("Reference", _e(utr)),
    ]
    memo = (
        f"{_e(BOT_NAME)} confirms this membership was settled by hand against the UPI reference "
        "the operator approved. The term starts at approval and ends on the expiry above. "
        "This memo is your record. Keep it with your papers."
    )
    title = f"{plan['name']} · {user.get('name') or 'Member'}"
    return _html_page(title, f"{BOT_NAME} · Membership memo", pairs, memo)

def send_html_doc(cid: int, row: dict, query: str) -> None:
    filename = f"{BOT_NAME}_Report_{sv(row.get('phoneNumber')) or 'note'}.html"
    send_html_file(cid, filename, generate_html(row, query), "📄 Your directory note is attached.")

def send_receipt(cid: int, user: dict, plan: dict, when: str, expiry: str, utr: str) -> None:
    filename = f"{BOT_NAME}_Memo_{utr}.html"
    send_html_file(
        cid,
        filename,
        generate_receipt_html(user, plan, when, expiry, utr),
        f"👑 <b>Premium is active</b> through {_e(expiry)}. Your memo is attached.",
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  MEMBERSHIP, REFERRALS, PAYMENTS, SPIN
# ═══════════════════════════════════════════════════════════════════════════════
def _apply_referral(cid: int, payload: str) -> None:
    match = re.fullmatch(r"ref_(\d+)", (payload or "").strip())
    if not match:
        return
    inviter_id = int(match.group(1))
    if inviter_id == cid:
        return
    grant_until: float | None = None
    with _sl:
        user = _get_ud(cid)
        if user.get("referred_by"):
            return
        inviter = user_data.get(inviter_id) or _load_user(inviter_id)
        if inviter is None:
            return
        user_data[inviter_id] = inviter
        user["referred_by"] = inviter_id
        inviter["referral_count"] = int(inviter.get("referral_count") or 0) + 1
        if inviter["referral_count"] == 5:
            grant_until = _extend_premium(inviter, 1)
        _save_user(user)
        _save_user(inviter)
    if grant_until is not None:
        _notify_referral_premium(inviter_id, grant_until)

def _notify_referral_premium(inviter_id: int, until: float) -> None:
    with _sql_lock:
        rows = _sql.execute(
            "SELECT name, username FROM users WHERE referred_by = ? ORDER BY chat_id",
            (inviter_id,),
        ).fetchall()
    names = []
    for row in rows:
        handle = (row["username"] or "").strip()
        names.append(f"@{handle}" if handle else (row["name"] or "Member"))
    listing = "\n".join(f"• {_e(name)}" for name in names) or "• New members"
    send_msg(
        inviter_id,
        (
            "<b>Your invitations landed.</b>\n\n"
            "Five people joined through your link. A day of Premium is now on your desk.\n\n"
            f"{listing}\n\n"
            f"Premium access runs until <b>{_e(fmt_until(until))}</b>."
        ),
    )

def _upi_uri(plan: dict) -> str:
    query = urllib.parse.urlencode({
        "pa": UPI_ID,
        "pn": BOT_NAME,
        "am": f"{plan['amount']:.2f}",
        "cu": "INR",
        "tn": f"{BOT_NAME} {plan['name']}",
    })
    return "upi://pay?" + query

def _open_checkout(cid: int, mid: int, plan: dict) -> None:
    flow[cid] = {"mode": "checkout", "plan": plan["id"]}
    uri = _upi_uri(plan)
    qr = (
        "https://api.qrserver.com/v1/create-qr-code/?size=640x640&margin=16&data="
        + urllib.parse.quote(uri, safe="")
    )
    caption = (
        f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']}\n"
        f"UPI ID · <code>{_e(UPI_ID)}</code>\n\n"
        "Pay the exact amount from any UPI app. Then tap the button and send the 12-digit UTR."
    )
    kb = [[
        {"text": "✅ I have Paid", "callback_data": f"paid:{plan['id']}"},
    ], [
        {"text": "Back", "callback_data": "premium"},
    ]]
    res = tg("sendPhoto", {
        "chat_id": cid,
        "photo": qr,
        "caption": caption,
        "parse_mode": "HTML",
        "reply_markup": {"inline_keyboard": kb},
    })
    if res.get("ok"):
        edit_msg(cid, mid, "The UPI code is just above. Pay the exact amount, then confirm.")
        return
    edit_msg(
        cid,
        mid,
        caption + f"\n\nPay link\n<code>{_e(uri)}</code>",
        kb=kb,
    )

def _store_payment(user: dict, plan: dict, utr: str) -> str | None:
    with _sql_lock:
        existing = _sql.execute(
            "SELECT id, status FROM payments WHERE utr = ? AND status IN ('pending', 'approved')",
            (utr,),
        ).fetchone()
        if existing:
            return None
        payment_id = uuid.uuid4().hex[:12]
        _sql.execute(
            """
            INSERT INTO payments (id, chat_id, plan_id, plan_name, amount, days, utr, status, created_at, settled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL)
            """,
            (payment_id, int(user["id"]), plan["id"], plan["name"], int(plan["amount"]), int(plan["days"]), utr, time.time()),
        )
        _sql.commit()
    return payment_id

def _forward_utr(user: dict, plan: dict, utr: str, payment_id: str) -> None:
    handle = f" (@{_e(user['username'])})" if user.get("username") else ""
    text = (
        "<b>Payment to review</b>\n\n"
        f"Member · {_e(user.get('name') or 'Member')}{handle}\n"
        f"ID · <code>{user['id']}</code>\n"
        f"Plan · {_e(plan['name'])}\n"
        f"Amount · ₹{plan['amount']}\n"
        f"UTR · <code>{utr}</code>"
    )
    kb = [[
        {"text": "Approve ✅", "callback_data": f"ok:{payment_id}"},
        {"text": "Reject ❌", "callback_data": f"no:{payment_id}"},
    ]]
    send_msg(ADMIN_ID, text, kb=kb)
    send_msg(
        int(user["id"]),
        "Thank you. That reference is with the operator. You'll hear back here once it is settled.",
    )

def _load_payment(payment_id: str) -> sqlite3.Row | None:
    with _sql_lock:
        return _sql.execute("SELECT * FROM payments WHERE id = ?", (payment_id,)).fetchone()

def _mark_payment(payment_id: str, status: str) -> None:
    with _sql_lock:
        _sql.execute(
            "UPDATE payments SET status = ?, settled_at = ? WHERE id = ?",
            (status, time.time(), payment_id),
        )
        _sql.commit()

def _settle_payment(payment_id: str, approve: bool, admin_mid: int | None) -> None:
    row = _load_payment(payment_id)
    if row is None:
        return
    if row["status"] != "pending":
        edit_msg(ADMIN_ID, admin_mid, f"This reference is already {row['status']}.")
        return
    plan = PLANS.get(row["plan_id"]) or {
        "id": row["plan_id"],
        "name": row["plan_name"],
        "days": int(row["days"]),
        "amount": int(row["amount"]),
    }
    member_id = int(row["chat_id"])
    if not approve:
        _mark_payment(payment_id, "rejected")
        edit_msg(ADMIN_ID, admin_mid, f"Declined · UTR <code>{_e(row['utr'])}</code> · {_e(plan['name'])}")
        send_msg(
            member_id,
            "The operator couldn't match that reference, so Premium wasn't opened. "
            "If the amount did leave your account, send the UTR once more, or write through Support.",
        )
        return
    when = ts_str()
    with _sl:
        user = _get_ud(member_id)
        until = _extend_premium(user, int(plan["days"]))
        _save_user(user)
        snapshot = dict(user)
        snapshot["history"] = list(user.get("history") or [])
    _mark_payment(payment_id, "approved")
    expiry = fmt_until(until)
    edit_msg(
        ADMIN_ID,
        admin_mid,
        f"Approved · {_e(snapshot.get('name') or 'Member')} · {_e(plan['name'])} · until {_e(expiry)}",
    )
    send_msg(
        member_id,
        f"👑 <b>Premium is open.</b>\n\n{_e(plan['name'])} runs until <b>{_e(expiry)}</b>. "
        "Your memo is attached as a file.",
    )
    send_receipt(member_id, snapshot, plan, when, expiry, str(row["utr"]))

def _present_spin(cid: int, pending: str | None) -> None:
    with _sl:
        user = _get_ud(cid)
        text = msg_spin(user, pending)
    flow[cid] = {"mode": "spin", "pending_query": pending}
    kb = [[{"text": "🎰 Spin Now", "callback_data": "spin"}]] if _spin_due(user) else back_kb()
    send_msg(cid, text, kb=kb)

def _finish_spin(cid: int, value: int) -> None:
    pending = None
    state = flow.get(cid) or {}
    if state.get("mode") == "spin":
        pending = state.get("pending_query")
    _clear_flow(cid)
    prize_line = "A quiet spin. Nothing extra this time — the next window will be here when it is."
    with _sl:
        user = _get_ud(cid)
        user["last_spin_date"] = _window_id()
        kind = SPIN_WINS.get(value)
        if kind == "premium":
            if _take_jackpot():
                until = _extend_premium(user, 1)
                prize_line = (
                    f"The reels settled on <b>64</b> — three sevens. "
                    f"A day of Premium is yours, through <b>{_e(fmt_until(until))}</b>."
                )
            else:
                _add_bonus(cid, 5)
                prize_line = (
                    "The reels settled on <b>64</b>, but the five Premium jackpots for this window "
                    "are already spoken for. Five extra lookups are on the house today instead."
                )
        elif isinstance(kind, int):
            _add_bonus(cid, kind)
            prize_line = (
                f"The reels settled on <b>{value}</b>. "
                f"{kind} extra lookup{'s' if kind != 1 else ''} {'are' if kind != 1 else 'is'} yours today."
            )
        else:
            prize_line = (
                f"The reels settled on <b>{value}</b>. "
                f"Nothing extra this time. Next window in <b>{_e(_countdown())}</b>."
            )
        _save_user(user)
    send_msg(cid, f"🎰 <b>Spin closed.</b>\n\n{prize_line}\n\n{_D}", kb=dashboard_kb())
    if pending and pending.isdigit() and len(pending) == 10:
        _enqueue_search(cid, pending)

def _run_spin(cid: int) -> None:
    with _sl:
        user = _get_ud(cid)
        if not _spin_due(user):
            send_msg(cid, f"You've already taken this window's pull. Next one in <b>{_e(_countdown())}</b>.")
            return
    dice = tg("sendDice", {"chat_id": cid, "emoji": "🎰"})
    value = ((dice.get("result") or {}).get("dice") or {}).get("value")
    if not isinstance(value, int):
        send_msg(cid, "The reels didn't leave the table. Your pull is still unused — try Spin Now again.")
        return
    _finish_spin(cid, value)

def _broadcast(audience: str, text: str) -> None:
    now = time.time()
    with _sql_lock:
        if audience == "prem":
            rows = _sql.execute(
                "SELECT chat_id FROM users WHERE is_premium = 1 AND premium_until > ?",
                (now,),
            ).fetchall()
        else:
            rows = _sql.execute("SELECT chat_id FROM users").fetchall()
    sent = 0
    failed = 0
    body = f"<b>Note from the desk</b>\n\n{_e(text)}"
    for row in rows:
        cid = int(row["chat_id"])
        mid = send_msg(cid, body)
        if mid:
            sent += 1
        else:
            failed += 1
        time.sleep(0.05)
    label = "Premium members" if audience == "prem" else "all members"
    send_msg(ADMIN_ID, f"Broadcast to {label} closed. Delivered · <b>{sent}</b>. Unreached · <b>{failed}</b>.")

def admin_stats_text() -> str:
    now = time.time()
    with _sql_lock:
        total = int(_sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        premium = int(_sql.execute(
            "SELECT COUNT(*) AS n FROM users WHERE is_premium = 1 AND premium_until > ?",
            (now,),
        ).fetchone()["n"])
        searches = int(_sql.execute("SELECT COALESCE(SUM(total_searches), 0) AS n FROM users").fetchone()["n"])
    free = max(0, total - premium)
    return (
        "<b>Admin</b>\n\n"
        f"Total Users · <b>{total}</b>\n"
        f"Free Users · <b>{free}</b>\n"
        f"Premium Users · <b>{premium}</b>\n"
        f"Total Searches · <b>{searches}</b>"
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER THREAD
# ═══════════════════════════════════════════════════════════════════════════════
_LOAD_STAGES = (
    "🔍 Scanning databases...",
    "📡 Fetching data nodes...",
    "🧬 Compiling intelligence...",
)

def worker() -> None:
    while True:
        cid, query, mid = _search_q.get()
        t0 = time.monotonic()
        try:
            tg("sendChatAction", {"chat_id": cid, "action": "typing"})
            with _ql:
                _queue_order[:] = [x for x in _queue_order if not (x["chat_id"] == cid and x["msg_id"] == mid)]
                snapshot = list(_queue_order)

            for i, item in enumerate(snapshot, start=1):
                try: edit_msg(item["chat_id"], item["msg_id"], msg_queue(i, item["query"]))
                except: pass

            edit_msg(cid, mid, msg_searching(query))
            for label in _LOAD_STAGES:
                time.sleep(_STAGE_GAP)
                edit_msg(cid, mid, msg_stage(query, label))
            t_db = time.monotonic()
            rows = db_search(query)
            elapsed = time.monotonic() - t_db
            _ = t0

            with _sl:
                global_stats["searches"] += 1
                user_data[cid]["searches"] += 1

            if not rows:
                with _sl:
                    _remember(cid, query, 0)
                edit_msg(cid, mid, msg_no_result(query), kb=back_kb())
            else:
                with _sl:
                    global_stats["results"] += len(rows)
                    user_data[cid]["results"] += len(rows)
                    user_sessions[cid] = {"results": rows, "page": 0, "query": query, "ts": time.time()}
                    _remember(cid, query, len(rows))
                edit_msg(cid, mid, msg_stage(query, "✅ Match Found!"))
                time.sleep(0.35)
                text = format_card(rows[0], query, elapsed)
                edit_msg(cid, mid, text, kb=result_kb(0, len(rows)))
        except Exception:
            log.exception("Worker error")
            edit_msg(cid, mid, "⚠️ The desk hit a snag on that lookup. Try once more in a moment.")
        finally:
            with _sl: active_users.discard(cid)
            _search_q.task_done()

# ═══════════════════════════════════════════════════════════════════════════════
#  QUEUE BROADCASTER & GC
# ═══════════════════════════════════════════════════════════════════════════════
def _broadcaster() -> None:
    while True:
        time.sleep(QUEUE_BROADCAST_IV)
        with _ql: snapshot = list(_queue_order)
        for i, item in enumerate(snapshot, start=1):
            try: edit_msg(item["chat_id"], item["msg_id"], msg_queue(i, item["query"]))
            except: pass

def _session_gc() -> None:
    while True:
        time.sleep(GC_INTERVAL)
        cutoff = time.time() - SESSION_TTL
        with _sl:
            stale = [cid for cid, s in user_sessions.items() if s["ts"] < cutoff]
            for cid in stale: del user_sessions[cid]

# ═══════════════════════════════════════════════════════════════════════════════
#  ADMIN UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
def admin_send_logs(cid: int) -> None:
    data = "\n".join(_ring.tail(150)).encode()
    requests.post(
        f"{BASE_URL}/sendDocument",
        data  = {"chat_id": cid, "caption": "📋 <b>Logs</b>", "parse_mode": "HTML"},
        files = {"document": ("bot_logs.txt", io.BytesIO(data), "text/plain")},
        timeout = 25,
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  SEARCH ENQUEUE
# ═══════════════════════════════════════════════════════════════════════════════
def _enqueue_search(cid: int, query: str) -> None:
    with _sl:
        if cid in active_users:
            busy = True
            user = user_data.get(cid) or _get_ud(cid)
        else:
            busy = False
            user = _get_ud(cid)
    if busy:
        send_msg(cid, "Your lookup is already on the desk. The result is on its way.")
        return

    counted = False
    if cid != ADMIN_ID and not _premium_active(user):
        today = _today()
        with _sl:
            user = _get_ud(cid)
            if user.get("last_search_date") != today:
                user["daily_searches"] = 0
            allowance = FREE_DAILY_LIMIT + _bonus_today(cid)
            used = int(user.get("daily_searches") or 0)
            if used >= allowance:
                blocked = True
                _save_user(user)
            else:
                blocked = False
                counted = True
                user["daily_searches"] = used + 1
                user["last_search_date"] = today
                _save_user(user)
        if blocked:
            send_msg(cid, msg_limit(user), kb=[[{"text": "👑 Premium", "callback_data": "premium"}]])
            return

    if _search_q.qsize() >= MAX_QUEUE:
        if counted:
            with _sl:
                user = user_data.get(cid)
                if user:
                    user["daily_searches"] = max(0, int(user.get("daily_searches") or 0) - 1)
                    _save_user(user)
        send_msg(cid, "The desk is at capacity. Give it a minute, then send the number again.")
        return

    with _sl:
        active_users.add(cid)
        if cid in user_data:
            user_data[cid]["last_search"] = time.time()
    with _ql:
        approx_pos = len(_queue_order) + 1
    mid = send_msg(cid, msg_queue(approx_pos, query))
    if not mid:
        with _sl:
            active_users.discard(cid)
            if counted and cid in user_data:
                user_data[cid]["daily_searches"] = max(0, int(user_data[cid].get("daily_searches") or 0) - 1)
                _save_user(user_data[cid])
        return
    with _ql:
        _queue_order.append({"chat_id": cid, "msg_id": mid, "query": query})
    _search_q.put((cid, query, mid))

def _maybe_spin(cid: int, raw: str) -> bool:
    """Hold the first note of the day, and any number, until this window's pull is taken."""
    is_num = raw.isdigit() and len(raw) == 10
    today = _today()
    first = seen_day.get(cid) != today
    seen_day[cid] = today
    with _sl:
        user = _get_ud(cid)
        due = _spin_due(user)
    if not due or not (first or is_num):
        return False
    _present_spin(cid, raw if is_num else None)
    return True

def _show_dashboard(cid: int, mid: int | None = None) -> None:
    _clear_flow(cid)
    with _sl:
        user = _get_ud(cid)
        text = msg_dashboard(user)
    if mid:
        edit_msg(cid, mid, text, kb=dashboard_kb())
    else:
        send_msg(cid, text, kb=dashboard_kb())

def _guard_join(cid: int, mid: int | None = None) -> bool:
    if not _needs_join(cid):
        return False
    if mid:
        edit_msg(cid, mid, msg_force(), kb=force_kb())
    else:
        send_msg(cid, msg_force(), kb=force_kb())
    return True

# ═══════════════════════════════════════════════════════════════════════════════
#  CALLBACK HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
def handle_cb(cb: dict) -> None:
    cid  = cb["message"]["chat"]["id"]
    mid  = cb["message"]["message_id"]
    data = cb["data"]
    name = cb.get("from", {}).get("first_name", "User")
    username = cb.get("from", {}).get("username", "")

    if data.startswith(("ok:", "no:")):
        if cid != ADMIN_ID:
            answer_cb(cb["id"], "Only the operator can settle this.", alert=True)
            return
        answer_cb(cb["id"])
        payment_id = data.split(":", 1)[1]
        _settle_payment(payment_id, data.startswith("ok:"), mid)
        return

    answer_cb(cb["id"])
    with _sl:
        _get_ud(cid, name=name, username=username)

    if data == "verify":
        if _needs_join(cid):
            edit_msg(cid, mid, msg_force() + "\n\nStill waiting on a channel. Join, then tap verify again.", kb=force_kb())
            return
        _show_dashboard(cid, mid)
        return

    if data in {"dashboard", "profile", "premium", "history", "support", "search", "stats", "help", "new_search"} or data.startswith(("plan:", "paid:", "pg:", "html:")) or data in {"spin", "noop"} or data.startswith("cast:"):
        if data != "verify" and _guard_join(cid, mid):
            return

    if data in {"dashboard", "stats"}:
        _show_dashboard(cid, mid)
    elif data == "profile":
        _clear_flow(cid)
        with _sl:
            user = _get_ud(cid)
            text = msg_profile(user)
        edit_msg(cid, mid, text, kb=back_kb())
    elif data == "premium":
        _clear_flow(cid)
        with _sl:
            user = _get_ud(cid)
            text = msg_pricing(user)
        edit_msg(cid, mid, text, kb=pricing_kb())
    elif data == "history":
        _clear_flow(cid)
        with _sl:
            user = _get_ud(cid)
            text = msg_history(user)
        edit_msg(cid, mid, text, kb=back_kb())
    elif data == "support":
        flow[cid] = {"mode": "WAITING_FOR_SUPPORT"}
        edit_msg(cid, mid, msg_support_prompt(), kb=back_kb())
    elif data in {"search", "new_search"}:
        _clear_flow(cid)
        edit_msg(cid, mid, get_text(cid, "search_prompt"), kb=back_kb())
    elif data == "help":
        edit_msg(cid, mid, msg_help(), kb=back_kb())
    elif data.startswith("plan:"):
        plan = PLANS.get(data.split(":", 1)[1])
        if plan:
            _open_checkout(cid, mid, plan)
    elif data.startswith("paid:"):
        plan = PLANS.get(data.split(":", 1)[1])
        if not plan:
            return
        flow[cid] = {"mode": "WAITING_FOR_UTR", "plan": plan["id"]}
        send_msg(
            cid,
            f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']}\n\n"
            "Send the 12-digit UTR from your UPI app. Digits only.",
            kb=back_kb(),
        )
    elif data == "spin":
        _run_spin(cid)
    elif data.startswith("cast:"):
        if cid != ADMIN_ID:
            return
        audience = "prem" if data.endswith("prem") else "all"
        label = "Premium members only" if audience == "prem" else "every member"
        flow[cid] = {"mode": "WAITING_FOR_BROADCAST", "audience": audience}
        edit_msg(cid, mid, f"Send the next message. It will go to <b>{label}</b>.\n\n/cancel aborts this.")
    elif data.startswith("pg:"):
        page = int(data.split(":")[1])
        with _sl:
            sess = user_sessions.get(cid)
        if sess and 0 <= page < len(sess["results"]):
            edit_msg(cid, mid, format_card(sess["results"][page], sess["query"]), kb=result_kb(page, len(sess["results"])))
    elif data.startswith("html:"):
        page = int(data.split(":")[1])
        with _sl:
            sess = user_sessions.get(cid)
            row = None
            query = ""
            if sess and 0 <= page < len(sess["results"]):
                row = sess["results"][page]
                query = sess["query"]
        if row is not None:
            send_html_doc(cid, row, query)
    elif data == "noop":
        return

# ═══════════════════════════════════════════════════════════════════════════════
#  MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
def _start_payload(text: str) -> str | None:
    head, _, rest = text.partition(" ")
    if not head.startswith("/start"):
        return None
    return rest.strip()

def handle_msg(msg: dict) -> None:
    cid   = msg["chat"]["id"]
    raw   = msg.get("text", "").strip()
    name  = msg.get("from", {}).get("first_name", "User")
    un    = msg.get("from", {}).get("username", "")

    with _sl:
        _get_ud(cid, name=name, username=un)

    if cid == ADMIN_ID and msg.get("reply_to_message"):
        src = msg["reply_to_message"].get("message_id")
        target = support_routes.get(src)
        if target and raw:
            send_msg(target, f"<b>Reply from the desk</b>\n\n{_e(raw)}")
            send_msg(cid, "Delivered to the member.")
            return

    state = flow.get(cid) or {}
    mode = state.get("mode")

    if cid == ADMIN_ID and mode == "WAITING_FOR_BROADCAST":
        if raw == "/cancel":
            _clear_flow(cid)
            send_msg(cid, "Broadcast cancelled.")
            return
        if raw.startswith("/"):
            return
        audience = state.get("audience") or "all"
        _clear_flow(cid)
        send_msg(cid, "Sending now.")
        threading.Thread(target=_broadcast, args=(audience, raw), daemon=True).start()
        return

    if cid == ADMIN_ID and raw == "/logs":
        admin_send_logs(cid)
        return

    if cid == ADMIN_ID and raw == "/admin":
        send_msg(cid, admin_stats_text())
        return

    if cid == ADMIN_ID and raw == "/broadcast":
        _clear_flow(cid)
        send_msg(cid, "<b>Broadcast</b>\n\nWho should receive the next message you send?", kb=broadcast_kb())
        return

    payload = _start_payload(raw)
    if payload is not None:
        _apply_referral(cid, payload)
        if _guard_join(cid):
            return
        _show_dashboard(cid)
        return

    if raw.startswith("/"):
        return

    if mode == "WAITING_FOR_UTR":
        plan = PLANS.get(state.get("plan") or "")
        if not plan:
            _clear_flow(cid)
            send_msg(cid, "That checkout expired. Open Premium and choose a term again.", kb=dashboard_kb())
            return
        if not re.fullmatch(r"\d{12}", raw):
            send_msg(cid, "The reference should be exactly 12 digits. Send only the UTR.")
            return
        with _sl:
            user = _get_ud(cid)
            snapshot = dict(user)
        payment_id = _store_payment(snapshot, plan, raw)
        _clear_flow(cid)
        if not payment_id:
            send_msg(cid, "That UTR is already on file. If it was yours, wait for the operator — or write through Support.")
            return
        _forward_utr(snapshot, plan, raw, payment_id)
        return

    if mode == "WAITING_FOR_SUPPORT":
        _clear_flow(cid)
        handle = f"@{un}" if un else "no username"
        note = (
            "<b>Support note</b>\n\n"
            f"From · {_e(name)} ({_e(handle)})\n"
            f"ID · <code>{cid}</code>\n\n"
            f"{_e(raw)}\n\n"
            "<i>Reply to this message and the member will receive it.</i>"
        )
        admin_mid = send_msg(ADMIN_ID, note)
        if admin_mid:
            support_routes[admin_mid] = cid
        send_msg(cid, "Your note is with the operator. Any reply will land here.", kb=dashboard_kb())
        return

    if _guard_join(cid):
        return

    if _maybe_spin(cid, raw):
        return

    with _sl:
        is_active = cid in active_users
    if is_active:
        send_msg(cid, "Your lookup is already on the desk. The result is on its way.")
        return

    if not (raw.isdigit() and len(raw) == 10):
        send_msg(cid, get_text(cid, "invalid"), kb=back_kb())
        return

    _enqueue_search(cid, raw)

# ═══════════════════════════════════════════════════════════════════════════════
#  LONG POLLING
# ═══════════════════════════════════════════════════════════════════════════════
def poll_telegram() -> None:
    try: requests.get(f"{BASE_URL}/deleteWebhook?drop_pending_updates=true", timeout=5)
    except: pass
    offset: int | None = None
    backoff: int = 1

    while True:
        try:
            url  = f"{BASE_URL}/getUpdates?timeout=30" + (f"&offset={offset}" if offset else "")
            resp = requests.get(url, timeout=35).json()
            if not resp.get("ok"):
                time.sleep(backoff); backoff = min(backoff * 2, 60); continue
            backoff = 1
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd: handle_cb(upd["callback_query"])
                    elif "message" in upd and "text" in upd["message"]: handle_msg(upd["message"])
                except Exception: log.exception("Update handler crash")
        except requests.RequestException:
            time.sleep(backoff); backoff = min(backoff * 2, 60)
        except Exception: time.sleep(3)

# ═══════════════════════════════════════════════════════════════════════════════
#  FASTAPI & STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/")
def health():
    return {"status": "alive", "queue": _search_q.qsize()}

for _fn in [worker, poll_telegram, _broadcaster, _session_gc, _persist_loop]:
    threading.Thread(target=_fn, daemon=True).start()
