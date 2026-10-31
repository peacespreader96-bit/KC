#!/usr/bin/env python3
"""
KaramChand OSINT Telegram Bot · Production Edition
FastAPI + DuckDB (httpfs/HuggingFace) + Long Polling + SQLite members
"""

import csv
import io
import json
import logging
import os
import random
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
HF_TOKEN    = os.getenv("HF_TOKEN",    "hf_YOUR_READ_TOKEN_HERE") # 🔴 PASTE YOUR HF READ TOKEN HERE
BOT_LINK    = os.getenv("BOT_LINK",    "https://t.me/Karam_Chand_bot")
DEV_LINK    = os.getenv("DEV_LINK",    "https://t.me/ansafrozkhan")
DEV_HANDLE  = os.getenv("DEV_HANDLE",  "@ansafrozkhan")
BOT_NAME    = os.getenv("BOT_NAME",    "KaramChand")
UPI_ID      = os.getenv("UPI_ID",      "admin@upi") # 🔴 CHANGE TO YOUR REAL UPI ID
BASE_HF     = "hf://buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/"
BASE_URL    = f"https://api.telegram.org/bot{TOKEN}"

FORCE_CHANNELS: list[str] = []  # Example: ["@YourChannel"] - Leave empty [] to skip force join
FREE_DAILY_LIMIT = 3
SPIN_INTERVAL = "daily"
SPIN_WINNER_LIMIT = 5
SPIN_JACKPOT = 64
SPIN_WINS = {64: "premium", 43: 5, 22: 3, 1: 1}
STREAK_INTERVAL = 3
STREAK_BONUS = 1
WELCOME_IMAGE_URL = "https://raw.githubusercontent.com/peacespreader96-bit/KC/refs/heads/main/banner.jpg" # 🔴 ADD YOUR BANNER IMAGE URL HERE

PLANS: dict[str, dict] = {
    "day":   {"id": "day",   "name": "1 Din ka Pass",   "days": 1,  "amount": 29},
    "week":  {"id": "week",  "name": "7 Din ka Pass",   "days": 7,  "amount": 79},
    "month": {"id": "month", "name": "30 Din ka Pass",  "days": 30, "amount": 150},
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
_BAN_TEXT = "Bhai tera ID ban hai. Koi feature nahi chalega — desk band. Operator se baat kar, yahan rona band."

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

app = FastAPI(title="KaramChand", docs_url=None, redoc_url=None)

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
                history          TEXT,
                last_streak_date TEXT,
                streak_count     INTEGER DEFAULT 0,
                utr_attempts     INTEGER DEFAULT 0,
                is_banned        INTEGER DEFAULT 0
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
        existing = {row[1] for row in _sql.execute("PRAGMA table_info(users)").fetchall()}
        for col, ddl in (
            ("last_streak_date", "ALTER TABLE users ADD COLUMN last_streak_date TEXT"),
            ("streak_count", "ALTER TABLE users ADD COLUMN streak_count INTEGER DEFAULT 0"),
            ("utr_attempts", "ALTER TABLE users ADD COLUMN utr_attempts INTEGER DEFAULT 0"),
            ("is_banned", "ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0"),
        ):
            if col not in existing:
                _sql.execute(ddl)
        _sql.commit()

_init_sqlite()
log.info("✅ SQLite ready · %s", _DB_PATH)

def _blank_user(cid: int, name: str | None, username: str | None) -> dict:
    return {
        "id":               cid,
        "name":             name or "User",
        "username":         username or "",
        "searches":         0,
        "results":          0,
        "last_search":      0.0,
        "last_msg":         0.0,
        "lang":             "en",
        "is_premium":       False,
        "premium_until":    0.0,
        "history":          [],
        "daily_searches":   0,
        "last_search_date": "",
        "referral_count":   0,
        "referred_by":      None,
        "last_spin_date":   "",
        "last_streak_date": "",
        "streak_count":     0,
        "utr_attempts":     0,
        "is_banned":        0,
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
        "id":               int(row["chat_id"]),
        "name":             row["name"] or "User",
        "username":         row["username"] or "",
        "searches":         int(row["total_searches"] or 0),
        "results":          0,
        "last_search":      0.0,
        "last_msg":         0.0,
        "lang":             "en",
        "is_premium":       bool(row["is_premium"]),
        "premium_until":    float(row["premium_until"] or 0),
        "history":          history,
        "daily_searches":   int(row["daily_searches"] or 0),
        "last_search_date": row["last_search_date"] or "",
        "referral_count":   int(row["referral_count"] or 0),
        "referred_by":      int(referred) if referred not in (None, "") else None,
        "last_spin_date":   row["last_spin_date"] or "",
        "last_streak_date": row["last_streak_date"] or "",
        "streak_count":     int(row["streak_count"] or 0),
        "utr_attempts":     int(row["utr_attempts"] or 0),
        "is_banned":        int(row["is_banned"] or 0),
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
                last_spin_date, history, last_streak_date, streak_count,
                utr_attempts, is_banned
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                history=excluded.history,
                last_streak_date=excluded.last_streak_date,
                streak_count=excluded.streak_count,
                utr_attempts=excluded.utr_attempts,
                is_banned=excluded.is_banned
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
                u.get("last_streak_date") or "",
                int(u.get("streak_count") or 0),
                int(u.get("utr_attempts") or 0),
                int(u.get("is_banned") or 0),
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

def _touch_streak(cid: int) -> bool:
    """Count a search toward the streak. Bonus when the gap is exactly STREAK_INTERVAL days."""
    today = _today()
    granted = False
    with _sl:
        user = _get_ud(cid)
        last = user.get("last_streak_date") or ""
        if last == today:
            return False
        today_dt = datetime.strptime(today, "%Y-%m-%d").date()
        last_dt = None
        if last:
            try:
                last_dt = datetime.strptime(last, "%Y-%m-%d").date()
            except ValueError:
                last_dt = None
        if last_dt is not None and (today_dt - last_dt).days == STREAK_INTERVAL:
            user["streak_count"] = int(user.get("streak_count") or 0) + 1
        else:
            user["streak_count"] = 1
        user["last_streak_date"] = today
        if int(user.get("streak_count") or 0) >= STREAK_INTERVAL:
            _add_bonus(cid, STREAK_BONUS)
            user["streak_count"] = 0
            granted = True
        _save_user(user)
    return granted

def _utr_lock_until(cid: int) -> float:
    try:
        return float(_kv_get(f"utrlock:{cid}", "0") or 0)
    except ValueError:
        return 0.0

def _utr_locked(cid: int) -> bool:
    return _utr_lock_until(cid) > time.time()

def _bump_utr_attempt(cid: int) -> int:
    with _sl:
        user = _get_ud(cid)
        user["utr_attempts"] = int(user.get("utr_attempts") or 0) + 1
        attempts = int(user["utr_attempts"])
        _save_user(user)
    if attempts > 3:
        _kv_set(f"utrlock:{cid}", str(time.time() + 3600))
    return attempts

def _reset_utr_attempts(cid: int) -> None:
    with _sl:
        user = user_data.get(cid)
        if user is None:
            loaded = _load_user(cid)
            if loaded is not None:
                user_data[cid] = loaded
                user = loaded
        if user is not None:
            user["utr_attempts"] = 0
            _save_user(user)
        else:
            with _sql_lock:
                _sql.execute("UPDATE users SET utr_attempts = 0 WHERE chat_id = ?", (cid,))
                _sql.commit()
    _kv_set(f"utrlock:{cid}", "0")

def _backup_loop() -> None:
    while True:
        time.sleep(300)
        try:
            with _sql_lock:
                _sql.execute("PRAGMA wal_checkpoint(PASSIVE);")
                _sql.commit()
            with open(_DB_PATH, "rb") as fh:
                blob = fh.read()
            requests.post(
                f"{BASE_URL}/sendDocument",
                data={"chat_id": str(ADMIN_ID), "caption": "🔄 Auto-Backup: users.db"},
                files={"document": ("users.db", io.BytesIO(blob), "application/x-sqlite3")},
                timeout=60,
            )
        except Exception:
            log.exception("auto-backup failed")

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

def send_photo(cid: int, photo: str, caption: str, kb=None) -> int | None:
    p: dict = {
        "chat_id": cid,
        "photo": photo,
        "caption": caption[:1024],
        "parse_mode": "HTML",
    }
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendPhoto", p).get("result", {}).get("message_id")

def edit_msg(cid: int, mid: int | None, text: str, kb=None) -> None:
    if not mid: return
    p: dict = {
        "chat_id": cid, "message_id": mid, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }
    if kb: p["reply_markup"] = {"inline_keyboard": kb}
    tg("editMessageText", p)

def desk_edit(cid: int, mid: int | None, text: str, kb=None) -> None:
    """Edit a text message, or the welcome photo caption, without touching queue mids."""
    if not mid:
        send_msg(cid, text, kb=kb)
        return
    p: dict = {
        "chat_id": cid, "message_id": mid, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    res = tg("editMessageText", p)
    desc = str(res.get("description") or "")
    if res.get("ok") or "message is not modified" in desc:
        return
    cap: dict = {
        "chat_id": cid, "message_id": mid, "caption": text[:1024], "parse_mode": "HTML",
    }
    if kb:
        cap["reply_markup"] = {"inline_keyboard": kb}
    res2 = tg("editMessageCaption", cap)
    desc2 = str(res2.get("description") or "")
    if res2.get("ok") or "message is not modified" in desc2:
        return
    if len(text) <= 1024:
        tg("deleteMessage", {"chat_id": cid, "message_id": mid})
        send_msg(cid, text, kb=kb)
        return
    tg("deleteMessage", {"chat_id": cid, "message_id": mid})
    send_msg(cid, text, kb=kb)

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
    if secs < 0.5: return "⚡ Bijli se tez (<0.5s)"
    if secs < 1.5: return "🚀 Rocket speed (~1s)"
    if secs < 5:   return f"⚡ Tez tha ({secs:.1f}s)"
    if secs < 15:  return f"🔥 {secs:.1f}s"
    return f"⏱ {secs:.1f}s"

def sv(v) -> str:
    s = str(v).strip() if v is not None else ""
    return "" if s.lower() == "nan" else s

def q_bar(pos: int, cap: int = 10) -> str:
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
    if d: return f"{d}d {h}h"
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def _today() -> str:
    return now_ist().strftime("%Y-%m-%d")

def _daily_used(u: dict) -> int:
    if u.get("last_search_date") != _today(): return 0
    return int(u.get("daily_searches") or 0)

def _bonus_today(cid: int) -> int:
    try: return int(_kv_get(f"bonus:{cid}:{_today()}", "0") or "0")
    except ValueError: return 0

def _add_bonus(cid: int, n: int) -> int:
    key = f"bonus:{cid}:{_today()}"
    total = _bonus_today(cid) + n
    _kv_set(key, str(total))
    return total

def _premium_active(u: dict) -> bool:
    if _expire_premium(u): _save_user(u)
    return bool(u.get("is_premium")) and float(u.get("premium_until") or 0) > time.time()

def _extend_premium(u: dict, days: int) -> float:
    now = time.time()
    base = float(u.get("premium_until") or 0)
    if not (u.get("is_premium") and base > now): base = now
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
    try: used = int(_kv_get(f"spinwin:{_window_id()}", "0") or "0")
    except ValueError: used = 0
    return max(0, SPIN_WINNER_LIMIT - used)

def _take_jackpot() -> bool:
    key = f"spinwin:{_window_id()}"
    try: used = int(_kv_get(key, "0") or "0")
    except ValueError: used = 0
    if used >= SPIN_WINNER_LIMIT: return False
    _kv_set(key, str(used + 1))
    return True

def _clear_flow(cid: int) -> None:
    flow.pop(cid, None)

def _member_of(cid: int, channel: str) -> bool:
    data = tg("getChatMember", {"chat_id": channel, "user_id": cid})
    result = data.get("result") or {}
    status = result.get("status", "")
    if status in ("creator", "administrator", "member"): return True
    return status == "restricted" and bool(result.get("is_member"))

def _needs_join(cid: int) -> bool:
    if cid == ADMIN_ID or not FORCE_CHANNELS: return False
    return any(not _member_of(cid, ch) for ch in FORCE_CHANNELS)

def _banned(u: dict | None) -> bool:
    return bool(u) and int(u.get("is_banned") or 0) == 1

# ═══════════════════════════════════════════════════════════════════════════════
#  UI DICTIONARY & KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════
T = {
    "en": {
        "welcome": "Kya scene hai bhai? Bata kis customer ki info chahiye.",
        "search_prompt": "Bhai 10 digit ka customer phone number daal. Baaki bakwaas nahi chahiye.",
        "not_found": "Kuch nahi mila bhai.",
        "invalid": "Bhai 10 digit ka customer phone number daal, dimag mat kha.",
    }
}

def get_text(cid: int, key: str) -> str:
    lang = user_data.get(cid, {}).get("lang", "en")
    return T.get(lang, T["en"]).get(key, "")

def dashboard_kb():
    return [
        [{"text": "👤 Mera Scene", "callback_data": "profile"},
         {"text": "👑 Premium", "callback_data": "premium"}],
        [{"text": "📜 Purani Search", "callback_data": "history"},
         {"text": "🆘 Support", "callback_data": "support"}],
        [{"text": "🔍 Number Daal", "callback_data": "search"}],
    ]

def main_kb(): return dashboard_kb()
def back_kb(): return [[{"text": "Wapas", "callback_data": "dashboard"}]]

def pricing_kb():
    rows = [
        [{"text": f"{p['name']} · ₹{p['amount']}", "callback_data": f"plan:{pid}"}]
        for pid, p in PLANS.items()
    ]
    rows.append([{"text": "Wapas", "callback_data": "dashboard"}])
    return rows

def result_kb(page: int, total: int):
    nav = []
    if page > 0: nav.append({"text": "◀️ Pichla", "callback_data": f"pg:{page - 1}"})
    if total > 1: nav.append({"text": f"📄 {page + 1}/{total}", "callback_data": "noop"})
    if page < total - 1: nav.append({"text": "Agla ▶️", "callback_data": f"pg:{page + 1}"})
    rows = [nav] if nav else []
    rows.append([{"text": "📄 HTML Report", "callback_data": f"html:{page}"}])
    rows.append([{"text": "Wapas", "callback_data": "dashboard"}])
    return rows

def force_kb():
    rows = []
    for ch in FORCE_CHANNELS:
        handle = ch[1:] if str(ch).startswith("@") else None
        if handle: rows.append([{"text": f"Join kar {ch}", "url": f"https://t.me/{handle}"}])
        else: rows.append([{"text": f"Join kar {ch}", "callback_data": "noop"}])
    rows.append([{"text": "Check kar ✅", "callback_data": "verify"}])
    return rows

def broadcast_kb():
    return [[{"text": "Sabko", "callback_data": "cast:all"}, {"text": "Sirf Premium", "callback_data": "cast:prem"}]]

# ═══════════════════════════════════════════════════════════════════════════════
#  MESSAGE TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════
def msg_dashboard(u: dict) -> str:
    name = _e(u.get("name") or "bhai")
    if _premium_active(u):
        until = fmt_until(float(u.get("premium_until") or 0))
        return (
            f"👑 <b>Waapas aa gaya, {name}?</b>\n\n"
            f"Premium desk khula hai bhai.\n\n"
            f"Membership <b>{_e(until)}</b> tak chalegi.\n\n{_D}"
        )
    used = _daily_used(u)
    allowance = FREE_DAILY_LIMIT + _bonus_today(int(u["id"]))
    left = max(0, allowance - used)
    extra = " Aaj ke spin wale extra pulls bhi count ho rahe hain.\n\n" if _bonus_today(int(u["id"])) else ""
    return (
        f"<b>Kya scene hai bhai, {name}?</b>\n\n"
        f"Bata kis customer ki info chahiye. 10 digit number daal, ya neeche se choose kar.\n\n"
        f"Aaj ke free lookups bache · <b>{left}</b> of {allowance}.\n{extra}\n{_D}"
    )

def msg_profile(u: dict) -> str:
    gold = _premium_active(u)
    used = _daily_used(u)
    bonus = _bonus_today(int(u["id"]))
    allowance = "Unlimited" if (gold or int(u["id"]) == ADMIN_ID) else f"{max(0, FREE_DAILY_LIMIT + bonus - used)} aaj bache"
    if gold: status = f"👑 Premium · {_e(fmt_until(float(u.get('premium_until') or 0)))} tak"
    elif int(u["id"]) == ADMIN_ID: status = "Operator · lookups unlimited"
    else: status = "Free wala desk"
    handle = f"@{_e(u['username'])}\n" if u.get("username") else ""
    link = f"{BOT_LINK}?start=ref_{u['id']}"
    streak = int(u.get("streak_count") or 0)
    return (
        f"{'👑 ' if gold else ''}<b>{_e(u.get('name') or 'Bhai')}</b>\n"
        f"{handle}ID · <code>{u['id']}</code>\n"
        f"Status · {status}\n\n"
        f"Lookups · <b>{int(u.get('searches') or 0)}</b>\n"
        f"Aaj · <b>{_e(str(allowance))}</b>\n"
        f"Streak · <b>{streak}</b> / {STREAK_INTERVAL}\n"
        f"Invites · <b>{int(u.get('referral_count') or 0)}</b>\n\n"
        f"Paanch log tere link se aaye toh ek din premium pakka.\n"
        f"<code>{_e(link)}</code>\n\n{_D}"
    )

def msg_history(u: dict) -> str:
    hist = list(u.get("history") or [])[-10:]
    if not hist:
        return f"<b>History</b>\n\nAbhi kuch file nahi hui bhai.\n\n{_D}"
    lines = ["<b>Pichli lookups</b>", ""]
    for item in reversed(hist):
        found = item.get("found")
        tag = "koi match nahi" if found == 0 else ("1 match" if found == 1 else f"{found} matches" if isinstance(found, int) else "file ho gayi")
        lines.append(f"• <code>{_e(str(item.get('query', '')))}</code> · {_e(str(item.get('at', '')))} · {tag}")
    lines += ["", _D]
    return "\n".join(lines)

def msg_pricing(u: dict) -> str:
    if _utr_locked(int(u["id"])):
        left = fmt_td(int(_utr_lock_until(int(u["id"])) - time.time()))
        return (
            f"<b>Premium lock hai bhai.</b>\n\n"
            f"UTR spam ka natija. { _e(left) } baad aana.\n\n{_D}"
        )
    if _premium_active(u):
        head = (
            f"👑 <b>Premium on hai</b> {_e(fmt_until(float(u.get('premium_until') or 0)))} tak.\n\n"
            f"Extend karna ho toh kar le. Payment manual UPI hai — koi insaan confirm karega."
        )
    else:
        head = (
            "<b>Premium membership</b>\n\n"
            "Unlimited lookups, gold desk, aur line mein thodi izzat. UPI kar. "
            "Operator reference dekhega, phir memo aayega."
        )
    return f"{head}\n\nTerm choose kar.\n\n{_D}"

def msg_support_prompt() -> str:
    return (
        f"<b>Support</b>\n\n"
        f"Agle message ko aise likh jaise insaan ko likhta hai. Operator ke paas jayega, jawab yahin aayega.\n\n{_D}"
    )

def msg_force() -> str:
    lines = [
        "<b>Pehle ye ek kaam kar</b>",
        "",
        "Neeche wale channels join kar, phir check daba. Bina membership ke lookup nahi chalega.",
        "",
    ]
    for ch in FORCE_CHANNELS:
        lines.append(f"• {_e(str(ch))}")
    lines += ["", _D]
    return "\n".join(lines)

def msg_help() -> str:
    return (
        f"ℹ️ <b>Desk kaise chalti hai</b>\n\n"
        f"10 digit customer number bhej. Free walon ke {FREE_DAILY_LIMIT} lookups roz. Premium pe cap hat jaata hai.\n\n"
        f"Number aur ID chhupe aate hain. Tap karke khol.\n\n{_D}"
    )

def msg_queue(pos: int, query: str) -> str:
    bar = q_bar(pos)
    if pos == 1:
        return f"<b>Teri baari hai bhai.</b>\n{bar}\n🔎 <code>{_e(query)}</code>\nFile khol raha hoon."
    return (
        f"<b>Line lag gayi hai bhai, tu #{pos} pe hai.</b>\n{bar}\n"
        f"🔎 <code>{_e(query)}</code>\nIntezar · ~{pos * 3}s"
    )

def msg_searching(query: str) -> str:
    return f"Bhai customer data dhoond raha hoon, thoda sabar kar...\n🔎 <code>{_e(query)}</code>"

def msg_stage(query: str, label: str) -> str:
    return f"{label}\n🔎 <code>{_e(query)}</code>"

def msg_no_result(query: str) -> str:
    return (
        f"<b>Kuch nahi mila bhai</b>\n🔎 <code>{_e(query)}</code>\n\n"
        f"Is number pe koi record nahi pada.\n\n{_D}"
    )

def msg_limit(u: dict) -> str:
    return (
        f"<b>Aaj ke free lookups khatam bhai.</b>\n\n"
        f"Free members ko {FREE_DAILY_LIMIT} roz milte hain. Premium pe cap nahi.\n\n"
        f"File pe lookups · <b>{int(u.get('searches') or 0)}</b>\n\n{_D}"
    )

def msg_spin(u: dict, pending: str | None) -> str:
    left = _jackpots_left()
    held = f"\n\nTera number <code>{_e(pending)}</code> ruk gaya hai. Spin ke baad chalega." if pending else ""
    return (
        f"🎰 <b>Lucky spin</b>\n\n"
        f"Agli window · <b>{_e(_countdown())}</b>\n"
        f"Cadence · {SPIN_INTERVAL}\n"
        f"Is window ke premium jackpot bache · <b>{left}</b> of {SPIN_WINNER_LIMIT}\n\n"
        f"Ek window, ek kheench. Teen seven pe ek din Premium.{held}\n\n{_D}"
    )

def msg_welcome(name: str) -> str:
    return msg_dashboard({
        "id": 0, "name": name, "is_premium": False, "premium_until": 0,
        "daily_searches": 0, "last_search_date": "", "searches": 0,
    })

# ═══════════════════════════════════════════════════════════════════════════════
#  RESULT CARD & HTML
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

    lines = ["<b>Customer file</b>", "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄", f"🔎 <code>{_e(query)}</code>", ""]
    if name:         lines.append(f"👤 <b>Naam</b> ➡️ {_e(name)}")
    if fathersName:  lines.append(f"👨 <b>Father</b> ➡️ {_e(fathersName)}")
    if phoneNumber:  lines.append(f"📱 <b>Number</b> ➡️ {_spoiler(phoneNumber)}")
    if otherNumber:  lines.append(f"📞 <b>Dusra number</b> ➡️ {_spoiler(otherNumber)}")
    if aadharNumber: lines.append(f"🪪 <b>ID</b> ➡️ {_spoiler(aadharNumber)}")
    if address:      lines.append(f"🏠 <b>Address</b>\n<blockquote expandable>{_e(address)}</blockquote>")
    loc = ", ".join(x for x in [town, district, state] if x)
    if loc:          lines.append(f"📍 <b>Location</b> ➡️ {_e(loc)}")
    if pincode:      lines.append(f"📮 <b>Zip</b> ➡️ <code>{_e(pincode)}</code>")
    lines += ["", f"🕰 {ts_str()}"]
    if elapsed is not None:
        lines.append(f"⚡ Speed: {elapsed_label(elapsed)}")
        if elapsed < 1.0: lines.append("🏎️ BHAI YE TOH ROCKET THA")
    lines.append(_D)
    return "\n".join(lines)

_HTML_CSS = """
:root { --ink:#16130f; --paper:#f7f1e6; --gold:#8a6d3b; --line:#e0d3bf; --mute:#6e6458; }
* { box-sizing: border-box; }
body { margin:0; background:#e7dcc6; color:var(--ink); font: 16px/1.55 Georgia, serif; }
article { max-width: 680px; margin: 36px auto; background: var(--paper); padding: 48px 44px 40px; }
.kicker { margin:0; font: 600 11px/1.4 sans-serif; letter-spacing: .22em; text-transform: uppercase; color: var(--gold); }
h1 { font-weight: 500; font-size: 36px; line-height: 1.15; margin: 10px 0 0; }
.rule { height: 1px; border: 0; margin: 22px 0; background: linear-gradient(90deg, #c6a15b, transparent); }
.rows { display: grid; grid-template-columns: 168px 1fr; gap: 12px 18px; word-wrap: break-word; max-width: 100%; overflow-wrap: break-word; }
.k { font: 600 11px/1.4 sans-serif; letter-spacing: .08em; text-transform: uppercase; color: var(--mute); padding-top: 4px; }
.v { margin:0; word-wrap: break-word; max-width: 100%; overflow-wrap: break-word; }
.memo { margin-top: 28px; padding: 16px 0 0; border-top: 1px solid var(--line); }
.memo b { font: 600 11px/1.4 sans-serif; letter-spacing: .18em; text-transform: uppercase; color: var(--gold); }
.memo p { margin: 8px 0 0; font: 14px/1.55 sans-serif; color: #3a332b; }
footer { margin-top: 28px; font: 12px/1.4 sans-serif; color: var(--mute); }
"""

def _html_page(title: str, kicker: str, pairs: list[tuple[str, str]], memo: str) -> str:
    cells = [f"<div class='k'>{_e(l)}</div><p class='v'>{v}</p>" for l, v in pairs if v]
    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>{_e(title)}</title><style>{_HTML_CSS}</style></head>
<body><article><p class="kicker">{_e(kicker)}</p><h1>{_e(title)}</h1><hr class="rule"/>
<section class="rows">{''.join(cells)}</section>
<section class="memo"><b>Memo</b><p>{memo}</p></section>
<footer>{_e(BOT_NAME)} · Private desk · {_e(ts_str())}</footer></article></body></html>"""

def generate_html(row: dict, query: str) -> str:
    address = clean_addr(row.get("address"))
    loc = ", ".join(x for x in [sv(row.get("town")), sv(row.get("district")), sv(row.get("state"))] if x)
    pairs = [
        ("Query", f"<code>{_e(query)}</code>"),
        ("Naam", _e(sv(row.get("name")))),
        ("Father", _e(sv(row.get("fathersName")))),
        ("Number", _e(sv(row.get("phoneNumber")))),
        ("Dusra number", _e(sv(row.get("otherNumber")))),
        ("ID", _e(sv(row.get("aadharNumber")))),
        ("Address", _e(address)),
        ("Location", _e(loc)),
        ("Zip", _e(sv(row.get("pincode")))),
    ]
    memo = f"{_e(BOT_NAME)} ne ye note directory se nikala. Private file hai. Rakh le, ya phaad de — teri marzi."
    title = sv(row.get("name")) or f"Note for {query}"
    return _html_page(title, f"{BOT_NAME} · Customer file", pairs, memo)

def generate_receipt_html(user: dict, plan: dict, when: str, expiry: str, utr: str = "") -> str:
    pairs = [
        ("Member", _e(user.get("name") or "Bhai")),
        ("Plan", _e(plan["name"])),
        ("Amount", f"₹{_e(str(plan['amount']))}"),
        ("Waqt", _e(when)),
        ("Expiry", _e(expiry)),
        ("Lookups ab tak", _e(str(int(user.get("searches") or 0)))),
        ("Reference", _e(utr)),
    ]
    memo = f"{_e(BOT_NAME)} confirm karta hai — ye membership haath se settle hui, jo UTR operator ne approve kiya."
    return _html_page(f"{plan['name']} · {user.get('name') or 'Bhai'}", f"{BOT_NAME} · Membership memo", pairs, memo)

def send_html_doc(cid: int, row: dict, query: str) -> None:
    send_html_file(
        cid,
        f"{BOT_NAME}_Report_{sv(row.get('phoneNumber')) or 'note'}.html",
        generate_html(row, query),
        "📄 Le bhai, teri directory note attach hai.",
    )

def send_receipt(cid: int, user: dict, plan: dict, when: str, expiry: str, utr: str) -> None:
    send_html_file(
        cid,
        f"{BOT_NAME}_Memo_{utr}.html",
        generate_receipt_html(user, plan, when, expiry, utr),
        f"👑 <b>Premium on hai</b> {_e(expiry)} tak. Memo file mein hai.",
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  MEMBERSHIP, REFERRALS, PAYMENTS, SPIN
# ═══════════════════════════════════════════════════════════════════════════════
def _apply_referral(cid: int, payload: str) -> None:
    match = re.fullmatch(r"ref_(\d+)", (payload or "").strip())
    if not match: return
    inviter_id = int(match.group(1))
    if inviter_id == cid: return
    grant_until: float | None = None
    with _sl:
        user = _get_ud(cid)
        if user.get("referred_by"): return
        inviter = user_data.get(inviter_id) or _load_user(inviter_id)
        if inviter is None: return
        user_data[inviter_id] = inviter
        user["referred_by"] = inviter_id
        inviter["referral_count"] = int(inviter.get("referral_count") or 0) + 1
        if inviter["referral_count"] == 5: grant_until = _extend_premium(inviter, 1)
        _save_user(user)
        _save_user(inviter)
    if grant_until is not None: _notify_referral_premium(inviter_id, grant_until)

def _notify_referral_premium(inviter_id: int, until: float) -> None:
    with _sql_lock:
        rows = _sql.execute(
            "SELECT name, username FROM users WHERE referred_by = ? ORDER BY chat_id",
            (inviter_id,),
        ).fetchall()
    names = [f"@{row['username']}" if row["username"] else (row["name"] or "Bhai") for row in rows]
    send_msg(
        inviter_id,
        "<b>Tere invites land ho gaye.</b>\n\n"
        "Paanch log tere link se aaye. Ek din ka Premium desk pe lag gaya.\n\n"
        + "\n".join(f"• {_e(name)}" for name in names)
        + f"\n\nPremium <b>{_e(fmt_until(until))}</b> tak chalega.",
    )

def _upi_uri(plan: dict) -> str:
    return "upi://pay?" + urllib.parse.urlencode({
        "pa": UPI_ID, "pn": BOT_NAME, "am": f"{plan['amount']:.2f}", "cu": "INR", "tn": f"{BOT_NAME} {plan['name']}",
    })

def _open_checkout(cid: int, mid: int, plan: dict) -> None:
    if _utr_locked(cid):
        left = fmt_td(int(_utr_lock_until(cid) - time.time()))
        desk_edit(cid, mid, f"Bhai Premium lock hai. {_e(left)} baad try kar. UTR spam ka natija.", kb=back_kb())
        return
    flow[cid] = {"mode": "checkout", "plan": plan["id"]}
    uri = _upi_uri(plan)
    qr = "https://api.qrserver.com/v1/create-qr-code/?size=640x640&margin=16&data=" + urllib.parse.quote(uri, safe="")
    caption = (
        f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']}\n"
        f"UPI ID · <code>{_e(UPI_ID)}</code>\n\n"
        f"Exact amount kisi bhi UPI app se bhej. Phir button daba ke 12 digit UTR daal."
    )
    kb = [[{"text": "✅ Payment ho gaya", "callback_data": f"paid:{plan['id']}"}], [{"text": "Wapas", "callback_data": "premium"}]]
    res = tg("sendPhoto", {"chat_id": cid, "photo": qr, "caption": caption, "parse_mode": "HTML", "reply_markup": {"inline_keyboard": kb}})
    if res.get("ok"):
        desk_edit(cid, mid, "UPI code upar hai. Exact amount bhej, phir confirm kar.")
        return
    desk_edit(cid, mid, caption + f"\n\nPay link\n<code>{_e(uri)}</code>", kb=kb)

def _store_payment(user: dict, plan: dict, utr: str) -> str | None:
    with _sql_lock:
        existing = _sql.execute(
            "SELECT id, status FROM payments WHERE utr = ? AND status IN ('pending', 'approved')",
            (utr,),
        ).fetchone()
        if existing: return None
        payment_id = uuid.uuid4().hex[:12]
        _sql.execute(
            "INSERT INTO payments (id, chat_id, plan_id, plan_name, amount, days, utr, status, created_at, settled_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL)",
            (payment_id, int(user["id"]), plan["id"], plan["name"], int(plan["amount"]), int(plan["days"]), utr, time.time()),
        )
        _sql.commit()
    return payment_id

def _forward_utr(user: dict, plan: dict, utr: str, payment_id: str) -> None:
    handle = f" (@{_e(user['username'])})" if user.get("username") else ""
    text = (
        f"<b>Payment check kar</b>\n\n"
        f"Banda · {_e(user.get('name') or 'Bhai')}{handle}\n"
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
    send_msg(int(user["id"]), "Shukriya. Reference operator ke paas hai. Settle hote hi yahan sunega.")

def _load_payment(payment_id: str) -> sqlite3.Row | None:
    with _sql_lock:
        return _sql.execute("SELECT * FROM payments WHERE id = ?", (payment_id,)).fetchone()

def _mark_payment(payment_id: str, status: str) -> None:
    with _sql_lock:
        _sql.execute("UPDATE payments SET status = ?, settled_at = ? WHERE id = ?", (status, time.time(), payment_id))
        _sql.commit()

def _settle_payment(payment_id: str, approve: bool, admin_mid: int | None) -> None:
    row = _load_payment(payment_id)
    if row is None: return
    if row["status"] != "pending":
        edit_msg(ADMIN_ID, admin_mid, f"Ye reference pehle hi {row['status']} ho chuka.")
        return
    plan = PLANS.get(row["plan_id"]) or {
        "id": row["plan_id"], "name": row["plan_name"], "days": int(row["days"]), "amount": int(row["amount"]),
    }
    member_id = int(row["chat_id"])
    if not approve:
        _mark_payment(payment_id, "rejected")
        _reset_utr_attempts(member_id)
        edit_msg(ADMIN_ID, admin_mid, f"Reject · UTR <code>{_e(row['utr'])}</code> · {_e(plan['name'])}")
        send_msg(member_id, "Operator ko ye reference match nahi hua, isliye Premium nahi khula.")
        return
    when = ts_str()
    with _sl:
        user = _get_ud(member_id)
        until = _extend_premium(user, int(plan["days"]))
        _save_user(user)
        snapshot = dict(user)
        snapshot["history"] = list(user.get("history") or [])
    _mark_payment(payment_id, "approved")
    _reset_utr_attempts(member_id)
    expiry = fmt_until(until)
    edit_msg(ADMIN_ID, admin_mid, f"Approve · {_e(snapshot.get('name') or 'Bhai')} · {_e(plan['name'])} · {_e(expiry)} tak")
    send_msg(member_id, f"👑 <b>Premium khul gaya.</b>\n\n{_e(plan['name'])} chalega <b>{_e(expiry)}</b> tak. Memo file mein hai.")
    send_receipt(member_id, snapshot, plan, when, expiry, str(row["utr"]))

def _present_spin(cid: int, pending: str | None) -> None:
    with _sl:
        user = _get_ud(cid)
        text = msg_spin(user, pending)
    flow[cid] = {"mode": "spin", "pending_query": pending}
    kb = [[{"text": "🎰 Spin Maar", "callback_data": "spin"}]] if _spin_due(user) else back_kb()
    send_msg(cid, text, kb=kb)

def _finish_spin(cid: int, value: int) -> None:
    pending = None
    state = flow.get(cid) or {}
    if state.get("mode") == "spin": pending = state.get("pending_query")
    _clear_flow(cid)
    with _sl:
        user = _get_ud(cid)
        user["last_spin_date"] = _window_id()
        kind = SPIN_WINS.get(value)
        if kind == "premium":
            if _take_jackpot():
                until = _extend_premium(user, 1)
                prize_line = f"Reels pe <b>64</b> aaya — teen seven. Ek din Premium tera, <b>{_e(fmt_until(until))}</b> tak."
            else:
                _add_bonus(cid, 5)
                prize_line = "Reels pe <b>64</b> aaya, par is window ke paanch Premium jackpot khatam. Aaj 5 extra lookups de raha hoon."
        elif isinstance(kind, int):
            _add_bonus(cid, kind)
            prize_line = f"Reels pe <b>{value}</b> aaya. {kind} extra lookup{'s' if kind != 1 else ''} aaj tere."
        else:
            prize_line = f"Reels pe <b>{value}</b> aaya. Is baar kuch extra nahi. Agli window <b>{_e(_countdown())}</b> mein."
        _save_user(user)
    send_msg(cid, f"🎰 <b>Spin khatam.</b>\n\n{prize_line}\n\n{_D}", kb=dashboard_kb())
    if pending and pending.isdigit() and len(pending) == 10: _enqueue_search(cid, pending)

def _run_spin(cid: int) -> None:
    with _sl:
        user = _get_ud(cid)
        if not _spin_due(user):
            send_msg(cid, f"Is window ka spin le chuka hai bhai. Agla <b>{_e(_countdown())}</b> mein.")
            return
    dice = tg("sendDice", {"chat_id": cid, "emoji": "🎰"})
    value = ((dice.get("result") or {}).get("dice") or {}).get("value")
    if not isinstance(value, int):
        send_msg(cid, "Reels table pe atak gaye. Tera pull abhi bhi bacha hai — Spin Maar phir daba.")
        return
    _finish_spin(cid, value)

def _broadcast(audience: str, text: str) -> None:
    now = time.time()
    with _sql_lock:
        if audience == "prem":
            rows = _sql.execute(
                "SELECT chat_id FROM users WHERE is_premium = 1 AND premium_until > ? AND COALESCE(is_banned, 0) = 0",
                (now,),
            ).fetchall()
        else:
            rows = _sql.execute("SELECT chat_id FROM users WHERE COALESCE(is_banned, 0) = 0").fetchall()
    sent, failed = 0, 0
    body = f"<b>Desk se note</b>\n\n{_e(text)}"
    for row in rows:
        mid = send_msg(int(row["chat_id"]), body)
        if mid: sent += 1
        else: failed += 1
        time.sleep(0.05)
    who = "Premium walon" if audience == "prem" else "sab members"
    send_msg(ADMIN_ID, f"Broadcast {who} ko band. Pahuncha · <b>{sent}</b>. Nahi pahuncha · <b>{failed}</b>.")

def admin_stats_text() -> str:
    now = time.time()
    with _sql_lock:
        total = int(_sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        premium = int(_sql.execute(
            "SELECT COUNT(*) AS n FROM users WHERE is_premium = 1 AND premium_until > ?",
            (now,),
        ).fetchone()["n"])
        searches = int(_sql.execute("SELECT COALESCE(SUM(total_searches), 0) AS n FROM users").fetchone()["n"])
        banned = int(_sql.execute("SELECT COUNT(*) AS n FROM users WHERE COALESCE(is_banned, 0) = 1").fetchone()["n"])
    return (
        f"<b>Admin desk</b>\n\n"
        f"Total users · <b>{total}</b>\n"
        f"Free wale · <b>{max(0, total - premium)}</b>\n"
        f"Premium wale · <b>{premium}</b>\n"
        f"Banned · <b>{banned}</b>\n"
        f"Total searches · <b>{searches}</b>\n\n"
        f"Commands · /grant user_id days · /ban user_id · /csv · /logs · /broadcast"
    )

def _admin_grant(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        send_msg(cid, "Format ye hai bhai: /grant <user_id> <days>")
        return
    target = int(parts[1])
    days = int(parts[2])
    if days <= 0:
        send_msg(cid, "Days positive daal bhai. Negative premium nahi hota.")
        return
    with _sl:
        user = _get_ud(target)
        until = _extend_premium(user, days)
        _save_user(user)
    expiry = fmt_until(until)
    send_msg(target, f"👑 Bhai sun, operator ne tujhe <b>{days}</b> din ka premium de diya. Chalega <b>{_e(expiry)}</b> tak.")
    send_msg(cid, f"Ho gaya. <code>{target}</code> ko {days} din premium. <b>{_e(expiry)}</b> tak.")

def _admin_ban(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) != 2 or not parts[1].isdigit():
        send_msg(cid, "Format: /ban <user_id> — itna bhi mushkil hai?")
        return
    target = int(parts[1])
    if target == ADMIN_ID:
        send_msg(cid, "Apne aap ko ban karega? Dimag theek kar.")
        return
    with _sl:
        user = _get_ud(target)
        user["is_banned"] = 1
        _save_user(user)
    send_msg(target, _BAN_TEXT)
    send_msg(cid, f"Ban lag gaya. <code>{target}</code> ab desk pe nahi aayega.")

def admin_send_csv(cid: int) -> None:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["chat_id", "username", "is_premium", "premium_until", "total_searches"])
    with _sql_lock:
        rows = _sql.execute(
            "SELECT chat_id, username, is_premium, premium_until, total_searches FROM users ORDER BY chat_id"
        ).fetchall()
    for row in rows:
        writer.writerow([
            row["chat_id"], row["username"] or "", row["is_premium"], row["premium_until"], row["total_searches"],
        ])
    data = buf.getvalue().encode("utf-8")
    try:
        requests.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": str(cid), "caption": "📋 Users CSV, le bhai."},
            files={"document": ("users.csv", io.BytesIO(data), "text/csv")},
            timeout=25,
        )
    except Exception:
        log.exception("csv send failed")

# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER THREAD
# ═══════════════════════════════════════════════════════════════════════════════
_HACKER_STAGES = (
    "Firewall tod raha hoon...",
    "Deep web mein ghus raha hoon...",
    "Data scrape ho raha hai...",
    "Bhai server ka lock pick kar raha hoon...",
    "Packet sniff kar raha hoon, chup chap...",
    "Database ka parda hata raha hoon...",
    "Encryption ko side se kaat raha hoon...",
    "Node se node jump maar raha hoon...",
    "Cache ko ulta padh raha hoon...",
    "Proxy chain laga ke ghus raha hoon...",
    "Log file ka postmortem chal raha hai...",
    "Token forge kar raha hoon, tension mat le...",
    "Index table ko hila raha hoon...",
    "Shadow record nikaal raha hoon...",
    "VPN ke peeche se jhaank raha hoon...",
    "Query ko tez kar raha hoon...",
    "Backup tape rewind ho rahi hai...",
    "Metadata ko suljha raha hoon...",
    "Port scan soft mode mein...",
    "Customer file ka taala khol raha hoon...",
    "Replica node se copy kheench raha hoon...",
    "Hash match dhoond raha hoon...",
    "Rate limit ko bypass karte hue...",
    "Archive dungeon mein torch leke...",
    "Schema ko samajh raha hoon bhai...",
    "Phone node pe trace laga raha hoon...",
    "Billing drawer khol raha hoon...",
    "Old ticket stack hata raha hoon...",
    "CDN cache ko ignore kar raha hoon...",
    "Root folder mein pair chhupake...",
    "Signal decrypt ho raha hai...",
    "Duplicate rows ko phenk raha hoon...",
    "Ledger ka last page palat raha hoon...",
    "Geo tag ko pin kar raha hoon...",
    "Session cookie tod raha hoon...",
    "Mirror DB se confirm kar raha hoon...",
    "Night batch ka dump khol raha hoon...",
    "Address string ko seedha kar raha hoon...",
    "ID vault ka combination try kar raha hoon...",
    "Queue ke neeche se file kheench raha hoon...",
    "Silent query, koi alarm nahi...",
    "Column join laga raha hoon...",
    "Parquet ke tukde jod raha hoon...",
    "Hot shard pe land kar raha hoon...",
    "Audit log ko side kar raha hoon...",
    "Contact graph unfold ho raha hai...",
    "Pincode grid pe plot kar raha hoon...",
    "Final checksum maarta hoon...",
    "Desk ke neeche se file nikaal raha hoon...",
    "Ek second, result ko polish kar raha hoon...",
)
_last_stage_pick: tuple[str, ...] | None = None

def _next_load_stages() -> tuple[str, ...]:
    global _last_stage_pick
    pick = tuple(random.sample(_HACKER_STAGES, 3))
    spins = 0
    while pick == _last_stage_pick and spins < 8:
        pick = tuple(random.sample(_HACKER_STAGES, 3))
        spins += 1
    if pick == _last_stage_pick:
        extra = next(s for s in _HACKER_STAGES if s not in pick)
        pick = (pick[0], pick[1], extra)
    _last_stage_pick = pick
    return pick

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
            for label in _next_load_stages():
                time.sleep(_STAGE_GAP)
                edit_msg(cid, mid, msg_stage(query, label))
            t_db = time.monotonic()
            rows = db_search(query)
            elapsed = time.monotonic() - t_db
            with _sl:
                global_stats["searches"] += 1
                user_data[cid]["searches"] += 1
            if not rows:
                with _sl: _remember(cid, query, 0)
                edit_msg(cid, mid, msg_no_result(query), kb=back_kb())
            else:
                with _sl:
                    global_stats["results"] += len(rows)
                    user_data[cid]["results"] += len(rows)
                    user_sessions[cid] = {"results": rows, "page": 0, "query": query, "ts": time.time()}
                    _remember(cid, query, len(rows))
                edit_msg(cid, mid, msg_stage(query, "✅ Match mil gaya!"))
                time.sleep(0.35)
                edit_msg(cid, mid, format_card(rows[0], query, elapsed), kb=result_kb(0, len(rows)))
        except Exception:
            log.exception("Worker error")
            edit_msg(cid, mid, "⚠️ Bhai desk phat gaya is lookup pe. Thodi der ruk, phir try kar.")
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
        data={"chat_id": cid, "caption": "📋 <b>Logs</b>", "parse_mode": "HTML"},
        files={"document": ("bot_logs.txt", io.BytesIO(data), "text/plain")},
        timeout=25,
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  SEARCH ENQUEUE
# ═══════════════════════════════════════════════════════════════════════════════
def _enqueue_search(cid: int, query: str) -> None:
    with _sl:
        if cid in active_users: busy = True; user = user_data.get(cid) or _get_ud(cid)
        else: busy = False; user = _get_ud(cid)
        banned = cid != ADMIN_ID and int(user.get("is_banned") or 0) == 1
    if banned:
        send_msg(cid, _BAN_TEXT)
        return
    if busy:
        send_msg(cid, "Arre ruk bhai, teri pehli search abhi desk pe hai.")
        return
    if _touch_streak(cid):
        send_msg(cid, "🔥 Bhai streak complete! Tere ko 1 free lookup mil gaya.")
    counted = False
    if cid != ADMIN_ID and not _premium_active(user):
        today = _today()
        with _sl:
            user = _get_ud(cid)
            if user.get("last_search_date") != today: user["daily_searches"] = 0
            allowance = FREE_DAILY_LIMIT + _bonus_today(cid)
            used = int(user.get("daily_searches") or 0)
            if used >= allowance: blocked = True; _save_user(user)
            else:
                blocked = False; counted = True; user["daily_searches"] = used + 1; user["last_search_date"] = today; _save_user(user)
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
        send_msg(cid, "Desk full hai bhai. Ek minute ruk, phir number bhej.")
        return
    with _sl:
        active_users.add(cid)
        if cid in user_data: user_data[cid]["last_search"] = time.time()
    with _ql: approx_pos = len(_queue_order) + 1
    mid = send_msg(cid, msg_queue(approx_pos, query))
    if not mid:
        with _sl:
            active_users.discard(cid)
            if counted and cid in user_data:
                user_data[cid]["daily_searches"] = max(0, int(user_data[cid].get("daily_searches") or 0) - 1)
                _save_user(user_data[cid])
        return
    with _ql: _queue_order.append({"chat_id": cid, "msg_id": mid, "query": query})
    _search_q.put((cid, query, mid))

def _maybe_spin(cid: int, raw: str) -> bool:
    is_num = raw.isdigit() and len(raw) == 10
    today = _today()
    first = seen_day.get(cid) != today
    seen_day[cid] = today
    with _sl:
        user = _get_ud(cid)
        due = _spin_due(user)
    if not due or not (first or is_num): return False
    _present_spin(cid, raw if is_num else None)
    return True

def _show_dashboard(cid: int, mid: int | None = None) -> None:
    _clear_flow(cid)
    with _sl:
        user = _get_ud(cid)
        text = msg_dashboard(user)
    kb = dashboard_kb()
    if mid:
        tg("deleteMessage", {"chat_id": cid, "message_id": mid})
    if not send_photo(cid, WELCOME_IMAGE_URL, text, kb=kb):
        send_msg(cid, text, kb=kb)

def _guard_join(cid: int, mid: int | None = None) -> bool:
    if not _needs_join(cid): return False
    if mid: desk_edit(cid, mid, msg_force(), kb=force_kb())
    else: send_msg(cid, msg_force(), kb=force_kb())
    return True

def _premium_lock_screen(cid: int, mid: int | None) -> bool:
    if not _utr_locked(cid): return False
    left = fmt_td(int(_utr_lock_until(cid) - time.time()))
    desk_edit(
        cid, mid,
        f"<b>Premium lock hai bhai.</b>\n\nUTR spam ka natija. {_e(left)} baad aana.\n\n{_D}",
        kb=back_kb(),
    )
    return True

# ═══════════════════════════════════════════════════════════════════════════════
#  CALLBACK & MESSAGE HANDLERS
# ═══════════════════════════════════════════════════════════════════════════════
def handle_cb(cb: dict) -> None:
    cid, mid, data = cb["message"]["chat"]["id"], cb["message"]["message_id"], cb["data"]
    name, username = cb.get("from", {}).get("first_name", "User"), cb.get("from", {}).get("username", "")

    if data.startswith(("ok:", "no:")):
        if cid != ADMIN_ID:
            answer_cb(cb["id"], "Sirf operator settle kar sakta hai.", alert=True)
            return
        answer_cb(cb["id"])
        _settle_payment(data.split(":", 1)[1], data.startswith("ok:"), mid)
        return

    answer_cb(cb["id"])
    with _sl:
        user = _get_ud(cid, name=name, username=username)
    if cid != ADMIN_ID and _banned(user):
        send_msg(cid, _BAN_TEXT)
        return

    if data == "verify":
        if _needs_join(cid):
            desk_edit(cid, mid, msg_force() + "\n\nAbhi bhi channel baaki hai. Join kar, phir check daba.", kb=force_kb())
            return
        _show_dashboard(cid, mid)
        return

    if data in {"dashboard", "profile", "premium", "history", "support", "search", "stats", "help", "new_search"} or data.startswith(("plan:", "paid:", "pg:", "html:")) or data in {"spin", "noop"} or data.startswith("cast:"):
        if data != "verify" and _guard_join(cid, mid): return

    if data in {"dashboard", "stats"}: _show_dashboard(cid, mid)
    elif data == "profile":
        _clear_flow(cid)
        with _sl:
            user = _get_ud(cid)
            text = msg_profile(user)
        desk_edit(cid, mid, text, kb=back_kb())
    elif data == "premium":
        if _premium_lock_screen(cid, mid): return
        _clear_flow(cid)
        with _sl:
            user = _get_ud(cid)
            text = msg_pricing(user)
        desk_edit(cid, mid, text, kb=pricing_kb())
    elif data == "history":
        _clear_flow(cid)
        with _sl:
            user = _get_ud(cid)
            text = msg_history(user)
        desk_edit(cid, mid, text, kb=back_kb())
    elif data == "support":
        flow[cid] = {"mode": "WAITING_FOR_SUPPORT"}
        desk_edit(cid, mid, msg_support_prompt(), kb=back_kb())
    elif data in {"search", "new_search"}:
        _clear_flow(cid)
        desk_edit(cid, mid, get_text(cid, "search_prompt"), kb=back_kb())
    elif data == "help":
        desk_edit(cid, mid, msg_help(), kb=back_kb())
    elif data.startswith("plan:"):
        if _premium_lock_screen(cid, mid): return
        plan = PLANS.get(data.split(":", 1)[1])
        if plan: _open_checkout(cid, mid, plan)
    elif data.startswith("paid:"):
        if _premium_lock_screen(cid, mid): return
        plan = PLANS.get(data.split(":", 1)[1])
        if not plan: return
        attempts = _bump_utr_attempt(cid)
        if attempts > 3:
            _clear_flow(cid)
            left = fmt_td(int(_utr_lock_until(cid) - time.time()))
            desk_edit(
                cid, mid,
                f"Bhai bahut UTR maara. Premium section 1 ghante ke liye lock. {_e(left)} baad aana.",
                kb=back_kb(),
            )
            return
        flow[cid] = {"mode": "WAITING_FOR_UTR", "plan": plan["id"]}
        send_msg(
            cid,
            f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']}\n\nUPI app se 12 digit UTR bhej. Sirf digits.",
            kb=back_kb(),
        )
    elif data == "spin": _run_spin(cid)
    elif data.startswith("cast:"):
        if cid != ADMIN_ID: return
        audience = "prem" if data.endswith("prem") else "all"
        flow[cid] = {"mode": "WAITING_FOR_BROADCAST", "audience": audience}
        who = "sirf Premium wale" if audience == "prem" else "har member"
        desk_edit(cid, mid, f"Agle message bhej. Ye <b>{who}</b> ke paas jayega.\n\n/cancel se ye band.")
    elif data.startswith("pg:"):
        page = int(data.split(":")[1])
        with _sl: sess = user_sessions.get(cid)
        if sess and 0 <= page < len(sess["results"]):
            edit_msg(cid, mid, format_card(sess["results"][page], sess["query"]), kb=result_kb(page, len(sess["results"])))
    elif data.startswith("html:"):
        page = int(data.split(":")[1])
        with _sl:
            sess = user_sessions.get(cid)
            row = query = None
            if sess and 0 <= page < len(sess["results"]):
                row, query = sess["results"][page], sess["query"]
        if row is not None: send_html_doc(cid, row, query)

def _start_payload(text: str) -> str | None:
    head, _, rest = text.partition(" ")
    return rest.strip() if head.startswith("/start") else None

def handle_msg(msg: dict) -> None:
    cid, raw = msg["chat"]["id"], msg.get("text", "").strip()
    name, un = msg.get("from", {}).get("first_name", "User"), msg.get("from", {}).get("username", "")

    with _sl:
        user = _get_ud(cid, name=name, username=un)

    if cid == ADMIN_ID and msg.get("reply_to_message"):
        target = support_routes.get(msg["reply_to_message"].get("message_id"))
        if target and raw:
            send_msg(target, f"<b>Desk se jawab</b>\n\n{_e(raw)}")
            send_msg(cid, "Member ke paas pahunch gaya.")
            return

    state = flow.get(cid) or {}
    mode = state.get("mode")

    if cid == ADMIN_ID and mode == "WAITING_FOR_BROADCAST":
        if raw == "/cancel":
            _clear_flow(cid)
            send_msg(cid, "Broadcast cancel.")
            return
        if raw.startswith("/"): return
        audience = state.get("audience") or "all"
        _clear_flow(cid)
        send_msg(cid, "Bhej raha hoon.")
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
        send_msg(cid, "<b>Broadcast</b>\n\nAgle message kisko bheju?", kb=broadcast_kb())
        return
    if cid == ADMIN_ID and raw.startswith("/grant"):
        _admin_grant(cid, raw)
        return
    if cid == ADMIN_ID and raw.startswith("/ban"):
        _admin_ban(cid, raw)
        return
    if cid == ADMIN_ID and raw == "/csv":
        admin_send_csv(cid)
        return

    if cid != ADMIN_ID and _banned(user):
        send_msg(cid, _BAN_TEXT)
        return

    payload = _start_payload(raw)
    if payload is not None:
        _apply_referral(cid, payload)
        if _guard_join(cid): return
        _show_dashboard(cid)
        return

    if raw.startswith("/"): return

    if mode == "WAITING_FOR_UTR":
        if _utr_locked(cid):
            left = fmt_td(int(_utr_lock_until(cid) - time.time()))
            _clear_flow(cid)
            send_msg(cid, f"Bhai Premium lock hai. {_e(left)} baad try kar.", kb=dashboard_kb())
            return
        plan = PLANS.get(state.get("plan") or "")
        if not plan:
            _clear_flow(cid)
            send_msg(cid, "Woh checkout expire ho gaya. Premium khol ke term phir choose kar.", kb=dashboard_kb())
            return
        if not re.fullmatch(r"\d{12}", raw):
            send_msg(cid, "Reference exactly 12 digit ka hona chahiye. Sirf UTR bhej.")
            return
        with _sl:
            user = _get_ud(cid)
            snapshot = dict(user)
        payment_id = _store_payment(snapshot, plan, raw)
        _clear_flow(cid)
        if not payment_id:
            send_msg(cid, "Ye UTR pehle se file pe hai. Tera tha toh operator ka wait kar — warna Support pe likh.")
            return
        _forward_utr(snapshot, plan, raw, payment_id)
        return

    if mode == "WAITING_FOR_SUPPORT":
        _clear_flow(cid)
        note = (
            f"<b>Support note</b>\n\n"
            f"From · {_e(name)} ({'@'+un if un else 'username nahi'})\n"
            f"ID · <code>{cid}</code>\n\n{_e(raw)}\n\n"
            f"<i>Is message pe reply kar, member ko pahunch jayega.</i>"
        )
        admin_mid = send_msg(ADMIN_ID, note)
        if admin_mid: support_routes[admin_mid] = cid
        send_msg(cid, "Teri note operator ke paas hai. Jawab yahin aayega.", kb=dashboard_kb())
        return

    if _guard_join(cid): return
    if _maybe_spin(cid, raw): return

    with _sl:
        is_active = cid in active_users
    if is_active:
        send_msg(cid, "Arre ruk bhai, teri pehli search abhi desk pe hai.")
        return
    if not (raw.isdigit() and len(raw) == 10):
        send_msg(cid, get_text(cid, "invalid"), kb=back_kb())
        return
    _enqueue_search(cid, raw)

# ═══════════════════════════════════════════════════════════════════════════════
#  LONG POLLING & FASTAPI
# ═══════════════════════════════════════════════════════════════════════════════
def poll_telegram() -> None:
    try: requests.get(f"{BASE_URL}/deleteWebhook?drop_pending_updates=true", timeout=5)
    except: pass
    offset, backoff = None, 1
    while True:
        try:
            url = f"{BASE_URL}/getUpdates?timeout=30" + (f"&offset={offset}" if offset else "")
            resp = requests.get(url, timeout=35).json()
            if not resp.get("ok"): time.sleep(backoff); backoff = min(backoff * 2, 60); continue
            backoff = 1
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd: handle_cb(upd["callback_query"])
                    elif "message" in upd and "text" in upd["message"]: handle_msg(upd["message"])
                except Exception: log.exception("Update handler crash")
        except requests.RequestException: time.sleep(backoff); backoff = min(backoff * 2, 60)
        except Exception: time.sleep(3)

@app.get("/")
def health(): return {"status": "alive", "queue": _search_q.qsize()}

for _fn in [worker, poll_telegram, _broadcaster, _session_gc, _persist_loop, _backup_loop]:
    threading.Thread(target=_fn, daemon=True).start()
