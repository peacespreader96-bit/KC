#!/usr/bin/env python3
"""
KaramChand OSINT Telegram Bot · Production Edition
FastAPI + DuckDB (httpfs/HuggingFace) + Long Polling
"""

# ═══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════
import io
import json
import logging
import os
import re
import threading
import time
import html as html_lib
import queue as _queue_mod
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
HF_TOKEN    = os.getenv("HF_TOKEN",    "hf_YOUR_READ_TOKEN_HERE")
BOT_LINK    = os.getenv("BOT_LINK",    "https://t.me/Karam_Chand_bot")
DEV_LINK    = os.getenv("DEV_LINK",    "https://t.me/ansafrozkhan")
DEV_HANDLE  = os.getenv("DEV_HANDLE",  "@ansafrozkhan")
BASE_HF     = "hf://buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/"
BASE_URL    = f"https://api.telegram.org/bot{TOKEN}"

MAX_QUEUE          = 15    # hard cap on concurrent queued searches
SEARCH_COOLDOWN    = 600   # 10 min between searches per user
MSG_COOLDOWN       = 3     # 3 s micro-cooldown (spam guard)
SESSION_TTL        = 1800  # 30 min — stale session RAM reclamation
QUEUE_BROADCAST_IV = 5     # seconds between automatic position broadcasts
GC_INTERVAL        = 300   # session GC runs every 5 min
PARQUET_PARTS      = 7     # idx_phone_0.parquet … idx_phone_6.parquet

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

# ═══════════════════════════════════════════════════════════════════════════════
#  FASTAPI
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="KaramChand OSINT", docs_url=None, redoc_url=None)

# ═══════════════════════════════════════════════════════════════════════════════
#  DUCKDB
# ═══════════════════════════════════════════════════════════════════════════════
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

def _make_db() -> duckdb.DuckDBPyConnection:
    db = duckdb.connect(config={"custom_user_agent": _UA})
    db.execute("SET memory_limit='200MB'; SET threads=1;")
    db.execute("INSTALL httpfs; LOAD httpfs;")
    if HF_TOKEN and HF_TOKEN != "hf_YOUR_READ_TOKEN_HERE":
        db.execute(f"CREATE SECRET (TYPE HUGGINGFACE, TOKEN '{HF_TOKEN}');")
    
    urls = [f"{BASE_HF}idx_phone.{i}.parquet" for i in range(PARQUET_PARTS)]
    url_list = "[" + ", ".join(f"'{u}'" for u in urls) + "]"
    db.execute(f"CREATE VIEW people_phone AS SELECT * FROM read_parquet({url_list})")
    return db

log.info("⏳ Initialising DuckDB worker connection …")
worker_db = _make_db()
log.info("✅ Worker DB ready")

log.info("⏳ Initialising DuckDB API connection …")
api_db    = _make_db()
_api_lock = threading.Lock()
log.info("✅ API DB ready")


def _to_dicts(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    cols = [d[0] for d in conn.description]
    return [
        {c: (None if str(v) == "nan" else v) for c, v in zip(cols, row)}
        for row in conn.fetchall()
    ]


def db_search(number: str, *, use_worker: bool) -> list[dict]:
    sql = "SELECT * FROM people_phone WHERE phoneNumber = ? LIMIT 5"
    if use_worker:
        conn = worker_db.execute(sql, [number])
        return _to_dicts(conn)
    with _api_lock:
        conn = api_db.execute(sql, [number])
        return _to_dicts(conn)

# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED STATE
# ═══════════════════════════════════════════════════════════════════════════════
_sl = threading.RLock()

user_data:     dict[int, dict] = {}
user_sessions: dict[int, dict] = {}
active_users:  set[int]        = set()
global_stats:  dict[str, int]  = {"searches": 0, "results": 0}


def _get_ud(cid: int, name: str | None = None, username: str | None = None) -> dict:
    if cid not in user_data:
        user_data[cid] = {
            "id":          cid,
            "name":        name or "User",
            "username":    username or "",
            "searches":    0,
            "results":     0,
            "last_search": 0.0,   
            "last_msg":    0.0,   
        }
    else:
        if name:     user_data[cid]["name"]     = name
        if username: user_data[cid]["username"] = username
    return user_data[cid]


def get_ud(cid: int, name: str | None = None, username: str | None = None) -> dict:
    with _sl:
        return _get_ud(cid, name, username)

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
_sess = requests.Session()
_sess.headers.update({"Content-Type": "application/json"})


def tg(method: str, payload: dict, *, retries: int = 2) -> dict:
    url = f"{BASE_URL}/{method}"
    for attempt in range(retries + 1):
        try:
            data = _sess.post(url, json=payload, timeout=10).json()
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
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendMessage", p).get("result", {}).get("message_id")


def edit_msg(cid: int, mid: int | None, text: str, kb=None) -> None:
    if not mid:
        return
    p: dict = {
        "chat_id": cid, "message_id": mid, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    tg("editMessageText", p)


def answer_cb(cb_id: str) -> None:
    tg("answerCallbackQuery", {"callback_query_id": cb_id})

# ═══════════════════════════════════════════════════════════════════════════════
#  UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
IST = timezone(timedelta(hours=5, minutes=30))
_e  = html_lib.escape                          
_D  = f"<i>👨‍💻 Dev: {DEV_HANDLE}</i>"          


def now_ist() -> datetime:
    return datetime.now(IST)


def ts_str() -> str:
    return now_ist().strftime("%d-%m-%Y %H:%M:%S IST")


def elapsed_label(secs: float) -> str:
    if secs < 0.5: return "⚡ Blazing fast (<0.5s)"
    if secs < 1.5: return "🚀 Lightning (~1s)"
    if secs < 5:   return f"⚡ Fast ({secs:.1f}s)"
    if secs < 15:  return f"🔥 {secs:.1f}s"
    return f"⏱ {secs:.1f}s"


def sv(v) -> str:
    s = str(v).strip() if v is not None else ""
    return "" if s.lower() == "nan" else s


def q_bar(pos: int, cap: int = 8) -> str:
    filled = min(pos, cap)
    return "🟥" * filled + "⬜" * (cap - filled)


def clean_addr(addr) -> str:
    if not addr:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"!+", ", ", str(addr))).strip(", ")


def fmt_td(secs: int) -> str:
    m, s = divmod(secs, 60)
    return f"{m}m {s}s" if m else f"{s}s"


def cooldown_left(cid: int) -> int:
    with _sl:
        last = user_data.get(cid, {}).get("last_search", 0.0)
    return max(0, int(SEARCH_COOLDOWN - (time.time() - last)))

# ═══════════════════════════════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════════════════════════════
def main_kb():
    return [
        [{"text": "🔍 New Search", "callback_data": "new_search"},
         {"text": "📊 My Stats",   "callback_data": "stats"}],
        [{"text": "ℹ️ Help",       "callback_data": "help"}],
    ]


def back_kb():
    return [
        [{"text": "🔍 Search Again", "callback_data": "new_search"},
         {"text": "ℹ️ Help",         "callback_data": "help"}],
    ]


def result_kb(page: int, total: int):
    nav = []
    if page > 0:
        nav.append({"text": "◀️ Prev", "callback_data": f"pg:{page - 1}"})
    if total > 1:
        nav.append({"text": f"📄 {page + 1}/{total}", "callback_data": "noop"})
    if page < total - 1:
        nav.append({"text": "Next ▶️", "callback_data": f"pg:{page + 1}"})

    rows = []
    if nav:
        rows.append(nav)
    rows.append([{"text": "📄 HTML Report", "callback_data": f"html:{page}"}])
    rows.append([
        {"text": "🔍 New Search", "callback_data": "new_search"},
        {"text": "📊 My Stats",   "callback_data": "stats"},
    ])
    return rows

# ═══════════════════════════════════════════════════════════════════════════════
#  MESSAGE TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════
def msg_welcome_new(name: str, username: str) -> str:
    un = f" (@{_e(username)})" if username else ""
    return (
        f"🎯 <b>Hey {_e(name)}{un}!</b>\n\n"
        f"🕵️ <b>I am KaramChand OSINT</b> — your intel machine.\n"
        f"📱 Send a strict <b>10-digit number</b> to look up records.\n\n"
        f"⏱ 10-minute cooldown between searches.\n"
        f"📋 Max {MAX_QUEUE} users in queue at once.\n\n"
        f"{_D}"
    )


def msg_welcome_back(name: str, username: str, u: dict) -> str:
    un = f" (@{_e(username)})" if username else ""
    with _sl:
        gs = dict(global_stats)
    return (
        f"👋 <b>Welcome back, {_e(name)}{un}!</b>\n\n"
        f"📊 <b>Your Stats</b>\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"🔍 Searches:   <b>{u['searches']}</b>\n"
        f"✅ Results:    <b>{u['results']}</b>\n\n"
        f"🌍 <b>Global Stats</b>\n"
        f"🔢 Searched:   <b>{gs['searches']}</b>\n"
        f"📦 Delivered:  <b>{gs['results']}</b>\n\n"
        f"{_D}"
    )


def msg_stats(name: str, username: str, u: dict) -> str:
    un  = f"@{_e(username)}" if username else "—"
    rem = cooldown_left(u["id"])
    cd  = f"⏳ <b>{fmt_td(rem)}</b> remaining" if rem else "✅ Ready to search!"
    with _sl:
        gs = dict(global_stats)
    return (
        f"📊 <b>Stats — {_e(name)}</b>\n"
        f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"🆔 User ID:    <code>{u['id']}</code>\n"
        f"👤 Username:   {un}\n\n"
        f"🔍 Searches:   <b>{u['searches']}</b>\n"
        f"✅ Results:    <b>{u['results']}</b>\n\n"
        f"🌍 <b>Global</b>\n"
        f"🔢 Total:      <b>{gs['searches']}</b> searches\n"
        f"📦 Delivered:  <b>{gs['results']}</b> results\n\n"
        f"🕒 Cooldown: {cd}\n\n"
        f"{_D}"
    )


def msg_help() -> str:
    return (
        f"ℹ️ <b>How to Use KaramChand</b>\n\n"
        f"📱 Send a <b>10-digit number</b> — nothing else.\n"
        f"❌ No spaces · No +91 prefix · No letters.\n\n"
        f"⏱ <b>10-minute</b> cooldown between searches.\n"
        f"📋 Max <b>{MAX_QUEUE}</b> searches in queue at once.\n\n"
        f"{_D}"
    )


def msg_queue(pos: int, query: str) -> str:
    if pos == 1:
        return (
            f"🎯 <b>You're Next!</b>  {q_bar(1)}\n"
            f"🔎 <code>{query}</code>\n"
            f"⏳ Processing very soon…\n\n"
            f"{_D}"
        )
    eta = pos * 3
    return (
        f"📋 <b>Queue #{pos}</b>  {q_bar(pos)}\n"
        f"🔎 <code>{query}</code>\n"
        f"⏳ Est. wait: ~<b>{eta}s</b>\n\n"
        f"{_D}"
    )


def msg_searching(query: str) -> str:
    return (
        f"⚡ <b>Searching…</b>\n"
        f"🔎 <code>{query}</code>\n\n"
        f"{_D}"
    )


def msg_no_result(query: str) -> str:
    return (
        f"❌ <b>No Records Found</b>\n\n"
        f"🔎 <code>{query}</code>\n"
        f"Nothing matched in the database.\n\n"
        f"{_D}"
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  RESULT CARD (SCHEMA MAPPED)
# ═══════════════════════════════════════════════════════════════════════════════
def format_card(row: dict, query: str, elapsed: float | None = None) -> str:
    name        = sv(row.get("name"))
    fathersName = sv(row.get("fathersName"))
    phoneNumber = sv(row.get("phoneNumber"))
    otherNumber = sv(row.get("otherNumber"))
    aadharNumber= sv(row.get("aadharNumber"))
    address     = clean_addr(row.get("address"))
    district    = sv(row.get("district"))
    pincode     = sv(row.get("pincode"))
    state       = sv(row.get("state"))
    town        = sv(row.get("town"))

    if otherNumber.startswith("91") and len(otherNumber) == 12:
        otherNumber = otherNumber[2:]

    lines = [
        "🕵️ <b>KaramChand Report</b>",
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄",
        f"🔎 <code>{query}</code>",
        "",
    ]
    if name:         lines.append(f"👤 <b>Name</b>       ➡️ {_e(name)}")
    if fathersName:  lines.append(f"👨 <b>Father</b>     ➡️ {_e(fathersName)}")
    if phoneNumber:  lines.append(f"📱 <b>Mobile</b>     ➡️ <code>{phoneNumber}</code>")
    if otherNumber:  lines.append(f"📞 <b>Alt. Mobile</b>➡️ <code>{otherNumber}</code>")
    if aadharNumber: lines.append(f"🪪 <b>Aadhar</b>     ➡️ <code>{_e(aadharNumber)}</code>")
    if address:      lines.append(f"🏠 <b>Address</b>\n<blockquote>{_e(address)}</blockquote>")

    loc = ", ".join(x for x in [town, district, state] if x)
    if loc:          lines.append(f"📍 <b>Town / Dist / State</b> ➡️ {_e(loc)}")
    if pincode:      lines.append(f"📮 <b>Pincode</b>    ➡️ <code>{pincode}</code>")

    lines += ["", "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄", f"🕰 {ts_str()}"]
    if elapsed is not None:
        lines.append(f"⚡ Speed: {elapsed_label(elapsed)}")
    lines.append(_D)
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════════════
#  HTML REPORT (SCHEMA MAPPED)
# ═══════════════════════════════════════════════════════════════════════════════
def _html_field(icon: str, label: str, val: str, mono: bool = False) -> str:
    if not val:
        return ""
    cls = ' class="mono"' if mono else ""
    return (
        f'<div class="field">'
        f'<span class="icon">{icon}</span>'
        f'<div class="content">'
        f'<div class="label">{_e(label)}</div>'
        f'<div class="value"{cls}>{_e(val)}</div>'
        f'</div></div>'
    )


def generate_html(row: dict, query: str) -> str:
    name        = sv(row.get("name"))
    fathersName = sv(row.get("fathersName"))
    phoneNumber = sv(row.get("phoneNumber"))
    otherNumber = sv(row.get("otherNumber"))
    aadharNumber= sv(row.get("aadharNumber"))
    address     = clean_addr(row.get("address"))
    district    = sv(row.get("district"))
    pincode     = sv(row.get("pincode"))
    state       = sv(row.get("state"))
    town        = sv(row.get("town"))

    if otherNumber.startswith("91") and len(otherNumber) == 12:
        otherNumber = otherNumber[2:]

    ts = now_ist().strftime("%d %b %Y, %I:%M %p IST")

    personal = _html_field("👤", "Name", name) + _html_field("👨", "Father's Name", fathersName)
    contact  = (
        _html_field("📱", "Mobile", phoneNumber, True) +
        _html_field("📞", "Alt. Mobile", otherNumber, True) +
        _html_field("🏠", "Address", address) +
        _html_field("📍", "Town / District / State", f"{town}, {district}, {state}".strip(", ")) +
        _html_field("📮", "Pincode", pincode, True)
    )
    ids_card = (
        f'<div class="card">'
        f'<div class="ch"><span class="ci">🪪</span><span class="ct">Identity</span></div>'
        f'{_html_field("🪪", "Aadhar Number", aadharNumber, True)}'
        f'</div>'
    ) if aadharNumber else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>KaramChand Report — {_e(query)}</title>
<style>
:root{{--bg:#0d1117;--sur:#161b22;--sur2:#1c2128;--bdr:#30363d;--txt:#e6edf3;--mut:#8b949e;--acc:#388bfd;--grn:#3fb950}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--txt);font-family:-apple-system,'Segoe UI',Roboto,sans-serif;padding:20px 16px 40px;min-height:100vh}}
.wrap{{max-width:520px;margin:0 auto}}
.hdr{{text-align:center;padding:26px 0 12px}}
.hdr h1{{font-size:22px;font-weight:700;margin-top:6px;letter-spacing:-.4px}}
.sub{{color:var(--mut);font-size:13px;margin-top:3px}}
.pill{{display:inline-block;background:var(--sur2);border:1px solid var(--bdr);border-radius:20px;padding:4px 14px;font-family:monospace;font-size:13px;color:var(--acc);margin-top:10px}}
.ts{{color:var(--mut);font-size:11px;margin-top:5px}}
.card{{background:var(--sur);border:1px solid var(--bdr);border-radius:12px;overflow:hidden;margin:12px 0}}
.ch{{background:var(--sur2);padding:9px 14px;border-bottom:1px solid var(--bdr);display:flex;align-items:center;gap:8px}}
.ci{{font-size:15px}}.ct{{font-weight:600;font-size:13px}}
.field{{padding:9px 14px;border-bottom:1px solid var(--bdr);display:flex;gap:9px;align-items:flex-start}}
.field:last-child{{border-bottom:none}}
.field .icon{{font-size:13px;width:17px;flex-shrink:0;padding-top:2px}}
.content{{flex:1}}
.label{{font-size:10px;color:var(--mut);text-transform:uppercase;letter-spacing:.6px;margin-bottom:2px}}
.value{{font-size:14px;word-break:break-all;line-height:1.4}}
.value.mono{{font-family:monospace;font-size:15px;color:var(--grn);font-weight:600}}
.ftr{{text-align:center;margin-top:22px;padding-top:14px;border-top:1px solid var(--bdr)}}
.brand{{display:inline-flex;align-items:center;gap:7px;background:var(--sur);border:1px solid var(--bdr);border-radius:20px;padding:6px 14px;text-decoration:none}}
.bn{{font-size:13px;font-weight:600;color:var(--acc)}}
.links{{margin-top:10px;display:flex;gap:12px;justify-content:center;flex-wrap:wrap}}
.links a{{color:var(--acc);text-decoration:none;font-size:12px}}
.disc{{color:var(--mut);font-size:10px;margin-top:12px;line-height:1.5}}
</style>
</head>
<body><div class="wrap">
<div class="hdr">
  <div style="font-size:34px">🕵️</div>
  <h1>KaramChand</h1>
  <div class="sub">Intelligence Report</div>
  <div class="pill">🔎 {_e(query)}</div>
  <div class="ts">Generated {ts}</div>
</div>
<div class="card">
  <div class="ch"><span class="ci">👤</span><span class="ct">Personal</span></div>
  {personal}
</div>
<div class="card">
  <div class="ch"><span class="ci">📱</span><span class="ct">Contact &amp; Location</span></div>
  {contact}
</div>
{ids_card}
<div class="ftr">
  <a href="{BOT_LINK}" class="brand" target="_blank">
    <span style="font-size:17px">🤖</span>
    <span class="bn">KaramChand Bot</span>
  </a>
  <div class="links">
    <a href="{DEV_LINK}" target="_blank">👨‍💻 {DEV_HANDLE}</a>
  </div>
  <div class="disc">Generated by KaramChand OSINT · Personal use only · Do not misuse</div>
</div>
</div></body></html>"""


def send_html_doc(cid: int, row: dict, query: str) -> None:
    try:
        content  = generate_html(row, query).encode("utf-8")
        filename = f"KC_{sv(row.get('phoneNumber')) or 'report'}.html"
        caption  = f"📄 <b>HTML Report</b>\n🔎 <code>{query}</code>\n\n{_D}"
        _sess.post(
            f"{BASE_URL}/sendDocument",
            data    = {"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
            files   = {"document": (filename, io.BytesIO(content), "text/html")},
            timeout = 25,
        )
        log.info("📄 HTML report → cid=%s", cid)
    except Exception as exc:
        log.error("send_html_doc cid=%s: %s", cid, exc)
    finally:
        try:    del content
        except: pass

# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER THREAD
# ═══════════════════════════════════════════════════════════════════════════════
def worker() -> None:
    log.info("🔧 Worker thread started")
    while True:
        cid, query, mid = _search_q.get()
        t0 = time.monotonic()
        try:
            tg("sendChatAction", {"chat_id": cid, "action": "typing"})

            with _ql:
                _queue_order[:] = [
                    x for x in _queue_order
                    if not (x["chat_id"] == cid and x["msg_id"] == mid)
                ]
                snapshot = list(_queue_order)   

            for i, item in enumerate(snapshot, start=1):
                try:
                    edit_msg(item["chat_id"], item["msg_id"],
                             msg_queue(i, item["query"]))
                except Exception:
                    pass

            edit_msg(cid, mid, msg_searching(query))

            rows    = db_search(query, use_worker=True)
            elapsed = time.monotonic() - t0

            with _sl:
                global_stats["searches"] += 1
                user_data[cid]["searches"] += 1

            if not rows:
                edit_msg(cid, mid, msg_no_result(query), kb=back_kb())
                log.info("❌ No result: %s (%.2fs)", query, elapsed)
            else:
                with _sl:
                    global_stats["results"] += len(rows)
                    user_data[cid]["results"] += len(rows)
                    user_sessions[cid] = {
                        "results": rows,
                        "page":    0,
                        "query":   query,
                        "ts":      time.time(),
                    }
                text = format_card(rows[0], query, elapsed)
                edit_msg(cid, mid, text, kb=result_kb(0, len(rows)))
                log.info("✅ Result: %s → %d row(s) (%.2fs)", query, len(rows), elapsed)

        except Exception:
            log.exception("Worker error: cid=%s query=%s", cid, query)
            try:
                edit_msg(cid, mid, "⚠️ <b>Server error.</b> Please try again shortly.")
            except Exception:
                pass
        finally:
            with _sl:
                active_users.discard(cid)
            _search_q.task_done()

# ═══════════════════════════════════════════════════════════════════════════════
#  QUEUE BROADCASTER
# ═══════════════════════════════════════════════════════════════════════════════
def _broadcaster() -> None:
    log.info("📡 Queue broadcaster started")
    while True:
        time.sleep(QUEUE_BROADCAST_IV)
        with _ql:
            snapshot = list(_queue_order)
        for i, item in enumerate(snapshot, start=1):
            try:
                edit_msg(item["chat_id"], item["msg_id"],
                         msg_queue(i, item["query"]))
            except Exception:
                pass

# ═══════════════════════════════════════════════════════════════════════════════
#  SESSION GARBAGE COLLECTOR
# ═══════════════════════════════════════════════════════════════════════════════
def _session_gc() -> None:
    log.info("🗑️  Session GC started")
    while True:
        time.sleep(GC_INTERVAL)
        cutoff = time.time() - SESSION_TTL
        with _sl:
            stale = [cid for cid, s in user_sessions.items() if s["ts"] < cutoff]
            for cid in stale:
                del user_sessions[cid]
        if stale:
            log.info("GC: removed %d stale sessions", len(stale))

# ═══════════════════════════════════════════════════════════════════════════════
#  ADMIN UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
def _is_admin(cid: int) -> bool:
    return cid == ADMIN_ID


def _upload_file(cid: int, data: bytes, filename: str,
                 mime: str, caption: str) -> None:
    try:
        _sess.post(
            f"{BASE_URL}/sendDocument",
            data  = {"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
            files = {"document": (filename, io.BytesIO(data), mime)},
            timeout = 25,
        )
    except Exception as exc:
        log.error("_upload_file: %s", exc)


def admin_send_data(cid: int) -> None:
    with _sl:
        export = {
            "timestamp":    ts_str(),
            "global_stats": dict(global_stats),
            "users": {
                str(k): {f: v for f, v in u.items()
                          if f not in ("last_search", "last_msg")}
                for k, u in user_data.items()
            },
        }
    data = json.dumps(export, ensure_ascii=False, indent=2).encode()
    threading.Thread(
        target=_upload_file,
        args=(cid, data, "bot_data.json", "application/json",
              f"📦 <b>Data Export</b> · {ts_str()}"),
        daemon=True,
    ).start()


def admin_send_logs(cid: int) -> None:
    data = "\n".join(_ring.tail(150)).encode()
    threading.Thread(
        target=_upload_file,
        args=(cid, data, "bot_logs.txt", "text/plain",
              f"📋 <b>Logs</b> (last 150 lines) · {ts_str()}"),
        daemon=True,
    ).start()


def admin_send_queue(cid: int) -> None:
    with _ql:
        qsnap = list(_queue_order)
    with _sl:
        active_count = len(active_users)
    processing = active_count - len(qsnap)
    lines = [
        "⚙️ <b>Queue Status</b>",
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄",
        f"🔧 Processing now: <b>{max(processing, 0)}</b>",
        f"📋 Waiting:        <b>{len(qsnap)}</b>",
        f"👥 Active users:   <b>{active_count}</b>",
        f"📊 Q depth:        <b>{_search_q.qsize()}</b>",
        "",
    ]
    for i, item in enumerate(qsnap, 1):
        lines.append(f"  #{i} · {item['chat_id']} · <code>{item['query']}</code>")
    send_msg(cid, "\n".join(lines))

# ═══════════════════════════════════════════════════════════════════════════════
#  CALLBACK HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
def handle_cb(cb: dict) -> None:
    cid  = cb["message"]["chat"]["id"]
    mid  = cb["message"]["message_id"]
    data = cb["data"]
    answer_cb(cb["id"])

    if data == "new_search":
        with _sl:
            is_active = cid in active_users
        if is_active:
            with _ql:
                pos = _q_pos(cid)
            notice = (f"⚙️ You're <b>#{pos}</b> in queue." if pos
                      else "⚙️ Your search is already running.")
            edit_msg(cid, mid, f"{notice}\n\nWait for your result. 🙏")
        else:
            edit_msg(
                cid, mid,
                f"🔍 <b>New Search</b>\n\n"
                f"Send a strict <b>10-digit number</b>.\n\n{_D}",
                kb=back_kb(),
            )
        return

    if data == "stats":
        with _sl:
            u = _get_ud(cid)
            name, un = u["name"], u["username"]
        edit_msg(cid, mid, msg_stats(name, un, u), kb=back_kb())
        return

    if data == "help":
        edit_msg(cid, mid, msg_help(), kb=back_kb())
        return

    if data == "noop":
        return

    if data.startswith("pg:"):
        page = int(data.split(":")[1])
        with _sl:
            sess = user_sessions.get(cid)
        if sess and 0 <= page < len(sess["results"]):
            sess["ts"] = time.time()          
            text = format_card(sess["results"][page], sess["query"])
            edit_msg(cid, mid, text, kb=result_kb(page, len(sess["results"])))
        return

    if data.startswith("html:"):
        page = int(data.split(":")[1])
        with _sl:
            sess = user_sessions.get(cid)
        if sess and 0 <= page < len(sess["results"]):
            threading.Thread(
                target=send_html_doc,
                args=(cid, sess["results"][page], sess["query"]),
                daemon=True,
            ).start()
        return

# ═══════════════════════════════════════════════════════════════════════════════
#  MESSAGE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
def handle_msg(msg: dict) -> None:
    cid   = msg["chat"]["id"]
    raw   = msg.get("text", "").strip()
    from_ = msg.get("from", {})
    name  = from_.get("first_name", "User")
    un    = from_.get("username", "")

    with _sl:
        is_new = cid not in user_data
        u = _get_ud(cid, name=name, username=un)

    if _is_admin(cid):
        if raw == "/data":
            admin_send_data(cid)
            send_msg(cid, "📦 Exporting data…")
            return
        if raw == "/logs":
            admin_send_logs(cid)
            send_msg(cid, "📋 Sending logs…")
            return
        if raw == "/queue":
            admin_send_queue(cid)
            return

    if raw == "/start":
        if is_new:
            send_msg(cid, msg_welcome_new(name, un), kb=main_kb())
        else:
            send_msg(cid, msg_welcome_back(name, un, u), kb=main_kb())
        return

    if raw == "/stats":
        with _sl:
            u = _get_ud(cid)
        send_msg(cid, msg_stats(name, un, u), kb=main_kb())
        return

    if raw == "/help":
        send_msg(cid, msg_help(), kb=main_kb())
        return

    if raw.startswith("/"):
        return

    if "forward_origin" in msg or "forward_from" in msg:
        send_msg(cid, "❌ Forwarded messages not allowed. Type directly.")
        return

    with _sl:
        is_active = cid in active_users
    if is_active:
        with _ql:
            pos = _q_pos(cid)
        if pos:
            send_msg(cid,
                     f"⚙️ You're <b>#{pos}</b> in queue.\n"
                     f"Please wait — your result is coming!")
        else:
            send_msg(cid, "⚡ Your search is <b>already running</b>. Result coming soon!")
        return

    if not (raw.isdigit() and len(raw) == 10):
        send_msg(
            cid,
            "❌ <b>Invalid Input</b>\n\n"
            "Send a <b>strict 10-digit number</b>.\n"
            "No spaces · No +91 prefix · No letters.",
            kb=back_kb(),
        )
        return

    now = time.time()
    with _sl:
        if now - u["last_msg"] < MSG_COOLDOWN:
            return                               
        u["last_msg"] = now

    rem = cooldown_left(cid)
    if rem:
        send_msg(
            cid,
            f"⏳ <b>Cooldown Active</b>\n\n"
            f"Next search available in <b>{fmt_td(rem)}</b>.\n"
            f"<i>10-minute limit per search.</i>",
        )
        return

    if _search_q.qsize() >= MAX_QUEUE:
        send_msg(cid, "😤 <b>Server at capacity.</b> Please try again in a minute.")
        return

    with _sl:
        active_users.add(cid)
        user_data[cid]["last_search"] = time.time()

    with _ql:
        approx_pos = len(_queue_order) + 1

    mid = send_msg(cid, msg_queue(approx_pos, raw))
    if mid is None:
        with _sl:
            active_users.discard(cid)
            user_data[cid]["last_search"] = 0.0
        log.error("send_msg failed for cid=%s — rolled back enqueue", cid)
        return

    with _ql:
        _queue_order.append({"chat_id": cid, "msg_id": mid, "query": raw})

    _search_q.put((cid, raw, mid))
    log.info("📥 Enqueued cid=%s query=%s pos≈%d", cid, raw, approx_pos)

# ═══════════════════════════════════════════════════════════════════════════════
#  LONG POLLING
# ═══════════════════════════════════════════════════════════════════════════════
def poll_telegram() -> None:
    log.info("🚀 Long polling started")
    tg("deleteWebhook", {})
    offset:  int | None = None
    backoff: int        = 1

    while True:
        try:
            url  = (
                f"{BASE_URL}/getUpdates?timeout=30"
                + (f"&offset={offset}" if offset else "")
            )
            resp = _sess.get(url, timeout=35).json()

            if not resp.get("ok"):
                log.warning("getUpdates !ok: %s", resp.get("description"))
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            backoff = 1   

            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd:
                        handle_cb(upd["callback_query"])
                    elif "message" in upd and "text" in upd["message"]:
                        handle_msg(upd["message"])
                except Exception:
                    log.exception("Update handler crash (update_id=%s)", upd.get("update_id"))

        except requests.RequestException as exc:
            log.error("Polling network error: %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception:
            log.exception("Unexpected polling crash")
            time.sleep(3)

# ═══════════════════════════════════════════════════════════════════════════════
#  FASTAPI ROUTES
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/")
def health():
    with _sl:
        users = len(user_data)
        gs    = dict(global_stats)
    return {
        "status":       "alive",
        "queue_depth":  _search_q.qsize(),
        "active_users": users,
        "global_stats": gs,
    }


@app.get("/search")
def api_search(q: str = FQ(...)):
    q = q.strip()
    if not (q.isdigit() and len(q) == 10):
        return JSONResponse(
            status_code=400,
            content={"error": "Strictly 10 digits required."},
        )
    try:
        rows = db_search(q, use_worker=False)
        return {"count": len(rows), "results": rows}
    except Exception as exc:
        log.error("/search API error for q=%s: %s", q, exc)
        return JSONResponse(status_code=500, content={"error": "Internal DB error."})

# ═══════════════════════════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
for _fn, _name in [
    (worker,        "worker"),
    (poll_telegram, "poller"),
    (_broadcaster,  "broadcaster"),
    (_session_gc,   "session-gc"),
]:
    _t = threading.Thread(target=_fn, name=_name, daemon=True)
    _t.start()
    log.info("🧵 Thread launched: %s", _name)
