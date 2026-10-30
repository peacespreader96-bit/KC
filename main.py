import os
import time
import queue
import threading
import requests
import duckdb
import re
import io
import html as html_lib
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from collections import OrderedDict

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TOKEN        = "8723126732:AAHVFOySgFaWN4_4InqtKrvkgN15crbPi0w"
ADMIN_ID     = 5505679330
MAX_QUEUE    = 15
HOURLY_LIMIT = 20
COOLDOWN_SEC = 3
BOT_LINK     = "https://t.me/Karam_Chand_bot"
DEV_LINK     = "https://t.me/ansafrozkhan"
BASE_URL     = f"https://api.telegram.org/bot{TOKEN}"

# 🔴 PUT YOUR HF READ TOKEN HERE (To bypass Cloudflare 403 blocks)
HF_TOKEN     = "hf_YOUR_READ_TOKEN_HERE" 

app = FastAPI()

# ─── 1. DUCKDB DATABASE SETUP ─────────────────────────────────────────────────
print("⏳ Initializing DuckDB for Render...")
db = duckdb.connect(config={
    'custom_user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
})

# Render memory limits (512MB RAM)
db.execute("SET memory_limit='256MB';")
db.execute("SET threads=1;")
db.execute("INSTALL httpfs; LOAD httpfs;")

if HF_TOKEN != "hf_YOUR_READ_TOKEN_HERE":
    db.execute(f"CREATE SECRET (TYPE HUGGINGFACE, TOKEN '{HF_TOKEN}');")

BASE_HF = "hf://buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/"
phone_urls = [f"{BASE_HF}idx_phone.{i}.parquet" for i in range(7)]
phone_urls_str = str(phone_urls).replace("'", '"')

try:
    db.execute(f"CREATE VIEW people_phone AS SELECT * FROM read_parquet({phone_urls_str})")
    print("✅ Database View Created!")
except Exception as e:
    print(f"❌ DB Error: {e}")

def fetch_dicts(cursor):
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

# ─── 2. STATE & TRANSLATIONS ──────────────────────────────────────────────────
user_data     = {}
active_users  = set()
user_sessions = {}
global_stats  = {"searches": 0, "results": 0}
search_queue  = queue.Queue()
queue_lock    = threading.Lock()
queue_order   = []
search_cache  = OrderedDict() 
CACHE_SIZE    = 500

T = {
    "en": {
        "lang_pick": "🌐 <b>Select your language to continue:</b>",
        "welcome_new": "😎 <b>I am KaramChand.</b>\n\nI search mobile numbers.\n\n📱 <b>Strictly 10-digit numbers only.</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "welcome_back": "👋 Welcome back, <b>{name}</b>!\n\n📊 <b>Your Stats</b>\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Searched: <b>{gs}</b>\n📦 Delivered: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "stats": "📊 <b>Your Stats — {name}</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Total searched: <b>{gs}</b>\n📦 Total delivered: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "help": "ℹ️ <b>How to Use KaramChand</b>\n\n📱 Send a <b>10-digit mobile number</b>.\n\n❌ No spaces, no +91, no letters.\n⏱ 3s cooldown\n📋 Max <b>{lim}</b> searches/hour\n\n<i>Dev: @ansafrozkhan</i>",
        "q1": "⚡ <b>Searching...</b>\n🎯 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "qn": "📋 <b>Queue #{p}</b>  {bar}\n🎯 <code>{q}</code>\n⏳ Est. ~{eta}s\n\n<i>Dev: @ansafrozkhan</i>",
        "no_result": "❌ <b>No Records Found</b>\n\n<code>{q}</code> — nothing in the database.\n\n<i>Dev: @ansafrozkhan</i>",
        "new_search": "🔍 <b>New Search</b>\n\nType a strict 10-digit mobile number.\n\n<i>Dev: @ansafrozkhan</i>",
        "report_cap": "📄 <b>KaramChand Report</b>\n🔎 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "e_digits": "❌ <b>Invalid Input</b>\n\nPlease send a strict <b>10-digit mobile number</b> only.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_cool": "⏳ Wait 3 seconds before searching again.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_active": "🔄 Your search is already running. Wait for the result.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_fwd": "❌ Forwarded messages not allowed. Type directly.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_full": "😤 Server busy. Try in a minute.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_limit": f"🚫 Hourly limit ({HOURLY_LIMIT}/hr). Come back later.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_server": "⚠️ Server crashed. Try again shortly.\n\n<i>Dev: @ansafrozkhan</i>",
        "lbl_name": "Name", "lbl_father": "Father", "lbl_mobile": "Mobile", "lbl_alt": "Alt. Mobile", "lbl_address": "Address", "lbl_aadhar": "Aadhar", "lbl_state": "State", "lbl_district": "District", "lbl_town": "Town", "lbl_pincode": "Pincode",
        "b_search": "🔍 New Search", "b_help": "ℹ️ Help", "b_back": "🔙 Back", "b_stats": "📊 My Stats", "b_prev": "◀️ Prev", "b_next": "Next ▶️", "b_lang": "🌐 Language", "b_report": "📄 HTML Report",
    }
}

# ─── 3. HELPERS & REGEX (From Kc.py) ──────────────────────────────────────────
def clean_address(addr):
    """Fixes the ! delimiter issue in the dataset."""
    if not addr: return ""
    clean = re.sub(r'!+', ', ', str(addr))
    clean = re.sub(r'\s+', ' ', clean).strip(', ')
    return clean

def sv(val):
    return str(val).strip() if val is not None and str(val).strip().lower() != 'nan' else ""

def tx(chat_id, key, **kwargs):
    lang = user_data.get(chat_id, {}).get("lang") or "en"
    tmpl = T.get(lang, T["en"]).get(key) or T["en"].get(key, key)
    return tmpl.format(**kwargs) if kwargs else tmpl

def ud(chat_id, name=None):
    if chat_id not in user_data:
        user_data[chat_id] = {"lang": None, "name": name or "User", "searches": 0, "results": 0, "hourly": [], "last": 0.0}
    elif name:
        user_data[chat_id]["name"] = name
    return user_data[chat_id]

def api_call(method, payload):
    try:
        return requests.post(f"{BASE_URL}/{method}", json=payload, timeout=10).json()
    except Exception:
        return {}

def send_msg(chat_id, text, kb=None):
    p = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb: p["reply_markup"] = {"inline_keyboard": kb}
    return api_call("sendMessage", p).get("result", {}).get("message_id")

def edit_msg(chat_id, msg_id, text, kb=None):
    if not msg_id: return
    p = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb: p["reply_markup"] = {"inline_keyboard": kb}
    api_call("editMessageText", p)

def queue_bar(pos, cap=5):
    return "🟥" * min(pos, cap) + "⬜" * (cap - min(pos, cap))

def push_queue_updates():
    with queue_lock: snapshot = list(queue_order)
    for i, item in enumerate(snapshot):
        cid, mid, q = item["chat_id"], item["msg_id"], item["query"]
        pos = i + 1
        text = tx(cid, "q1", q=q) if pos == 1 else tx(cid, "qn", p=pos, bar=queue_bar(pos), q=q, eta=pos*2)
        try:
            api_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
        except Exception: pass

# ─── 4. FORMATTING & HTML REPORT (From Kc.py adapted for dict) ────────────────
def format_card(chat_id, row, query):
    name = sv(row.get("name")); fname = sv(row.get("fathersName"))
    mobile = sv(row.get("phoneNumber")); alt = sv(row.get("otherNumber"))
    aadhar = sv(row.get("aadharNumber"))
    
    # 🟢 APPLY THE CLEANER HERE
    address = clean_address(row.get("address"))
    
    district = sv(row.get("district")); pincode = sv(row.get("pincode"))
    state = sv(row.get("state")); town = sv(row.get("town"))

    # Clean Alt Number: Remove 91 if 12 digits
    if alt.startswith("91") and len(alt) == 12:
        alt = alt[2:]

    ist = timezone(timedelta(hours=5, minutes=30))
    ts = datetime.now(ist).strftime("%d-%m-%Y %H:%M:%S")

    lines = [
        "🕵️ <b>KaramChand Report</b>",
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄",
        f"🔎 <code>{query}</code>",
        ""
    ]
    
    if name: lines.append(f"👤 <b>{tx(chat_id, 'lbl_name')}</b> ➡️ {html_lib.escape(name)}")
    if fname: lines.append(f"👨 <b>{tx(chat_id, 'lbl_father')}</b> ➡️ {html_lib.escape(fname)}")
    if mobile: lines.append(f"📱 <b>{tx(chat_id, 'lbl_mobile')}</b> ➡️ <code>{mobile}</code>")
    if alt: lines.append(f"📞 <b>{tx(chat_id, 'lbl_alt')}</b> ➡️ <code>{alt}</code>")
    if aadhar: lines.append(f"🪪 <b>{tx(chat_id, 'lbl_aadhar')}</b> ➡️ <code>{aadhar}</code>")
    
    if address:
        lines.append(f"🏠 <b>{tx(chat_id, 'lbl_address')}</b>")
        lines.append(f"<blockquote>{html_lib.escape(address)}</blockquote>")
        
    loc = [x for x in [town, district, state] if x]
    if loc:
        lines.append(f"📍 <b>{tx(chat_id, 'lbl_town')} / {tx(chat_id, 'lbl_district')} / {tx(chat_id, 'lbl_state')}</b> ➡️ {html_lib.escape(', '.join(loc))}")
    if pincode: 
        lines.append(f"📮 <b>{tx(chat_id, 'lbl_pincode')}</b> ➡️ <code>{pincode}</code>")

    lines += [
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄",
        f"⏱ <b>Generated:</b> {ts}",
        "<i>Dev: @ansafrozkhan</i>",
    ]
    return "\n".join(lines)

def generate_html_report(row, query):
    name = sv(row.get("name")); fname = sv(row.get("fathersName"))
    mobile = sv(row.get("phoneNumber")); alt = sv(row.get("otherNumber"))
    aadhar = sv(row.get("aadharNumber")); address = clean_address(row.get("address"))
    district = sv(row.get("district")); pincode = sv(row.get("pincode"))
    state = sv(row.get("state")); town = sv(row.get("town"))

    if alt.startswith("91") and len(alt) == 12: alt = alt[2:]
    e = html_lib.escape

    def field(icon, label, value, mono=False):
        if not value: return ""
        val_class = "mono" if mono else ""
        return f'<div class="field"><span class="icon">{icon}</span><div class="content"><div class="label">{e(label)}</div><div class="value {val_class}">{e(value)}</div></div></div>'

    personal_block = field("👤", "Name", name) + field("👨", "Father's Name", fname)
    contact_block = field("📱", "Mobile", mobile, mono=True) + field("📞", "Alt. Mobile", alt, mono=True) + field("🏠", "Address", address) + field("📍", "District / State", f"{district}, {state}") + field("📮", "Pincode", pincode, mono=True) + field("🏙️", "Town", town)
    
    ids_block = ""
    if aadhar:
        ids_block = f'<div class="card"><div class="card-header"><span class="icon">🪪</span><span class="title">Identity Documents</span></div>{field("🪪", "Aadhar Number", aadhar, mono=True)}</div>'

    ist = timezone(timedelta(hours=5, minutes=30))
    ts = datetime.now(ist).strftime("%d %b %Y, %I:%M %p IST")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>KaramChand Report — {e(query)}</title>
  <style>
    :root {{ --bg: #0d1117; --surface: #161b22; --surface2: #1c2128; --border: #30363d; --text: #e6edf3; --muted: #8b949e; --accent: #388bfd; --green: #3fb950; }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: var(--bg); color: var(--text); font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; min-height: 100vh; padding: 20px 16px 40px; }}
    .container {{ max-width: 520px; margin: 0 auto; }}
    .header {{ text-align: center; padding: 32px 0 20px; }}
    .header .logo {{ font-size: 40px; }}
    .header h1 {{ font-size: 26px; font-weight: 700; margin-top: 8px; letter-spacing: -0.5px; }}
    .header .sub {{ color: var(--muted); font-size: 13px; margin-top: 4px; }}
    .query-pill {{ display: inline-block; background: var(--surface2); border: 1px solid var(--border); border-radius: 20px; padding: 5px 14px; font-family: 'Courier New', monospace; font-size: 13px; color: var(--accent); margin-top: 14px; }}
    .timestamp {{ color: var(--muted); font-size: 11px; margin-top: 6px; }}
    .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; margin: 16px 0; }}
    .card-header {{ background: var(--surface2); padding: 12px 18px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 10px; }}
    .card-header .icon {{ font-size: 18px; }}
    .card-header .title {{ font-weight: 600; font-size: 14px; color: var(--text); }}
    .field {{ padding: 12px 18px; border-bottom: 1px solid var(--border); display: flex; gap: 12px; align-items: flex-start; }}
    .field:last-child {{ border-bottom: none; }}
    .field .icon {{ font-size: 15px; width: 20px; flex-shrink: 0; padding-top: 3px; }}
    .field .content {{ flex: 1; }}
    .field .label {{ font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 3px; font-weight: 500; }}
    .field .value {{ font-size: 14px; color: var(--text); word-break: break-all; line-height: 1.4; }}
    .field .value.mono {{ font-family: 'Courier New', monospace; font-size: 15px; color: var(--green); font-weight: 600; }}
    .footer {{ text-align: center; margin-top: 28px; padding-top: 20px; border-top: 1px solid var(--border); }}
    .branding {{ display: inline-flex; align-items: center; gap: 8px; background: var(--surface); border: 1px solid var(--border); border-radius: 24px; padding: 8px 18px; text-decoration: none; }}
    .branding .bot-icon {{ font-size: 20px; }}
    .branding .bot-name {{ font-size: 14px; font-weight: 600; color: var(--accent); }}
    .links {{ margin-top: 14px; display: flex; gap: 16px; justify-content: center; flex-wrap: wrap; }}
    .links a {{ color: var(--accent); text-decoration: none; font-size: 13px; }}
    .links a:hover {{ text-decoration: underline; }}
    .disclaimer {{ color: var(--muted); font-size: 11px; margin-top: 16px; line-height: 1.5; }}
  </style>
</head>
<body>
<div class="container">
  <div class="header">
    <div class="logo">🕵️</div>
    <h1>KaramChand</h1>
    <div class="sub">Intelligence Report</div>
    <div class="query-pill">🔎 {e(query)}</div>
    <div class="timestamp">Generated on {ts}</div>
  </div>
  <div class="card"><div class="card-header"><span class="icon">👤</span><span class="title">Personal Information</span></div>{personal_block}</div>
  <div class="card"><div class="card-header"><span class="icon">📱</span><span class="title">Contact Details</span></div>{contact_block}</div>
  {ids_block}
  <div class="footer">
    <a href="{BOT_LINK}" class="branding" target="_blank"><span class="bot-icon">🤖</span><span class="bot-name">@Karam_Chand_bot</span></a>
    <div class="links">
        <a href="{DEV_LINK}" target="_blank">👨‍💻 Dev: @ansafrozkhan</a>
    </div>
    <div class="disclaimer">This report was generated by KaramChand Bot.<br>For personal use only. Do not misuse.</div>
  </div>
</div>
</body>
</html>"""

def send_html_doc(chat_id, row, query, page, total):
    html_bytes = generate_html_report(row, query).encode("utf-8")
    filename   = f"KC_{sv(row.get('phoneNumber')) or 'report'}.html"
    caption    = tx(chat_id, "report_cap", q=query)
    try:
        requests.post(f"{BASE_URL}/sendDocument", data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}, files={"document": (filename, io.BytesIO(html_bytes), "text/html")}, timeout=15)
    except Exception as e:
        print(f"[DOC ERR] {e}")

# ─── 5. KEYBOARDS & UI ────────────────────────────────────────────────────────
def lang_kb(): return [[{"text": "🇬🇧 English", "callback_data": "lang:en"}]]
def main_kb(chat_id): return [[{"text": tx(chat_id, "b_search"), "callback_data": "new_search"},{"text": tx(chat_id, "b_stats"), "callback_data": "stats"}],[{"text": tx(chat_id, "b_help"), "callback_data": "help"}]]
def err_kb(chat_id): return [[{"text": tx(chat_id, "b_help"), "callback_data": "help"},{"text": tx(chat_id, "b_search"), "callback_data": "new_search"}]]

def result_kb(chat_id, page, total):
    nav = []
    if page > 0: nav.append({"text": tx(chat_id, "b_prev"), "callback_data": f"pg:{page-1}"})
    if total > 1: nav.append({"text": f"📄 {page+1}/{total}", "callback_data": "noop"})
    if page < total - 1: nav.append({"text": tx(chat_id, "b_next"), "callback_data": f"pg:{page+1}"})
    
    keyboard = []
    if nav: keyboard.append(nav)
    keyboard.append([{"text": tx(chat_id, "b_report"), "callback_data": f"html:{page}"}])
    keyboard.append([{"text": tx(chat_id, "b_search"), "callback_data": "new_search"},{"text": tx(chat_id, "b_stats"), "callback_data": "stats"}])
    return keyboard

# ─── 6. THE BOT WORKER ────────────────────────────────────────────────────────
def worker():
    while True:
        chat_id, query, msg_id = search_queue.get()
        try:
            api_call("sendChatAction", {"chat_id": chat_id, "action": "typing"})
            
            with queue_lock:
                for i, item in enumerate(queue_order):
                    if item["chat_id"] == chat_id and item["msg_id"] == msg_id:
                        queue_order.pop(i); break
            push_queue_updates()

            global_stats["searches"] += 1
            ud(chat_id)["searches"] += 1

            if query in search_cache:
                rows = search_cache[query]
                search_cache.move_to_end(query)
            else:
                # DUCKDB SEARCH (Strictly Phone Number only)
                q = "SELECT * FROM people_phone WHERE phoneNumber = ? LIMIT 5"
                results = fetch_dicts(db.execute(q, [query]))
                
                # Clean NaN
                rows = [{k: (None if str(v) == 'nan' else v) for k, v in r.items()} for r in results]
                search_cache[query] = rows
                if len(search_cache) > CACHE_SIZE: search_cache.popitem(last=False)

            if not rows:
                edit_msg(chat_id, msg_id, tx(chat_id, "no_result", q=query), kb=err_kb(chat_id))
            else:
                cnt = len(rows)
                global_stats["results"] += cnt
                ud(chat_id)["results"] += cnt
                user_sessions[chat_id] = {"results": rows, "page": 0, "query": query}
                text = format_card(chat_id, rows[0], query)
                edit_msg(chat_id, msg_id, text, kb=result_kb(chat_id, 0, cnt))

        except Exception as e:
            print(f"[WORKER ERR] {e}")
            edit_msg(chat_id, msg_id, tx(chat_id, "e_server"))
        finally:
            active_users.discard(chat_id)
            search_queue.task_done()

# ─── 7. TELEGRAM LONG POLLING LOOP ────────────────────────────────────────────
def poll_telegram():
    print("🚀 Starting Telegram Long Polling...")
    requests.get(f"{BASE_URL}/deleteWebhook") 
    offset = None
    
    while True:
        try:
            res = requests.get(f"{BASE_URL}/getUpdates?timeout=30" + (f"&offset={offset}" if offset else ""), timeout=35).json()
            
            for update in res.get("result", []):
                offset = update["update_id"] + 1
                
                if "callback_query" in update:
                    cb = update["callback_query"]
                    chat_id = cb["message"]["chat"]["id"]
                    msg_id = cb["message"]["message_id"]
                    data = cb["data"]
                    api_call("answerCallbackQuery", {"callback_query_id": cb["id"]})
                    
                    if data == "new_search": edit_msg(chat_id, msg_id, tx(chat_id, "new_search"), kb=err_kb(chat_id))
                    elif data == "stats":
                        u = ud(chat_id)
                        edit_msg(chat_id, msg_id, tx(chat_id, "stats", name=u["name"], s=u["searches"], r=u["results"], gs=global_stats["searches"], gr=global_stats["results"]), kb=err_kb(chat_id))
                    elif data == "help": edit_msg(chat_id, msg_id, tx(chat_id, "help", lim=HOURLY_LIMIT), kb=err_kb(chat_id))
                    elif data.startswith("pg:"):
                        page = int(data.split(":")[1])
                        session = user_sessions.get(chat_id)
                        if session:
                            session["page"] = page
                            text = format_card(chat_id, session["results"][page], session["query"])
                            edit_msg(chat_id, msg_id, text, kb=result_kb(chat_id, page, len(session["results"])))
                    elif data.startswith("html:"):
                        page = int(data.split(":")[1])
                        session = user_sessions.get(chat_id)
                        if session and 0 <= page < len(session["results"]):
                            threading.Thread(target=send_html_doc, args=(chat_id, session["results"][page], session["query"], page, len(session["results"])), daemon=True).start()
                    continue

                if "message" in update and "text" in update["message"]:
                    msg = update["message"]
                    chat_id = msg["chat"]["id"]
                    raw = msg["text"].strip()
                    ud(chat_id, name=msg.get("from", {}).get("first_name", "User"))

                    if raw == "/start":
                        send_msg(chat_id, tx(chat_id, "welcome_new"), kb=main_kb(chat_id))
                        continue
                    
                    if raw.startswith("/"): continue
                    if chat_id in active_users: send_msg(chat_id, tx(chat_id, "e_active")); continue
                    if search_queue.qsize() >= MAX_QUEUE: send_msg(chat_id, tx(chat_id, "e_full")); continue

                    u = ud(chat_id)
                    now = time.time()
                    if now - u["last"] < COOLDOWN_SEC: send_msg(chat_id, tx(chat_id, "e_cool")); continue
                    
                    # STRICT 10-DIGIT VALIDATION
                    clean = raw
                    if not (clean.isdigit() and len(clean) == 10):
                        send_msg(chat_id, tx(chat_id, "e_digits"), kb=err_kb(chat_id))
                        continue
                    
                    u["last"] = now
                    active_users.add(chat_id)
                    
                    msg_id = send_msg(chat_id, tx(chat_id, "q1", q=clean))
                    with queue_lock: queue_order.append({"chat_id": chat_id, "msg_id": msg_id, "query": clean})
                    search_queue.put((chat_id, clean, msg_id))
                    
        except Exception as e:
            print(f"[POLLING ERR] {e}")
            time.sleep(2) 

# ─── 8. FASTAPI ROUTES & STARTUP ──────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "alive", "message": "KaramChand Engine is Running"}

@app.get("/search")
def api_search(q: str = Query(...)):
    """API Endpoint that strictly accepts 10-digit numbers."""
    try:
        q = q.strip()
        
        if not (q.isdigit() and len(q) == 10):
            return JSONResponse(status_code=400, content={"error": "Invalid input. Strictly 10 digits allowed."})

        res = fetch_dicts(db.execute("SELECT * FROM people_phone WHERE phoneNumber = ? LIMIT 5", [q]))
        return {"count": len(res), "results": res}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# Start Background Threads
threading.Thread(target=worker, daemon=True).start()
threading.Thread(target=poll_telegram, daemon=True).start()
