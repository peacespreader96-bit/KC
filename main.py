import os
import time
import queue
import threading
import requests
import duckdb
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import html as html_lib
from collections import OrderedDict

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TOKEN        = "8723126732:AAHVFOySgFaWN4_4InqtKrvkgN15crbPi0w"
ADMIN_ID     = 5505679330
MAX_QUEUE    = 15
HOURLY_LIMIT = 20
COOLDOWN_SEC = 3
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

# ─── 2. TELEGRAM BOT STATE & HELPERS ──────────────────────────────────────────
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
        "welcome_back": "👋 Welcome back, <b>{name}</b>!\n\n📊 <b>Your Stats</b>\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "stats": "📊 <b>Your Stats — {name}</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Total searched: <b>{gs}</b>\n📦 Total delivered: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "help": "ℹ️ <b>How to Use KaramChand</b>\n\n📱 Send a <b>10-digit mobile number</b> (e.g., 9876543210).\n\n❌ No spaces, no +91, no letters.\n⏱ 3s cooldown\n📋 Max <b>{lim}</b> searches/hour\n\n<i>Dev: @ansafrozkhan</i>",
        "q1": "⚡ <b>Searching...</b>\n🎯 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "qn": "📋 <b>Queue #{p}</b>  {bar}\n🎯 <code>{q}</code>\n⏳ Est. ~{eta}s\n\n<i>Dev: @ansafrozkhan</i>",
        "no_result": "❌ <b>No Records Found</b>\n\n<code>{q}</code> — nothing in the database.\n\n<i>Dev: @ansafrozkhan</i>",
        "new_search": "🔍 <b>New Search</b>\n\nType a strict 10-digit mobile number.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_digits": "❌ <b>Invalid Input</b>\n\nPlease send a strict <b>10-digit mobile number</b> only.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_cool": "⏳ Wait 3 seconds before searching again.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_active": "🔄 Your search is already running.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_fwd": "❌ Forwarded messages not allowed.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_full": "😤 Server busy. Try in a minute.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_limit": f"🚫 Hourly limit ({HOURLY_LIMIT}/hr).\n\n<i>Dev: @ansafrozkhan</i>",
        "e_server": "⚠️ Server crashed. Try again shortly.\n\n<i>Dev: @ansafrozkhan</i>",
        "lbl_name": "Name", "lbl_father": "Father", "lbl_mobile": "Mobile", "lbl_alt": "Alt. Mobile", 
        "lbl_aadhar": "Aadhar", "lbl_address": "Address", "lbl_district": "District", 
        "lbl_pincode": "Pincode", "lbl_state": "State", "lbl_town": "Town",
        "b_search": "🔍 New Search", "b_help": "ℹ️ Help", "b_back": "🔙 Back", "b_stats": "📊 My Stats"
    }
}

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

def format_card(chat_id, row, query):
    name = sv(row.get("name")); fname = sv(row.get("fathersName"))
    mobile = sv(row.get("phoneNumber")); alt = sv(row.get("otherNumber"))
    aadhar = sv(row.get("aadharNumber")); address = sv(row.get("address"))
    district = sv(row.get("district")); pincode = sv(row.get("pincode"))
    state = sv(row.get("state")); town = sv(row.get("town"))

    lines = ["🕵️ <b>KaramChand Report</b>", "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄", f"🔎 <code>{query}</code>", ""]
    if name: lines.append(f"👤 <b>Name:</b> {html_lib.escape(name)}")
    if fname: lines.append(f"👨 <b>Father:</b> {html_lib.escape(fname)}")
    if mobile: lines.append(f"📱 <b>Mobile:</b> <code>{mobile}</code>")
    if alt: lines.append(f"📞 <b>Alt. Mobile:</b> <code>{alt}</code>")
    if aadhar: lines.append(f"🪪 <b>Aadhar:</b> <code>{aadhar}</code>")
    if address: lines.append(f"🏠 <b>Address:</b>\n<blockquote>{html_lib.escape(address)}</blockquote>")
    if district or state: lines.append(f"📍 <b>Loc:</b> {town}, {district}, {state} - {pincode}")
    
    lines += ["┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄", "<i>Dev: @ansafrozkhan</i>"]
    return "\n".join(lines)

def main_kb(chat_id): return [[{"text": tx(chat_id, "b_search"), "callback_data": "new_search"},{"text": tx(chat_id, "b_stats"), "callback_data": "stats"}],[{"text": tx(chat_id, "b_help"), "callback_data": "help"}]]
def err_kb(chat_id): return [[{"text": tx(chat_id, "b_help"), "callback_data": "help"},{"text": tx(chat_id, "b_search"), "callback_data": "new_search"}]]

# ─── 3. THE BOT WORKER ────────────────────────────────────────────────────────
def worker():
    while True:
        chat_id, query, msg_id = search_queue.get()
        try:
            api_call("sendChatAction", {"chat_id": chat_id, "action": "typing"})
            
            with queue_lock:
                for i, item in enumerate(queue_order):
                    if item["chat_id"] == chat_id and item["msg_id"] == msg_id:
                        queue_order.pop(i); break

            global_stats["searches"] += 1
            ud(chat_id)["searches"] += 1

            # Check Cache
            if query in search_cache:
                rows = search_cache[query]
            else:
                # 🔥 DUCKDB SEARCH (Strictly Phone Number only)
                q = "SELECT * FROM people_phone WHERE phoneNumber = ? LIMIT 5"
                results = fetch_dicts(db.execute(q, [query]))
                
                # Clean NaN
                rows = [{k: (None if str(v) == 'nan' else v) for k, v in r.items()} for r in results]
                search_cache[query] = rows
                if len(search_cache) > CACHE_SIZE: search_cache.popitem(last=False)

            if not rows:
                edit_msg(chat_id, msg_id, tx(chat_id, "no_result", q=query), kb=err_kb(chat_id))
            else:
                global_stats["results"] += len(rows)
                ud(chat_id)["results"] += len(rows)
                text = format_card(chat_id, rows[0], query)
                edit_msg(chat_id, msg_id, text, kb=main_kb(chat_id))

        except Exception as e:
            print(f"[WORKER ERR] {e}")
            edit_msg(chat_id, msg_id, tx(chat_id, "e_server"))
        finally:
            active_users.discard(chat_id)
            search_queue.task_done()

# ─── 4. TELEGRAM LONG POLLING LOOP ────────────────────────────────────────────
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
                    
                    # 🔴 STRICT 10-DIGIT VALIDATION
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

# ─── 5. FASTAPI ROUTES & STARTUP ──────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "alive", "message": "KaramChand Engine is Running"}

@app.get("/search")
def api_search(q: str = Query(...)):
    """API Endpoint that strictly accepts 10-digit numbers."""
    try:
        q = q.strip()
        
        # Enforce 10-digit rule on API as well
        if not (q.isdigit() and len(q) == 10):
            return JSONResponse(status_code=400, content={"error": "Invalid input. Strictly 10 digits allowed."})

        res = fetch_dicts(db.execute("SELECT * FROM people_phone WHERE phoneNumber = ? LIMIT 5", [q]))
        return {"count": len(res), "results": res}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# Start Background Threads
threading.Thread(target=worker, daemon=True).start()
threading.Thread(target=poll_telegram, daemon=True).start()
