import requests, threading, time, queue, io, re, os
import html as html_lib
from datetime import datetime, timezone, timedelta
from flask import Flask, request
from collections import OrderedDict

# ─── CONFIG ───────────────────────────────────────────────────────────────────
TOKEN        = "8723126732:AAHVFOySgFaWN4_4InqtKrvkgN15crbPi0w"

# 🔴 PUT YOUR LIVE RENDER API URL HERE
API_URL      = "https://kc-1-t1b9.onrender.com/search"

ADMIN_ID     = 5505679330
MAX_QUEUE    = 15
HOURLY_LIMIT = 20
COOLDOWN_SEC = 3
BOT_LINK     = "https://t.me/Karam_Chand_bot"
DEV_LINK     = "https://t.me/ansafrozkhan"
BASE_URL     = f"https://api.telegram.org/bot{TOKEN}"

app = Flask(__name__)

# ─── STATE ────────────────────────────────────────────────────────────────────
user_data     = {}
active_users  = set()
user_sessions = {}
global_stats  = {"searches": 0, "results": 0}

search_queue  = queue.Queue()
queue_lock    = threading.Lock()
queue_order   = []

# ─── CACHE ────────────────────────────────────────────────────────────────────
search_cache = OrderedDict() 
CACHE_SIZE = 500

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def clean_address(addr):
    """Fixes the ! delimiter issue in the dataset."""
    if not addr: return ""
    clean = re.sub(r'!+', ', ', str(addr))
    clean = re.sub(r'\s+', ' ', clean).strip(', ')
    return clean

def tx(chat_id, key, **kwargs):
    lang = user_data.get(chat_id, {}).get("lang") or "en"
    tmpl = T.get(lang, T["en"]).get(key) or T["en"].get(key, key)
    return tmpl.format(**kwargs) if kwargs else tmpl

def ud(chat_id, name=None):
    if chat_id not in user_data:
        user_data[chat_id] = {"lang": None, "name": name or "User",
                              "searches": 0, "results": 0, "hourly": [], "last": 0.0}
    elif name:
        user_data[chat_id]["name"] = name
    return user_data[chat_id]

def sv(val):
    return str(val).strip() if val is not None and str(val).strip().lower() != 'nan' else ""

def queue_bar(pos, cap=5):
    filled = min(pos, cap)
    return "🟥" * filled + "⬜" * (cap - filled)

def api_call(method, payload):
    try:
        r = requests.post(f"{BASE_URL}/{method}", json=payload, timeout=10)
        return r.json()
    except Exception as e:
        print(f"[API ERR] {method}: {e}")
        return {}

def send_msg(chat_id, text, kb=None):
    p = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb: p["reply_markup"] = {"inline_keyboard": kb}
    resp = api_call("sendMessage", p)
    return resp.get("result", {}).get("message_id")

def edit_msg(chat_id, msg_id, text, kb=None):
    if not msg_id:
        send_msg(chat_id, text, kb)
        return
    p = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb: p["reply_markup"] = {"inline_keyboard": kb}
    result = api_call("editMessageText", p)
    if not result.get("ok"): send_msg(chat_id, text, kb)

def cb_answer(cb_id, text=""):
    api_call("answerCallbackQuery", {"callback_query_id": cb_id, "text": text})

def typing(chat_id):
    api_call("sendChatAction", {"chat_id": chat_id, "action": "typing"})

# ─── STRINGS ──────────────────────────────────────────────────────────────────
T = {
    "en": {
        "lang_pick": "🌐 <b>Select your language to continue:</b>",
        "welcome_new": "😎 <b>I am KaramChand.</b>\n\nI know everything. But I am not your servant.\n\n📱 <b>Mobile (10-digit):</b> Full record\n🪪 <b>Aadhar (12-digit):</b> Full record\n\n<i>Dev: @ansafrozkhan</i>",
        "welcome_back": "👋 Welcome back, <b>{name}</b>!\n\n📊 <b>Your Stats</b>\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Searched: <b>{gs}</b>\n📦 Delivered: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "stats": "📊 <b>Your Stats — {name}</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Total searched: <b>{gs}</b>\n📦 Total delivered: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "help": "ℹ️ <b>How to Use KaramChand</b>\n\n📱 <b>Mobile:</b> 10-digit number\n🪪 <b>Aadhar:</b> 12-digit number\n\n❌ Only numbers allowed (Names disabled)\n⏱ 3s cooldown between searches\n📋 Max <b>{lim}</b> searches/hour\n\n<i>Dev: @ansafrozkhan</i>",
        "q1": "⚡ <b>Searching...</b>\n🎯 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "qn": "📋 <b>Queue #{p}</b>  {bar}\n🎯 <code>{q}</code>\n⏳ Est. ~{eta}s\n\n<i>Dev: @ansafrozkhan</i>",
        "no_result": "❌ <b>No Records Found</b>\n\n<code>{q}</code> — nothing in the database.\nCheck spelling or try a different term.\n\n<i>Dev: @ansafrozkhan</i>",
        "new_search": "🔍 <b>New Search</b>\n\nType 10-digit mobile or 12-digit Aadhar.\n\n<i>Dev: @ansafrozkhan</i>",
        "report_cap": "📄 <b>KaramChand Report</b>\n🔎 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "e_digits": "❌ Need 10 or 12 digits — you sent <b>{n}</b>.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_mixed": "❌ Only numbers are allowed. Names are disabled.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_cool": "⏳ Wait 3 seconds before searching again.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_active": "🔄 Your search is already running. Wait for the result.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_fwd": "❌ Forwarded messages not allowed. Type directly.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_full": "😤 Server busy. Try in a minute.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_limit": f"🚫 Hourly limit ({HOURLY_LIMIT}/hr). Come back later.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_server": "⚠️ Server crashed. Try again shortly.\n\n<i>Dev: @ansafrozkhan</i>",
        "lbl_name": "Name", "lbl_father": "Father", "lbl_mobile": "Mobile", "lbl_alt": "Alt. Mobile", "lbl_address": "Address", "lbl_circle": "Circle / ISP", "lbl_id": "ID No.", "lbl_email": "Email",
        "lbl_aadhar": "Aadhar", "lbl_state": "State", "lbl_district": "District", "lbl_town": "Town", "lbl_pincode": "Pincode",
        "b_search": "🔍 New Search", "b_help": "ℹ️ Help", "b_back": "🔙 Back", "b_stats": "📊 My Stats", "b_prev": "◀️ Prev", "b_next": "Next ▶️", "b_lang": "🌐 Language", "b_report": "📄 HTML Report",
    },
    "hi": {
        "lang_pick": "🌐 <b>Apni bhasha chuniye:</b>",
        "welcome_new": "😎 <b>Main hoon KaramChand.</b>\n\nDuniya ki har cheez ki jankari hai mujhe. Par main tera naukar nahi.\n\n📱 <b>Mobile (10-digit):</b> Poori kundli\n🪪 <b>Aadhar (12-digit):</b> Poori kundli\n\n<i>Dev: @ansafrozkhan</i>",
        "welcome_back": "👋 Wapas aa gaya, <b>{name}</b>!\n\n📊 <b>Teri Stats</b>\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Searched: <b>{gs}</b>\n📦 Delivered: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "stats": "📊 <b>Teri Stats — {name}</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n🔍 Searches: <b>{s}</b>\n✅ Results: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Total searched: <b>{gs}</b>\n📦 Total diye: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "help": "ℹ️ <b>KaramChand Kaise Use Kare</b>\n\n📱 <b>Mobile:</b> 10-digit number\n🪪 <b>Aadhar:</b> 12-digit number\n\n❌ Sirf numbers allowed hain\n⏱ Har search ke beech 3s ruk\n📋 Max <b>{lim}</b> searches per ghante\n\n<i>Dev: @ansafrozkhan</i>",
        "q1": "⚡ <b>Dhoondh raha hoon...</b>\n🎯 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "qn": "📋 <b>Queue #{p}</b>  {bar}\n🎯 <code>{q}</code>\n⏳ ~{eta}s intezaar\n\n<i>Dev: @ansafrozkhan</i>",
        "no_result": "❌ <b>Koi Record Nahi Mila</b>\n\n<code>{q}</code> — database mein kuch nahi.\nSpelling check kar ya kuch aur try kar.\n\n<i>Dev: @ansafrozkhan</i>",
        "new_search": "🔍 <b>Nayi Search</b>\n\n10-digit mobile ya 12-digit Aadhar likh.\n\n<i>Dev: @ansafrozkhan</i>",
        "report_cap": "📄 <b>KaramChand Report</b>\n🔎 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "e_digits": "❌ 10 ya 12-digit chahiye — tune <b>{n}</b>-digit bheja.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_mixed": "❌ Sirf numbers allowed hain. Naam band hai.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_cool": "⏳ 3 second ruk ja next search se pehle.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_active": "🔄 Tera search chal raha hai. Result aane tak ruk.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_fwd": "❌ Forward kiya message nahi chalega. Seedha likh.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_full": "😤 Server busy hai. Thodi der baad try kar.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_limit": f"🚫 Ghante ki limit ({HOURLY_LIMIT}/ghante) khatam. Baad mein aana.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_server": "⚠️ Server crash ho gaya. Thodi der baad try kar.\n\n<i>Dev: @ansafrozkhan</i>",
        "lbl_name": "Naam", "lbl_father": "Pita ka Naam", "lbl_mobile": "Mobile", "lbl_alt": "Alt. Number", "lbl_address": "Pata", "lbl_circle": "Circle / ISP", "lbl_id": "ID Number", "lbl_email": "Email",
        "lbl_aadhar": "Aadhar No.", "lbl_state": "Rajya", "lbl_district": "Zila", "lbl_town": "Kasba", "lbl_pincode": "Pincode",
        "b_search": "🔍 Nayi Search", "b_help": "ℹ️ Help", "b_back": "🔙 Wapas", "b_stats": "📊 Meri Stats", "b_prev": "◀️ Pichla", "b_next": "Agla ▶️", "b_lang": "🌐 Bhasha", "b_report": "📄 HTML Report",
    },
    "kn": {
        "lang_pick": "🌐 <b>ನಿಮ್ಮ ಭಾಷೆ ಆರಿಸಿ:</b>",
        "welcome_new": "😎 <b>ನಾನು ಕರಮಚಂದ್.</b>\n\nಎಲ್ಲ ತಿಳಿದಿದೆ ನನಗೆ. ಆದರೆ ನಾನು ನಿನ್ನ ಆಳಲ್ಲ.\n\n📱 <b>ಮೊಬೈಲ್ (10-ಅಂಕಿ):</b> ಪೂರ್ಣ ವಿವರ\n🪪 <b>ಆಧಾರ್ (12-ಅಂಕಿ):</b> ಪೂರ್ಣ ವಿವರ\n\n<i>Dev: @ansafrozkhan</i>",
        "welcome_back": "👋 ಮತ್ತೆ ಬಂದೆ, <b>{name}</b>!\n\n📊 <b>ನಿನ್ನ Stats</b>\n🔍 ಹುಡುಕಾಟ: <b>{s}</b>\n✅ ಫಲಿತಾಂಶ: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 Searched: <b>{gs}</b>\n📦 Delivered: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "stats": "📊 <b>Stats — {name}</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n🔍 ಹುಡುಕಾಟ: <b>{s}</b>\n✅ ಫಲಿತಾಂಶ: <b>{r}</b>\n\n🌍 <b>Global Stats</b>\n🔢 ಒಟ್ಟು: <b>{gs}</b>\n📦 ಒಟ್ಟು ಫಲಿತಾಂಶ: <b>{gr}</b>\n\n<i>Dev: @ansafrozkhan</i>",
        "help": "ℹ️ <b>ಹೇಗೆ ಬಳಸಬೇಕು</b>\n\n📱 <b>ಮೊಬೈಲ್:</b> 10-ಅಂಕಿ\n🪪 <b>ಆಧಾರ್:</b> 12-ಅಂಕಿ\n\n❌ ಸಂಖ್ಯೆಗಳು ಮಾತ್ರ\n⏱ 3s cooldown\n📋 Max <b>{lim}</b> searches/ಗಂಟೆ\n\n<i>Dev: @ansafrozkhan</i>",
        "q1": "⚡ <b>ಹುಡುಕುತ್ತಿದ್ದೇನೆ...</b>\n🎯 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "qn": "📋 <b>Queue #{p}</b>  {bar}\n🎯 <code>{q}</code>\n⏳ ~{eta}s ಕಾಯಿ\n\n<i>Dev: @ansafrozkhan</i>",
        "no_result": "❌ <b>ಯಾವ ದಾಖಲೆಯೂ ಸಿಗಲಿಲ್ಲ</b>\n\n<code>{q}</code> — ಏನೂ ಇಲ್ಲ.\nಬೇರೆ ಹುಡುಕು.\n\n<i>Dev: @ansafrozkhan</i>",
        "new_search": "🔍 <b>ಹೊಸ ಹುಡುಕಾಟ</b>\n\n10-ಅಂಕಿ ಮೊಬೈಲ್ ಅಥವಾ 12-ಅಂಕಿ ಆಧಾರ್ ಟೈಪ್ ಮಾಡು.\n\n<i>Dev: @ansafrozkhan</i>",
        "report_cap": "📄 <b>KaramChand ವರದಿ</b>\n🔎 <code>{q}</code>\n\n<i>Dev: @ansafrozkhan</i>",
        "e_digits": "❌ 10 ಅಥವಾ 12 ಅಂಕಿ ಬೇಕು — ನೀನು <b>{n}</b> ಅಂಕಿ ಕಳಿಸಿದೆ.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_mixed": "❌ ಸಂಖ್ಯೆಗಳು ಮಾತ್ರ ಅನುಮತಿಸಲಾಗಿದೆ. ಹೆಸರು ನಿಷ್ಕ್ರಿಯಗೊಳಿಸಲಾಗಿದೆ.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_cool": "⏳ 3 ಸೆಕೆಂಡ್ ಕಾಯಿ.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_active": "🔄 ನಿನ್ನ search ನಡೆಯುತ್ತಿದೆ. ಕಾಯಿ.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_fwd": "❌ Forward ಸಂದೇಶ ಬೇಡ. ನೇರವಾಗಿ ಟೈಪ್ ಮಾಡು.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_full": "😤 Server ಬ್ಯುಸಿ. ಸ್ವಲ್ಪ ಸಮಯದ ನಂತರ ಪ್ರಯತ್ನಿಸು.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_limit": f"🚫 ಗಂಟೆಯ ಮಿತಿ ({HOURLY_LIMIT}/ಗಂಟೆ) ಮುಗಿಯಿತು.\n\n<i>Dev: @ansafrozkhan</i>",
        "e_server": "⚠️ Server ಕ್ರಾಶ್. ಸ್ವಲ್ಪ ಸಮಯದ ನಂತರ ಪ್ರಯತ್ನಿಸು.\n\n<i>Dev: @ansafrozkhan</i>",
        "lbl_name": "ಹೆಸರು", "lbl_father": "ತಂದೆಯ ಹೆಸರು", "lbl_mobile": "ಮೊಬೈಲ್", "lbl_alt": "ಪರ್ಯಾಯ ಸಂಖ್ಯೆ", "lbl_address": "ವಿಳಾಸ", "lbl_circle": "Circle / ISP", "lbl_id": "ID ಸಂಖ್ಯೆ", "lbl_email": "ಇಮೇಲ್",
        "lbl_aadhar": "ಆಧಾರ್", "lbl_state": "ರಾಜ್ಯ", "lbl_district": "ಜಿಲ್ಲೆ", "lbl_town": "ಪಟ್ಟಣ", "lbl_pincode": "ಪಿನ್ಕೋಡ್",
        "b_search": "🔍 ಹೊಸ ಹುಡುಕಾಟ", "b_help": "ℹ️ ಸಹಾಯ", "b_back": "🔙 ಹಿಂದೆ", "b_stats": "📊 ನನ್ನ Stats", "b_prev": "◀️ ಹಿಂದಿನ", "b_next": "ಮುಂದಿನ ▶️", "b_lang": "🌐 ಭಾಷೆ", "b_report": "📄 HTML ವರದಿ",
    },
}

# ─── RESULT CARD FORMATTER (Exact UI Match) ────────────────────────────────────
def format_card(chat_id, row, page, total, query):
    # row is now a dictionary from the API
    name = sv(row.get("name"))
    fname = sv(row.get("fathersName"))
    phone = sv(row.get("phoneNumber"))
    alt = sv(row.get("otherNumber"))
    aadhar = sv(row.get("aadharNumber"))
    addr = clean_address(row.get("address"))
    dist = sv(row.get("district"))
    pin = sv(row.get("pincode"))
    state = sv(row.get("state"))
    town = sv(row.get("town"))

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
    if phone: lines.append(f"📱 <b>{tx(chat_id, 'lbl_mobile')}</b> ➡️ <code>{phone}</code>")
    if alt: lines.append(f"📞 <b>{tx(chat_id, 'lbl_alt')}</b> ➡️ <code>{alt}</code>")
    if aadhar: lines.append(f"🪪 <b>{tx(chat_id, 'lbl_aadhar')}</b> ➡️ <code>{aadhar}</code>")
    
    if addr:
        lines.append(f"🏠 <b>{tx(chat_id, 'lbl_address')}</b>")
        lines.append(f"<blockquote>{html_lib.escape(addr)}</blockquote>")
        
    loc = [x for x in [town, dist, state] if x]
    if loc:
        lines.append(f"📍 <b>{tx(chat_id, 'lbl_town')} / {tx(chat_id, 'lbl_district')} / {tx(chat_id, 'lbl_state')}</b> ➡️ {html_lib.escape(', '.join(loc))}")
    if pin: 
        lines.append(f"📮 <b>{tx(chat_id, 'lbl_pincode')}</b> ➡️ <code>{pin}</code>")

    lines += [
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄",
        f"⏱ <b>Generated:</b> {ts}",
        "<i>Dev: @ansafrozkhan</i>",
    ]
    return "\n".join(lines)

# ─── HTML REPORT GENERATOR ────────────────────────────────────────────────────
def generate_html_report(row, query):
    name = sv(row.get("name"))
    fname = sv(row.get("fathersName"))
    phone = sv(row.get("phoneNumber"))
    alt = sv(row.get("otherNumber"))
    aadhar = sv(row.get("aadharNumber"))
    addr = clean_address(row.get("address"))
    dist = sv(row.get("district"))
    pin = sv(row.get("pincode"))
    state = sv(row.get("state"))
    town = sv(row.get("town"))

    if alt.startswith("91") and len(alt) == 12:
        alt = alt[2:]

    e = html_lib.escape

    def field(icon, label, value, mono=False):
        if not value: return ""
        val_class = "mono" if mono else ""
        return f'<div class="field"><span class="icon">{icon}</span><div class="content"><div class="label">{e(label)}</div><div class="value {val_class}">{e(value)}</div></div></div>'

    personal_block = field("👤", "Name", name) + field("👨", "Father's Name", fname)
    contact_block = field("📱", "Mobile", phone, mono=True) + field("📞", "Alt. Mobile", alt, mono=True) + field("🏠", "Address", addr) + field("📍", "District / State", f"{dist}, {state}") + field("📮", "Pincode", pin, mono=True) + field("🏙️", "Town", town)
    
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
        send_msg(chat_id, tx(chat_id, "e_server"))

# ─── KEYBOARDS & UI ───────────────────────────────────────────────────────────
def lang_kb(): return [[{"text": "🇬🇧 English", "callback_data": "lang:en"},{"text": "🇮🇳 हिंदी", "callback_data": "lang:hi"},{"text": "🔡 ಕನ್ನಡ", "callback_data": "lang:kn"}]]
def main_kb(chat_id): return [[{"text": tx(chat_id, "b_search"), "callback_data": "new_search"},{"text": tx(chat_id, "b_stats"), "callback_data": "stats"}],[{"text": tx(chat_id, "b_help"), "callback_data": "help"},{"text": tx(chat_id, "b_lang"), "callback_data": "change_lang"}]]
def err_kb(chat_id): return [[{"text": tx(chat_id, "b_help"), "callback_data": "help"},{"text": tx(chat_id, "b_search"), "callback_data": "new_search"}]]

def result_kb(chat_id, page, total):
    nav = []
    if page > 0: nav.append({"text": tx(chat_id, "b_prev"), "callback_data": f"pg:{page-1}"})
    
    if total > 1:
        nav.append({"text": f"📄 {page+1}/{total}", "callback_data": "noop"})
        
    if page < total - 1: nav.append({"text": tx(chat_id, "b_next"), "callback_data": f"pg:{page+1}"})
    
    keyboard = []
    if nav: keyboard.append(nav)
    keyboard.append([{"text": tx(chat_id, "b_report"), "callback_data": f"html:{page}"}])
    keyboard.append([{"text": tx(chat_id, "b_search"), "callback_data": "new_search"},{"text": tx(chat_id, "b_stats"), "callback_data": "stats"}])
    
    return keyboard

def push_queue_updates():
    with queue_lock: snapshot = list(queue_order)
    for i, item in enumerate(snapshot):
        cid, mid, q = item["chat_id"], item["msg_id"], item["query"]
        pos = i + 1
        text = tx(cid, "q1", q=q) if pos == 1 else tx(cid, "qn", p=pos, bar=queue_bar(pos), q=q, eta=pos*2)
        try:
            api_call("editMessageText", {"chat_id": cid, "message_id": mid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True})
        except Exception: pass

# ─── WORKER (STEP 2 IMPLEMENTED) ──────────────────────────────────────────────
def worker():
    while True:
        chat_id, query, msg_id = search_queue.get()
        try:
            typing(chat_id)
            with queue_lock:
                for i, item in enumerate(queue_order):
                    if item["chat_id"] == chat_id and item["msg_id"] == msg_id:
                        queue_order.pop(i)
                        break
            push_queue_updates()

            global_stats["searches"] += 1
            ud(chat_id)["searches"] += 1

            # CHECK CACHE FIRST (0.01 seconds)
            if query in search_cache:
                rows = search_cache[query]
                search_cache.move_to_end(query)
            else:
                # 🔥 HIT THE RENDER API
                try:
                    response = requests.get(f"{API_URL}?q={query}", timeout=30)
                    data = response.json()
                    rows = data.get("results", [])
                except Exception as api_e:
                    print(f"[API FETCH ERR] {api_e}")
                    rows = []

                # Save to cache for next time
                search_cache[query] = rows
                if len(search_cache) > CACHE_SIZE:
                    search_cache.popitem(last=False)

            if not rows:
                edit_msg(chat_id, msg_id, tx(chat_id, "no_result", q=query), kb=err_kb(chat_id))
            else:
                cnt = len(rows)
                global_stats["results"] += cnt
                ud(chat_id)["results"] += cnt
                user_sessions[chat_id] = {"results": rows, "page": 0, "query": query}
                text = format_card(chat_id, rows[0], 0, cnt, query)
                edit_msg(chat_id, msg_id, text, kb=result_kb(chat_id, 0, cnt))

        except Exception as e:
            print(f"[WORKER ERR] {e}")
            try: edit_msg(chat_id, msg_id, tx(chat_id, "e_server"))
            except Exception: send_msg(chat_id, tx(chat_id, "e_server"))
        finally:
            active_users.discard(chat_id)
            search_queue.task_done()

# ─── ROUTES ───────────────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home(): return "KaramChand v7 is live."

@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    update = request.get_json()
    if "callback_query" in update:
        cb      = update["callback_query"]
        chat_id = cb["message"]["chat"]["id"]
        msg_id  = cb["message"]["message_id"]
        data    = cb["data"]
        cb_answer(cb["id"])

        if data.startswith("lang:"):
            lang = data.split(":")[1]
            ud(chat_id)["lang"] = lang
            u = ud(chat_id)
            if u["searches"] == 0: edit_msg(chat_id, msg_id, tx(chat_id, "welcome_new"), kb=main_kb(chat_id))
            else: edit_msg(chat_id, msg_id, tx(chat_id, "welcome_back", name=u["name"], s=u["searches"], r=u["results"], gs=global_stats["searches"], gr=global_stats["results"]), kb=main_kb(chat_id))
        elif data == "change_lang": edit_msg(chat_id, msg_id, tx(chat_id, "lang_pick"), kb=lang_kb())
        elif data.startswith("pg:"):
            page = int(data.split(":")[1])
            session = user_sessions.get(chat_id)
            if session:
                session["page"] = page
                text = format_card(chat_id, session["results"][page], page, len(session["results"]), session["query"])
                edit_msg(chat_id, msg_id, text, kb=result_kb(chat_id, page, len(session["results"])))
        elif data.startswith("html:"):
            page = int(data.split(":")[1])
            session = user_sessions.get(chat_id)
            if session and 0 <= page < len(session["results"]):
                threading.Thread(target=send_html_doc, args=(chat_id, session["results"][page], session["query"], page, len(session["results"])), daemon=True).start()
        elif data == "new_search": edit_msg(chat_id, msg_id, tx(chat_id, "new_search"), kb=[[{"text": tx(chat_id, "b_help"), "callback_data": "help"}]])
        elif data == "stats":
            u = ud(chat_id)
            edit_msg(chat_id, msg_id, tx(chat_id, "stats", name=u["name"], s=u["searches"], r=u["results"], gs=global_stats["searches"], gr=global_stats["results"]), kb=[[{"text": tx(chat_id, "b_back"), "callback_data": "new_search"}]])
        elif data == "help": edit_msg(chat_id, msg_id, tx(chat_id, "help", lim=HOURLY_LIMIT), kb=[[{"text": tx(chat_id, "b_back"), "callback_data": "new_search"}]])
        return "OK"

    if "message" not in update or "text" not in update["message"]: return "OK"
    msg     = update["message"]
    chat_id = msg["chat"]["id"]
    raw     = msg["text"].strip()
    name    = msg.get("from", {}).get("first_name", "User")
    ud(chat_id, name=name)

    if raw == "/start":
        u = ud(chat_id)
        if not u["lang"]: send_msg(chat_id, T["en"]["lang_pick"], kb=lang_kb())
        elif u["searches"] == 0: send_msg(chat_id, tx(chat_id, "welcome_new"), kb=main_kb(chat_id))
        else: send_msg(chat_id, tx(chat_id, "welcome_back", name=u["name"], s=u["searches"], r=u["results"], gs=global_stats["searches"], gr=global_stats["results"]), kb=main_kb(chat_id))
        return "OK"

    if str(chat_id) == str(ADMIN_ID):
        if raw.startswith("/broadcast "):
            body = raw[11:].strip()
            for uid_key in list(user_data.keys()):
                send_msg(uid_key, f"📢 <b>ANNOUNCEMENT</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n{body}\n\n<i>Dev: @ansafrozkhan</i>")
                time.sleep(0.05)
            send_msg(chat_id, f"✅ Sent to {len(user_data)} users.")
            return "OK"
        if raw == "/stats":
            send_msg(chat_id, f"📊 <b>Admin Stats</b>\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n👥 Users: <b>{len(user_data)}</b>\n🔍 Total Searches: <b>{global_stats['searches']}</b>\n📦 Total Results: <b>{global_stats['results']}</b>\n📋 Queue: <b>{search_queue.qsize()}</b>\n🔄 Active: <b>{len(active_users)}</b>\n⚡ Cache Size: <b>{len(search_cache)}</b>\n\n<i>Dev: @ansafrozkhan</i>")
            return "OK"

    if raw.startswith("/"): return "OK"
    if not ud(chat_id)["lang"]: send_msg(chat_id, T["en"]["lang_pick"], kb=lang_kb()); return "OK"
    if any(k in msg for k in ("forward_origin", "forward_from", "forward_from_chat")): send_msg(chat_id, tx(chat_id, "e_fwd"), kb=err_kb(chat_id)); return "OK"
    if chat_id in active_users: send_msg(chat_id, tx(chat_id, "e_active"), kb=err_kb(chat_id)); return "OK"
    if search_queue.qsize() >= MAX_QUEUE: send_msg(chat_id, tx(chat_id, "e_full"), kb=err_kb(chat_id)); return "OK"

    u   = ud(chat_id)
    now = time.time()
    if now - u["last"] < COOLDOWN_SEC: send_msg(chat_id, tx(chat_id, "e_cool"), kb=err_kb(chat_id)); return "OK"
    u["hourly"] = [t for t in u["hourly"] if now - t < 3600]
    if len(u["hourly"]) >= HOURLY_LIMIT: send_msg(chat_id, tx(chat_id, "e_limit"), kb=err_kb(chat_id)); return "OK"

    clean = raw
    if raw.isdigit():
        if len(raw) == 12 and raw.startswith("91"): clean = raw[2:]
        if len(clean) not in (10, 12): 
            send_msg(chat_id, tx(chat_id, "e_digits", n=len(clean)), kb=err_kb(chat_id)); 
            return "OK"
    else:
        send_msg(chat_id, tx(chat_id, "e_mixed"), kb=err_kb(chat_id)); 
        return "OK"

    u["last"] = now
    u["hourly"].append(now)
    active_users.add(chat_id)

    pos = search_queue.qsize() + 1
    load_text = (tx(chat_id, "q1", q=clean) if pos == 1 else tx(chat_id, "qn", p=pos, bar=queue_bar(pos), q=clean, eta=pos*2))
    msg_id = send_msg(chat_id, load_text)

    with queue_lock: queue_order.append({"chat_id": chat_id, "msg_id": msg_id, "query": clean})
    search_queue.put((chat_id, clean, msg_id))
    return "OK"

# ─── STARTUP ──────────────────────────────────────────────────────────────────
threading.Thread(target=worker, daemon=True).start()

port = int(os.environ.get("PORT", 5000))
render_url = os.environ.get("RENDER_EXTERNAL_URL")

if render_url:
    print(f"🚀 Setting webhook to Render URL: {render_url}")
    requests.get(f"{BASE_URL}/setWebhook?url={render_url}/{TOKEN}")
else:
    print("⚠️ RENDER_EXTERNAL_URL not found. Running locally.")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)
