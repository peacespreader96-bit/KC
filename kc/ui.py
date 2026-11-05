"""UI dictionaries, keyboards, and message templates."""
import time

from kc import shared
from kc.config import (
    ADMIN_ID,
    BOT_LINK,
    FORCE_CHANNELS,
    SPIN_WINNER_LIMIT,
    STREAK_INTERVAL,
)
from kc.user_ops import (
    _bonus_today,
    _daily_used,
    _free_limit,
    _gw_on,
    _plans,
    _premium_active,
    _utr_lock_until,
    _utr_locked,
)
from kc.utils import _D, _clip, _e, fmt_td, fmt_until, q_bar

T = {
    "en": {
        "welcome": "Kya scene hai bhai? Bata kis customer ki info chahiye.",
        "search_prompt": "Bhai 10 digit ka customer phone number daal. Baaki bakwaas nahi chahiye.",
        "not_found": "Kuch nahi mila bhai.",
        "invalid": "Bhai 10 digit ka customer phone number daal, dimag mat kha.",
    }
}


def get_text(cid: int, key: str) -> str:
    lang = shared.user_data.get(cid, {}).get("lang", "en")
    return T.get(lang, T["en"]).get(key, "")


def _channel_link(ch: str) -> str | None:
    s = str(ch).strip()
    if s.startswith(("http://", "https://")):
        return s
    if s.startswith("@"):
        return "https://t.me/" + s[1:]
    return None


def dashboard_kb():
    return [
        [{"text": "🔍 Number Daal", "callback_data": "search"}],
        [{"text": "👤 Mera Scene", "callback_data": "profile"}, {"text": "👑 Premium Pass", "callback_data": "premium"}],
        [{"text": "📜 Purani Search", "callback_data": "history"}, {"text": "🎰 Lucky Spin", "callback_data": "spin"}],
        [{"text": "🆘 Support", "callback_data": "support"}, {"text": "ℹ️ Help", "callback_data": "help"}],
    ]


def main_kb():
    return dashboard_kb()


def back_kb():
    return [
        [{"text": "🔍 Nayi Search", "callback_data": "search"}, {"text": "🔙 Wapas", "callback_data": "dashboard"}]
    ]


def pricing_kb():
    rows = [
        [{"text": f"⚡ {p['name']} · ₹{p['amount']}", "callback_data": f"plan:{pid}"}]
        for pid, p in _plans().items()
    ]
    rows.append([{"text": "🔙 Wapas", "callback_data": "dashboard"}])
    return rows


def _gateway_kb(plan_id: str):
    rows = []
    if _gw_on("upi"):
        rows.append([{"text": "💳 UPI Instant", "callback_data": f"gwpay:upi:{plan_id}"}])
    if _gw_on("stars"):
        rows.append([{"text": "⭐ Telegram Stars", "callback_data": f"gwpay:stars:{plan_id}"}])
    if _gw_on("crypto"):
        rows.append([{"text": "💎 Crypto USDT", "callback_data": f"gwpay:crypto:{plan_id}"}])
    rows.append([{"text": "❌ Cancel", "callback_data": "paycancel"}, {"text": "🔙 Wapas", "callback_data": "premium"}])
    return rows


def result_kb(page: int, total: int):
    rows = []
    if total > 1:
        nav = []
        if page > 0:
            nav.append({"text": "◀️ Pichla", "callback_data": f"pg:{page - 1}"})
        nav.append({"text": f"📄 {page + 1}/{total}", "callback_data": "noop"})
        if page < total - 1:
            nav.append({"text": "Agla ▶️", "callback_data": f"pg:{page + 1}"})
        rows.append(nav)
    rows.append([
        {"text": "📄 Download Report", "callback_data": f"html:{page}"},
        {"text": "🔍 Nayi Search", "callback_data": "new_search"},
    ])
    rows.append([
        {"text": "🏠 Main Menu", "callback_data": "dashboard"},
        {"text": "👑 Premium", "callback_data": "premium"},
    ])
    return rows


def force_kb():
    rows = []
    for ch in FORCE_CHANNELS:
        link = _channel_link(str(ch))
        if link:
            rows.append([{"text": f"Join {ch} ↗", "url": link}])
        else:
            rows.append([{"text": f"Join {ch}", "callback_data": "noop"}])
    rows.append([{"text": "Verify Join ✅", "callback_data": "verify"}])
    return rows


def broadcast_kb():
    return [[{"text": "📢 Sabko Bhej", "callback_data": "cast:all"}, {"text": "👑 Sirf Premium", "callback_data": "cast:prem"}]]


def msg_dashboard(u: dict) -> str:
    name = _e(u.get("name") or "bhai")
    if _premium_active(u):
        until = fmt_until(float(u.get("premium_until") or 0))
        return (
            f"👑 <b>Waapas aa gaya, {name}?</b>\n\n"
            f"Kya scene hai bhai? Bata kis customer ki info chahiye.\n\n"
            f"⚡ <b>Status:</b> Premium Active\n"
            f"⏳ <b>Validity:</b> <b>{_e(until)}</b>\n\n{_D}"
        )
    used = _daily_used(u)
    allowance = _free_limit() + _bonus_today(int(u["id"]))
    left = max(0, allowance - used)
    extra = " (Lucky spin bonus included)" if _bonus_today(int(u["id"])) else ""
    return (
        f"<b>Kya scene hai bhai, {name}?</b>\n\n"
        f"Bata kis customer ki info chahiye. 10 digit number daal, ya neeche button se start kar.\n\n"
        f"📊 <b>Aaj ke free lookups:</b> <b>{left}</b> / {allowance}{extra}\n\n{_D}"
    )


def msg_profile(u: dict) -> str:
    gold = _premium_active(u)
    used = _daily_used(u)
    bonus = _bonus_today(int(u["id"]))
    allowance = "Unlimited 👑" if (gold or int(u["id"]) == ADMIN_ID) else f"{max(0, _free_limit() + bonus - used)} bache"
    if gold:
        status = f"👑 Premium · {_e(fmt_until(float(u.get('premium_until') or 0)))} tak"
    elif int(u["id"]) == ADMIN_ID:
        status = "⚡ Operator · Lookups unlimited"
    else:
        status = "Free Desk"
    handle = f"@{_e(u['username'])}\n" if u.get("username") else ""
    link = f"{BOT_LINK}?start=ref_{u['id']}"
    streak = int(u.get("streak_count") or 0)
    role = _e(str(u.get("role") or "USER"))
    return (
        f"{'👑 ' if gold else '👤 '}<b>{_e(u.get('name') or 'Bhai')}</b>\n"
        f"{handle}🆔 <code>{u['id']}</code>\n"
        f"🔰 Role: <b>{role}</b>\n"
        f"⭐ Plan: <b>{status}</b>\n\n"
        f"🔍 Total Searches: <b>{int(u.get('searches') or 0)}</b>\n"
        f"📊 Aaj ki limit: <b>{_e(str(allowance))}</b>\n"
        f"🔥 Streak: <b>{streak}</b> / {STREAK_INTERVAL}\n"
        f"👥 Referrals: <b>{int(u.get('referral_count') or 0)}</b>\n\n"
        f"💡 <i>5 logon ko invite kar ke 1 din ka premium free le!</i>\n"
        f"🔗 <code>{_e(link)}</code>\n\n{_D}"
    )


def msg_history(u: dict) -> str:
    hist = list(u.get("history") or [])[-10:]
    if not hist:
        return f"<b>📜 Search History</b>\n\nAbhi tak koi search nahi hui bhai.\n\n{_D}"
    lines = ["<b>📜 Pichli Search Records</b>", ""]
    for item in reversed(hist):
        found = item.get("found")
        tag = "❌ 0 match" if found == 0 else (f"✅ {found} match{'es' if found != 1 else ''}" if isinstance(found, int) else "✅ Done")
        lines.append(f"• <code>{_e(str(item.get('query', '')))}</code> · {_e(str(item.get('at', '')))} · {tag}")
    lines += ["", _D]
    return "\n".join(lines)


def msg_pricing(u: dict) -> str:
    if _utr_locked(int(u["id"])):
        left = fmt_td(int(_utr_lock_until(int(u["id"])) - time.time()))
        return f"<b>⚠️ Premium locked</b>\n\nZyada invalid UTR attempts ki wajah se locked hai. {_e(left)} baad aana.\n\n{_D}"
    gates = []
    if _gw_on("upi"):
        gates.append("UPI")
    if _gw_on("stars"):
        gates.append("Stars")
    if _gw_on("crypto"):
        gates.append("Crypto")
    gate_line = ", ".join(gates) if gates else "Temporarily unavailable"
    if _premium_active(u):
        head = (
            f"👑 <b>Premium Active Hai!</b>\n"
            f"Expiry: <b>{_e(fmt_until(float(u.get('premium_until') or 0)))}</b>\n\n"
            f"Extend karna ho toh neeche se pass choose kar.\nPayment Options: <b>{gate_line}</b>."
        )
    else:
        head = (
            "👑 <b>KaramChand Premium Pass</b>\n\n"
            "• Unlimited Lookups bina kisi rukawat ke\n"
            "• High priority queue execution\n"
            "• Multi-source combined reports & HTML downloads\n\n"
            f"Supported Payments: <b>{gate_line}</b>"
        )
    return f"{head}\n\nApna pass choose kar:\n\n{_D}"


def msg_support_prompt() -> str:
    return (
        f"🆘 <b>KaramChand Desk Support</b>\n\n"
        f"Apni dikkat ya sawal neeche likh kar bhej. Direct ticket banega aur support team reply karegi.\n\n{_D}"
    )


def msg_force() -> str:
    lines = ["<b>⚠️ Channel Join Required</b>", "", "Bot use karne ke liye neeche diye gaye channels join karna zaroori hai:", ""]
    for ch in FORCE_CHANNELS:
        lines.append(f"• {_e(str(ch))}")
    lines += ["", "Join karne ke baad <b>Verify Join ✅</b> daba.", "", _D]
    return "\n".join(lines)


def msg_help() -> str:
    return (
        f"ℹ️ <b>KaramChand Help & Instructions</b>\n\n"
        f"• 10-digit phone number direct chat mein bhej kar search kar sakte ho.\n"
        f"• Free users ko daily <b>{_free_limit()}</b> lookups milte hain.\n"
        f"• Premium users ke paas unlimited lookups hain.\n"
        f"• Sensitive fields (phone, aadhar, alt numbers) spoiler format mein aate hain; tap karke dekh sakte ho.\n\n{_D}"
    )


def msg_queue(pos: int, query: str) -> str:
    bar = q_bar(pos)
    if pos == 1:
        return f"⚡ <b>Teri baari hai bhai!</b>\n{bar}\n🔎 <code>{_e(query)}</code>\nData scan chalu hai..."
    return (
        f"⏳ <b>Line mein ho: #{pos}</b>\n{bar}\n"
        f"🔎 <code>{_e(query)}</code>\nEstimated wait: ~{pos * 2}s"
    )


def msg_searching(query: str) -> str:
    return f"⚡ <b>Searching Multi-Source KC Repositories...</b>\n🔎 <code>{_e(query)}</code>"


def msg_stage(query: str, label: str) -> str:
    return f"⚡ <b>{label}</b>\n🔎 <code>{_e(query)}</code>"


def msg_no_result(query: str) -> str:
    return f"<b>❌ Koi Record Nahi Mila</b>\n🔎 Target: <code>{_e(query)}</code>\n\nIs number ka koi entry dono directories mein match nahi hua.\n\n{_D}"


def msg_limit(u: dict) -> str:
    return (
        f"<b>⚠️ Aaj ke free lookups khatam ho gaye!</b>\n\n"
        f"Free users ko roz {_free_limit()} searches milte hain. Premium pass le kar unlimited searches unlock kar sakte ho.\n\n"
        f"Total lookups: <b>{int(u.get('searches') or 0)}</b>\n\n{_D}"
    )


def msg_spin(u: dict, pending: str | None) -> str:
    from kc.spin import _countdown, _jackpots_left
    left = _jackpots_left()
    held = f"\n\n🎯 Target <code>{_e(pending)}</code> paused. Spin ke baad scan shuru hoga." if pending else ""
    return (
        f"🎰 <b>KaramChand Daily Lucky Spin</b>\n\n"
        f"Agla spin window: <b>{_e(_countdown())}</b>\n"
        f"Jackpot winners remaining: <b>{left}</b> / {SPIN_WINNER_LIMIT}\n\n"
        f"Har din ek spin! Reels ghuma aur jeeto free lookups ya premium pass.{held}\n\n{_D}"
    )


def msg_welcome(name: str) -> str:
    return msg_dashboard({
        "id": 0, "name": name, "is_premium": False, "premium_until": 0,
        "daily_searches": 0, "last_search_date": "", "searches": 0,
    })
