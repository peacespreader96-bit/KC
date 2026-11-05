"""Admin and agent single-edit desk UI, sections, navigation, and actions."""
import csv
import io
import os
import time

from kc import shared
from kc.admin_cmds import _ban_history_lines, _busy_gateway, _set_ban
from kc.config import ADMIN_ID, BASE_URL, _BACKUP_DIR, _BAN_TEXT
from kc.duckdb_conn import configure_system_settings
from kc.logging_setup import log
from kc.payments import _settle_payment
from kc.result_ui import _save_receipt, generate_receipt_html
from kc.sqlite_db import (
    _blank_user,
    _kv_get,
    _load_user,
)
from kc.telegram_api import (
    _session,
    edit_msg,
    send_html_file,
    send_msg,
    tg,
)
from kc.tickets import (
    _add_reply,
    _claim_ticket,
    _resolve_ticket,
    _ticket,
    _ticket_replies,
)
from kc.ui import msg_profile
from kc.user_ops import _free_limit, _gw_on, _plans
from kc.utils import _D, _clip, _e, fmt_ts, ts_str


def _backup_files() -> list[str]:
    if not os.path.isdir(_BACKUP_DIR):
        return []
    files = [os.path.join(_BACKUP_DIR, n) for n in os.listdir(_BACKUP_DIR) if n.endswith(".db")]
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files


def _db_role(cid: int) -> str:
    if cid == ADMIN_ID:
        return "SUPER_ADMIN"
    if shared._sql is None:
        return "USER"
    try:
        with shared._sql_lock:
            row = shared._sql.execute("SELECT role FROM users WHERE chat_id = ?", (int(cid),)).fetchone()
    except Exception:
        return "USER"
    if not row:
        return "USER"
    role = str(row["role"] or "USER").upper()
    return role if role in ("FINANCE_MANAGER", "SUPPORT_AGENT") else "USER"


def _can_desk(cid: int) -> bool:
    return cid == ADMIN_ID or _db_role(cid) in ("FINANCE_MANAGER", "SUPPORT_AGENT")


def _can_settle(cid: int) -> bool:
    return cid == ADMIN_ID or _db_role(cid) == "FINANCE_MANAGER"


def _desk_sections(cid: int) -> list[str]:
    if cid == ADMIN_ID:
        return ["bak", "tik", "usr", "set", "inv"]
    role = _db_role(cid)
    if role == "FINANCE_MANAGER":
        return ["inv", "usr"]
    if role == "SUPPORT_AGENT":
        return ["tik"]
    return []


def _desk_nav(cid: int) -> list:
    labels = {
        "bak": "📁 Backups", "tik": "🎫 Tickets", "usr": "👥 Users",
        "set": "⚙️ Settings", "inv": "📜 Invoices",
    }
    row: list = []
    rows: list = []
    for key in _desk_sections(cid):
        row.append({"text": labels[key], "callback_data": f"d:{key}"})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def _render_home(cid: int) -> tuple[str, list]:
    if shared._sql is None:
        text = "<b>Admin desk</b>\n\nBhai users.db gayab hai. Empty file nahi banaunga.\n/restore karke .db document bhej.\n\n" + _D
        return text, _desk_nav(cid)
    now = time.time()
    with shared._sql_lock:
        total = int(shared._sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        open_t = int(shared._sql.execute("SELECT COUNT(*) AS n FROM tickets WHERE status = 'open'").fetchone()["n"])
        claimed = int(shared._sql.execute("SELECT COUNT(*) AS n FROM tickets WHERE status = 'claimed'").fetchone()["n"])
        pend = int(shared._sql.execute("SELECT COUNT(*) AS n FROM invoices WHERE status = 'pending'").fetchone()["n"])
        premium = int(shared._sql.execute(
            "SELECT COUNT(*) AS n FROM users WHERE is_premium = 1 AND premium_until > ?",
            (now,),
        ).fetchone()["n"])
    maint = "ON — log bahar hain" if shared.MAINTENANCE else "OFF"
    text = (
        f"<b>Kya scene hai operator?</b>\n"
        f"{ts_str()}\n\n"
        f"Maintenance · <b>{maint}</b>\n"
        f"Users · <b>{total}</b> · premium <b>{premium}</b>\n"
        f"Tickets · open <b>{open_t}</b> · locked <b>{claimed}</b>\n"
        f"Invoices pending · <b>{pend}</b>\n"
        f"Free limit · <b>{_free_limit()}</b>\n\n"
        f"Neeche ek hi message hai. Button daba, yahin edit hoga. Chat mat bhar.\n\n{_D}"
    )
    return _clip(text), _desk_nav(cid)


def _render_backups(cid: int) -> tuple[str, list]:
    files = _backup_files()[:8]
    lines = ["<b>📁 Backups</b>", "", "Auto backup chat mein nahi aata. Yahan se ek khol.", ""]
    kb = []
    if not files:
        lines.append("Abhi koi backup file nahi padi.")
    for i, path in enumerate(files):
        mtime = fmt_ts(os.path.getmtime(path))
        lines.append(f"• <code>{_e(os.path.basename(path))}</code> · {mtime}")
        kb.append([{"text": f"Bhej {i + 1}", "callback_data": f"d:bk:{i}"}])
    kb.append([{"text": "🔙 Wapas", "callback_data": "d:home"}])
    return _clip("\n".join(lines) + f"\n\n{_D}"), kb


def _render_tickets(cid: int, page: int) -> tuple[str, list]:
    if shared._sql is None:
        return "Bhai DB missing hai. Ticket kahan se aayega?", [[{"text": "🔙 Wapas", "callback_data": "d:home"}]]
    page = max(0, page)
    size = 4
    with shared._sql_lock:
        total = int(shared._sql.execute("SELECT COUNT(*) AS n FROM tickets").fetchone()["n"])
        rows = shared._sql.execute(
            "SELECT * FROM tickets ORDER BY CASE status WHEN 'open' THEN 0 WHEN 'claimed' THEN 1 ELSE 2 END, id DESC LIMIT ? OFFSET ?",
            (size, page * size),
        ).fetchall()
    lines = [f"<b>🎫 Tickets</b> · page {page + 1}", ""]
    kb = []
    if not rows:
        lines.append("Koi ticket nahi bhai. Shanti hai.")
    for row in rows:
        owner = int(row["agent_id"] or 0)
        lock = f" · lock <code>{owner}</code>" if owner else ""
        lines.append(f"• #{row['id']} · {_e(row['status'])}{lock} · user <code>{row['chat_id']}</code> · {fmt_ts(row['created_at'])}")
        kb.append([{"text": f"Khol #{row['id']}", "callback_data": f"d:op:{row['id']}"}])
    nav = []
    if page > 0:
        nav.append({"text": "◀️", "callback_data": f"d:tik:{page - 1}"})
    if (page + 1) * size < total:
        nav.append({"text": "▶️", "callback_data": f"d:tik:{page + 1}"})
    if nav:
        kb.append(nav)
    kb.append([{"text": "🔙 Wapas", "callback_data": "d:home"}])
    return _clip("\n".join(lines) + f"\n\n{_D}"), kb


def _render_ticket(cid: int, tid: int) -> tuple[str, list]:
    row = _ticket(tid)
    if row is None:
        return "Bhai ye ticket gayab hai.", [[{"text": "🔙 Wapas", "callback_data": "d:tik"}]]
    owner = int(row["agent_id"] or 0)
    user = _load_user(int(row["chat_id"]))
    uname = (user or {}).get("name") or "Bhai"
    uhandle = ("@" + user["username"]) if user and user.get("username") else "username nahi"
    lines = [
        f"<b>🎫 Ticket #{row['id']}</b>",
        f"Status · <b>{_e(row['status'])}</b>",
        f"User · {_e(uname)} ({_e(uhandle)})",
        f"ID · <code>{row['chat_id']}</code>",
        f"Agent · <code>{owner or '—'}</code>",
        f"Khula · {fmt_ts(row['created_at'])}",
        f"Band · {fmt_ts(row['resolved_at'])}",
        "",
        "<b>Note</b>",
        _e((row["agent_notes"] or "")[:800] or "—"),
        "",
        "<b>Replies</b>",
    ]
    replies = list(reversed(_ticket_replies(tid, 6)))
    if not replies:
        lines.append("Abhi koi reply nahi.")
    for rep in replies:
        lines.append(f"• {fmt_ts(rep['created_at'])} · <code>{rep['agent_id']}</code> · {_e((rep['body'] or '')[:180])}")
    kb = []
    if row["status"] == "open" and _db_role(cid) in ("SUPPORT_AGENT", "SUPER_ADMIN"):
        kb.append([{"text": "Claim Ticket", "callback_data": f"d:cl:{tid}"}])
    elif row["status"] == "claimed" and owner == cid:
        kb.append([
            {"text": "Reply", "callback_data": f"d:rp:{tid}"},
            {"text": "Resolve", "callback_data": f"d:rs:{tid}"},
        ])
    elif row["status"] == "claimed" and owner != cid:
        lines += ["", "Bhai ye ticket kisi aur pe lock hai. Interfere mat kar. Super admin /removerole se unfreeze kare."]
    kb.append([{"text": "🔙 Wapas tickets", "callback_data": "d:tik"}])
    return _clip("\n".join(lines) + f"\n\n{_D}"), kb


def _render_users(cid: int, page: int) -> tuple[str, list]:
    if shared._sql is None:
        return "Bhai DB missing hai.", [[{"text": "🔙 Wapas", "callback_data": "d:home"}]]
    page = max(0, page)
    size = 6
    with shared._sql_lock:
        total = int(shared._sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        rows = shared._sql.execute(
            "SELECT chat_id, name, username, role, is_premium, is_banned FROM users ORDER BY chat_id DESC LIMIT ? OFFSET ?",
            (size, page * size),
        ).fetchall()
    lines = [f"<b>👥 Users</b> · page {page + 1} · {total}", ""]
    kb = []
    for row in rows:
        flag = "BAN" if int(row["is_banned"] or 0) else ("PREM" if row["is_premium"] else "free")
        lines.append(f"• {_e(row['name'] or 'Bhai')} · <code>{row['chat_id']}</code> · {_e(row['role'] or 'USER')} · {flag}")
        buttons = [{"text": f"Info {row['chat_id']}", "callback_data": f"d:ui:{row['chat_id']}"}]
        if cid == ADMIN_ID and int(row["chat_id"]) != ADMIN_ID:
            if int(row["is_banned"] or 0):
                buttons.append({"text": "Unban", "callback_data": f"d:ub:{row['chat_id']}"})
            else:
                buttons.append({"text": "Ban", "callback_data": f"d:bn:{row['chat_id']}"})
        kb.append(buttons)
    nav = []
    if page > 0:
        nav.append({"text": "◀️", "callback_data": f"d:usr:{page - 1}"})
    if (page + 1) * size < total:
        nav.append({"text": "▶️", "callback_data": f"d:usr:{page + 1}"})
    if nav:
        kb.append(nav)
    kb.append([{"text": "🔙 Wapas", "callback_data": "d:home"}])
    return _clip("\n".join(lines) + f"\n\n{_D}"), kb


def _render_user(cid: int, target: int) -> tuple[str, list]:
    user = _load_user(target)
    if user is None:
        return "Bhai ye user file pe nahi.", [[{"text": "🔙 Wapas", "callback_data": "d:usr"}]]
    text = msg_profile(user) + "\n<b>Ban history</b>\n" + "\n".join(_ban_history_lines(target, 8))
    kb = []
    if cid == ADMIN_ID and target != ADMIN_ID:
        if int(user.get("is_banned") or 0):
            kb.append([{"text": "Unban", "callback_data": f"d:ub:{target}"}])
        else:
            kb.append([{"text": "Ban", "callback_data": f"d:bn:{target}"}])
    kb.append([{"text": "🔙 Wapas users", "callback_data": "d:usr"}])
    return _clip(text), kb


def _render_settings(cid: int) -> tuple[str, list]:
    sys_cfg = configure_system_settings()
    lines = [
        "<b>⚙️ Settings & System Tuning</b>", "",
        f"Free daily limit · <b>{_free_limit()}</b>",
        "",
        "<b>Manual System Architecture Settings</b>",
        f"• RAM · <code>{_e(str(sys_cfg['ram_limit']))}</code> · Threads · <code>{sys_cfg['cpu_threads']}</code>",
        f"• Concurrent Sources · <code>{sys_cfg['concurrent_sources']} / 7</code>",
        f"• Query Timeout · <code>{sys_cfg['search_timeout']}s</code> · Queue · <code>{sys_cfg['max_queue_size']}</code>",
        f"• HTTP Timeout · <code>{sys_cfg['http_timeout_ms']}ms</code>",
        "Tune command · /sysconfig",
        "",
        "<b>Gateways</b>"
    ]
    for gw, title in (("upi", "UPI"), ("stars", "Stars"), ("crypto", "Crypto")):
        busy = _busy_gateway(gw)
        lines.append(f"• {title} · <b>{'ON' if _gw_on(gw) else 'OFF'}</b> · flow mein {len(busy)}")
    lines += ["", "<b>Plans</b>"]
    for pid, plan in _plans().items():
        lines.append(f"• {pid} · {_e(str(plan.get('name')))} · ₹{plan.get('amount')} · {plan.get('days')}d · {plan.get('stars', plan.get('amount'))} stars")
    lines += [
        "",
        "Limit badalne ko /setlimit number.",
        "Plan badalne ko /editplan id amount days [stars] [name].",
        "Price badli toh bot khud broadcast poochega.",
        "",
        _D,
    ]
    kb = []
    if cid == ADMIN_ID:
        for gw, title in (("upi", "UPI"), ("stars", "Stars"), ("crypto", "Crypto")):
            label = f"{title} {'band kar' if _gw_on(gw) else 'chalu kar'}"
            kb.append([{"text": label, "callback_data": f"gwtog:{gw}"}])
    kb.append([{"text": "🔙 Wapas", "callback_data": "d:home"}])
    return _clip("\n".join(lines)), kb


def _render_invoices(cid: int, page: int) -> tuple[str, list]:
    if shared._sql is None:
        return "Bhai DB missing hai.", [[{"text": "🔙 Wapas", "callback_data": "d:home"}]]
    page = max(0, page)
    size = 5
    with shared._sql_lock:
        total = int(shared._sql.execute("SELECT COUNT(*) AS n FROM invoices").fetchone()["n"])
        rows = shared._sql.execute(
            "SELECT * FROM invoices ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (size, page * size),
        ).fetchall()
    lines = [f"<b>📜 Invoices</b> · page {page + 1}", ""]
    kb = []
    if not rows:
        lines.append("Abhi koi invoice nahi.")
    for row in rows:
        lines.append(
            f"• <code>{_e(row['invoice_id'])}</code> · {_e(row['gateway'] or '')} · ₹{row['amount']} · "
            f"{_e(row['status'])} · {fmt_ts(row['paid_at'] or row['created_at'])}"
        )
        buttons = [{"text": f"Receipt {row['invoice_id']}", "callback_data": f"d:rc:{row['invoice_id']}"}]
        if row["status"] == "pending" and _can_settle(cid):
            with shared._sql_lock:
                pay = shared._sql.execute(
                    "SELECT id FROM payments WHERE invoice_id = ? AND status = 'pending'",
                    (row["invoice_id"],),
                ).fetchone()
            if pay:
                buttons = [
                    {"text": "Approve", "callback_data": f"d:ap:{pay['id']}"},
                    {"text": "Reject", "callback_data": f"d:rj:{pay['id']}"},
                ]
        kb.append(buttons)
    nav = []
    if page > 0:
        nav.append({"text": "◀️", "callback_data": f"d:inv:{page - 1}"})
    if (page + 1) * size < total:
        nav.append({"text": "▶️", "callback_data": f"d:inv:{page + 1}"})
    if nav:
        kb.append(nav)
    kb.append([{"text": "🔙 Wapas", "callback_data": "d:home"}])
    return _clip("\n".join(lines) + f"\n\n{_D}"), kb


def _render_desk(cid: int, section: str, page: int = 0, focus: int | None = None) -> tuple[str, list]:
    allowed = set(_desk_sections(cid)) | {"home"}
    if section not in ("ticket", "user") and section not in allowed and section != "home":
        section = "home"
    if section == "bak":
        return _render_backups(cid)
    if section == "tik":
        return _render_tickets(cid, page)
    if section == "ticket" and focus:
        return _render_ticket(cid, focus)
    if section == "usr":
        return _render_users(cid, page)
    if section == "user" and focus:
        return _render_user(cid, focus)
    if section == "set":
        return _render_settings(cid)
    if section == "inv":
        return _render_invoices(cid, page)
    return _render_home(cid)


def _desk_show(cid: int, mid: int | None, section: str, page: int = 0, focus: int | None = None) -> None:
    shared._desk[cid] = {"mid": mid, "section": section, "page": page, "focus": focus}
    text, kb = _render_desk(cid, section, page, focus)
    if mid:
        edit_msg(cid, mid, text, kb)
        return
    new_mid = send_msg(cid, text, kb)
    if new_mid:
        shared._desk[cid]["mid"] = new_mid


def _open_desk(cid: int, section: str = "home") -> None:
    if not _can_desk(cid):
        send_msg(cid, "Bhai ye command teri post ki nahi hai.")
        return
    if section not in set(_desk_sections(cid)) | {"home"}:
        section = "home"
    st = shared._desk.get(cid) or {}
    mid = st.get("mid")
    text, kb = _render_desk(cid, section, 0, None)
    shared._desk[cid] = {"mid": mid, "section": section, "page": 0, "focus": None}
    if mid:
        res = tg("editMessageText", {
            "chat_id": cid, "message_id": mid, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True,
            "reply_markup": {"inline_keyboard": kb},
        })
        desc = str(res.get("description") or "")
        if res.get("ok") or "message is not modified" in desc:
            return
    new_mid = send_msg(cid, text, kb)
    if new_mid:
        shared._desk[cid]["mid"] = new_mid


def _poke_desks() -> None:
    for cid, st in list(shared._desk.items()):
        mid = st.get("mid")
        section = st.get("section") or "home"
        if not mid or section not in ("home", "tik", "inv", "ticket"):
            continue
        try:
            text, kb = _render_desk(cid, section, int(st.get("page") or 0), st.get("focus"))
            edit_msg(cid, mid, text, kb)
        except Exception:
            log.exception("desk poke failed")


def _send_backup(cid: int, idx: int) -> None:
    files = _backup_files()
    if idx < 0 or idx >= len(files):
        send_msg(cid, "Bhai ye backup nahi mila.")
        return
    path = files[idx]
    try:
        with open(path, "rb") as fh:
            blob = fh.read()
        _session.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": str(cid), "caption": "📁 Backup · " + os.path.basename(path)},
            files={"document": (os.path.basename(path), io.BytesIO(blob), "application/x-sqlite3")},
            timeout=60,
        )
    except Exception:
        log.exception("backup send failed")


def _resend_receipt(cid: int, inv: str) -> None:
    path = _kv_get(f"rcpt:{inv}", "")
    if path and os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as fh:
            html = fh.read()
        send_html_file(cid, f"{inv}.html", html, f"📜 Receipt copy · <code>{_e(inv)}</code>")
        return
    if shared._sql is None:
        send_msg(cid, "Bhai receipt file bhi nahi, DB bhi nahi.")
        return
    with shared._sql_lock:
        row = shared._sql.execute("SELECT * FROM invoices WHERE invoice_id = ?", (inv,)).fetchone()
    if not row:
        send_msg(cid, "Bhai ye invoice nahi mili.")
        return
    user = _load_user(int(row["chat_id"])) or _blank_user(int(row["chat_id"]), "Bhai", "")
    plan = _plans().get(row["plan_id"]) or {"id": row["plan_id"], "name": row["plan_id"], "amount": row["amount"], "days": 0, "stars": 0}
    when = fmt_ts(row["paid_at"] or row["created_at"])
    html = generate_receipt_html(user, plan, when, "—", "", str(row["gateway"] or ""), inv)
    _save_receipt(inv, html)
    send_html_file(cid, f"{inv}.html", html, f"📜 Receipt copy · <code>{_e(inv)}</code>")


def _cmd_mywork(cid: int) -> None:
    role = _db_role(cid)
    if role not in ("SUPPORT_AGENT", "SUPER_ADMIN"):
        send_msg(cid, "Bhai /mywork sirf support agent ka scene hai.")
        return
    if shared._sql is None:
        send_msg(cid, "Bhai DB missing hai.")
        return
    with shared._sql_lock:
        rows = shared._sql.execute("SELECT * FROM tickets WHERE agent_id = ? ORDER BY id", (cid,)).fetchall()
        reply_n = int(shared._sql.execute(
            "SELECT COUNT(*) AS n FROM ticket_replies WHERE agent_id = ?", (cid,),
        ).fetchone()["n"])
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["ticket_id", "chat_id", "status", "created_at_ist", "resolved_at_ist", "notes"])
    resolved = 0
    for row in rows:
        if row["status"] == "resolved":
            resolved += 1
        writer.writerow([
            row["id"], row["chat_id"], row["status"], fmt_ts(row["created_at"]),
            fmt_ts(row["resolved_at"]), (row["agent_notes"] or "").replace("\n", " ")[:300],
        ])
    data = buf.getvalue().encode("utf-8")
    caption = f"📋 Tera kaam · tickets {len(rows)} · resolved {resolved} · replies {reply_n}"
    try:
        _session.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": str(cid), "caption": caption},
            files={"document": ("mywork.csv", io.BytesIO(data), "text/csv")},
            timeout=25,
        )
    except Exception:
        log.exception("mywork failed")


def _cmd_data(cid: int) -> None:
    role = _db_role(cid)
    if role not in ("SUPPORT_AGENT", "SUPER_ADMIN"):
        send_msg(cid, "Bhai /data sirf ticket ke andar agent ke liye hai.")
        return
    st = shared.flow.get(cid) or {}
    desk = shared._desk.get(cid) or {}
    tid = st.get("ticket_id")
    if not tid and desk.get("section") == "ticket":
        tid = desk.get("focus")
    if not tid:
        send_msg(cid, "Bhai pehle ticket khol, phir /data maar. Bahar se kisi ka profile nahi dunga.")
        return
    row = _ticket(int(tid))
    if row is None:
        send_msg(cid, "Bhai ticket gayab hai.")
        return
    if row["status"] == "claimed" and int(row["agent_id"] or 0) != cid:
        if not (cid == ADMIN_ID or _db_role(cid) == "SUPER_ADMIN"):
            send_msg(cid, "Bhai ye ticket doosre agent pe lock hai. Interfere nahi chalega. /removerole ke baad super admin dekh sakta hai.")
            return
    user = _load_user(int(row["chat_id"]))
    if user is None:
        send_msg(cid, "Bhai jisne ticket kholi woh file pe nahi mila.")
        return
    send_msg(cid, "<b>Ticket wala profile — sirf yahi</b>\n\n" + msg_profile(user))



def _handle_desk_cb(cid: int, mid: int, data: str) -> None:
    parts = data[2:].split(":")
    head = parts[0] if parts else "home"
    arg = parts[1] if len(parts) > 1 else ""
    if head == "home":
        _desk_show(cid, mid, "home")
        return
    if head == "bak" and "bak" in _desk_sections(cid):
        _desk_show(cid, mid, "bak")
        return
    if head == "tik" and "tik" in _desk_sections(cid):
        page = int(arg) if arg.isdigit() else 0
        _desk_show(cid, mid, "tik", page)
        return
    if head == "usr" and "usr" in _desk_sections(cid):
        page = int(arg) if arg.isdigit() else 0
        _desk_show(cid, mid, "usr", page)
        return
    if head == "set" and "set" in _desk_sections(cid):
        _desk_show(cid, mid, "set")
        return
    if head == "inv" and "inv" in _desk_sections(cid):
        page = int(arg) if arg.isdigit() else 0
        _desk_show(cid, mid, "inv", page)
        return
    if head == "op" and arg.isdigit() and "tik" in _desk_sections(cid):
        tid = int(arg)
        shared.flow[cid] = {"mode": "IN_TICKET", "ticket_id": tid}
        _desk_show(cid, mid, "ticket", focus=tid)
        return
    if head == "cl" and arg.isdigit():
        if _db_role(cid) not in ("SUPPORT_AGENT", "SUPER_ADMIN"):
            send_msg(cid, "Bhai claim sirf support agent ya super admin karega.")
            return
        result = _claim_ticket(int(arg), cid)
        if result == "locked":
            send_msg(cid, "Bhai ye ticket pehle se kisi pe lock hai. Haath mat laga.")
        elif result == "missing":
            send_msg(cid, "Bhai ticket gayab hai.")
        else:
            shared.flow[cid] = {"mode": "IN_TICKET", "ticket_id": int(arg)}
        _desk_show(cid, mid, "ticket", focus=int(arg))
        _poke_desks()
        return
    if head == "rs" and arg.isdigit():
        ok = _resolve_ticket(int(arg), cid)
        if not ok:
            send_msg(cid, "Bhai resolve nahi hua. Ya toh lock teri nahi, ya ticket pehle hi band hai.")
        else:
            shared.flow[cid] = {"mode": "IN_TICKET", "ticket_id": int(arg)}
            row = _ticket(int(arg))
            if row:
                send_msg(int(row["chat_id"]), "Bhai tera support ticket resolve ho gaya. Aur dikkat ho toh naya likh dena.")
        _desk_show(cid, mid, "ticket", focus=int(arg))
        return
    if head == "rp" and arg.isdigit():
        row = _ticket(int(arg))
        if not row or row["status"] != "claimed" or int(row["agent_id"] or 0) != cid:
            send_msg(cid, "Bhai reply sirf jisne claim kiya woh kar sakta hai.")
            return
        shared.flow[cid] = {"mode": "TICKET_REPLY", "ticket_id": int(arg)}
        _desk_show(cid, mid, "ticket", focus=int(arg))
        send_msg(cid, "Agli message is ticket ka reply hai. /cancel se nikal.")
        return
    if head == "ui" and arg.isdigit() and "usr" in _desk_sections(cid):
        _desk_show(cid, mid, "user", focus=int(arg))
        return
    if head == "bn" and arg.isdigit() and cid == ADMIN_ID:
        target = int(arg)
        if target == ADMIN_ID:
            return
        _set_ban(target, True, cid, "dashboard")
        send_msg(target, _BAN_TEXT)
        _desk_show(cid, mid, "user", focus=target)
        return
    if head == "ub" and arg.isdigit() and cid == ADMIN_ID:
        target = int(arg)
        _set_ban(target, False, cid, "dashboard unban")
        send_msg(target, "Bhai ban hat gaya. Desk wapas khul gayi.")
        _desk_show(cid, mid, "user", focus=target)
        return
    if head == "bk" and arg.isdigit() and cid == ADMIN_ID:
        _send_backup(cid, int(arg))
        return
    if head == "rc" and arg:
        _resend_receipt(cid, arg)
        return
    if head == "ap" and _can_settle(cid):
        _settle_payment(arg, True, None, actor=cid)
        _desk_show(cid, mid, "inv", int((shared._desk.get(cid) or {}).get("page") or 0))
        return
    if head == "rj" and _can_settle(cid):
        _settle_payment(arg, False, None, actor=cid)
        _desk_show(cid, mid, "inv", int((shared._desk.get(cid) or {}).get("page") or 0))
        return
    _desk_show(cid, mid, "home")


# Up-call registration
shared._poke_desks_fn = _poke_desks
