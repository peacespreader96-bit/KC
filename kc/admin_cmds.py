"""Admin console commands, system config tuning, role management, and audits."""
import csv
import io
import json
import time

from kc import shared
from kc.config import (
    ADMIN_ID,
    BASE_URL,
    _BAN_TEXT,
    _GW_MODES,
    _PAY_TTL,
    _ROLES_ASSIGNABLE,
)
from kc.duckdb_conn import configure_system_settings
from kc.logging_setup import _ring, log
from kc.sqlite_db import (
    _kv_get,
    _kv_set,
    _load_user,
    _restore_db,
    _save_user,
)
from kc.telegram_api import _download_tg_file, _session, send_msg
from kc.ui import msg_profile
from kc.user_ops import (
    _clear_flow,
    _extend_premium,
    _free_limit,
    _get_ud,
    _gw_on,
    _plans,
)
from kc.utils import _D, _clip, _e, fmt_ts, fmt_until


def _flow_gateway(st: dict) -> str | None:
    if st.get("gateway") in ("upi", "stars", "crypto"):
        return st["gateway"]
    mode = st.get("mode")
    for gw, modes in _GW_MODES.items():
        if mode in modes:
            return gw
    return None


def _flow_fresh(st: dict) -> bool:
    ts = float(st.get("ts") or 0)
    return True if not ts else time.time() - ts <= _PAY_TTL


def _busy_gateway(gw: str) -> list[int]:
    return [int(cid) for cid, st in list(shared.flow.items()) if _flow_fresh(st) and _flow_gateway(st) == gw]


def admin_stats_text() -> str:
    if shared._sql is None:
        return "<b>Admin desk</b>\n\nBhai DB missing hai. /restore kar. Empty DB nahi banaunga."
    now = time.time()
    with shared._sql_lock:
        total = int(shared._sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        premium = int(shared._sql.execute(
            "SELECT COUNT(*) AS n FROM users WHERE is_premium = 1 AND premium_until > ?",
            (now,),
        ).fetchone()["n"])
        searches = int(shared._sql.execute("SELECT COALESCE(SUM(total_searches), 0) AS n FROM users").fetchone()["n"])
        banned = int(shared._sql.execute("SELECT COUNT(*) AS n FROM users WHERE COALESCE(is_banned, 0) = 1").fetchone()["n"])
        open_t = int(shared._sql.execute("SELECT COUNT(*) AS n FROM tickets WHERE status = 'open'").fetchone()["n"])
        pend = int(shared._sql.execute("SELECT COUNT(*) AS n FROM invoices WHERE status = 'pending'").fetchone()["n"])
    flag = "ON" if shared.MAINTENANCE else "OFF"
    return (
        f"<b>Admin desk</b>\n\n"
        f"Maintenance · <b>{flag}</b>\n"
        f"Total users · <b>{total}</b>\n"
        f"Free wale · <b>{max(0, total - premium)}</b>\n"
        f"Premium wale · <b>{premium}</b>\n"
        f"Banned · <b>{banned}</b>\n"
        f"Open tickets · <b>{open_t}</b>\n"
        f"Pending invoices · <b>{pend}</b>\n"
        f"Total searches · <b>{searches}</b>\n"
        f"Free limit · <b>{_free_limit()}</b>\n\n"
        f"Commands · /dashboard /sysconfig /grant /ban /unban /userinfo /assignrole /removerole /role /setlimit /editplan /payments /restore /broadcast /csv /logs"
    )


def _cmd_sysconfig(cid: int, raw: str) -> None:
    parts = raw.split()
    cfg = configure_system_settings()
    if len(parts) == 1:
        text = (
            "<b>⚙️ Manual System Settings (Host Architecture)</b>\n"
            "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"🧠 <b>DuckDB RAM Limit:</b> <code>{_e(str(cfg['ram_limit']))}</code>\n"
            f"⚡ <b>DuckDB CPU Threads:</b> <code>{cfg['cpu_threads']}</code>\n"
            f"🚦 <b>Concurrent Sources to Search:</b> <code>{cfg['concurrent_sources']} / 7</code>\n"
            f"⏱ <b>Query Timeout:</b> <code>{cfg['search_timeout']}s</code>\n"
            f"📥 <b>Search Queue Limit:</b> <code>{cfg['max_queue_size']}</code>\n"
            f"🌐 <b>HTTP Timeout:</b> <code>{cfg['http_timeout_ms']}ms</code>\n\n"
            "<b>🛠️ Manual Tuning Commands:</b>\n"
            "• <code>/sysconfig ram 300MB</code>\n"
            "• <code>/sysconfig threads 1</code>\n"
            "• <code>/sysconfig concurrent 2</code>\n"
            "• <code>/sysconfig timeout 12</code>\n"
            "• <code>/sysconfig queue 15</code>\n"
            f"• <code>/sysconfig http_timeout 15000</code>\n\n{_D}"
        )
        send_msg(cid, text)
        return

    sub = parts[1].lower()
    if len(parts) < 3:
        send_msg(cid, "Format: /sysconfig [ram|threads|concurrent|timeout|queue|http_timeout] [value]")
        return

    val = parts[2]
    if sub == "ram":
        cfg = configure_system_settings(ram=val)
        send_msg(cid, f"✅ Manual RAM limit set to: <code>{_e(val)}</code>")
    elif sub == "threads":
        if not val.isdigit():
            send_msg(cid, "Threads positive integer hona chahiye.")
            return
        cfg = configure_system_settings(threads=int(val))
        send_msg(cid, f"✅ Manual CPU threads set to: <code>{_e(val)}</code>")
    elif sub in ("concurrent", "sources"):
        if not val.isdigit() or not (1 <= int(val) <= 7):
            send_msg(cid, "Concurrent sources 1 se 7 ke beech hona chahiye.")
            return
        cfg = configure_system_settings(concurrent_sources=int(val))
        send_msg(cid, f"✅ Manual concurrent sources set to: <code>{_e(val)} / 7</code>")
    elif sub in ("timeout", "time"):
        try:
            t_val = float(val)
        except ValueError:
            send_msg(cid, "Timeout number hona chahiye.")
            return
        cfg = configure_system_settings(timeout=t_val)
        send_msg(cid, f"✅ Manual search timeout set to: <code>{_e(val)}s</code>")
    elif sub in ("queue", "maxqueue"):
        if not val.isdigit():
            send_msg(cid, "Queue size integer hona chahiye.")
            return
        cfg = configure_system_settings(max_queue=int(val))
        send_msg(cid, f"✅ Manual search queue set to: <code>{_e(val)}</code>")
    elif sub in ("http_timeout", "httptimeout"):
        if not val.isdigit():
            send_msg(cid, "HTTP timeout ms integer hona chahiye.")
            return
        cfg = configure_system_settings(http_timeout_ms=int(val))
        send_msg(cid, f"✅ Manual HTTP timeout set to: <code>{_e(val)}ms</code>")
    else:
        send_msg(cid, "Unknown parameter. Options: ram, threads, concurrent, timeout, queue, http_timeout")


def _admin_grant(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        send_msg(cid, "Format ye hai bhai: /grant user_id days")
        return
    target = int(parts[1])
    days = int(parts[2])
    if days <= 0:
        send_msg(cid, "Days positive daal bhai. Negative premium nahi hota.")
        return
    with shared._sl:
        user = _get_ud(target)
        until = _extend_premium(user, days)
        _save_user(user)
    expiry = fmt_until(until)
    send_msg(target, f"👑 Bhai sun, operator ne tujhe <b>{days}</b> din ka premium de diya. Chalega <b>{_e(expiry)}</b> tak.")
    send_msg(cid, f"Ho gaya. <code>{target}</code> ko {days} din premium. <b>{_e(expiry)}</b> tak.")


def _log_ban(chat_id: int, action: str, admin_id: int, reason: str) -> None:
    if shared._sql is None:
        return
    with shared._sql_lock:
        shared._sql.execute(
            "INSERT INTO ban_history (chat_id, action, admin_id, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
            (int(chat_id), action, int(admin_id), time.time(), reason or ""),
        )
        shared._sql.commit()


def _set_ban(target: int, banned: bool, admin_id: int, reason: str) -> None:
    with shared._sl:
        user = _get_ud(target)
        user["is_banned"] = 1 if banned else 0
        _save_user(user)
    _log_ban(target, "ban" if banned else "unban", admin_id, reason)


def _admin_ban(cid: int, raw: str) -> None:
    parts = raw.split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        send_msg(cid, "Format: /ban user_id reason — itna bhi mushkil hai?")
        return
    target = int(parts[1])
    reason = parts[2] if len(parts) > 2 else "admin ban"
    if target == ADMIN_ID:
        send_msg(cid, "Apne aap ko ban karega? Dimag theek kar.")
        return
    _set_ban(target, True, cid, reason)
    send_msg(target, _BAN_TEXT)
    send_msg(cid, f"Ban lag gaya. <code>{target}</code> ab desk pe nahi aayega. Reason · {_e(reason)}")


def _admin_unban(cid: int, raw: str) -> None:
    parts = raw.split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        send_msg(cid, "Format: /unban user_id reason")
        return
    target = int(parts[1])
    reason = parts[2] if len(parts) > 2 else "admin unban"
    if _load_user(target) is None and target not in shared.user_data:
        send_msg(cid, "Bhai ye ID file pe nahi hai.")
        return
    _set_ban(target, False, cid, reason)
    send_msg(target, "Bhai ban hat gaya. Desk wapas khul gayi. Sharafat se chal.")
    send_msg(cid, f"Unban ho gaya. <code>{target}</code>. Reason · {_e(reason)}")


def _ban_history_lines(target: int, limit: int = 15) -> list[str]:
    if shared._sql is None:
        return ["DB missing hai bhai."]
    with shared._sql_lock:
        rows = shared._sql.execute(
            "SELECT action, admin_id, timestamp, reason FROM ban_history WHERE chat_id = ? ORDER BY id DESC LIMIT ?",
            (target, limit),
        ).fetchall()
    if not rows:
        return ["Ban history khaali hai."]
    return [
        f"• {_e(row['action'])} · {fmt_ts(row['timestamp'])} · admin <code>{row['admin_id']}</code> · {_e(row['reason'] or '—')}"
        for row in rows
    ]


def _admin_userinfo(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) != 2 or not parts[1].isdigit():
        send_msg(cid, "Format: /userinfo user_id")
        return
    target = int(parts[1])
    user = _load_user(target)
    if user is None:
        send_msg(cid, "Bhai ye ID file pe nahi hai.")
        return
    text = msg_profile(user) + "\n<b>Ban history</b>\n" + "\n".join(_ban_history_lines(target))
    send_msg(cid, _clip(text))


def _normalise_role(token: str) -> str | None:
    key = token.strip().upper().replace("-", "_")
    aliases = {
        "FINANCE": "FINANCE_MANAGER", "FINANCE_MANAGER": "FINANCE_MANAGER",
        "SUPPORT": "SUPPORT_AGENT", "SUPPORT_AGENT": "SUPPORT_AGENT",
        "AGENT": "SUPPORT_AGENT", "USER": "USER",
    }
    return aliases.get(key)


def _admin_assignrole(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) != 3 or not parts[1].isdigit():
        send_msg(cid, "Format: /assignrole user_id FINANCE_MANAGER|SUPPORT_AGENT|USER")
        return
    target = int(parts[1])
    role = _normalise_role(parts[2])
    if role not in _ROLES_ASSIGNABLE:
        send_msg(cid, "Bhai role ye ho sakta hai: FINANCE_MANAGER, SUPPORT_AGENT, USER. SUPER_ADMIN sirf ek hai — tu.")
        return
    if target == ADMIN_ID:
        send_msg(cid, "Super admin ka role mat ched. Woh fixed hai.")
        return
    with shared._sl:
        user = _get_ud(target)
        user["role"] = role
        _save_user(user)
    send_msg(target, f"Bhai tere ko naya role mil gaya: <b>{_e(role)}</b>. Desk pe apna kaam kar.")
    send_msg(cid, f"Role set. <code>{target}</code> ab <b>{_e(role)}</b> hai.")


def _admin_removerole(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) != 2 or not parts[1].isdigit():
        send_msg(cid, "Format: /removerole agent_id")
        return
    target = int(parts[1])
    if target == ADMIN_ID:
        send_msg(cid, "Khud ko fire karega? Scene kharab mat kar.")
        return
    n = 0
    with shared._sl:
        user = _get_ud(target)
        user["role"] = "USER"
        _save_user(user)
    if shared._sql is not None:
        with shared._sql_lock:
            cur = shared._sql.execute(
                "UPDATE tickets SET status = 'open', agent_id = NULL WHERE agent_id = ? AND status = 'claimed'",
                (target,),
            )
            n = cur.rowcount if cur.rowcount is not None else 0
            shared._sql.commit()
    send_msg(target, "Bhai tujhe role se hata diya. Ticket jo lock thi, woh unfreeze ho gayi. Ab user wala desk hai.")
    send_msg(cid, f"Fire ho gaya <code>{target}</code>. {n} ticket unfreeze. Ab tu unmein ghuss sakta hai.")
    shared._poke_desks_fn()


def _admin_setlimit(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) != 2 or not parts[1].isdigit():
        send_msg(cid, "Format: /setlimit number")
        return
    n = int(parts[1])
    _kv_set("FREE_DAILY_LIMIT", str(n))
    send_msg(cid, f"Free daily limit ab <b>{n}</b> hai bhai. Kv mein baith gaya.")


def _admin_editplan(cid: int, raw: str) -> None:
    parts = raw.split()
    if len(parts) < 4 or not parts[2].isdigit() or not parts[3].isdigit():
        send_msg(cid, "Format: /editplan plan_id amount days [stars] [name]")
        return
    pid, amount_s, days_s = parts[1], parts[2], parts[3]
    stars = None
    name = None
    rest = parts[4:]
    if rest and rest[0].isdigit():
        stars = int(rest[0])
        rest = rest[1:]
    if rest:
        name = " ".join(rest)
    plans = _plans()
    if pid not in plans:
        send_msg(cid, "Bhai ye plan id nahi mili. day, week, month — ya jo tune pehle banayi.")
        return
    if int(amount_s) <= 0 or int(days_s) <= 0 or (stars is not None and stars <= 0):
        send_msg(cid, "Amount, days, stars positive hone chahiye bhai.")
        return
    plan = dict(plans[pid])
    plan["id"] = pid
    plan["amount"] = int(amount_s)
    plan["days"] = int(days_s)
    plan["stars"] = stars if stars is not None else int(plan.get("stars") or plan["amount"])
    if name:
        plan["name"] = name
    plans[pid] = plan
    _kv_set("PLANS", json.dumps(plans))
    blurb = f"{plan['name']} ab ₹{plan['amount']} ka hai, {plan['days']} din, {plan['stars']} stars."
    shared.flow[cid] = {"mode": "PLAN_BCAST", "blurb": blurb, "ts": time.time()}
    send_msg(
        cid,
        f"Plan update ho gaya.\n\n{_e(blurb)}\n\nNayi price sabko broadcast karun?",
        kb=[[{"text": "Haan broadcast", "callback_data": "planbc:yes"}, {"text": "Nahi rehne de", "callback_data": "planbc:no"}]],
    )


def admin_send_csv(cid: int) -> None:
    if shared._sql is None:
        send_msg(cid, "Bhai DB missing hai. CSV kiski?")
        return
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["chat_id", "username", "role", "is_premium", "premium_until", "total_searches", "is_banned"])
    with shared._sql_lock:
        rows = shared._sql.execute(
            "SELECT chat_id, username, role, is_premium, premium_until, total_searches, is_banned FROM users ORDER BY chat_id"
        ).fetchall()
    for row in rows:
        writer.writerow([
            row["chat_id"], row["username"] or "", row["role"] or "USER",
            row["is_premium"], row["premium_until"], row["total_searches"], row["is_banned"],
        ])
    data = buf.getvalue().encode("utf-8")
    try:
        _session.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": str(cid), "caption": "📋 Users CSV, le bhai."},
            files={"document": ("users.csv", io.BytesIO(data), "text/csv")},
            timeout=25,
        )
    except Exception:
        log.exception("csv send failed")


def admin_send_logs(cid: int) -> None:
    data = "\n".join(_ring.tail(150)).encode()
    try:
        _session.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": cid, "caption": "📋 <b>Logs</b> · 12h IST", "parse_mode": "HTML"},
            files={"document": ("bot_logs.txt", io.BytesIO(data), "text/plain")},
            timeout=25,
        )
    except Exception:
        log.exception("logs send failed")


def _handle_restore_doc(cid: int, msg: dict) -> bool:
    state = shared.flow.get(cid) or {}
    doc = msg.get("document") or {}
    name = str(doc.get("file_name") or "")
    if state.get("mode") != "WAITING_FOR_DB" and not (shared.MAINTENANCE and name.lower().endswith(".db")):
        return False
    blob = _download_tg_file(str(doc.get("file_id") or ""))
    if not blob:
        send_msg(cid, "Bhai file download nahi hui. Phir se bhej.")
        return True
    err = _restore_db(blob)
    _clear_flow(cid)
    if err:
        send_msg(cid, _e(err))
        return True
    send_msg(cid, "Ho gaya bhai. users.db baith gayi, SQLite init ho gaya, maintenance OFF. Log wapas aa sakte hain.")
    return True


def _cmd_role_audit(cid: int, raw: str) -> None:
    if cid != ADMIN_ID:
        send_msg(cid, "Bhai audit sirf super admin ka hai.")
        return
    if shared._sql is None:
        send_msg(cid, "Bhai DB missing hai.")
        return
    parts = raw.split()
    if len(parts) == 1:
        with shared._sql_lock:
            rows = shared._sql.execute(
                "SELECT chat_id, name, username, role FROM users WHERE role IN ('FINANCE_MANAGER', 'SUPPORT_AGENT', 'SUPER_ADMIN') ORDER BY chat_id"
            ).fetchall()
        if not rows:
            send_msg(cid, "Abhi koi agent file pe nahi. /assignrole se bana.")
            return
        lines = ["<b>Roles</b>", ""]
        for row in rows:
            lines.append(f"• {_e(row['name'] or 'Bhai')} · <code>{row['chat_id']}</code> · {_e(row['role'] or '')}")
        lines.append("\nEk banda dekhne ko /role user_id")
        send_msg(cid, "\n".join(lines))
        return
    if not parts[1].isdigit():
        send_msg(cid, "Format: /role agent_id")
        return
    target = int(parts[1])
    user = _load_user(target)
    with shared._sql_lock:
        tickets = shared._sql.execute("SELECT * FROM tickets WHERE agent_id = ? ORDER BY id", (target,)).fetchall()
        replies = shared._sql.execute("SELECT * FROM ticket_replies WHERE agent_id = ? ORDER BY id", (target,)).fetchall()
    lines = [
        f"<b>Audit</b> · <code>{target}</code>",
        f"Naam · {_e((user or {}).get('name') or 'file pe nahi')}",
        f"Role · {_e(_normalise_role(str((user or {}).get('role') or 'USER')) or 'USER')}",
        f"Tickets · {len(tickets)} · replies · {len(replies)}",
        "",
    ]
    for row in tickets:
        lines.append(f"🎫 #{row['id']} · {_e(row['status'])} · user <code>{row['chat_id']}</code> · {fmt_ts(row['created_at'])} → {fmt_ts(row['resolved_at'])}")
    lines.append("")
    for rep in replies:
        lines.append(f"💬 ticket #{rep['ticket_id']} · {fmt_ts(rep['created_at'])} · {_e((rep['body'] or '')[:240])}")
    text = "\n".join(lines)
    if len(text) < 3800:
        send_msg(cid, text)
        return
    try:
        _session.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": str(cid), "caption": f"📜 Role audit · {target}"},
            files={"document": (f"role_{target}.txt", io.BytesIO(text.encode('utf-8')), "text/plain")},
            timeout=25,
        )
    except Exception:
        log.exception("role audit file failed")


def _toggle_gateway(gw: str) -> str:
    if gw not in ("upi", "stars", "crypto"):
        return "Bhai ye gateway pata nahi."
    if _gw_on(gw):
        busy = _busy_gateway(gw)
        if busy:
            return f"Bhai {gw} band nahi kar sakta. {len(busy)} user abhi is payment flow mein atka hai."
        _kv_set(f"gw:{gw}", "0")
        return f"{gw} band kar diya."
    _kv_set(f"gw:{gw}", "1")
    return f"{gw} chalu kar diya."
