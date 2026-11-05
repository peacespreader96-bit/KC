"""Telegram message and callback query handlers, command dispatcher, and dashboard."""
import re
import threading
import time

from kc import shared
from kc.admin_cmds import (
    _admin_assignrole,
    _admin_ban,
    _admin_editplan,
    _admin_grant,
    _admin_removerole,
    _admin_setlimit,
    _admin_unban,
    _admin_userinfo,
    _busy_gateway,
    _cmd_role_audit,
    _cmd_sysconfig,
    _handle_restore_doc,
    _toggle_gateway,
    admin_send_csv,
    admin_send_logs,
    admin_stats_text,
)
from kc.broadcast import broadcast_kb
from kc.config import (
    ADMIN_ID,
    FORCE_CHANNELS,
    WELCOME_IMAGE_URL,
    _BAN_TEXT,
    _GW_MODES,
    _MAINT_TEXT,
    _PAY_MODES,
    _PAY_TTL,
)
from kc.desk import (
    _can_desk,
    _can_settle,
    _cmd_data,
    _cmd_mywork,
    _handle_desk_cb,
    _open_desk,
    _poke_desks,
)
from kc.logging_setup import log
from kc.payments import (
    _begin_gateway,
    _handle_stars_paid,
    _settle_payment,
    _start_plan,
)
from kc.result_ui import (
    format_card_combined,
    format_card_sources,
    generate_html_all,
    generate_html_combined,
)
from kc.spin import _present_spin, _run_spin, _spin_due
from kc.sqlite_db import (
    _create_fresh_db,
    _save_user,
)
from kc.telegram_api import (
    answer_cb,
    desk_edit,
    edit_msg,
    send_html_file,
    send_msg,
    send_photo,
    tg,
)
from kc.tickets import _add_reply, _open_ticket, _ticket
from kc.ui import (
    _channel_link,
    _gateway_kb,
    back_kb,
    dashboard_kb,
    force_kb,
    get_text,
    msg_dashboard,
    msg_force,
    msg_help,
    msg_history,
    msg_pricing,
    msg_profile,
    msg_support_prompt,
    pricing_kb,
    result_kb,
)
from kc.user_ops import (
    _apply_referral,
    _banned,
    _bump_utr_attempt,
    _clear_flow,
    _get_ud,
    _plans,
    _utr_lock_until,
    _utr_locked,
)
from kc.utils import (
    _D,
    _e,
    _today,
    fmt_td,
    sv,
)
from kc.worker import _enqueue_search


def _chat_ref(channel: str):
    s = str(channel).strip()
    return int(s) if re.fullmatch(r"-?\d+", s) else s


def _member_of(cid: int, channel: str) -> bool:
    data = tg("getChatMember", {"chat_id": _chat_ref(channel), "user_id": cid})
    if not data or not data.get("ok"):
        desc = str((data or {}).get("description") or "").lower()
        kicked = any(x in desc for x in ("kicked", "not a member", "chat not found", "forbidden", "need administrator"))
        if kicked and channel not in shared._chan_fail:
            shared._chan_fail.add(channel)
            send_msg(ADMIN_ID, "Bhai bot channel se nikal diya gaya: <code>" + _e(str(channel)) + "</code>. Fail-open ON.")
        return True if kicked else False
    result = data.get("result") or {}
    status = result.get("status", "")
    if status in ("creator", "administrator", "member"):
        return True
    return status == "restricted" and bool(result.get("is_member"))


def _needs_join(cid: int) -> bool:
    if cid == ADMIN_ID or not FORCE_CHANNELS:
        return False
    return any(not _member_of(cid, ch) for ch in FORCE_CHANNELS)


def _guard_join(cid: int, mid: int | None = None) -> bool:
    if not _needs_join(cid):
        return False
    if mid:
        desk_edit(cid, mid, msg_force(), kb=force_kb())
    else:
        send_msg(cid, msg_force(), kb=force_kb())
    return True


def _premium_lock_screen(cid: int, mid: int | None) -> bool:
    if not _utr_locked(cid):
        return False
    left = fmt_td(int(_utr_lock_until(cid) - time.time()))
    desk_edit(
        cid, mid,
        f"<b>Premium lock hai bhai.</b>\n\nUTR spam ka natija. {_e(left)} baad aana.\n\n{_D}",
        kb=back_kb(),
    )
    return True


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


def _pay_active(cid: int) -> bool:
    st = shared.flow.get(cid) or {}
    return st.get("mode") in _PAY_MODES and _flow_fresh(st)


def _blocked_maint(cid: int) -> bool:
    if not shared.MAINTENANCE or cid == ADMIN_ID:
        return False
    send_msg(cid, _MAINT_TEXT)
    return True


def _start_payload(text: str) -> str | None:
    head, _, rest = text.partition(" ")
    return rest.strip() if head.startswith("/start") else None


def _maybe_spin(cid: int, raw: str) -> bool:
    is_num = raw.isdigit() and len(raw) == 10
    today = _today()
    first = shared.seen_day.get(cid) != today
    shared.seen_day[cid] = today
    with shared._sl:
        user = _get_ud(cid)
        due = _spin_due(user)
    if not due or not (first or is_num):
        return False
    _present_spin(cid, raw if is_num else None)
    return True


def _show_dashboard(cid: int, mid: int | None = None) -> None:
    _clear_flow(cid)
    with shared._sl:
        user = _get_ud(cid)
        text = msg_dashboard(user)
    kb = dashboard_kb()
    if mid:
        tg("deleteMessage", {"chat_id": cid, "message_id": mid})
    if not send_photo(cid, WELCOME_IMAGE_URL, text, kb=kb):
        send_msg(cid, text, kb=kb)


def _ask_pay_search(cid: int, number: str) -> None:
    st = shared.flow.setdefault(cid, {})
    st["pending_search"] = number
    send_msg(
        cid,
        "Bhai tu payment mein hai. Confirm kar, agar tu payment cancel karke search karna chahta hai?",
        kb=[[
            {"text": "Haan, cancel karke search", "callback_data": "paysearch:yes"},
            {"text": "Nahi, payment chalu", "callback_data": "paysearch:no"},
        ]],
    )


def _dispatch_cmd(cid: int, raw: str) -> None:
    cmd = raw.split()[0].split("@")[0].lower()
    if cmd == "/start":
        payload = _start_payload(raw) or ""
        if payload:
            _apply_referral(cid, payload)
        if cid != ADMIN_ID and shared.MAINTENANCE:
            send_msg(cid, _MAINT_TEXT)
            return
        if _guard_join(cid):
            return
        _show_dashboard(cid)
        if cid == ADMIN_ID and shared.MAINTENANCE:
            send_msg(cid, "Maintenance ON hai bhai. /restore karke users.db daal, ya /dashboard dekh.")
        return
    if cmd == "/cancel":
        _clear_flow(cid)
        send_msg(cid, "Cancel kar diya bhai.", kb=dashboard_kb())
        return
    if cmd == "/help":
        send_msg(cid, msg_help(), kb=dashboard_kb())
        return

    if cid == ADMIN_ID:
        if cmd == "/logs":
            admin_send_logs(cid)
            return
        if cmd == "/admin":
            send_msg(cid, admin_stats_text())
            return
        if cmd == "/broadcast":
            _clear_flow(cid)
            send_msg(cid, "<b>Broadcast</b>\n\nAgla message kisko bheju?", kb=broadcast_kb())
            return
        if cmd == "/grant":
            _admin_grant(cid, raw)
            return
        if cmd == "/ban":
            _admin_ban(cid, raw)
            return
        if cmd == "/unban":
            _admin_unban(cid, raw)
            return
        if cmd == "/userinfo":
            _admin_userinfo(cid, raw)
            return
        if cmd == "/csv":
            admin_send_csv(cid)
            return
        if cmd == "/assignrole":
            _admin_assignrole(cid, raw)
            return
        if cmd == "/removerole":
            _admin_removerole(cid, raw)
            return
        if cmd == "/setlimit":
            _admin_setlimit(cid, raw)
            return
        if cmd == "/editplan":
            _admin_editplan(cid, raw)
            return
        if cmd == "/payments":
            _open_desk(cid, "set")
            return
        if cmd == "/restore":
            shared.flow[cid] = {"mode": "WAITING_FOR_DB", "ts": time.time()}
            send_msg(cid, "Bhai users.db file yahin document karke bhej. Text mat likh, file chipka.")
            return

        if cmd == "/initdb":
            send_msg(
                cid,
                "⚠️ Bhai ye purana DB delete karke naya khali DB banayega. Tu sure hai?",
                kb=[[{"text": "Haan, DB naya bana", "callback_data": "dbinit:yes"}, {"text": "Nahi, cancel", "callback_data": "dbinit:no"}]],
            )
            return

        if cmd == "/dashboard":
            if cid in shared._desk:
                shared._desk[cid]["mid"] = None
            _open_desk(cid, "home")
            return

        if cmd in ("/sysconfig", "/tune"):
            _cmd_sysconfig(cid, raw)
            return
        if cmd == "/role":
            _cmd_role_audit(cid, raw)
            return
        if cmd == "/mywork":
            _cmd_mywork(cid)
            return
        if cmd == "/data":
            _cmd_data(cid)
            return

    if cmd == "/dashboard" and _can_desk(cid):
        if cid in shared._desk:
            shared._desk[cid]["mid"] = None
        _open_desk(cid, "home")
        return

    if cmd == "/mywork":
        _cmd_mywork(cid)
        return
    if cmd == "/data":
        _cmd_data(cid)
        return


def handle_pre_checkout(q: dict) -> None:
    qid = q.get("id")
    if not qid:
        return
    try:
        tg("answerPreCheckoutQuery", {"pre_checkout_query_id": qid, "ok": True})
    except Exception:
        log.exception("pre_checkout answer failed")


def handle_cb(cb: dict) -> None:
    cid = cb["message"]["chat"]["id"]
    mid = cb["message"]["message_id"]
    data = cb["data"]
    name = cb.get("from", {}).get("first_name", "User")
    username = cb.get("from", {}).get("username", "")

    if data.startswith(("ok:", "no:")):
        if not _can_settle(cid):
            answer_cb(cb["id"], "Sirf operator settle kar sakta hai.", alert=True)
            return
        answer_cb(cb["id"])
        _settle_payment(data.split(":", 1)[1], data.startswith("ok:"), mid, actor=cid)
        return

    if data.startswith("dbinit:"):
        if cid != ADMIN_ID:
            answer_cb(cb["id"], "Sirf super admin kar sakta hai.", alert=True)
            return
        answer_cb(cb["id"])
        if data.endswith("yes"):
            send_msg(cid, "⏳ Naya DB bana raha hoon...")
            err = _create_fresh_db()
            if err:
                send_msg(cid, f"❌ {_e(err)}")
            else:
                send_msg(cid, "✅ Fresh users.db ban gaya! Maintenance OFF. Ab normal chalega.")
        else:
            send_msg(cid, "Theek hai, cancel.")
        return

    answer_cb(cb["id"])
    with shared._sl:
        user = _get_ud(cid, name=name, username=username)
    if cid != ADMIN_ID and _banned(user):
        send_msg(cid, _BAN_TEXT)
        return
    if cid != ADMIN_ID and shared.MAINTENANCE and not data.startswith("d:"):
        send_msg(cid, _MAINT_TEXT)
        return

    if data.startswith("d:"):
        if not _can_desk(cid):
            send_msg(cid, "Bhai desk teri post ki nahi.")
            return
        _handle_desk_cb(cid, mid, data)
        return

    if data.startswith("gwtog:"):
        from kc.desk import _render_settings
        if cid != ADMIN_ID:
            send_msg(cid, "Bhai gateway sirf super admin hilata hai.")
            return
        note = _toggle_gateway(data.split(":", 1)[1])
        text, kb = _render_settings(cid)
        text = "<b>" + _e(note) + "</b>\n\n" + text
        shared._desk[cid] = {"mid": mid, "section": "set", "page": 0, "focus": None}
        edit_msg(cid, mid, _clip(text), kb)
        return

    if data.startswith("planbc:"):
        from kc.broadcast import _broadcast
        if cid != ADMIN_ID:
            return
        blurb = (shared.flow.get(cid) or {}).get("blurb") or "Plan ki price update ho gayi."
        _clear_flow(cid)
        if data.endswith("yes"):
            send_msg(cid, "Theek hai, nayi price broadcast kar raha hoon.")
            threading.Thread(target=_broadcast, args=("all", blurb), daemon=True).start()
        else:
            send_msg(cid, "Broadcast nahi kiya. Price file pe update hai.")
        return

    if data == "noop":
        send_msg(cid, "Private channel hai — admin se public invite link le.")
        return

    if data == "paycancel":
        _clear_flow(cid)
        desk_edit(cid, mid, "Payment cancel kar diya bhai. Jab mann kare number daal.", kb=dashboard_kb())
        return

    if data == "paysearch:yes":
        number = (shared.flow.get(cid) or {}).get("pending_search")
        _clear_flow(cid)
        send_msg(cid, "Theek hai, payment cancel. Search chala raha hoon.")
        if number and number.isdigit() and len(number) == 10:
            _enqueue_search(cid, number)
        return

    if data == "paysearch:no":
        if cid in shared.flow:
            shared.flow[cid].pop("pending_search", None)
        send_msg(cid, "Theek hai bhai, payment chalu rakh. Number baad mein daalna.")
        return

    if data == "verify":
        if _needs_join(cid):
            desk_edit(cid, mid, msg_force() + "\n\nAbhi bhi channel baaki hai. Join kar, phir verify daba.", kb=force_kb())
            return
        _show_dashboard(cid, mid)
        return

    if (data in {"dashboard", "profile", "premium", "history", "support", "search",
                 "stats", "help", "new_search"}
            or data.startswith(("plan:", "paid:", "pg:", "html:", "gwpay:"))
            or data in {"spin", "noop"}
            or data.startswith("cast:")):
        if _guard_join(cid, mid):
            return

    if data in {"dashboard", "stats"}:
        _show_dashboard(cid, mid)
    elif data == "profile":
        _clear_flow(cid)
        with shared._sl:
            user = _get_ud(cid)
            text = msg_profile(user)
        desk_edit(cid, mid, text, kb=back_kb())
    elif data == "premium":
        if _premium_lock_screen(cid, mid):
            return
        _clear_flow(cid)
        with shared._sl:
            user = _get_ud(cid)
            text = msg_pricing(user)
        desk_edit(cid, mid, text, kb=pricing_kb())
    elif data == "history":
        _clear_flow(cid)
        with shared._sl:
            user = _get_ud(cid)
            text = msg_history(user)
        desk_edit(cid, mid, text, kb=back_kb())
    elif data == "support":
        shared.flow[cid] = {"mode": "WAITING_FOR_SUPPORT", "ts": time.time()}
        desk_edit(cid, mid, msg_support_prompt(), kb=back_kb())
    elif data in {"search", "new_search"}:
        _clear_flow(cid)
        desk_edit(cid, mid, get_text(cid, "search_prompt"), kb=back_kb())
    elif data == "help":
        desk_edit(cid, mid, msg_help(), kb=back_kb())
    elif data.startswith("plan:"):
        if _premium_lock_screen(cid, mid):
            return
        plan = _plans().get(data.split(":", 1)[1])
        if plan:
            _start_plan(cid, mid, plan)
    elif data.startswith("gwpay:"):
        if _premium_lock_screen(cid, mid):
            return
        _, gw, pid = (data.split(":", 2) + ["", ""])[:3]
        plan = _plans().get(pid)
        if plan and gw in ("upi", "stars", "crypto"):
            _begin_gateway(cid, mid, plan, gw)
    elif data.startswith("paid:"):
        if _premium_lock_screen(cid, mid):
            return
        plan = _plans().get(data.split(":", 1)[1])
        if not plan:
            return
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
        shared.flow[cid] = {"mode": "WAITING_FOR_UTR", "plan": plan["id"], "gateway": "upi", "ts": time.time()}
        send_msg(
            cid,
            f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']}\n\nUPI app se 12 digit UTR bhej. Sirf digits.",
            kb=back_kb(),
        )
    elif data == "spin":
        _run_spin(cid)
    elif data.startswith("cast:"):
        if cid != ADMIN_ID:
            return
        audience = "prem" if data.endswith("prem") else "all"
        shared.flow[cid] = {"mode": "WAITING_FOR_BROADCAST", "audience": audience}
        who = "sirf Premium wale" if audience == "prem" else "har member"
        desk_edit(cid, mid, f"Agla message bhej. Ye <b>{who}</b> ke paas jayega.\n\n/cancel se ye band.")
    elif data.startswith("pg:"):
        page = int(data.split(":")[1])
        cb_mid = cb["message"]["message_id"]
        with shared._sl:
            sess = shared.user_sessions.get((cid, cb_mid))
            if not sess:
                cid_matches = [s for k, s in shared.user_sessions.items() if k[0] == cid]
                if len(cid_matches) == 1:
                    sess = cid_matches[0]
        if not sess:
            send_msg(cid, "Session expire ho gaya, nayi search kar.", kb=back_kb())
            return
        sources = sess.get("sources")
        if sources:
            max_pages = max([len(r) for r in sources.values() if r] or [0])
            if 0 <= page < max_pages:
                page_data = {k: (v[page] if page < len(v) else None) for k, v in sources.items()}
                edit_msg(
                    cid, mid,
                    format_card_sources(page_data, sess["query"]),
                    kb=result_kb(page, max_pages),
                )
        else:
            rows_main = sess.get("results", [])
            rows_umba = sess.get("results_umba", [])
            total = max(len(rows_main), len(rows_umba))
            if 0 <= page < total:
                row_main = rows_main[page] if page < len(rows_main) else None
                row_umba = rows_umba[page] if page < len(rows_umba) else None
                edit_msg(
                    cid, mid,
                    format_card_combined(row_main, row_umba, sess["query"]),
                    kb=result_kb(page, total),
                )

    elif data.startswith("html:"):
        page = int(data.split(":")[1])
        cb_mid = cb["message"]["message_id"]
        with shared._sl:
            sess = shared.user_sessions.get((cid, cb_mid))
            if not sess:
                cid_matches = [s for k, s in shared.user_sessions.items() if k[0] == cid]
                if len(cid_matches) == 1:
                    sess = cid_matches[0]
        if not sess:
            send_msg(cid, "Session expire ho gaya, nayi search kar.", kb=back_kb())
            return
        query = sess["query"]
        sources = sess.get("sources")
        if sources:
            page_data = {k: (v[page] if page < len(v) else (v[0] if v else None)) for k, v in sources.items()}
            phone = (
                sv((page_data.get("main") or {}).get("phoneNumber"))
                or sv((page_data.get("umba") or {}).get("phoneNumber"))
                or sv((page_data.get("bsnl") or {}).get("number"))
                or sv((page_data.get("cars") or {}).get("mobile_no"))
                or sv((page_data.get("air") or {}).get("number"))
                or sv((page_data.get("flipkart") or {}).get("phone"))
                or sv((page_data.get("telegram") or {}).get("phone"))
                or query
            )
            from kc.config import BOT_NAME
            send_html_file(
                cid,
                f"{BOT_NAME}_Report_{phone}.html",
                generate_html_all(page_data, query),
                "📄 Le bhai, teri KC multi-source intelligence report attach hai.",
            )
        else:
            rows_main = sess.get("results", [])
            rows_umba = sess.get("results_umba", [])
            row_main = rows_main[page] if page < len(rows_main) else None
            row_umba = rows_umba[page] if page < len(rows_umba) else None
            if row_main is not None or row_umba is not None:
                phone = (
                    sv((row_main or {}).get("phoneNumber"))
                    or sv((row_umba or {}).get("phoneNumber"))
                    or "note"
                )
                from kc.config import BOT_NAME
                send_html_file(
                    cid,
                    f"{BOT_NAME}_Report_{phone}.html",
                    generate_html_combined(row_main, row_umba, query),
                    "📄 Le bhai, teri KC directory note attach hai.",
                )


def handle_msg(msg: dict) -> None:
    cid = msg["chat"]["id"]
    raw = (msg.get("text") or "").strip()
    name = msg.get("from", {}).get("first_name", "User")
    un = msg.get("from", {}).get("username", "")

    if cid == ADMIN_ID and msg.get("document"):
        if _handle_restore_doc(cid, msg):
            return

    if msg.get("successful_payment"):
        _handle_stars_paid(msg)
        return

    with shared._sl:
        user = _get_ud(cid, name=name, username=un)

    if cid != ADMIN_ID and shared.MAINTENANCE:
        send_msg(cid, _MAINT_TEXT)
        return

    state = shared.flow.get(cid) or {}
    mode = state.get("mode")

    if cid == ADMIN_ID and mode == "WAITING_FOR_BROADCAST":
        from kc.broadcast import _broadcast
        if raw == "/cancel":
            _clear_flow(cid)
            send_msg(cid, "Broadcast cancel.", kb=dashboard_kb())
            return
        if not raw.startswith("/"):
            audience = state.get("audience") or "all"
            _clear_flow(cid)
            send_msg(cid, "Bhej raha hoon.")
            threading.Thread(target=_broadcast, args=(audience, raw), daemon=True).start()
            return

    if raw.startswith("/"):
        if cid != ADMIN_ID and _banned(user):
            send_msg(cid, _BAN_TEXT)
            return
        _dispatch_cmd(cid, raw)
        return

    if cid != ADMIN_ID and _banned(user):
        send_msg(cid, _BAN_TEXT)
        return

    if _pay_active(cid) and raw.isdigit() and len(raw) == 10:
        _ask_pay_search(cid, raw)
        return

    if mode == "WAITING_FOR_UTR":
        from kc.payments import _forward_payment, _store_payment
        if _utr_locked(cid):
            left = fmt_td(int(_utr_lock_until(cid) - time.time()))
            _clear_flow(cid)
            send_msg(cid, f"Bhai Premium lock hai. {_e(left)} baad try kar.", kb=dashboard_kb())
            return
        plan = _plans().get(state.get("plan") or "")
        if not plan:
            _clear_flow(cid)
            send_msg(cid, "Woh checkout expire ho gaya. Premium khol ke term phir choose kar.", kb=dashboard_kb())
            return
        if not re.fullmatch(r"\d{12}", raw):
            send_msg(cid, "Reference exactly 12 digit ka hona chahiye. Sirf UTR bhej.")
            return
        with shared._sl:
            user = _get_ud(cid)
            snapshot = dict(user)
        payment_id, invoice_id = _store_payment(snapshot, plan, raw, "UPI")
        _clear_flow(cid)
        if not payment_id:
            send_msg(cid, "Ye UTR pehle se file pe hai. Tera tha toh operator ka wait kar — warna Support pe likh.")
            return
        _forward_payment(snapshot, plan, raw, payment_id, "UPI", invoice_id or "")
        return

    if mode == "WAITING_FOR_TX":
        from kc.payments import _forward_payment, _store_payment
        plan = _plans().get(state.get("plan") or "")
        if not plan:
            _clear_flow(cid)
            send_msg(cid, "Crypto checkout expire ho gaya. Premium khol ke phir try kar.", kb=dashboard_kb())
            return
        if not re.fullmatch(r"[A-Za-z0-9]{16,80}", raw):
            send_msg(cid, "Bhai sahi tx hash bhej. 16 se 80 letters ya digits. UTR nahi, crypto hash.")
            return
        with shared._sl:
            user = _get_ud(cid)
            snapshot = dict(user)
        payment_id, invoice_id = _store_payment(snapshot, plan, raw, "Crypto")
        _clear_flow(cid)
        if not payment_id:
            send_msg(cid, "Ye tx hash pehle se file pe hai. Operator ka wait kar.")
            return
        _forward_payment(snapshot, plan, raw, payment_id, "Crypto", invoice_id or "")
        return

    if mode == "stars":
        send_msg(cid, "Bhai Stars wala invoice upar pada hai. Pay kar, ya Cancel daba.")
        return

    if mode in {"checkout", "pay_choose", "crypto"}:
        send_msg(cid, "Bhai tu payment screen pe hai. Upar wale button use kar, ya 10 digit daal ke search confirm kar.")
        return

    if mode == "WAITING_FOR_DB" and cid == ADMIN_ID:
        send_msg(cid, "Bhai text nahi, users.db document bhej.")
        return

    if mode == "TICKET_REPLY":
        if not raw:
            send_msg(cid, "Bhai reply text mein likh.")
            return
        tid = int(state.get("ticket_id") or 0)
        row = _ticket(tid)
        if not row or row["status"] != "claimed" or int(row["agent_id"] or 0) != cid:
            _clear_flow(cid)
            send_msg(cid, "Bhai ye ticket ab tere pe lock nahi hai. Reply cancel.")
            return
        _add_reply(tid, cid, raw)
        send_msg(int(row["chat_id"]), f"<b>Desk se jawab</b>\n\n{_e(raw)}")
        shared.flow[cid] = {"mode": "IN_TICKET", "ticket_id": tid}
        st = shared._desk.get(cid) or {}
        if st.get("mid"):
            from kc.desk import _desk_show
            _desk_show(cid, st["mid"], "ticket", focus=tid)
        return

    if mode == "WAITING_FOR_SUPPORT":
        if time.time() - float(state.get("ts") or 0) > 300:
            _clear_flow(cid)
            state = {}
            mode = None
        else:
            if not raw:
                send_msg(cid, "Bhai khali message se ticket nahi khulta. Likh ke bhej.")
                return
            if shared._sql is None:
                send_msg(cid, _MAINT_TEXT)
                return
            tid = _open_ticket(cid, raw)
            _clear_flow(cid)
            send_msg(cid, f"Ticket <b>#{tid}</b> khul gaya bhai. Agent claim karega, jawab yahin aayega.", kb=dashboard_kb())
            _poke_desks()
            return

    if _guard_join(cid):
        return
    if _maybe_spin(cid, raw):
        return

    with shared._sl:
        is_active = cid in shared.active_users
    if is_active:
        send_msg(cid, "Arre ruk bhai, teri pehli search abhi desk pe hai.")
        return
    if not (raw.isdigit() and len(raw) == 10):
        send_msg(cid, get_text(cid, "invalid"), kb=back_kb())
        return
    _enqueue_search(cid, raw)


# Up-call registration
shared._show_dashboard_fn = _show_dashboard
