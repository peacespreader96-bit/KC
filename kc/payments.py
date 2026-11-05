"""Payment handling, gateways (UPI, Stars, Crypto), invoice generation and settlement."""
import sqlite3
import time
import urllib.parse
import uuid

from kc import shared
from kc.config import (
    ADMIN_ID,
    BOT_NAME,
    UPI_ID,
    USDT_BEP20,
    USDT_TRC20,
)
from kc.result_ui import send_receipt
from kc.sqlite_db import _row_get, _save_user
from kc.telegram_api import desk_edit, edit_msg, send_msg, tg
from kc.ui import _gateway_kb, back_kb
from kc.user_ops import (
    _clear_flow,
    _extend_premium,
    _get_ud,
    _gw_on,
    _plans,
    _reset_utr_attempts,
    _utr_lock_until,
    _utr_locked,
)
from kc.utils import _e, fmt_td, fmt_until, ts_str


def _upi_uri(plan: dict) -> str:
    return "upi://pay?" + urllib.parse.urlencode({
        "pa": UPI_ID, "pn": BOT_NAME, "am": f"{float(plan['amount']):.2f}",
        "cu": "INR", "tn": f"{BOT_NAME} {plan['name']}",
    })


def _enabled_gateways() -> list[str]:
    return [gw for gw in ("upi", "stars", "crypto") if _gw_on(gw)]


def _begin_gateway(cid: int, mid: int | None, plan: dict, gw: str) -> None:
    if not _gw_on(gw):
        desk_edit(cid, mid, "Bhai ye rasta abhi band hai. Doosra choose kar.", kb=_gateway_kb(plan["id"]))
        return
    if gw == "upi":
        _open_checkout(cid, mid, plan)
    elif gw == "stars":
        _open_stars(cid, mid, plan)
    elif gw == "crypto":
        _open_crypto(cid, mid, plan)


def _start_plan(cid: int, mid: int | None, plan: dict) -> None:
    enabled = _enabled_gateways()
    if not enabled:
        desk_edit(cid, mid, "Bhai abhi saare payment raste band hain. Thodi der baad aana.", kb=back_kb())
        return
    shared.flow[cid] = {"mode": "pay_choose", "plan": plan["id"], "ts": time.time()}
    if len(enabled) == 1:
        _begin_gateway(cid, mid, plan, enabled[0])
        return
    desk_edit(
        cid, mid,
        f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']}\n\nKaunsa payment method use karna hai?",
        kb=_gateway_kb(plan["id"]),
    )


def _open_checkout(cid: int, mid: int | None, plan: dict) -> None:
    if _utr_locked(cid):
        left = fmt_td(int(_utr_lock_until(cid) - time.time()))
        desk_edit(cid, mid, f"Bhai Premium lock hai. {_e(left)} baad try kar. UTR spam ka natija.", kb=back_kb())
        return
    shared.flow[cid] = {"mode": "checkout", "plan": plan["id"], "gateway": "upi", "ts": time.time()}
    uri = _upi_uri(plan)
    qr = "https://api.qrserver.com/v1/create-qr-code/?size=640x640&margin=16&data=" + urllib.parse.quote(uri, safe="")
    caption = (
        f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']}\n"
        f"UPI ID · <code>{_e(UPI_ID)}</code>\n\n"
        f"Exact amount kisi bhi UPI app se bhej. Phir button daba ke 12 digit UTR daal."
    )
    kb = [
        [{"text": "✅ Payment ho gaya (UTR daal)", "callback_data": f"paid:{plan['id']}"}],
        [{"text": "❌ Cancel", "callback_data": "paycancel"}, {"text": "🔙 Wapas", "callback_data": "premium"}],
    ]
    res = tg("sendPhoto", {
        "chat_id": cid, "photo": qr, "caption": caption,
        "parse_mode": "HTML", "reply_markup": {"inline_keyboard": kb},
    })
    if res.get("ok"):
        desk_edit(cid, mid, "UPI code upar hai. Exact amount bhej, phir confirm kar.")
        return
    desk_edit(cid, mid, caption + f"\n\nPay link\n<code>{_e(uri)}</code>", kb=kb)


def _open_stars(cid: int, mid: int | None, plan: dict) -> None:
    stars = int(plan.get("stars") or plan["amount"])
    shared.flow[cid] = {"mode": "stars", "plan": plan["id"], "gateway": "stars", "ts": time.time()}
    payload = f"{plan['id']}|{cid}"[:128]
    res = tg("sendInvoice", {
        "chat_id": cid,
        "title": str(plan["name"])[:32],
        "description": f"{plan['name']} · {BOT_NAME}"[:255],
        "payload": payload,
        "provider_token": "",
        "currency": "XTR",
        "prices": [{"label": str(plan["name"])[:32], "amount": stars}],
    })
    kb = [[{"text": "❌ Cancel", "callback_data": "paycancel"}], [{"text": "🔙 Wapas", "callback_data": "premium"}]]
    if res.get("ok"):
        desk_edit(cid, mid, f"Stars invoice upar hai bhai. <b>{stars}</b> stars · {_e(plan['name'])}. Pay kar, desk khud khul jayegi.", kb=kb)
        return
    shared.flow.pop(cid, None)
    desk_edit(cid, mid, "Bhai Stars invoice nahi bana. Thodi der baad try kar, ya doosra rasta le.", kb=_gateway_kb(plan["id"]))


def _open_crypto(cid: int, mid: int | None, plan: dict) -> None:
    shared.flow[cid] = {"mode": "WAITING_FOR_TX", "plan": plan["id"], "gateway": "crypto", "ts": time.time()}
    text = (
        f"<b>{_e(plan['name'])}</b> · ₹{plan['amount']} ke barabar USDT\n\n"
        f"BEP20 · <code>{_e(USDT_BEP20)}</code>\n"
        f"TRC20 · <code>{_e(USDT_TRC20)}</code>\n\n"
        f"Tx hash yahin bhej. Admin check karega tab premium khulega. Random bakwaas mat daal."
    )
    kb = [[{"text": "❌ Cancel", "callback_data": "paycancel"}], [{"text": "🔙 Wapas", "callback_data": "premium"}]]
    desk_edit(cid, mid, text, kb=kb)


def _new_invoice_id() -> str:
    for _ in range(8):
        inv = "INV-" + uuid.uuid4().hex[:4].upper()
        if shared._sql is None:
            return inv
        with shared._sql_lock:
            row = shared._sql.execute("SELECT invoice_id FROM invoices WHERE invoice_id = ?", (inv,)).fetchone()
        if not row:
            return inv
    return "INV-" + uuid.uuid4().hex[:8].upper()


def _create_invoice(cid: int, gateway: str, amount: int, plan_id: str, status: str = "pending") -> str:
    inv = _new_invoice_id()
    now = time.time()
    paid_at = now if status == "paid" else None
    if shared._sql is None:
        return inv
    with shared._sql_lock:
        shared._sql.execute(
            "INSERT INTO invoices (invoice_id, chat_id, gateway, amount, plan_id, status, created_at, paid_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (inv, int(cid), gateway, int(amount), plan_id, status, now, paid_at),
        )
        shared._sql.commit()
    return inv


def _mark_invoice(inv: str, status: str) -> None:
    if shared._sql is None or not inv:
        return
    with shared._sql_lock:
        if status == "paid":
            shared._sql.execute("UPDATE invoices SET status = 'paid', paid_at = ? WHERE invoice_id = ?", (time.time(), inv))
        else:
            shared._sql.execute("DELETE FROM invoices WHERE invoice_id = ? AND status = 'pending'", (inv,))
        shared._sql.commit()


def _store_payment(user: dict, plan: dict, ref: str, gateway: str) -> tuple[str | None, str | None]:
    if shared._sql is None:
        return None, None
    with shared._sql_lock:
        existing = shared._sql.execute(
            "SELECT id, status FROM payments WHERE utr = ? AND status IN ('pending', 'approved')",
            (ref,),
        ).fetchone()
        if existing:
            return None, None
    invoice_id = _create_invoice(int(user["id"]), gateway, int(plan["amount"]), plan["id"], "pending")
    payment_id = uuid.uuid4().hex[:12]
    with shared._sql_lock:
        shared._sql.execute(
            "INSERT INTO payments (id, chat_id, plan_id, plan_name, amount, days, utr, status, created_at, settled_at, gateway, invoice_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?, ?)",
            (
                payment_id, int(user["id"]), plan["id"], plan["name"], int(plan["amount"]),
                int(plan["days"]), ref, time.time(), gateway, invoice_id,
            ),
        )
        shared._sql.commit()
    return payment_id, invoice_id


def _forward_payment(user: dict, plan: dict, ref: str, payment_id: str, gateway: str, invoice_id: str) -> None:
    handle = f" (@{_e(user['username'])})" if user.get("username") else ""
    text = (
        f"<b>Payment check kar</b>\n\n"
        f"Banda · {_e(user.get('name') or 'Bhai')}{handle}\n"
        f"ID · <code>{user['id']}</code>\n"
        f"Plan · {_e(plan['name'])}\n"
        f"Amount · ₹{plan['amount']}\n"
        f"Rasta · {_e(gateway)}\n"
        f"Invoice · <code>{_e(invoice_id)}</code>\n"
        f"Ref · <code>{_e(ref)}</code>"
    )
    kb = [[
        {"text": "Approve ✅", "callback_data": f"ok:{payment_id}"},
        {"text": "Reject ❌", "callback_data": f"no:{payment_id}"},
    ]]
    send_msg(ADMIN_ID, text, kb=kb)
    send_msg(
        int(user["id"]),
        f"Shukriya. Reference operator ke paas hai. Invoice <code>{_e(invoice_id)}</code>. Settle hote hi yahan sunega.",
    )
    shared._poke_desks_fn()


def _load_payment(payment_id: str) -> sqlite3.Row | None:
    if shared._sql is None:
        return None
    with shared._sql_lock:
        return shared._sql.execute("SELECT * FROM payments WHERE id = ?", (payment_id,)).fetchone()


def _claim_payment(payment_id: str) -> bool:
    """Atomic claim: only the first caller transitions pending→processing."""
    if shared._sql is None:
        return False
    with shared._sql_lock:
        cur = shared._sql.execute(
            "UPDATE payments SET status = 'processing', settled_at = ? WHERE id = ? AND status = 'pending'",
            (time.time(), payment_id),
        )
        shared._sql.commit()
        return cur.rowcount == 1


def _mark_payment(payment_id: str, status: str) -> None:
    if shared._sql is None:
        return
    with shared._sql_lock:
        shared._sql.execute(
            "UPDATE payments SET status = ?, settled_at = ? WHERE id = ?",
            (status, time.time(), payment_id),
        )
        shared._sql.commit()


def _settle_payment(payment_id: str, approve: bool, admin_mid: int | None, actor: int | None = None) -> None:
    row = _load_payment(payment_id)
    if row is None:
        return
    if row["status"] != "pending":
        if admin_mid:
            edit_msg(actor or ADMIN_ID, admin_mid, f"Ye reference pehle hi {row['status']} ho chuka.")
        return
    if not _claim_payment(payment_id):
        if admin_mid:
            edit_msg(actor or ADMIN_ID, admin_mid, "Ye reference abhi doosre operator ne pakda hai.")
        return
    plan = _plans().get(row["plan_id"]) or {
        "id": row["plan_id"], "name": row["plan_name"], "days": int(row["days"]),
        "amount": int(row["amount"]), "stars": int(row["amount"]),
    }
    member_id = int(row["chat_id"])
    gateway = str(_row_get(row, "gateway", "") or "")
    invoice_id = str(_row_get(row, "invoice_id", "") or "")
    who = actor or ADMIN_ID
    if not approve:
        _mark_payment(payment_id, "rejected")
        _mark_invoice(invoice_id, "rejected")
        _reset_utr_attempts(member_id)
        if admin_mid:
            edit_msg(who, admin_mid, f"Reject · ref <code>{_e(row['utr'])}</code> · {_e(plan['name'])}")
        send_msg(member_id, "Operator ko ye reference match nahi hua, isliye Premium nahi khula.")
        shared._poke_desks_fn()
        return
    when = ts_str()
    with shared._sl:
        user = _get_ud(member_id)
        until = _extend_premium(user, int(plan["days"]))
        _save_user(user)
        snapshot = dict(user)
        snapshot["history"] = list(user.get("history") or [])
    _mark_payment(payment_id, "approved")
    _mark_invoice(invoice_id, "paid")
    _reset_utr_attempts(member_id)
    expiry = fmt_until(until)
    if admin_mid:
        edit_msg(who, admin_mid, f"Approve · {_e(snapshot.get('name') or 'Bhai')} · {_e(plan['name'])} · {_e(expiry)} tak · <code>{_e(invoice_id)}</code>")
    send_msg(
        member_id,
        f"👑 <b>Premium khul gaya.</b>\n\n{_e(plan['name'])} chalega <b>{_e(expiry)}</b> tak. "
        f"Invoice <code>{_e(invoice_id)}</code>. Memo file mein hai.",
    )
    send_receipt(member_id, snapshot, plan, when, expiry, str(row["utr"]), gateway, invoice_id)
    shared._poke_desks_fn()


def _handle_stars_paid(msg: dict) -> None:
    sp = msg.get("successful_payment") or {}
    cid = int(msg["chat"]["id"])
    payload = str(sp.get("invoice_payload") or "")
    plan_id = payload.split("|", 1)[0].strip()
    charge = str(sp.get("telegram_payment_charge_id") or "")
    if not charge:
        charge = f"stars:{cid}:{sp.get('telegram_payment_charge_id') or uuid.uuid4().hex[:8]}"
    stars_paid = int(sp.get("total_amount") or 0)
    plan = _plans().get(plan_id)
    if shared._sql is None:
        send_msg(cid, "Bhai Telegram ne payment confirm kar di, par DB maintenance mein hai. Admin ko pakad.")
        send_msg(ADMIN_ID, f"Stars payment maintenance ke time. user <code>{cid}</code> charge <code>{_e(charge)}</code>")
        return
    with shared._sql_lock:
        exist = shared._sql.execute("SELECT id FROM payments WHERE utr = ?", (charge,)).fetchone()
    if exist:
        send_msg(cid, "Bhai ye Stars payment pehle se file pe hai.")
        return
    if not plan:
        send_msg(cid, "Bhai payment aa gayi par plan pehchaan nahi paya. Admin dekh lega.")
        send_msg(ADMIN_ID, f"Stars plan missing <code>{_e(plan_id)}</code> user <code>{cid}</code>")
        return
    when = ts_str()
    with shared._sl:
        user = _get_ud(
            cid,
            name=(msg.get("from") or {}).get("first_name"),
            username=(msg.get("from") or {}).get("username"),
        )
        until = _extend_premium(user, int(plan["days"]))
        _save_user(user)
        snapshot = dict(user)
    inv = _create_invoice(cid, "Stars", int(plan["amount"]), plan["id"], "paid")
    payment_id = uuid.uuid4().hex[:12]
    with shared._sql_lock:
        shared._sql.execute(
            "INSERT INTO payments (id, chat_id, plan_id, plan_name, amount, days, utr, status, created_at, settled_at, gateway, invoice_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'approved', ?, ?, 'Stars', ?)",
            (
                payment_id, cid, plan["id"], plan["name"], int(plan["amount"]),
                int(plan["days"]), charge, time.time(), time.time(), inv,
            ),
        )
        shared._sql.commit()
    expiry = fmt_until(until)
    _clear_flow(cid)
    send_msg(
        cid,
        f"👑 <b>Premium khul gaya.</b>\n\nStars se {_e(plan['name'])} lag gaya. "
        f"Chalega <b>{_e(expiry)}</b> tak.\nInvoice · <code>{_e(inv)}</code>",
    )
    send_receipt(cid, snapshot, plan, when, expiry, charge, "Stars", inv)
    send_msg(ADMIN_ID, f"Stars payment paid · <code>{_e(inv)}</code> · <code>{cid}</code> · {_e(plan['name'])} · {stars_paid} stars")
    shared._poke_desks_fn()
