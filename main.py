#!/usr/bin/env python3
"""
KaramChand OSINT Telegram Bot · Enterprise Multi-Source Edition
FastAPI + DuckDB (httpfs/HuggingFace) + Long Polling + SQLite members
Optimized for ultra-fast multi-source lookups across 7 distinct repositories:
  • KC-SOURCE-01 : Core Telecom & Citizen File (Hitek mixed)
  • KC-SOURCE-02 : Telecom Archive (Umba bucket)
  • KC-SOURCE-03 : BSNL Mobile Directory (18.1M records)
  • KC-SOURCE-04 : Indian Cars & Asset Registry (966k records)
  • KC-SOURCE-05 : Cellular Air Repository (73.1M records)
  • KC-SOURCE-06 : E-Commerce Customer Directory (3.8M records)
  • KC-SOURCE-07 : Telegram Master Registry (429.6M records)
Reconstructed dynamic UI: only non-empty sources are displayed.
"""

import csv
import io
import json
import logging
import os
import random
import re
import shutil
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

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
TOKEN       = os.getenv("BOT_TOKEN",  "8723126732:AAHVFOySgFaWN4_4InqtKrvkgN15crbPi0w")
ADMIN_ID    = int(os.getenv("ADMIN_ID", "5505679330"))
HF_TOKEN    = os.getenv("HF_TOKEN",   "hf_YOUR_READ_TOKEN_HERE")
BOT_LINK    = os.getenv("BOT_LINK",   "https://t.me/Karam_Chand_bot")
DEV_LINK    = os.getenv("DEV_LINK",   "https://t.me/ansafrozkhan")
DEV_HANDLE  = os.getenv("DEV_HANDLE", "@ansafrozkhan")
BOT_NAME    = os.getenv("BOT_NAME",   "KaramChand")
UPI_ID      = os.getenv("UPI_ID",     "admin@upi")
USDT_BEP20  = os.getenv("USDT_BEP20", "0x0000000000000000000000000000000000000000")
USDT_TRC20  = os.getenv("USDT_TRC20", "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx")
BASE_HF          = os.getenv("BASE_HF",          "hf://buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/")
BASE_HF_UMBA     = os.getenv("BASE_HF_UMBA",     "hf://buckets/Khanenterprises/umba-bucket/")
BASE_HF_BSNL     = os.getenv("BASE_HF_BSNL",     "hf://buckets/Khanenterprises/Bsnalumba/19M-BSNL_Mobile.parquet")
BASE_HF_CAR      = os.getenv("BASE_HF_CAR",      "hf://buckets/Khanenterprises/Carumba/Indian Cars Owner Details.parquet")
BASE_HF_AIR      = os.getenv("BASE_HF_AIR",      "hf://buckets/Khanenterprises/Umbaairbucket/data.parquet")
BASE_HF_FLIP     = os.getenv("BASE_HF_FLIP",     "hf://buckets/Khanenterprises/Umbaflip-bucket/flipkart.parquet")
BASE_HF_TG       = os.getenv("BASE_HF_TG",       "hf://buckets/Khanenterprises/Umbat-bucket/TELEGRAM_MASTER_DB.parquet")
DUCKDB_MEM_LIMIT = os.getenv("DUCKDB_MEM_LIMIT", "500MB")
DUCKDB_THREADS   = int(os.getenv("DUCKDB_THREADS", "4"))
BASE_URL         = f"https://api.telegram.org/bot{TOKEN}"

# ═══════════════════════════════════════════════════════════════════════════════
#  MANUAL SYSTEM CONFIGURATION (TUNE HERE FOR ANY SERVER / HOSTING SPECS)
# ═══════════════════════════════════════════════════════════════════════════════
# Configure your server architecture settings manually here (or via environment variables):
SYS_RAM_LIMIT          = os.getenv("SYS_RAM_LIMIT",          "50s = [f"@{row['username']}" if row["username"] else (row["name"] or "Bhai") for row in rows]
    send_msg(
        inviter_id,
        "<b>Tere invites land ho gaye.</b>\n\n"
        "Paanch log tere link se aaye. Ek din ka Premium desk pe lag gaya.\n\n"
        + "\n".join(f"• {_e(name)}" for name in names)
        + f"\n\nPremium <b>{_e(fmt_until(until))}</b> tak chalega.",
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  PAYMENTS · GATEWAYS · INVOICES
# ═══════════════════════════════════════════════════════════════════════════════
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
    flow[cid] = {"mode": "pay_choose", "plan": plan["id"], "ts": time.time()}
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
    flow[cid] = {"mode": "checkout", "plan": plan["id"], "gateway": "upi", "ts": time.time()}
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
    flow[cid] = {"mode": "stars", "plan": plan["id"], "gateway": "stars", "ts": time.time()}
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
    flow.pop(cid, None)
    desk_edit(cid, mid, "Bhai Stars invoice nahi bana. Thodi der baad try kar, ya doosra rasta le.", kb=_gateway_kb(plan["id"]))

def _open_crypto(cid: int, mid: int | None, plan: dict) -> None:
    flow[cid] = {"mode": "WAITING_FOR_TX", "plan": plan["id"], "gateway": "crypto", "ts": time.time()}
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
        if _sql is None:
            return inv
        with _sql_lock:
            row = _sql.execute("SELECT invoice_id FROM invoices WHERE invoice_id = ?", (inv,)).fetchone()
        if not row:
            return inv
    return "INV-" + uuid.uuid4().hex[:8].upper()

def _create_invoice(cid: int, gateway: str, amount: int, plan_id: str, status: str = "pending") -> str:
    inv = _new_invoice_id()
    now = time.time()
    paid_at = now if status == "paid" else None
    if _sql is None:
        return inv
    with _sql_lock:
        _sql.execute(
            "INSERT INTO invoices (invoice_id, chat_id, gateway, amount, plan_id, status, created_at, paid_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (inv, int(cid), gateway, int(amount), plan_id, status, now, paid_at),
        )
        _sql.commit()
    return inv

def _mark_invoice(inv: str, status: str) -> None:
    if _sql is None or not inv:
        return
    with _sql_lock:
        if status == "paid":
            _sql.execute("UPDATE invoices SET status = 'paid', paid_at = ? WHERE invoice_id = ?", (time.time(), inv))
        else:
            _sql.execute("DELETE FROM invoices WHERE invoice_id = ? AND status = 'pending'", (inv,))
        _sql.commit()

def _store_payment(user: dict, plan: dict, ref: str, gateway: str) -> tuple[str | None, str | None]:
    if _sql is None:
        return None, None
    with _sql_lock:
        existing = _sql.execute(
            "SELECT id, status FROM payments WHERE utr = ? AND status IN ('pending', 'approved')",
            (ref,),
        ).fetchone()
        if existing:
            return None, None
    invoice_id = _create_invoice(int(user["id"]), gateway, int(plan["amount"]), plan["id"], "pending")
    payment_id = uuid.uuid4().hex[:12]
    with _sql_lock:
        _sql.execute(
            "INSERT INTO payments (id, chat_id, plan_id, plan_name, amount, days, utr, status, created_at, settled_at, gateway, invoice_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, ?, ?)",
            (
                payment_id, int(user["id"]), plan["id"], plan["name"], int(plan["amount"]),
                int(plan["days"]), ref, time.time(), gateway, invoice_id,
            ),
        )
        _sql.commit()
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
    _poke_desks()

def _load_payment(payment_id: str) -> sqlite3.Row | None:
    if _sql is None:
        return None
    with _sql_lock:
        return _sql.execute("SELECT * FROM payments WHERE id = ?", (payment_id,)).fetchone()

def _claim_payment(payment_id: str) -> bool:
    """Atomic claim: only the first caller transitions pending→processing."""
    if _sql is None:
        return False
    with _sql_lock:
        cur = _sql.execute(
            "UPDATE payments SET status = 'processing', settled_at = ? WHERE id = ? AND status = 'pending'",
            (time.time(), payment_id),
        )
        _sql.commit()
        return cur.rowcount == 1

def _mark_payment(payment_id: str, status: str) -> None:
    if _sql is None:
        return
    with _sql_lock:
        _sql.execute(
            "UPDATE payments SET status = ?, settled_at = ? WHERE id = ?",
            (status, time.time(), payment_id),
        )
        _sql.commit()

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
        _poke_desks()
        return
    when = ts_str()
    with _sl:
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
    _poke_desks()

def _handle_stars_paid(msg: dict) -> None:
    sp = msg.get("successful_payment") or {}
    cid = int(msg["chat"]["id"])
    payload = str(sp.get("invoice_payload") or "")
    plan_id = payload.split("|", 1)[0].strip()
    charge = str(sp.get("telegram_payment_charge_id") or "")
    stars_paid = int(sp.get("total_amount") or 0)
    plan = _plans().get(plan_id)
    if _sql is None:
        send_msg(cid, "Bhai Telegram ne payment confirm kar di, par DB maintenance mein hai. Admin ko pakad.")
        send_msg(ADMIN_ID, f"Stars payment maintenance ke time. user <code>{cid}</code> charge <code>{_e(charge)}</code>")
        return
    if charge:
        with _sql_lock:
            exist = _sql.execute("SELECT id FROM payments WHERE utr = ?", (charge,)).fetchone()
        if exist:
            send_msg(cid, "Bhai ye Stars payment pehle se file pe hai.")
            return
    if not plan:
        send_msg(cid, "Bhai payment aa gayi par plan pehchaan nahi paya. Admin dekh lega.")
        send_msg(ADMIN_ID, f"Stars plan missing <code>{_e(plan_id)}</code> user <code>{cid}</code>")
        return
    when = ts_str()
    with _sl:
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
    with _sql_lock:
        _sql.execute(
            "INSERT INTO payments (id, chat_id, plan_id, plan_name, amount, days, utr, status, created_at, settled_at, gateway, invoice_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'approved', ?, ?, 'Stars', ?)",
            (
                payment_id, cid, plan["id"], plan["name"], int(plan["amount"]),
                int(plan["days"]), charge or payment_id, time.time(), time.time(), inv,
            ),
        )
        _sql.commit()
    expiry = fmt_until(until)
    _clear_flow(cid)
    send_msg(
        cid,
        f"👑 <b>Premium khul gaya.</b>\n\nStars se {_e(plan['name'])} lag gaya. "
        f"Chalega <b>{_e(expiry)}</b> tak.\nInvoice · <code>{_e(inv)}</code>",
    )
    send_receipt(cid, snapshot, plan, when, expiry, charge or payment_id, "Stars", inv)
    send_msg(ADMIN_ID, f"Stars payment paid · <code>{_e(inv)}</code> · <code>{cid}</code> · {_e(plan['name'])} · {stars_paid} stars")
    _poke_desks()

# ═══════════════════════════════════════════════════════════════════════════════
#  SPIN
# ═══════════════════════════════════════════════════════════════════════════════
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
    if state.get("mode") == "spin":
        pending = state.get("pending_query")
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
    if pending and pending.isdigit() and len(pending) == 10:
        _enqueue_search(cid, pending)

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

# ═══════════════════════════════════════════════════════════════════════════════
#  BROADCAST
# ═══════════════════════════════════════════════════════════════════════════════
def _broadcast(audience: str, text: str) -> None:
    if _sql is None:
        send_msg(ADMIN_ID, "Bhai DB hi nahi hai, broadcast kiske paas jayega?")
        return
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
    for i, row in enumerate(rows, 1):
        mid = send_msg(int(row["chat_id"]), body)
        if mid:
            sent += 1
        else:
            failed += 1
        time.sleep(0.04)
        if i % 100 == 0:
            send_msg(ADMIN_ID, f"Broadcast progress · {i}/{len(rows)} sent · {sent} ok · {failed} fail")
    who = "Premium walon" if audience == "prem" else "sab members"
    send_msg(ADMIN_ID, f"Broadcast {who} ko band. Pahuncha · <b>{sent}</b>. Nahi pahuncha · <b>{failed}</b>.")

# ═══════════════════════════════════════════════════════════════════════════════
#  ADMIN CONSOLE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def admin_stats_text() -> str:
    if _sql is None:
        return "<b>Admin desk</b>\n\nBhai DB missing hai. /restore kar. Empty DB nahi banaunga."
    now = time.time()
    with _sql_lock:
        total = int(_sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        premium = int(_sql.execute(
            "SELECT COUNT(*) AS n FROM users WHERE is_premium = 1 AND premium_until > ?",
            (now,),
        ).fetchone()["n"])
        searches = int(_sql.execute("SELECT COALESCE(SUM(total_searches), 0) AS n FROM users").fetchone()["n"])
        banned = int(_sql.execute("SELECT COUNT(*) AS n FROM users WHERE COALESCE(is_banned, 0) = 1").fetchone()["n"])
        open_t = int(_sql.execute("SELECT COUNT(*) AS n FROM tickets WHERE status = 'open'").fetchone()["n"])
        pend = int(_sql.execute("SELECT COUNT(*) AS n FROM invoices WHERE status = 'pending'").fetchone()["n"])
    flag = "ON" if MAINTENANCE else "OFF"
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
    with _sl:
        user = _get_ud(target)
        until = _extend_premium(user, days)
        _save_user(user)
    expiry = fmt_until(until)
    send_msg(target, f"👑 Bhai sun, operator ne tujhe <b>{days}</b> din ka premium de diya. Chalega <b>{_e(expiry)}</b> tak.")
    send_msg(cid, f"Ho gaya. <code>{target}</code> ko {days} din premium. <b>{_e(expiry)}</b> tak.")

def _log_ban(chat_id: int, action: str, admin_id: int, reason: str) -> None:
    if _sql is None:
        return
    with _sql_lock:
        _sql.execute(
            "INSERT INTO ban_history (chat_id, action, admin_id, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
            (int(chat_id), action, int(admin_id), time.time(), reason or ""),
        )
        _sql.commit()

def _set_ban(target: int, banned: bool, admin_id: int, reason: str) -> None:
    with _sl:
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
    if _load_user(target) is None and target not in user_data:
        send_msg(cid, "Bhai ye ID file pe nahi hai.")
        return
    _set_ban(target, False, cid, reason)
    send_msg(target, "Bhai ban hat gaya. Desk wapas khul gayi. Sharafat se chal.")
    send_msg(cid, f"Unban ho gaya. <code>{target}</code>. Reason · {_e(reason)}")

def _ban_history_lines(target: int, limit: int = 15) -> list[str]:
    if _sql is None:
        return ["DB missing hai bhai."]
    with _sql_lock:
        rows = _sql.execute(
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
    with _sl:
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
    with _sl:
        user = _get_ud(target)
        user["role"] = "USER"
        _save_user(user)
    if _sql is not None:
        with _sql_lock:
            cur = _sql.execute(
                "UPDATE tickets SET status = 'open', agent_id = NULL WHERE agent_id = ? AND status = 'claimed'",
                (target,),
            )
            n = cur.rowcount if cur.rowcount is not None else 0
            _sql.commit()
    send_msg(target, "Bhai tujhe role se hata diya. Ticket jo lock thi, woh unfreeze ho gayi. Ab user wala desk hai.")
    send_msg(cid, f"Fire ho gaya <code>{target}</code>. {n} ticket unfreeze. Ab tu unmein ghuss sakta hai.")
    _poke_desks()

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
        stars = int(rest[0]); rest = rest[1:]
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
    flow[cid] = {"mode": "PLAN_BCAST", "blurb": blurb, "ts": time.time()}
    send_msg(
        cid,
        f"Plan update ho gaya.\n\n{_e(blurb)}\n\nNayi price sabko broadcast karun?",
        kb=[[{"text": "Haan broadcast", "callback_data": "planbc:yes"}, {"text": "Nahi rehne de", "callback_data": "planbc:no"}]],
    )

def admin_send_csv(cid: int) -> None:
    if _sql is None:
        send_msg(cid, "Bhai DB missing hai. CSV kiski?")
        return
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["chat_id", "username", "role", "is_premium", "premium_until", "total_searches", "is_banned"])
    with _sql_lock:
        rows = _sql.execute(
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

def _restore_db(blob: bytes) -> str | None:
    global _sql, MAINTENANCE
    if len(blob) < 16 or not blob.startswith(b"SQLite format 3\x00"):
        return "Bhai ye SQLite file nahi hai. Sahi users.db bhej."
    tmp = _DB_PATH + ".incoming"
    try:
        with open(tmp, "wb") as fh:
            fh.write(blob)
        test = sqlite3.connect(tmp)
        test.execute("SELECT name FROM sqlite_master LIMIT 1").fetchall()
        test.close()
    except Exception as exc:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return "File phati padi hai bhai: " + str(exc)
    with _sql_lock:
        if _sql is not None:
            try:
                _sql.close()
            except Exception:
                pass
            _sql = None
    for side in (_DB_PATH + "-wal", _DB_PATH + "-shm"):
        try:
            os.remove(side)
        except OSError:
            pass
    os.replace(tmp, _DB_PATH)
    try:
        _sql = sqlite3.connect(_DB_PATH, check_same_thread=False)
        _sql.row_factory = sqlite3.Row
        _init_sqlite()
        _seed_settings()
    except Exception as exc:
        MAINTENANCE = True
        return "DB file lagi nahi bhai: " + str(exc)
    MAINTENANCE = False
    try:
        db_search("7417679558")
    except Exception as exc:
        log.warning("DuckDB health still failing post-restore: %s", exc)
        send_msg(
            ADMIN_ID,
            "ℹ️ SQLite restore ho gayi aur maintenance OFF kar diya. "
            f"Par DuckDB health abhi bhi fail hai (<i>{_e(str(exc)[:120])}</i>) — searches tab tak timeout honge.",
        )
    return None

def _handle_restore_doc(cid: int, msg: dict) -> bool:
    state = flow.get(cid) or {}
    doc = msg.get("document") or {}
    name = str(doc.get("file_name") or "")
    if state.get("mode") != "WAITING_FOR_DB" and not (MAINTENANCE and name.lower().endswith(".db")):
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

# ═══════════════════════════════════════════════════════════════════════════════
#  TICKETS · ROLES · SINGLE-EDIT DESK
# ═══════════════════════════════════════════════════════════════════════════════
def _ticket(tid: int) -> sqlite3.Row | None:
    if _sql is None:
        return None
    with _sql_lock:
        return _sql.execute("SELECT * FROM tickets WHERE id = ?", (tid,)).fetchone()

def _ticket_replies(tid: int, limit: int = 8) -> list[sqlite3.Row]:
    if _sql is None:
        return []
    with _sql_lock:
        return _sql.execute(
            "SELECT * FROM ticket_replies WHERE ticket_id = ? ORDER BY id DESC LIMIT ?",
            (tid, limit),
        ).fetchall()

def _add_reply(tid: int, agent: int, body: str) -> None:
    if _sql is None:
        return
    with _sql_lock:
        _sql.execute(
            "INSERT INTO ticket_replies (ticket_id, agent_id, body, created_at) VALUES (?, ?, ?, ?)",
            (tid, agent, body, time.time()),
        )
        _sql.commit()

def _open_ticket(cid: int, text: str) -> int | None:
    if _sql is None:
        return None
    with _sql_lock:
        cur = _sql.execute(
            "INSERT INTO tickets (chat_id, status, agent_id, created_at, resolved_at, agent_notes) "
            "VALUES (?, 'open', NULL, ?, NULL, ?)",
            (cid, time.time(), text),
        )
        _sql.commit()
        return int(cur.lastrowid)

def _claim_ticket(tid: int, agent: int) -> str:
    if _sql is None:
        return "missing"
    with _sql_lock:
        row = _sql.execute("SELECT status, agent_id FROM tickets WHERE id = ?", (tid,)).fetchone()
        if row is None:
            return "missing"
        owner = int(row["agent_id"] or 0)
        if row["status"] == "claimed" and owner == agent:
            return "yours"
        if row["status"] != "open" or owner:
            return "locked"
        cur = _sql.execute(
            "UPDATE tickets SET status = 'claimed', agent_id = ? WHERE id = ? AND status = 'open' AND (agent_id IS NULL OR agent_id = 0)",
            (agent, tid),
        )
        _sql.commit()
        if cur.rowcount != 1:
            return "locked"
    return "ok"

def _resolve_ticket(tid: int, agent: int) -> bool:
    if _sql is None:
        return False
    with _sql_lock:
        row = _sql.execute("SELECT status, agent_id FROM tickets WHERE id = ?", (tid,)).fetchone()
        if not row or row["status"] != "claimed" or int(row["agent_id"] or 0) != agent:
            return False
        _sql.execute("UPDATE tickets SET status = 'resolved', resolved_at = ? WHERE id = ?", (time.time(), tid))
        _sql.commit()
    return True

def _can_touch_ticket(cid: int, row: sqlite3.Row) -> bool:
    if row["status"] != "claimed":
        return True
    return int(row["agent_id"] or 0) == cid

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
            rows.append(row); row = []
    if row:
        rows.append(row)
    return rows

def _render_home(cid: int) -> tuple[str, list]:
    if _sql is None:
        text = "<b>Admin desk</b>\n\nBhai users.db gayab hai. Empty file nahi banaunga.\n/restore karke .db document bhej.\n\n" + _D
        return text, _desk_nav(cid)
    now = time.time()
    with _sql_lock:
        total = int(_sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        open_t = int(_sql.execute("SELECT COUNT(*) AS n FROM tickets WHERE status = 'open'").fetchone()["n"])
        claimed = int(_sql.execute("SELECT COUNT(*) AS n FROM tickets WHERE status = 'claimed'").fetchone()["n"])
        pend = int(_sql.execute("SELECT COUNT(*) AS n FROM invoices WHERE status = 'pending'").fetchone()["n"])
        premium = int(_sql.execute(
            "SELECT COUNT(*) AS n FROM users WHERE is_premium = 1 AND premium_until > ?",
            (now,),
        ).fetchone()["n"])
    maint = "ON — log bahar hain" if MAINTENANCE else "OFF"
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
    if _sql is None:
        return "Bhai DB missing hai. Ticket kahan se aayega?", [[{"text": "🔙 Wapas", "callback_data": "d:home"}]]
    page = max(0, page)
    size = 4
    with _sql_lock:
        total = int(_sql.execute("SELECT COUNT(*) AS n FROM tickets").fetchone()["n"])
        rows = _sql.execute(
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
    if _sql is None:
        return "Bhai DB missing hai.", [[{"text": "🔙 Wapas", "callback_data": "d:home"}]]
    page = max(0, page)
    size = 6
    with _sql_lock:
        total = int(_sql.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        rows = _sql.execute(
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
    if _sql is None:
        return "Bhai DB missing hai.", [[{"text": "🔙 Wapas", "callback_data": "d:home"}]]
    page = max(0, page)
    size = 5
    with _sql_lock:
        total = int(_sql.execute("SELECT COUNT(*) AS n FROM invoices").fetchone()["n"])
        rows = _sql.execute(
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
            with _sql_lock:
                pay = _sql.execute(
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
    _desk[cid] = {"mid": mid, "section": section, "page": page, "focus": focus}
    text, kb = _render_desk(cid, section, page, focus)
    if mid:
        edit_msg(cid, mid, text, kb)
        return
    new_mid = send_msg(cid, text, kb)
    if new_mid:
        _desk[cid]["mid"] = new_mid

def _open_desk(cid: int, section: str = "home") -> None:
    if not _can_desk(cid):
        send_msg(cid, "Bhai ye command teri post ki nahi hai.")
        return
    if section not in set(_desk_sections(cid)) | {"home"}:
        section = "home"
    st = _desk.get(cid) or {}
    mid = st.get("mid")
    text, kb = _render_desk(cid, section, 0, None)
    _desk[cid] = {"mid": mid, "section": section, "page": 0, "focus": None}
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
        _desk[cid]["mid"] = new_mid

def _poke_desks() -> None:
    for cid, st in list(_desk.items()):
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
    if _sql is None:
        send_msg(cid, "Bhai receipt file bhi nahi, DB bhi nahi.")
        return
    with _sql_lock:
        row = _sql.execute("SELECT * FROM invoices WHERE invoice_id = ?", (inv,)).fetchone()
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
    if _sql is None:
        send_msg(cid, "Bhai DB missing hai.")
        return
    with _sql_lock:
        rows = _sql.execute("SELECT * FROM tickets WHERE agent_id = ? ORDER BY id", (cid,)).fetchall()
        reply_n = int(_sql.execute(
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
    st = flow.get(cid) or {}
    desk = _desk.get(cid) or {}
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
        send_msg(cid, "Bhai ye ticket doosre agent pe lock hai. Interfere nahi chalega. /removerole ke baad super admin dekh sakta hai.")
        return
    user = _load_user(int(row["chat_id"]))
    if user is None:
        send_msg(cid, "Bhai jisne ticket kholi woh file pe nahi mila.")
        return
    send_msg(cid, "<b>Ticket wala profile — sirf yahi</b>\n\n" + msg_profile(user))

def _cmd_role_audit(cid: int, raw: str) -> None:
    if cid != ADMIN_ID:
        send_msg(cid, "Bhai audit sirf super admin ka hai.")
        return
    if _sql is None:
        send_msg(cid, "Bhai DB missing hai.")
        return
    parts = raw.split()
    if len(parts) == 1:
        with _sql_lock:
            rows = _sql.execute(
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
    with _sql_lock:
        tickets = _sql.execute("SELECT * FROM tickets WHERE agent_id = ? ORDER BY id", (target,)).fetchall()
        replies = _sql.execute("SELECT * FROM ticket_replies WHERE agent_id = ? ORDER BY id", (target,)).fetchall()
    lines = [
        f"<b>Audit</b> · <code>{target}</code>",
        f"Naam · {_e((user or {}).get('name') or 'file pe nahi')}",
        f"Role · {_e(_db_role(target))}",
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

def _ask_pay_search(cid: int, number: str) -> None:
    st = flow.setdefault(cid, {})
    st["pending_search"] = number
    send_msg(
        cid,
        "Bhai tu payment mein hai. Confirm kar, agar tu payment cancel karke search karna chahta hai?",
        kb=[[
            {"text": "Haan, cancel karke search", "callback_data": "paysearch:yes"},
            {"text": "Nahi, payment chalu", "callback_data": "paysearch:no"},
        ]],
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  WORKER THREAD (High-Speed Concurrent Multi-Source Querying)
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
        try:
            tg("sendChatAction", {"chat_id": cid, "action": "typing"})
            with _ql:
                _queue_order[:] = [x for x in _queue_order if not (x["chat_id"] == cid and x["msg_id"] == mid)]
                snapshot = list(_queue_order)
            for i, item in enumerate(snapshot, start=1):
                try:
                    edit_msg(item["chat_id"], item["msg_id"], msg_queue(i, item["query"]))
                except Exception:
                    pass

            edit_msg(cid, mid, msg_searching(query))

            # Run all 7 source searches concurrently in parallel threads
            stages = _next_load_stages()
            all_sources: dict[str, list[dict]] = {s["id"]: [] for s in SOURCES}

            def _make_fetcher(src_def: dict):
                def _fetch():
                    with _source_semaphore:
                        try:
                            all_sources[src_def["id"]] = src_def["search_fn"](query)
                        except Exception as ex:
                            log.warning("%s query error: %s", src_def["name"], ex)
                            all_sources[src_def["id"]] = []
                return _fetch

            timeout_secs = float(SYS_SEARCH_TIMEOUT)
            t_db = time.monotonic()
            threads = [
                threading.Thread(target=_make_fetcher(s), daemon=True)
                for s in SOURCES
            ]
            for th in threads:
                th.start()

            # Display quick animated hacker stages
            for label in stages[:2]:
                time.sleep(_STAGE_GAP)
                edit_msg(cid, mid, msg_stage(query, label))

            for th in threads:
                th.join(timeout=timeout_secs)

            elapsed = time.monotonic() - t_db

            with _sl:
                global_stats["searches"] += 1
                if cid in user_data:
                    user_data[cid]["searches"] += 1

            total_found = sum(len(r) for r in all_sources.values())
            max_pages = max([len(r) for r in all_sources.values() if r] or [0])

            if total_found == 0:
                with _sl:
                    _remember(cid, query, 0)
                edit_msg(cid, mid, msg_no_result(query), kb=back_kb())
            else:
                with _sl:
                    global_stats["results"] += total_found
                    if cid in user_data:
                        user_data[cid]["results"] += total_found
                    _remember(cid, query, total_found)
                    user_sessions[cid] = {
                        "sources":      all_sources,
                        "results":      all_sources.get("main", []),
                        "results_umba": all_sources.get("umba", []),
                        "page":         0,
                        "query":        query,
                        "ts":           time.time(),
                    }

                # Page 0: Take first row of each source that found records
                page_0_data = {k: (v[0] if v else None) for k, v in all_sources.items()}
                edit_msg(cid, mid, msg_stage(query, "✅ Match mil gaya!"))
                time.sleep(0.2)
                edit_msg(
                    cid, mid,
                    format_card_sources(page_0_data, query, elapsed),
                    kb=result_kb(0, max_pages),
                )

        except Exception:
            log.exception("Worker error")
            edit_msg(cid, mid, "⚠️ Bhai lookup mein dikkat aayi. Thodi der ruk, phir try kar.", kb=back_kb())
        finally:
            with _sl:
                active_users.discard(cid)
            _search_q.task_done()

# ═══════════════════════════════════════════════════════════════════════════════
#  QUEUE BROADCASTER & GC
# ═══════════════════════════════════════════════════════════════════════════════
def _broadcaster() -> None:
    while True:
        time.sleep(QUEUE_BROADCAST_IV)
        with _ql:
            snapshot = list(_queue_order)
        for i, item in enumerate(snapshot, start=1):
            try:
                edit_msg(item["chat_id"], item["msg_id"], msg_queue(i, item["query"]))
            except Exception:
                pass

def _session_gc() -> None:
    while True:
        time.sleep(GC_INTERVAL)
        cutoff = time.time() - SESSION_TTL
        with _sl:
            stale = [cid for cid, s in user_sessions.items() if s["ts"] < cutoff]
            for cid in stale:
                del user_sessions[cid]

# ═══════════════════════════════════════════════════════════════════════════════
#  ADMIN UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════════════
#  SEARCH ENQUEUE
# ═══════════════════════════════════════════════════════════════════════════════
def _enqueue_search(cid: int, query: str) -> None:
    if MAINTENANCE and cid != ADMIN_ID:
        send_msg(cid, _MAINT_TEXT)
        return
    with _sl:
        if cid in active_users:
            busy = True
            user = user_data.get(cid) or _get_ud(cid)
        else:
            busy = False
            user = _get_ud(cid)
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
            if user.get("last_search_date") != today:
                user["daily_searches"] = 0
            allowance = _free_limit() + _bonus_today(cid)
            used = int(user.get("daily_searches") or 0)
            if used >= allowance:
                blocked = True
                _save_user(user)
            else:
                blocked = False
                counted = True
                user["daily_searches"] = used + 1
                user["last_search_date"] = today
                _save_user(user)
        if blocked:
            send_msg(cid, msg_limit(user), kb=[[{"text": "👑 Premium Pass", "callback_data": "premium"}]])
            return
    if _search_q.qsize() >= SYS_MAX_QUEUE:
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
        if cid in user_data:
            user_data[cid]["last_search"] = time.time()
    with _ql:
        approx_pos = len(_queue_order) + 1
    mid = send_msg(cid, msg_queue(approx_pos, query))
    if not mid:
        with _sl:
            active_users.discard(cid)
            if counted and cid in user_data:
                user_data[cid]["daily_searches"] = max(0, int(user_data[cid].get("daily_searches") or 0) - 1)
                _save_user(user_data[cid])
        return
    with _ql:
        _queue_order.append({"chat_id": cid, "msg_id": mid, "query": query})
    _search_q.put((cid, query, mid))

def _maybe_spin(cid: int, raw: str) -> bool:
    is_num = raw.isdigit() and len(raw) == 10
    today = _today()
    first = seen_day.get(cid) != today
    seen_day[cid] = today
    with _sl:
        user = _get_ud(cid)
        due = _spin_due(user)
    if not due or not (first or is_num):
        return False
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

# ═══════════════════════════════════════════════════════════════════════════════
#  DESK CALLBACK HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
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
        flow[cid] = {"mode": "IN_TICKET", "ticket_id": tid}
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
            flow[cid] = {"mode": "IN_TICKET", "ticket_id": int(arg)}
        _desk_show(cid, mid, "ticket", focus=int(arg))
        _poke_desks()
        return
    if head == "rs" and arg.isdigit():
        ok = _resolve_ticket(int(arg), cid)
        if not ok:
            send_msg(cid, "Bhai resolve nahi hua. Ya toh lock teri nahi, ya ticket pehle hi band hai.")
        else:
            flow[cid] = {"mode": "IN_TICKET", "ticket_id": int(arg)}
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
        flow[cid] = {"mode": "TICKET_REPLY", "ticket_id": int(arg)}
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
        _desk_show(cid, mid, "inv", int((_desk.get(cid) or {}).get("page") or 0))
        return
    if head == "rj" and _can_settle(cid):
        _settle_payment(arg, False, None, actor=cid)
        _desk_show(cid, mid, "inv", int((_desk.get(cid) or {}).get("page") or 0))
        return
    _desk_show(cid, mid, "home")

# ═══════════════════════════════════════════════════════════════════════════════
#  PRE-CHECKOUT & CALLBACK HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
def handle_pre_checkout(q: dict) -> None:
    qid = q.get("id")
    if not qid:
        return
    try:
        _session.post(
            f"{BASE_URL}/answerPreCheckoutQuery",
            json={"pre_checkout_query_id": qid, "ok": True},
            timeout=5,
        )
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
    with _sl:
        user = _get_ud(cid, name=name, username=username)
    if cid != ADMIN_ID and _banned(user):
        send_msg(cid, _BAN_TEXT)
        return
    if cid != ADMIN_ID and MAINTENANCE and not data.startswith("d:"):
        send_msg(cid, _MAINT_TEXT)
        return

    if data.startswith("d:"):
        if not _can_desk(cid):
            send_msg(cid, "Bhai desk teri post ki nahi.")
            return
        _handle_desk_cb(cid, mid, data)
        return

    if data.startswith("gwtog:"):
        if cid != ADMIN_ID:
            send_msg(cid, "Bhai gateway sirf super admin hilata hai.")
            return
        note = _toggle_gateway(data.split(":", 1)[1])
        text, kb = _render_settings(cid)
        text = "<b>" + _e(note) + "</b>\n\n" + text
        _desk[cid] = {"mid": mid, "section": "set", "page": 0, "focus": None}
        edit_msg(cid, mid, _clip(text), kb)
        return

    if data.startswith("planbc:"):
        if cid != ADMIN_ID:
            return
        blurb = (flow.get(cid) or {}).get("blurb") or "Plan ki price update ho gayi."
        _clear_flow(cid)
        if data.endswith("yes"):
            send_msg(cid, "Theek hai, nayi price broadcast kar raha hoon.")
            threading.Thread(target=_broadcast, args=("all", blurb), daemon=True).start()
        else:
            send_msg(cid, "Broadcast nahi kiya. Price file pe update hai.")
        return

    if data == "paycancel":
        _clear_flow(cid)
        desk_edit(cid, mid, "Payment cancel kar diya bhai. Jab mann kare number daal.", kb=dashboard_kb())
        return

    if data == "paysearch:yes":
        number = (flow.get(cid) or {}).get("pending_search")
        _clear_flow(cid)
        send_msg(cid, "Theek hai, payment cancel. Search chala raha hoon.")
        if number and number.isdigit() and len(number) == 10:
            _enqueue_search(cid, number)
        return

    if data == "paysearch:no":
        if cid in flow:
            flow[cid].pop("pending_search", None)
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
        with _sl:
            user = _get_ud(cid)
            text = msg_profile(user)
        desk_edit(cid, mid, text, kb=back_kb())
    elif data == "premium":
        if _premium_lock_screen(cid, mid):
            return
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
        flow[cid] = {"mode": "WAITING_FOR_UTR", "plan": plan["id"], "gateway": "upi", "ts": time.time()}
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
        flow[cid] = {"mode": "WAITING_FOR_BROADCAST", "audience": audience}
        who = "sirf Premium wale" if audience == "prem" else "har member"
        desk_edit(cid, mid, f"Agla message bhej. Ye <b>{who}</b> ke paas jayega.\n\n/cancel se ye band.")
    elif data.startswith("pg:"):
        page = int(data.split(":")[1])
        with _sl:
            sess = user_sessions.get(cid)
        if sess:
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
        with _sl:
            sess = user_sessions.get(cid)
        if sess:
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
                    send_html_file(
                        cid,
                        f"{BOT_NAME}_Report_{phone}.html",
                        generate_html_combined(row_main, row_umba, query),
                        "📄 Le bhai, teri KC directory note attach hai.",
                    )

# ═══════════════════════════════════════════════════════════════════════════════
#  COMMAND DISPATCH
# ═══════════════════════════════════════════════════════════════════════════════
def _start_payload(text: str) -> str | None:
    head, _, rest = text.partition(" ")
    return rest.strip() if head.startswith("/start") else None

def _dispatch_cmd(cid: int, raw: str) -> None:
    cmd = raw.split()[0].split("@")[0].lower()
    if cmd == "/start":
        payload = _start_payload(raw) or ""
        if payload:
            _apply_referral(cid, payload)
        if cid != ADMIN_ID and MAINTENANCE:
            send_msg(cid, _MAINT_TEXT)
            return
        if _guard_join(cid):
            return
        _show_dashboard(cid)
        if cid == ADMIN_ID and MAINTENANCE:
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
            flow[cid] = {"mode": "WAITING_FOR_DB", "ts": time.time()}
            send_msg(cid, "Bhai users.db file yahin document karke bhej. Text mat likh, file chipka.")
            return
            
        if cmd == "/initdb":
            send_msg(cid, "⚠️ Bhai ye purana DB delete karke naya khali DB banayega. Tu sure hai?", 
                     kb=[[{"text": "Haan, DB naya bana", "callback_data": "dbinit:yes"}, {"text": "Nahi, cancel", "callback_data": "dbinit:no"}]])
            return
            
        if cmd == "/dashboard":
            if cid in _desk:
                _desk[cid]["mid"] = None
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
        if cid in _desk:
            _desk[cid]["mid"] = None
        _open_desk(cid, "home")
        return
    
    if cmd == "/mywork":
        _cmd_mywork(cid)
        return
    if cmd == "/data":
        _cmd_data(cid)
        return

# ═══════════════════════════════════════════════════════════════════════════════
#  MESSAGE ROUTER
# ═══════════════════════════════════════════════════════════════════════════════
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

    with _sl:
        user = _get_ud(cid, name=name, username=un)

    if cid != ADMIN_ID and MAINTENANCE:
        send_msg(cid, _MAINT_TEXT)
        return

    if cid == ADMIN_ID and msg.get("reply_to_message") and raw:
        target = support_routes.get(msg["reply_to_message"].get("message_id"))
        if target:
            send_msg(target, f"<b>Desk se jawab</b>\n\n{_e(raw)}")
            send_msg(cid, "Member ke paas pahunch gaya.")
            return

    state = flow.get(cid) or {}
    mode = state.get("mode")

    if cid == ADMIN_ID and mode == "WAITING_FOR_BROADCAST":
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
        with _sl:
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
        plan = _plans().get(state.get("plan") or "")
        if not plan:
            _clear_flow(cid)
            send_msg(cid, "Crypto checkout expire ho gaya. Premium khol ke phir try kar.", kb=dashboard_kb())
            return
        if not re.fullmatch(r"[A-Za-z0-9]{16,80}", raw):
            send_msg(cid, "Bhai sahi tx hash bhej. 16 se 80 letters ya digits. UTR nahi, crypto hash.")
            return
        with _sl:
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
        flow[cid] = {"mode": "IN_TICKET", "ticket_id": tid}
        st = _desk.get(cid) or {}
        if st.get("mid"):
            _desk_show(cid, st["mid"], "ticket", focus=tid)
        return

    if mode == "WAITING_FOR_SUPPORT":
        if not raw:
            send_msg(cid, "Bhai khali message se ticket nahi khulta. Likh ke bhej.")
            return
        if _sql is None:
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
    try:
        _session.get(f"{BASE_URL}/deleteWebhook?drop_pending_updates=true", timeout=5)
    except Exception:
        pass
    offset, backoff = None, 1
    while True:
        try:
            allowed = urllib.parse.quote(json.dumps(["message", "callback_query", "pre_checkout_query"]))
            url = f"{BASE_URL}/getUpdates?timeout=30&allowed_updates={allowed}" + (f"&offset={offset}" if offset else "")
            resp = _session.get(url, timeout=35).json()
            if not resp.get("ok"):
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            backoff = 1
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd:
                        handle_cb(upd["callback_query"])
                    elif "pre_checkout_query" in upd:
                        handle_pre_checkout(upd["pre_checkout_query"])
                    elif "message" in upd:
                        handle_msg(upd["message"])
                except Exception:
                    log.exception("Update handler crash")
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception:
            time.sleep(3)

@app.api_route("/", methods=["GET", "HEAD"])
def health():
    return {"status": "alive", "queue": _search_q.qsize(), "maintenance": MAINTENANCE}

def _startup_health() -> None:
    global MAINTENANCE
    reasons = []
    if _sql is None or not os.path.isfile(_DB_PATH):
        MAINTENANCE = True
        reasons.append("users.db missing hai. /restore se file daal, ya /initdb se fresh start kar.")
    try:
        db_search("7417679558")
    except Exception as exc:
        MAINTENANCE = True
        reasons.append("DuckDB test number 7417679558 fail: " + str(exc))
        log.exception("startup duckdb health failed")
    if reasons:
        MAINTENANCE = True
        try:
            send_msg(ADMIN_ID, "⚠️ <b>Maintenance ON</b>\n\n" + "\n".join("• " + _e(r) for r in reasons))
        except Exception:
            log.exception("admin health alert failed")
    else:
        log.info("health check ok")

_startup_health()

for _fn in [worker, poll_telegram, _broadcaster, _session_gc, _persist_loop, _backup_loop, _expiry_loop]:
    threading.Thread(target=_fn, daemon=True).start()

if __name__ == "__main__":
    try:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), log_level="info")
    except Exception:
        log.exception("http server failed, parking process")
        while True:
            time.sleep(3600)
