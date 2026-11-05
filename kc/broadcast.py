"""Broadcast messaging to users and premium subscribers."""
import time

from kc import shared
from kc.config import ADMIN_ID
from kc.telegram_api import send_msg
from kc.utils import _e


def _broadcast_kb():
    return [[{"text": "📢 Sabko Bhej", "callback_data": "cast:all"}, {"text": "👑 Sirf Premium", "callback_data": "cast:prem"}]]


broadcast_kb = _broadcast_kb


def _broadcast(audience: str, text: str) -> None:
    if shared._sql is None:
        send_msg(ADMIN_ID, "Bhai DB hi nahi hai, broadcast kiske paas jayega?")
        return
    now = time.time()
    with shared._sql_lock:
        if audience == "prem":
            rows = shared._sql.execute(
                "SELECT chat_id FROM users WHERE is_premium = 1 AND premium_until > ?",
                (now,),
            ).fetchall()
        else:
            rows = shared._sql.execute("SELECT chat_id FROM users WHERE COALESCE(is_banned, 0) = 0").fetchall()
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
