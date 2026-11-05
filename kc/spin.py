"""Daily lucky spin feature, jackpot limits, and windows."""
import threading
import time
from datetime import datetime, timedelta

from kc import shared
from kc.config import (
    SPIN_INTERVAL,
    SPIN_JACKPOT,
    SPIN_WINNER_LIMIT,
    SPIN_WINS,
)
from kc.sqlite_db import _kv_get, _kv_set, _save_user
from kc.telegram_api import send_msg, tg
from kc.user_ops import (
    _add_bonus,
    _clear_flow,
    _extend_premium,
    _get_ud,
)
from kc.utils import (
    IST,
    _D,
    _e,
    fmt_td,
    fmt_until,
    now_ist,
)


def _window_id(now: datetime | None = None) -> str:
    now = now or now_ist()
    if SPIN_INTERVAL == "weekly":
        y, w, _ = now.isocalendar()
        return f"{y}-W{w:02d}"
    if SPIN_INTERVAL == "alternate":
        epoch = datetime(2026, 1, 1, tzinfo=IST).date()
        return f"alt-{(now.date() - epoch).days // 2}"
    return now.strftime("%Y-%m-%d")


def _spin_due(u: dict) -> bool:
    return (u.get("last_spin_date") or "") != _window_id()


def _next_window_dt() -> datetime:
    now = now_ist()
    if SPIN_INTERVAL == "weekly":
        days = (7 - now.weekday()) % 7 or 7
        return (now + timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    if SPIN_INTERVAL == "alternate":
        epoch = datetime(2026, 1, 1, tzinfo=IST).date()
        add = 2 if (now.date() - epoch).days % 2 == 0 else 1
        return (now + timedelta(days=add)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def _countdown() -> str:
    return fmt_td(max(int((_next_window_dt() - now_ist()).total_seconds()), 0))


def _jackpots_left() -> int:
    try:
        used = int(_kv_get(f"spinwin:{_window_id()}", "0") or "0")
    except ValueError:
        used = 0
    return max(0, SPIN_WINNER_LIMIT - used)


def _take_jackpot() -> bool:
    key = f"spinwin:{_window_id()}"
    try:
        used = int(_kv_get(key, "0") or "0")
    except ValueError:
        used = 0
    if used >= SPIN_WINNER_LIMIT:
        return False
    _kv_set(key, str(used + 1))
    return True


def _present_spin(cid: int, pending: str | None) -> None:
    from kc.ui import back_kb, msg_spin
    with shared._sl:
        user = _get_ud(cid)
        text = msg_spin(user, pending)
    curr = shared.flow.get(cid) or {}
    pq = curr.get("pending_query")
    p_list = list(curr.get("pending_queries") or ([] if not pq else [pq]))
    if pending:
        if not pq:
            pq = pending
            p_list = [pending]
        elif pending != pq and pending not in p_list:
            p_list.append(pending)
    shared.flow[cid] = {"mode": "spin", "pending_query": pq, "pending_queries": p_list}
    kb = [[{"text": "🎰 Spin Maar", "callback_data": "spin"}]] if _spin_due(user) else back_kb()
    send_msg(cid, text, kb=kb)


def _finish_spin(cid: int, value: int) -> None:
    from kc.ui import dashboard_kb
    from kc.worker import _enqueue_search

    pending_list = []
    state = shared.flow.get(cid) or {}
    if state.get("mode") == "spin":
        if state.get("pending_queries"):
            pending_list = [q for q in state["pending_queries"] if q]
        elif state.get("pending_query"):
            pending_list = [state["pending_query"]]
    _clear_flow(cid)
    with shared._sl:
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
    if pending_list:
        if len(pending_list) == 1:
            q = pending_list[0]
            if q and q.isdigit() and len(q) == 10:
                _enqueue_search(cid, q)
        else:
            def _run_pending_seq(c_id: int, q_list: list[str]):
                for q in q_list:
                    if q and q.isdigit() and len(q) == 10:
                        while True:
                            with shared._sl:
                                if c_id not in shared.active_users:
                                    break
                            time.sleep(0.5)
                        _enqueue_search(c_id, q)
            threading.Thread(target=_run_pending_seq, args=(cid, pending_list), daemon=True).start()


def _run_spin(cid: int) -> None:
    with shared._sl:
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
