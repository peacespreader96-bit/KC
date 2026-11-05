"""User state management, streaks, bonuses, referrals, and limits."""
import json
import re
import time
from datetime import datetime

from kc import shared
from kc.config import (
    ADMIN_ID,
    FREE_DAILY_LIMIT,
    PLANS,
    STREAK_BONUS,
    STREAK_INTERVAL,
)
from kc.logging_setup import log
from kc.sqlite_db import (
    _blank_user,
    _kv_get,
    _kv_set,
    _load_user,
    _save_user,
)
from kc.telegram_api import send_msg
from kc.utils import _e, _today, fmt_until, ts_str


def _expire_premium(u: dict) -> bool:
    until = float(u.get("premium_until") or 0)
    if u.get("is_premium") and until <= time.time():
        u["is_premium"] = False
        return True
    return False


def _get_ud(cid: int, name: str | None = None, username: str | None = None) -> dict:
    dirty = False
    u = shared.user_data.get(cid)
    if u is None:
        u = _load_user(cid)
        if u is None:
            u = _blank_user(cid, name, username)
            shared.user_data[cid] = u
            _save_user(u)
        else:
            shared.user_data[cid] = u
    if name and name != u.get("name"):
        u["name"] = name
        dirty = True
    if username and username != u.get("username"):
        u["username"] = username
        dirty = True
    if cid == ADMIN_ID and u.get("role") != "SUPER_ADMIN":
        u["role"] = "SUPER_ADMIN"
        dirty = True
    if _expire_premium(u):
        dirty = True
    if dirty:
        _save_user(u)
    return u


def _remember(cid: int, query: str, found: int) -> None:
    u = shared.user_data.get(cid)
    if not u:
        return
    hist = list(u.get("history") or [])
    hist.append({"query": query, "at": ts_str(), "found": found})
    u["history"] = hist[-20:]
    try:
        _save_user(u)
    except Exception:
        log.exception("history save failed")


def _free_limit() -> int:
    raw = _kv_get("FREE_DAILY_LIMIT", "")
    try:
        return max(0, int(raw)) if raw != "" else FREE_DAILY_LIMIT
    except ValueError:
        return FREE_DAILY_LIMIT


def _plans() -> dict:
    raw = _kv_get("PLANS", "")
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and data:
                return data
        except Exception:
            pass
    return {k: dict(v) for k, v in PLANS.items()}


def _gw_on(gw: str) -> bool:
    return _kv_get(f"gw:{gw}", "1") != "0"


def _daily_used(u: dict) -> int:
    if u.get("last_search_date") != _today():
        return 0
    return int(u.get("daily_searches") or 0)


def _bonus_today(cid: int) -> int:
    try:
        return int(_kv_get(f"bonus:{cid}:{_today()}", "0") or "0")
    except ValueError:
        return 0


def _add_bonus(cid: int, n: int) -> int:
    key = f"bonus:{cid}:{_today()}"
    total = _bonus_today(cid) + n
    _kv_set(key, str(total))
    return total


def _premium_active(u: dict) -> bool:
    if _expire_premium(u):
        _save_user(u)
    return bool(u.get("is_premium")) and float(u.get("premium_until") or 0) > time.time()


def _extend_premium(u: dict, days: int) -> float:
    now = time.time()
    base = float(u.get("premium_until") or 0)
    if not (u.get("is_premium") and base > now):
        base = now
    u["is_premium"] = True
    u["premium_until"] = base + days * 86400
    return float(u["premium_until"])


def _touch_streak(cid: int) -> bool:
    today = _today()
    granted = False
    with shared._sl:
        user = _get_ud(cid)
        last = user.get("last_streak_date") or ""
        if last == today:
            return False
        today_dt = datetime.strptime(today, "%Y-%m-%d").date()
        last_dt = None
        if last:
            try:
                last_dt = datetime.strptime(last, "%Y-%m-%d").date()
            except ValueError:
                last_dt = None
        if last_dt is not None and (today_dt - last_dt).days == STREAK_INTERVAL:
            user["streak_count"] = int(user.get("streak_count") or 0) + 1
        else:
            user["streak_count"] = 1
        user["last_streak_date"] = today
        if int(user.get("streak_count") or 0) >= STREAK_INTERVAL:
            _add_bonus(cid, STREAK_BONUS)
            user["streak_count"] = 0
            granted = True
        _save_user(user)
    return granted


def _utr_lock_until(cid: int) -> float:
    try:
        return float(_kv_get(f"utrlock:{cid}", "0") or 0)
    except ValueError:
        return 0.0


def _utr_locked(cid: int) -> bool:
    return _utr_lock_until(cid) > time.time()


def _bump_utr_attempt(cid: int) -> int:
    with shared._sl:
        user = _get_ud(cid)
        user["utr_attempts"] = int(user.get("utr_attempts") or 0) + 1
        attempts = int(user["utr_attempts"])
        _save_user(user)
    if attempts > 3:
        _kv_set(f"utrlock:{cid}", str(time.time() + 3600))
    return attempts


def _reset_utr_attempts(cid: int) -> None:
    with shared._sl:
        user = shared.user_data.get(cid)
        if user is None:
            loaded = _load_user(cid)
            if loaded is not None:
                shared.user_data[cid] = loaded
                user = loaded
        if user is not None:
            user["utr_attempts"] = 0
            _save_user(user)
        elif shared._sql is not None:
            with shared._sql_lock:
                shared._sql.execute("UPDATE users SET utr_attempts = 0 WHERE chat_id = ?", (cid,))
                shared._sql.commit()
    _kv_set(f"utrlock:{cid}", "0")


def _banned(u: dict | None) -> bool:
    return bool(u) and int(u.get("is_banned") or 0) == 1


def _clear_flow(cid: int) -> None:
    shared.flow.pop(cid, None)


def _apply_referral(cid: int, payload: str) -> None:
    if shared._sql is None:
        return
    match = re.fullmatch(r"ref_(\d+)", (payload or "").strip())
    if not match:
        return
    inviter_id = int(match.group(1))
    if inviter_id == cid:
        return
    grant_until: float | None = None
    with shared._sl:
        user = _get_ud(cid)
        if user.get("referred_by"):
            return
        inviter = shared.user_data.get(inviter_id) or _load_user(inviter_id)
        if inviter is None:
            return
        shared.user_data[inviter_id] = inviter
        user["referred_by"] = inviter_id
        inviter["referral_count"] = int(inviter.get("referral_count") or 0) + 1
        if inviter["referral_count"] == 5:
            grant_until = _extend_premium(inviter, 1)
        _save_user(user)
        _save_user(inviter)
    if grant_until is not None:
        _notify_referral_premium(inviter_id, grant_until)


def _notify_referral_premium(inviter_id: int, until: float) -> None:
    if shared._sql is None:
        return
    with shared._sql_lock:
        rows = shared._sql.execute(
            "SELECT name, username FROM users WHERE referred_by = ? ORDER BY chat_id",
            (inviter_id,),
        ).fetchall()
    names = [f"@{row['username']}" if row["username"] else (row["name"] or "Bhai") for row in rows]
    send_msg(
        inviter_id,
        "<b>Tere invites land ho gaye.</b>\n\n"
        "Paanch log tere link se aaye. Ek din ka Premium desk pe lag gaya.\n\n"
        + "\n".join(f"• {_e(name)}" for name in names)
        + f"\n\nPremium <b>{_e(fmt_until(until))}</b> tak chalega.",
    )

