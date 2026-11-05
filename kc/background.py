"""Background loops for health checks, persistence, backups, expiry, and GC."""
import os
import shutil
import threading
import time

from kc import shared
from kc.config import (
    ADMIN_ID,
    GC_INTERVAL,
    QUEUE_BROADCAST_IV,
    SESSION_TTL,
    _BACKUP_DIR,
    _DB_PATH,
)
from kc.duckdb_conn import _SRC_VIEW_MAP
from kc.logging_setup import log
from kc.sources import SOURCES
from kc.sqlite_db import _kv_get, _kv_set
from kc.telegram_api import edit_msg, send_msg
from kc.ui import msg_queue
from kc.utils import _e, fmt_until, now_ist


def _backup_files() -> list[str]:
    if not os.path.isdir(_BACKUP_DIR):
        return []
    files = [os.path.join(_BACKUP_DIR, n) for n in os.listdir(_BACKUP_DIR) if n.endswith(".db")]
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files


def startup_health() -> None:
    shared.MAINTENANCE = True
    results: dict[str, bool] = {}
    lock = threading.Lock()

    def _check(src):
        s_id = src["id"]
        vname = _SRC_VIEW_MAP.get(s_id, "")
        ok = False
        try:
            src["search_fn"]("7417679558")
            ok = (not vname or shared._view_health.get(vname, True)) \
                 and not shared._source_query_error.get(src["name"], False)
        except Exception as exc:
            log.warning("Health %s FAIL: %s", s_id, exc)
        with lock:
            results[s_id] = ok
        log.info("Startup health %s (%s): %s", s_id, src["name"], "PASS" if ok else "FAIL")

    threads = [threading.Thread(target=_check, args=(s,), daemon=True) for s in SOURCES]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=45)

    failed = [sid for sid, ok in results.items() if not ok]
    if len(failed) >= 5:
        shared.MAINTENANCE = True
        try:
            send_msg(ADMIN_ID, f"⚠️ Maintenance ON: DuckDB health {len(failed)}/7 failed ({', '.join(failed)})")
        except Exception:
            log.exception("admin alert failed")
    else:
        shared.MAINTENANCE = False
        log.info("health check ok — maintenance OFF")


def _broadcaster() -> None:
    while True:
        time.sleep(QUEUE_BROADCAST_IV)
        now = time.time()
        with shared._ql:
            kept = []
            for item in shared._queue_order:
                enq = float(item.get("enqueued_at") or now)
                if now - enq > 90:
                    log.warning("Dropping stale queue entry for cid %s (older than 90s)", item.get("chat_id"))
                else:
                    kept.append(item)
            shared._queue_order[:] = kept
            snapshot = list(shared._queue_order)
        for i, item in enumerate(snapshot, start=1):
            try:
                edit_msg(item["chat_id"], item["msg_id"], msg_queue(i, item["query"]))
            except Exception:
                pass


def _session_gc() -> None:
    while True:
        time.sleep(GC_INTERVAL)
        cutoff = time.time() - SESSION_TTL
        with shared._sl:
            stale = [key for key, s in shared.user_sessions.items() if s["ts"] < cutoff]
            for key in stale:
                del shared.user_sessions[key]


def _persist_loop() -> None:
    while True:
        time.sleep(3)
        if shared._sql is None:
            continue
        with shared._sl:
            snap = [(int(u["id"]), int(u.get("searches") or 0)) for u in shared.user_data.values()]
        for cid, searches in snap:
            try:
                with shared._sql_lock:
                    shared._sql.execute(
                        "UPDATE users SET total_searches = ? WHERE chat_id = ? AND COALESCE(total_searches, 0) < ?",
                        (searches, cid, searches),
                    )
                    shared._sql.commit()
            except Exception:
                log.exception("persist failed")


def _backup_loop() -> None:
    while True:
        time.sleep(300)
        try:
            if shared._sql is None or not os.path.isfile(_DB_PATH):
                continue
            os.makedirs(_BACKUP_DIR, exist_ok=True)
            with shared._sql_lock:
                shared._sql.execute("PRAGMA wal_checkpoint(PASSIVE);")
                shared._sql.commit()
            stamp = now_ist().strftime("%Y%m%d_%I%M%p")
            dest = os.path.join(_BACKUP_DIR, f"users_{stamp}.db")
            shutil.copy2(_DB_PATH, dest)
            for old in _backup_files()[20:]:
                try:
                    os.remove(old)
                except OSError:
                    pass
            log.info("local backup %s", os.path.basename(dest))
        except Exception:
            log.exception("auto-backup failed")


def _expiry_loop() -> None:
    while True:
        time.sleep(600)
        if shared._sql is None:
            continue
        now = time.time()
        try:
            with shared._sql_lock:
                rows = shared._sql.execute(
                    "SELECT chat_id, premium_until FROM users "
                    "WHERE is_premium = 1 AND premium_until BETWEEN ? AND ? AND COALESCE(is_banned, 0) = 0",
                    (now + 23 * 3600, now + 25 * 3600),
                ).fetchall()
        except Exception:
            log.exception("expiry scan failed")
            continue
        for row in rows:
            cid = int(row["chat_id"])
            until = float(row["premium_until"])
            key = f"expnote:{cid}:{int(until)}"
            if _kv_get(key, ""):
                continue
            mid = send_msg(
                cid,
                "Bhai yaad rakh, tera premium <b>" + _e(fmt_until(until))
                + "</b> ke aas-paas khatam ho raha hai — lagbhag 24 ghante baaki hain. "
                "Renew kar le, warna lookups ruk jayenge.",
            )
            if mid:
                _kv_set(key, "1")


# Aliases for thread targets
_startup_health = startup_health
broadcaster = _broadcaster
session_gc = _session_gc
persist_loop = _persist_loop
backup_loop = _backup_loop
expiry_loop = _expiry_loop

