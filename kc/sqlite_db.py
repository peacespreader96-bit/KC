"""SQLite database management, schema setup, user persistence, and KV store."""
import json
import os
import sqlite3

from kc import shared
from kc.config import ADMIN_ID, FREE_DAILY_LIMIT, PLANS, _DB_PATH
from kc.logging_setup import log
from kc.utils import _e


def _init_sqlite() -> None:
    if shared._sql is None:
        return
    with shared._sql_lock:
        shared._sql.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA busy_timeout=5000;
            CREATE TABLE IF NOT EXISTS users (
                chat_id          INTEGER PRIMARY KEY,
                name             TEXT,
                username         TEXT,
                is_premium       INTEGER DEFAULT 0,
                premium_until    REAL DEFAULT 0,
                total_searches   INTEGER DEFAULT 0,
                daily_searches   INTEGER DEFAULT 0,
                last_search_date TEXT,
                referral_count   INTEGER DEFAULT 0,
                referred_by      INTEGER,
                last_spin_date   TEXT,
                history          TEXT,
                last_streak_date TEXT,
                streak_count     INTEGER DEFAULT 0,
                utr_attempts     INTEGER DEFAULT 0,
                is_banned        INTEGER DEFAULT 0,
                role             TEXT DEFAULT 'USER'
            );
            CREATE TABLE IF NOT EXISTS payments (
                id TEXT PRIMARY KEY, chat_id INTEGER, plan_id TEXT, plan_name TEXT,
                amount INTEGER, days INTEGER, utr TEXT, status TEXT,
                created_at REAL, settled_at REAL, gateway TEXT, invoice_id TEXT
            );
            CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT);
            CREATE TABLE IF NOT EXISTS ban_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, action TEXT,
                admin_id INTEGER, timestamp REAL, reason TEXT
            );
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, status TEXT,
                agent_id INTEGER, created_at REAL, resolved_at REAL, agent_notes TEXT
            );
            CREATE TABLE IF NOT EXISTS invoices (
                invoice_id TEXT PRIMARY KEY, chat_id INTEGER, gateway TEXT, amount INTEGER,
                plan_id TEXT, status TEXT, created_at REAL, paid_at REAL
            );
            CREATE TABLE IF NOT EXISTS ticket_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ticket_id INTEGER, agent_id INTEGER,
                body TEXT, created_at REAL
            );
            """
        )
        existing = {row[1] for row in shared._sql.execute("PRAGMA table_info(users)").fetchall()}
        for col, ddl in (
            ("last_streak_date", "ALTER TABLE users ADD COLUMN last_streak_date TEXT"),
            ("streak_count", "ALTER TABLE users ADD COLUMN streak_count INTEGER DEFAULT 0"),
            ("utr_attempts", "ALTER TABLE users ADD COLUMN utr_attempts INTEGER DEFAULT 0"),
            ("is_banned", "ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0"),
            ("role", "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'USER'"),
        ):
            if col not in existing:
                shared._sql.execute(ddl)
        pay_cols = {row[1] for row in shared._sql.execute("PRAGMA table_info(payments)").fetchall()}
        for col, ddl in (
            ("gateway", "ALTER TABLE payments ADD COLUMN gateway TEXT"),
            ("invoice_id", "ALTER TABLE payments ADD COLUMN invoice_id TEXT"),
        ):
            if col not in pay_cols:
                shared._sql.execute(ddl)
        shared._sql.commit()


def _blank_user(cid: int, name: str | None, username: str | None) -> dict:
    return {
        "id": cid, "name": name or "User", "username": username or "",
        "searches": 0, "results": 0, "last_search": 0.0, "last_msg": 0.0,
        "lang": "en", "is_premium": False, "premium_until": 0.0, "history": [],
        "daily_searches": 0, "last_search_date": "", "referral_count": 0,
        "referred_by": None, "last_spin_date": "", "last_streak_date": "",
        "streak_count": 0, "utr_attempts": 0, "is_banned": 0,
        "role": "SUPER_ADMIN" if cid == ADMIN_ID else "USER",
    }


def _row_get(row: sqlite3.Row, key: str, default=None):
    try:
        val = row[key]
    except Exception:
        return default
    return default if val is None else val


def _from_row(row: sqlite3.Row) -> dict:
    try:
        history = json.loads(row["history"] or "[]")
        if not isinstance(history, list):
            history = []
    except Exception:
        history = []
    referred = row["referred_by"]
    role = str(_row_get(row, "role", "USER") or "USER")
    if int(row["chat_id"]) == ADMIN_ID:
        role = "SUPER_ADMIN"
    elif role not in ("FINANCE_MANAGER", "SUPPORT_AGENT", "USER"):
        role = "USER"
    return {
        "id": int(row["chat_id"]), "name": row["name"] or "User",
        "username": row["username"] or "", "searches": int(row["total_searches"] or 0),
        "results": 0, "last_search": 0.0, "last_msg": 0.0, "lang": "en",
        "is_premium": bool(row["is_premium"]), "premium_until": float(row["premium_until"] or 0),
        "history": history, "daily_searches": int(row["daily_searches"] or 0),
        "last_search_date": row["last_search_date"] or "",
        "referral_count": int(row["referral_count"] or 0),
        "referred_by": int(referred) if referred not in (None, "") else None,
        "last_spin_date": row["last_spin_date"] or "",
        "last_streak_date": row["last_streak_date"] or "",
        "streak_count": int(row["streak_count"] or 0),
        "utr_attempts": int(row["utr_attempts"] or 0),
        "is_banned": int(row["is_banned"] or 0), "role": role,
    }


def _save_user(u: dict) -> None:
    if shared._sql is None:
        return
    history = json.dumps(u.get("history") or [], ensure_ascii=False)
    referred = u.get("referred_by")
    role = "SUPER_ADMIN" if int(u["id"]) == ADMIN_ID else (u.get("role") or "USER")
    if role not in ("SUPER_ADMIN", "FINANCE_MANAGER", "SUPPORT_AGENT", "USER"):
        role = "USER"
    u["role"] = role
    with shared._sql_lock:
        shared._sql.execute(
            """
            INSERT INTO users (
                chat_id, name, username, is_premium, premium_until, total_searches,
                daily_searches, last_search_date, referral_count, referred_by,
                last_spin_date, history, last_streak_date, streak_count,
                utr_attempts, is_banned, role
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                name=excluded.name, username=excluded.username,
                is_premium=excluded.is_premium, premium_until=excluded.premium_until,
                total_searches=excluded.total_searches, daily_searches=excluded.daily_searches,
                last_search_date=excluded.last_search_date, referral_count=excluded.referral_count,
                referred_by=excluded.referred_by, last_spin_date=excluded.last_spin_date,
                history=excluded.history, last_streak_date=excluded.last_streak_date,
                streak_count=excluded.streak_count, utr_attempts=excluded.utr_attempts,
                is_banned=excluded.is_banned, role=excluded.role
            """,
            (
                int(u["id"]), u.get("name") or "User", u.get("username") or "",
                1 if u.get("is_premium") else 0, float(u.get("premium_until") or 0),
                int(u.get("searches") or 0), int(u.get("daily_searches") or 0),
                u.get("last_search_date") or "", int(u.get("referral_count") or 0),
                int(referred) if referred else None, u.get("last_spin_date") or "",
                history, u.get("last_streak_date") or "", int(u.get("streak_count") or 0),
                int(u.get("utr_attempts") or 0), int(u.get("is_banned") or 0), role,
            ),
        )
        shared._sql.commit()


def _load_user(cid: int) -> dict | None:
    if shared._sql is None:
        return None
    with shared._sql_lock:
        row = shared._sql.execute("SELECT * FROM users WHERE chat_id = ?", (cid,)).fetchone()
    return _from_row(row) if row else None


def _kv_get(key: str, default: str = "") -> str:
    if shared._sql is None:
        return default
    with shared._sql_lock:
        row = shared._sql.execute("SELECT v FROM kv WHERE k = ?", (key,)).fetchone()
    return row["v"] if row else default


def _kv_set(key: str, value: str) -> None:
    if shared._sql is None:
        return
    with shared._sql_lock:
        shared._sql.execute(
            "INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )
        shared._sql.commit()


def _seed_settings() -> None:
    if shared._sql is None:
        return
    if _kv_get("FREE_DAILY_LIMIT", "") == "":
        _kv_set("FREE_DAILY_LIMIT", str(FREE_DAILY_LIMIT))
    if _kv_get("PLANS", "") == "":
        _kv_set("PLANS", json.dumps(PLANS))
    for gw in ("upi", "stars", "crypto"):
        if _kv_get(f"gw:{gw}", "") == "":
            _kv_set(f"gw:{gw}", "1")


def _create_fresh_db() -> str | None:
    """Wipes the existing DB (if any) and creates a brand new one."""
    with shared._sql_lock:
        if shared._sql is not None:
            try:
                shared._sql.close()
            except Exception:
                pass
            shared._sql = None
        for side in (_DB_PATH, _DB_PATH + "-wal", _DB_PATH + "-shm"):
            if os.path.isfile(side):
                try:
                    os.remove(side)
                except OSError as exc:
                    return f"Purani file delete nahi hui: {exc}. Host manually delete kar."

        try:
            shared._sql = sqlite3.connect(_DB_PATH, check_same_thread=False)
            shared._sql.row_factory = sqlite3.Row
            _init_sqlite()
            _seed_settings()
            log.info("✅ Fresh SQLite DB created by admin request")
            return None
        except Exception as exc:
            log.exception("Failed to create fresh DB")
            shared.MAINTENANCE = True
            return f"Fresh DB banane mein error aaya: {exc}"


def _bootstrap_sqlite() -> None:
    auto_init = os.getenv("AUTO_INIT_DB", "1").lower() in ("1", "true", "yes")
    if not os.path.isfile(_DB_PATH):
        if auto_init:
            log.info("users.db missing on fresh container — auto-initializing fresh database...")
            err = _create_fresh_db()
            if err:
                shared.MAINTENANCE = True
                log.error("Failed to auto-init users.db: %s", err)
            else:
                log.info("✅ Auto-initialized fresh users.db at %s", _DB_PATH)
            return
        else:
            shared.MAINTENANCE = True
            log.error("users.db missing — maintenance, empty database will not be created (AUTO_INIT_DB=0)")
            return
    try:
        shared._sql = sqlite3.connect(_DB_PATH, check_same_thread=False)
        shared._sql.row_factory = sqlite3.Row
        _init_sqlite()
        _seed_settings()
        log.info("✅ SQLite ready · %s", _DB_PATH)
    except Exception:
        log.exception("sqlite open failed")
        shared.MAINTENANCE = True
        try:
            if shared._sql is not None:
                shared._sql.close()
        except Exception:
            pass
        shared._sql = None


bootstrap = _bootstrap_sqlite


def _restore_db(blob: bytes) -> str | None:
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
    with shared._sql_lock:
        if shared._sql is not None:
            try:
                shared._sql.close()
            except Exception:
                pass
            shared._sql = None
    for side in (_DB_PATH + "-wal", _DB_PATH + "-shm"):
        try:
            os.remove(side)
        except OSError:
            pass
    os.replace(tmp, _DB_PATH)
    try:
        shared._sql = sqlite3.connect(_DB_PATH, check_same_thread=False)
        shared._sql.row_factory = sqlite3.Row
        _init_sqlite()
        _seed_settings()
    except Exception as exc:
        shared.MAINTENANCE = True
        return "DB file lagi nahi bhai: " + str(exc)
    shared.MAINTENANCE = False
    try:
        from kc.sources.main_source import db_search_main
        db_search_main("7417679558")
    except Exception as exc:
        log.warning("DuckDB health still failing post-restore: %s", exc)
        from kc.telegram_api import send_msg
        send_msg(
            ADMIN_ID,
            "ℹ️ SQLite restore ho gayi aur maintenance OFF kar diya. "
            f"Par DuckDB health abhi bhi fail hai (<i>{_e(str(exc)[:120])}</i>) — searches tab tak timeout honge.",
        )
    return None
