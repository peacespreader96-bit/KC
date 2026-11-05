"""Support ticket management, persistence, and assignment."""
import sqlite3
import time

from kc import shared


def _ticket(tid: int) -> sqlite3.Row | None:
    if shared._sql is None:
        return None
    with shared._sql_lock:
        return shared._sql.execute("SELECT * FROM tickets WHERE id = ?", (tid,)).fetchone()


def _ticket_replies(tid: int, limit: int = 8) -> list[sqlite3.Row]:
    if shared._sql is None:
        return []
    with shared._sql_lock:
        return shared._sql.execute(
            "SELECT * FROM ticket_replies WHERE ticket_id = ? ORDER BY id DESC LIMIT ?",
            (tid, limit),
        ).fetchall()


def _add_reply(tid: int, agent: int, body: str) -> None:
    if shared._sql is None:
        return
    with shared._sql_lock:
        shared._sql.execute(
            "INSERT INTO ticket_replies (ticket_id, agent_id, body, created_at) VALUES (?, ?, ?, ?)",
            (tid, agent, body, time.time()),
        )
        shared._sql.commit()


def _open_ticket(cid: int, text: str) -> int | None:
    if shared._sql is None:
        return None
    with shared._sql_lock:
        cur = shared._sql.execute(
            "INSERT INTO tickets (chat_id, status, agent_id, created_at, resolved_at, agent_notes) "
            "VALUES (?, 'open', NULL, ?, NULL, ?)",
            (cid, time.time(), text),
        )
        shared._sql.commit()
        return int(cur.lastrowid)


def _claim_ticket(tid: int, agent: int) -> str:
    if shared._sql is None:
        return "missing"
    with shared._sql_lock:
        row = shared._sql.execute("SELECT status, agent_id FROM tickets WHERE id = ?", (tid,)).fetchone()
        if row is None:
            return "missing"
        owner = int(row["agent_id"] or 0)
        if row["status"] == "claimed" and owner == agent:
            return "yours"
        if row["status"] != "open" or owner:
            return "locked"
        cur = shared._sql.execute(
            "UPDATE tickets SET status = 'claimed', agent_id = ? WHERE id = ? AND status = 'open' AND (agent_id IS NULL OR agent_id = 0)",
            (agent, tid),
        )
        shared._sql.commit()
        if cur.rowcount != 1:
            return "locked"
    return "ok"


def _resolve_ticket(tid: int, agent: int) -> bool:
    if shared._sql is None:
        return False
    with shared._sql_lock:
        row = shared._sql.execute("SELECT status, agent_id FROM tickets WHERE id = ?", (tid,)).fetchone()
        if not row or row["status"] != "claimed" or int(row["agent_id"] or 0) != agent:
            return False
        shared._sql.execute("UPDATE tickets SET status = 'resolved', resolved_at = ? WHERE id = ?", (time.time(), tid))
        shared._sql.commit()
    return True


def _can_touch_ticket(cid: int, row: sqlite3.Row) -> bool:
    if row["status"] != "claimed":
        return True
    return int(row["agent_id"] or 0) == cid

