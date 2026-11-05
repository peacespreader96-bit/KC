"""Search worker, queue handling, and multi-source concurrent query execution."""
import random
import threading
import time

from kc import shared
from kc.config import ADMIN_ID, _BAN_TEXT, _MAINT_TEXT, _STAGE_GAP
from kc.logging_setup import log
from kc.result_ui import format_card_sources
from kc.sources import SOURCES
from kc.sqlite_db import _save_user
from kc.telegram_api import edit_msg, send_msg, tg
from kc.ui import (
    back_kb,
    msg_limit,
    msg_no_result,
    msg_queue,
    msg_searching,
    msg_stage,
    result_kb,
)
from kc.user_ops import (
    _bonus_today,
    _free_limit,
    _get_ud,
    _premium_active,
    _remember,
    _touch_streak,
)
from kc.utils import _today, _e

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


def _q_pos(cid: int) -> int:
    for i, item in enumerate(shared._queue_order):
        if item["chat_id"] == cid:
            return i + 1
    return 0


def worker() -> None:
    while True:
        task = shared._search_q.get()
        if len(task) == 4:
            cid, query, mid, target_source = task
        else:
            cid, query, mid = task
            target_source = None
        try:
            tg("sendChatAction", {"chat_id": cid, "action": "typing"})
            with shared._ql:
                shared._queue_order[:] = [x for x in shared._queue_order if not (x["chat_id"] == cid and x["msg_id"] == mid)]
                snapshot = list(shared._queue_order)
            for i, item in enumerate(snapshot, start=1):
                try:
                    edit_msg(item["chat_id"], item["msg_id"], msg_queue(i, item["query"]))
                except Exception:
                    pass

            if target_source and target_source != "all":
                active_sources = [s for s in SOURCES if s["id"] == target_source]
                if not active_sources:
                    active_sources = SOURCES
                src_label = active_sources[0]["name"]
                search_txt = f'⚡ <b>Searching {src_label}...</b>\n🔎 <code>{_e(query)}</code>'
                edit_msg(cid, mid, search_txt)
            else:
                active_sources = SOURCES
                edit_msg(cid, mid, msg_searching(query))

            stages = _next_load_stages()
            all_sources: dict[str, list[dict]] = {s["id"]: [] for s in SOURCES}
            cancelled = threading.Event()

            def _make_fetcher(src_def: dict):
                def _fetch():
                    if cancelled.is_set():
                        return
                    with shared._source_semaphore:
                        if cancelled.is_set():
                            return
                        try:
                            all_sources[src_def["id"]] = src_def["search_fn"](query)
                        except Exception as ex:
                            log.warning("Source %s query error: %s", src_def["id"], ex)
                            all_sources[src_def["id"]] = []
                return _fetch

            deadline = time.monotonic() + shared.SYS_SEARCH_TIMEOUT
            t_db = time.monotonic()
            threads = [
                (s, threading.Thread(target=_make_fetcher(s), daemon=True))
                for s in active_sources
            ]
            for _, th in threads:
                th.start()

            for label in stages[:2]:
                time.sleep(_STAGE_GAP)
                edit_msg(cid, mid, msg_stage(query, label))

            while time.monotonic() < deadline:
                all_done = all(not th.is_alive() for _, th in threads)
                found_count = sum(1 for s in active_sources if all_sources.get(s["id"]))
                if all_done:
                    break
                if len(active_sources) > 1 and found_count >= 3 and (time.monotonic() - t_db >= 10.0):
                    break
                time.sleep(0.3)

            for s, th in threads:
                if th.is_alive():
                    log.warning("Source %s query timed out", s["id"])
                    all_sources[s["id"]] = []

            # Signal any remaining or queued threads to bail out immediately
            cancelled.set()

            elapsed = time.monotonic() - t_db

            with shared._sl:
                shared.global_stats["searches"] += 1
                if cid in shared.user_data:
                    shared.user_data[cid]["searches"] += 1

            total_found = sum(len(r) for r in all_sources.values())
            max_pages = max([len(r) for r in all_sources.values() if r] or [0])

            if total_found == 0:
                with shared._sl:
                    _remember(cid, query, 0)
                edit_msg(
                    cid, mid,
                    msg_no_result(query),
                    kb=[
                        [{"text": "🔄 Doosra Source Check Kar", "callback_data": f"chgsrc:{query}"}],
                        [{"text": "🔍 Nayi Search", "callback_data": "new_search"}, {"text": "🏠 Main Menu", "callback_data": "dashboard"}],
                    ],
                )
            else:
                with shared._sl:
                    shared.global_stats["results"] += total_found
                    if cid in shared.user_data:
                        shared.user_data[cid]["results"] += total_found
                    _remember(cid, query, total_found)
                    shared.user_sessions[(cid, mid)] = {
                        "sources":      all_sources,
                        "results":      all_sources.get("main", []),
                        "results_umba": all_sources.get("umba", []),
                        "page":         0,
                        "query":        query,
                        "ts":           time.time(),
                    }

                page_0_data = {k: (v[0] if v else None) for k, v in all_sources.items()}
                edit_msg(cid, mid, msg_stage(query, "✅ Match mil gaya!"))
                time.sleep(0.2)
                edit_msg(
                    cid, mid,
                    format_card_sources(page_0_data, query, elapsed),
                    kb=result_kb(0, max_pages, query),
                )

        except Exception:
            log.exception("Worker error")
            edit_msg(cid, mid, "⚠️ Bhai lookup mein dikkat aayi. Thodi der ruk, phir try kar.", kb=back_kb())
        finally:
            with shared._sl:
                shared.active_users.discard(cid)
            shared._search_q.task_done()


def _enqueue_search(cid: int, query: str, target_source: str | None = None) -> None:
    if shared.MAINTENANCE:
        send_msg(cid, _MAINT_TEXT)
        return
    with shared._sl:
        user = shared.user_data.get(cid) or _get_ud(cid)
        banned = cid != ADMIN_ID and int(user.get("is_banned") or 0) == 1
        if banned:
            send_msg(cid, _BAN_TEXT)
            return
        if cid in shared.active_users:
            send_msg(cid, "Arre ruk bhai, teri pehli search abhi desk pe hai.")
            return

        if _touch_streak(cid):
            send_msg(cid, "🔥 Bhai streak complete! Tere ko 1 free lookup mil gaya.")
        counted = False
        if cid != ADMIN_ID and not _premium_active(user):
            today = _today()
            if user.get("last_search_date") != today:
                user["daily_searches"] = 0
            allowance = _free_limit() + _bonus_today(cid)
            used = int(user.get("daily_searches") or 0)
            if used >= allowance:
                _save_user(user)
                send_msg(cid, msg_limit(user), kb=[[{"text": "👑 Premium Pass", "callback_data": "premium"}]])
                return
            counted = True
            user["daily_searches"] = used + 1
            user["last_search_date"] = today
            _save_user(user)

        if shared._search_q.qsize() >= shared.SYS_MAX_QUEUE:
            if counted:
                user["daily_searches"] = max(0, int(user.get("daily_searches") or 0) - 1)
                _save_user(user)
            send_msg(cid, "Desk full hai bhai. Ek minute ruk, phir number bhej.")
            return

        shared.active_users.add(cid)
        if cid in shared.user_data:
            shared.user_data[cid]["last_search"] = time.time()
        with shared._ql:
            approx_pos = len(shared._queue_order) + 1

        mid = send_msg(cid, msg_queue(approx_pos, query))
        if not mid:
            shared.shared.active_users.discard(cid)
            if counted:
                user["daily_searches"] = max(0, int(user.get("daily_searches") or 0) - 1)
                _save_user(user)
            return

        with shared._ql:
            shared._queue_order.append({"chat_id": cid, "msg_id": mid, "query": query, "enqueued_at": time.time()})
        shared._search_q.put((cid, query, mid, target_source))

