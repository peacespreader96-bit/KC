"""KC-SOURCE-07: Telegram Master Registry."""
from kc import shared
from kc.duckdb_conn import _safe_query, _phone_variants, recreate_view
from kc.utils import _e, sv


def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"


def db_search_telegram(number: str) -> list[dict]:
    vname = "people_telegram"
    if not shared._view_health.get(vname, True):
        recreate_view(vname)
    vars_list = _phone_variants(number)
    ph = ", ".join(["?"] * len(vars_list))
    sql = f"SELECT * FROM people_telegram WHERE phone IN ({ph}) LIMIT 5"
    return _safe_query(sql, vars_list, "KC-SOURCE-07")


def _format_telegram_block(row: dict) -> list[str]:
    lines = ["✈️ <b>KC-SOURCE-07 · TELEGRAM MASTER</b>"]
    first_name    = sv(row.get("first_name"))
    last_name     = sv(row.get("last_name"))
    full_name     = " ".join(x for x in [first_name, last_name] if x)
    username      = sv(row.get("username"))
    user_id       = sv(row.get("user_id"))
    phone         = sv(row.get("phone"))
    email         = sv(row.get("email"))
    status        = sv(row.get("status"))
    linked_id     = sv(row.get("linked_id"))
    linked_name   = sv(row.get("linked_name"))
    linked_handle = sv(row.get("linked_handle"))
    if full_name:     lines.append(f"  👤 <b>Name:</b> {_e(full_name)}")
    if username:      lines.append(f"  🌐 <b>Username:</b> @{_e(username)}")
    if user_id:       lines.append(f"  🆔 <b>TG ID:</b> <code>{_e(user_id)}</code>")
    if phone:         lines.append(f"  📱 <b>Phone:</b> {_spoiler(phone)}")
    if email:         lines.append(f"  📧 <b>Email:</b> {_spoiler(email)}")
    if status:        lines.append(f"  🔰 <b>Status:</b> {_e(status)}")
    if linked_name or linked_handle or linked_id:
        acc = " · ".join(x for x in [linked_name, linked_handle, linked_id] if x)
        lines.append(f"  🔗 <b>Linked:</b> {_e(acc)}")
    return lines

