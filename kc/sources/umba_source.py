"""KC-SOURCE-02: Telecom Archive (Umba bucket)."""
from kc import shared
from kc.duckdb_conn import _safe_query, _phone_variants, recreate_view
from kc.utils import _e, clean_addr, sv


def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"


def db_search_umba(number: str) -> list[dict]:
    vname = "people_umba"
    if not shared._view_health.get(vname, True):
        recreate_view(vname)
    vars_list = _phone_variants(number)
    ph = ", ".join(["?"] * len(vars_list))
    sql = f"SELECT * FROM people_umba WHERE phoneNumber IN ({ph}) LIMIT 5"
    return _safe_query(sql, vars_list, "KC-SOURCE-02")


def _format_umba_block(row: dict) -> list[str]:
    lines = ["📂 <b>KC-SOURCE-02 · TELECOM ARCHIVE</b>"]
    u_phone   = sv(row.get("phoneNumber"))
    u_name    = sv(row.get("name"))
    u_address = clean_addr(row.get("address"))
    u_email   = sv(row.get("email"))
    u_gender  = sv(row.get("gender"))
    u_carrier = sv(row.get("carrier"))
    if u_name:    lines.append(f"  👤 <b>Naam:</b> {_e(u_name)}")
    if u_phone:   lines.append(f"  📱 <b>Number:</b> {_spoiler(u_phone)}")
    if u_address: lines.append(f"  🏠 <b>Address:</b>\n<blockquote expandable>{_e(u_address)}</blockquote>")
    if u_email:   lines.append(f"  📧 <b>Email:</b> {_spoiler(u_email)}")
    if u_gender:  lines.append(f"  🧬 <b>Gender:</b> {_e(u_gender)}")
    if u_carrier: lines.append(f"  📡 <b>Carrier:</b> {_e(u_carrier)}")
    return lines
