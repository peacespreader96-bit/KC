"""KC-SOURCE-06: E-Commerce Customer Directory."""
from kc import shared
from kc.duckdb_conn import _safe_query, _phone_variants, recreate_view
from kc.utils import _e, clean_addr, sv


def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"


def db_search_flipkart(number: str) -> list[dict]:
    vname = "people_flipkart"
    if not shared._view_health.get(vname, True):
        recreate_view(vname)
    vars_list = _phone_variants(number)
    ph = ", ".join(["?"] * len(vars_list))
    sql = f"SELECT * FROM people_flipkart WHERE phone IN ({ph}) LIMIT 5"
    return _safe_query(sql, vars_list, "KC-SOURCE-06")


def _format_flipkart_block(row: dict) -> list[str]:
    lines = ["🛍️ <b>KC-SOURCE-06 · FLIPKART COMMERCE</b>"]
    name    = sv(row.get("name"))
    phone   = sv(row.get("phone"))
    email   = sv(row.get("email"))
    address = clean_addr(row.get("address"))
    city    = sv(row.get("city"))
    state   = sv(row.get("state"))
    pincode = sv(row.get("pincode"))
    if name:    lines.append(f"  👤 <b>Customer:</b> {_e(name)}")
    if phone:   lines.append(f"  📱 <b>Phone:</b> {_spoiler(phone)}")
    if email:   lines.append(f"  📧 <b>Email:</b> {_spoiler(email)}")
    if address: lines.append(f"  🏠 <b>Address:</b>\n<blockquote expandable>{_e(address)}</blockquote>")
    loc = ", ".join(x for x in [city, state, pincode] if x)
    if loc:     lines.append(f"  📍 <b>Location:</b> {_e(loc)}")
    return lines
