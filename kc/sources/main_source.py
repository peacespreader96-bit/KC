"""KC-SOURCE-01: Core Telecom & Citizen Directory."""
from kc import shared
from kc.duckdb_conn import _safe_query, _phone_variants, recreate_view
from kc.utils import _e, clean_addr, sv


def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"


def db_search_main(number: str) -> list[dict]:
    vname = "people_phone"
    if not shared._view_health.get(vname, True):
        recreate_view(vname)
    vars_list = _phone_variants(number)
    ph = ", ".join(["?"] * len(vars_list))
    sql = f"SELECT * FROM people_phone WHERE phoneNumber IN ({ph}) LIMIT 5"
    return _safe_query(sql, vars_list, "KC-SOURCE-01")


def _format_main_block(row: dict) -> list[str]:
    lines = ["📁 <b>KC-SOURCE-01 · CORE CITIZEN DIRECTORY</b>"]
    name         = sv(row.get("name"))
    fathersName  = sv(row.get("fathersName"))
    phoneNumber  = sv(row.get("phoneNumber"))
    otherNumber  = sv(row.get("otherNumber"))
    aadharNumber = sv(row.get("aadharNumber"))
    address      = clean_addr(row.get("address"))
    state        = sv(row.get("state"))
    pincode      = sv(row.get("pincode"))
    district     = sv(row.get("district"))
    town         = sv(row.get("town"))
    if name:         lines.append(f"  👤 <b>Naam:</b> {_e(name)}")
    if fathersName:  lines.append(f"  👨 <b>Father:</b> {_e(fathersName)}")
    if phoneNumber:  lines.append(f"  📱 <b>Number:</b> {_spoiler(phoneNumber)}")
    if otherNumber:  lines.append(f"  📞 <b>Dusra:</b> {_spoiler(otherNumber)}")
    if aadharNumber: lines.append(f"  🪪 <b>ID:</b> {_spoiler(aadharNumber)}")
    if address:      lines.append(f"  🏠 <b>Address:</b>\n<blockquote expandable>{_e(address)}</blockquote>")
    loc = ", ".join(x for x in [town, district, state] if x)
    if loc:          lines.append(f"  📍 <b>Location:</b> {_e(loc)}")
    if pincode:      lines.append(f"  📮 <b>Zip:</b> <code>{_e(pincode)}</code>")
    return lines
