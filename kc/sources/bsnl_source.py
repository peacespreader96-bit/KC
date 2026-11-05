"""KC-SOURCE-03: BSNL Mobile Directory."""
from kc import shared
from kc.duckdb_conn import _safe_query, _phone_variants, recreate_view
from kc.utils import _e, clean_addr, sv


def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"


def db_search_bsnl(number: str) -> list[dict]:
    vname = "people_bsnl"
    if not shared._view_health.get(vname, True):
        recreate_view(vname)
    vars_list = _phone_variants(number)
    ph = ", ".join(["?"] * len(vars_list))
    sql = f"SELECT * FROM people_bsnl WHERE number IN ({ph}) LIMIT 5"
    return _safe_query(sql, vars_list, "KC-SOURCE-03")


def _format_bsnl_block(row: dict) -> list[str]:
    lines = ["📡 <b>KC-SOURCE-03 · BSNL MOBILE</b>"]
    name         = sv(row.get("name"))
    number       = sv(row.get("number"))
    carrier      = sv(row.get("carrier"))
    gender       = sv(row.get("gender"))
    address      = clean_addr(row.get("address"))
    job_title    = sv(row.get("job_title"))
    company_name = sv(row.get("company_name"))
    email        = sv(row.get("email"))
    facebook     = sv(row.get("facebook"))
    twitter      = sv(row.get("twitter"))
    if name:         lines.append(f"  👤 <b>Naam:</b> {_e(name)}")
    if number:       lines.append(f"  📱 <b>Number:</b> {_spoiler(number)}")
    if carrier:      lines.append(f"  📡 <b>Carrier:</b> {_e(carrier)}")
    if gender:       lines.append(f"  🧬 <b>Gender:</b> {_e(gender)}")
    if address:      lines.append(f"  🏠 <b>Address:</b>\n<blockquote expandable>{_e(address)}</blockquote>")
    if job_title or company_name:
        work = " at ".join(x for x in [job_title, company_name] if x)
        lines.append(f"  💼 <b>Work:</b> {_e(work)}")
    if email:        lines.append(f"  📧 <b>Email:</b> {_spoiler(email)}")
    if facebook:     lines.append(f"  🌐 <b>Facebook:</b> <code>{_e(facebook)}</code>")
    if twitter:      lines.append(f"  🐦 <b>Twitter:</b> <code>{_e(twitter)}</code>")
    return lines

