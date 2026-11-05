"""KC-SOURCE-04: Indian Cars & Asset Registry."""
from kc import shared
from kc.duckdb_conn import _safe_query, _phone_variants, recreate_view
from kc.utils import _e, clean_addr, sv


def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"


def db_search_cars(number: str) -> list[dict]:
    vname = "people_cars"
    if not shared._view_health.get(vname, True):
        recreate_view(vname)
    vars_list = _phone_variants(number)
    ph = ", ".join(["?"] * len(vars_list))
    sql = f"SELECT * FROM people_cars WHERE mobile_no IN ({ph}) LIMIT 5"
    return _safe_query(sql, vars_list, "KC-SOURCE-04")


def _format_cars_block(row: dict) -> list[str]:
    lines = ["🚗 <b>KC-SOURCE-04 · VEHICLE REGISTRY</b>"]
    name        = sv(row.get("name"))
    mobile_no   = sv(row.get("mobile_no"))
    address     = clean_addr(row.get("address"))
    city        = sv(row.get("city"))
    pincode     = sv(row.get("pincode"))
    model       = sv(row.get("model"))
    submodel    = sv(row.get("submodel"))
    asset_type  = sv(row.get("asset_type"))
    product     = sv(row.get("product"))
    emi_amt     = sv(row.get("emi_amt"))
    tenor       = sv(row.get("tenor"))
    roi         = sv(row.get("roi"))
    irr         = sv(row.get("irr"))
    mis_status  = sv(row.get("mis_status"))
    if name:        lines.append(f"  👤 <b>Naam:</b> {_e(name)}")
    if mobile_no:   lines.append(f"  📱 <b>Mobile:</b> {_spoiler(mobile_no)}")
    if model or submodel:
        car = " · ".join(x for x in [model, submodel] if x)
        lines.append(f"  🚘 <b>Car:</b> {_e(car)}")
    if asset_type or product:
        prod = " · ".join(x for x in [asset_type, product] if x)
        lines.append(f"  🏷 <b>Asset:</b> {_e(prod)}")
    if address:     lines.append(f"  🏠 <b>Address:</b>\n<blockquote expandable>{_e(address)}</blockquote>")
    loc = ", ".join(x for x in [city, pincode] if x)
    if loc:         lines.append(f"  📍 <b>City/Pin:</b> {_e(loc)}")
    if emi_amt:     lines.append(f"  💰 <b>EMI:</b> ₹{_e(emi_amt)}")
    if tenor:       lines.append(f"  ⏳ <b>Tenor:</b> {_e(tenor)}m")
    if roi or irr:
        rates = " / ".join(f"ROI: {r}" for r in [roi, irr] if r)
        lines.append(f"  📈 <b>Rate:</b> {_e(rates)}")
    if mis_status:  lines.append(f"  📊 <b>Status:</b> {_e(mis_status)}")
    return lines
