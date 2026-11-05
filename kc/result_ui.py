"""Result formatting, card generators, and HTML document exports."""
import os

from kc.config import BOT_NAME, _RECEIPT_DIR
from kc.sqlite_db import _kv_set
from kc.telegram_api import send_html_file
from kc.ui import msg_no_result
from kc.utils import _D, _clip, _e, clean_addr, elapsed_label, sv, ts_str


def _spoiler(value: str) -> str:
    return f"<tg-spoiler><code>{_e(value)}</code></tg-spoiler>"


def format_card_sources(
    sources_data: dict[str, dict | None],
    query: str,
    elapsed: float | None = None,
) -> str:
    """Renders telegram message containing ONLY sources that returned records."""
    from kc.sources import SOURCES

    blocks = []
    for src in SOURCES:
        row = sources_data.get(src["id"])
        if row:
            b_lines = src["format_fn"](row)
            if b_lines:
                blocks.append("\n".join(b_lines))

    if not blocks:
        return msg_no_result(query)

    header = [
        "<b>⚡ KC CUSTOMER FILE</b>",
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄",
        f"🔎 <b>Target:</b> <code>{_e(query)}</code>",
        "",
    ]
    body = "\n\n┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n\n".join(blocks)
    footer = ["", f"🕰 {ts_str()}"]
    if elapsed is not None:
        footer.append(f"⚡ Speed: {elapsed_label(elapsed)}")
        if elapsed < 1.0:
            footer.append("🏎️ <b>BHAI YE TOH ROCKET THA</b>")
    footer.append(_D)

    full_text = "\n".join(header) + body + "\n" + "\n".join(footer)
    return _clip(full_text, 4000)


def format_card(row: dict, query: str, elapsed: float | None = None) -> str:
    return format_card_sources({"main": row}, query, elapsed)


def format_card_combined(
    row_main: dict | None, row_umba: dict | None,
    query: str, elapsed: float | None = None
) -> str:
    return format_card_sources({"main": row_main, "umba": row_umba}, query, elapsed)


_HTML_CSS = """
:root { --ink:#16130f; --paper:#f7f1e6; --gold:#8a6d3b; --line:#e0d3bf; --mute:#6e6458; }
* { box-sizing: border-box; }
body { margin:0; background:#e7dcc6; color:var(--ink); font: 16px/1.55 Georgia, serif; }
article { max-width: 720px; margin: 36px auto; background: var(--paper); padding: 48px 44px 40px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); border-radius: 6px; }
.kicker { margin:0; font: 600 11px/1.4 sans-serif; letter-spacing: .22em; text-transform: uppercase; color: var(--gold); }
h1 { font-weight: 500; font-size: 32px; line-height: 1.15; margin: 10px 0 0; }
.rule { height: 1px; border: 0; margin: 22px 0; background: linear-gradient(90deg, #c6a15b, transparent); }
.sec-title { font: 700 13px/1.4 sans-serif; letter-spacing: .12em; text-transform: uppercase; color: var(--gold); margin: 20px 0 8px; border-bottom: 1px solid var(--line); padding-bottom: 4px; }
.rows { display: grid; grid-template-columns: 168px 1fr; gap: 10px 18px; word-wrap: break-word; max-width: 100%; overflow-wrap: break-word; margin-bottom: 16px; }
.k { font: 600 11px/1.4 sans-serif; letter-spacing: .08em; text-transform: uppercase; color: var(--mute); padding-top: 4px; }
.v { margin:0; word-wrap: break-word; max-width: 100%; overflow-wrap: break-word; }
.memo { margin-top: 28px; padding: 16px 0 0; border-top: 1px solid var(--line); }
.memo b { font: 600 11px/1.4 sans-serif; letter-spacing: .18em; text-transform: uppercase; color: var(--gold); }
.memo p { margin: 8px 0 0; font: 14px/1.55 sans-serif; color: #3a332b; }
footer { margin-top: 28px; font: 12px/1.4 sans-serif; color: var(--mute); }
"""


def _html_page(title: str, kicker: str, pairs: list[tuple[str, str]], memo: str) -> str:
    cells = []
    for l, v in pairs:
        if l.startswith("SECTION:"):
            sec_name = l.split(":", 1)[1]
            cells.append(f"<div style='grid-column: 1 / -1;' class='sec-title'>{_e(sec_name)}</div>")
        elif v:
            cells.append(f"<div class='k'>{_e(l)}</div><p class='v'>{v}</p>")

    return f"""<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/><title>{_e(title)}</title><style>{_HTML_CSS}</style></head>
<body><article><p class="kicker">{_e(kicker)}</p><h1>{_e(title)}</h1><hr class="rule"/>
<section class="rows">{''.join(cells)}</section>
<section class="memo"><b>Memo</b><p>{memo}</p></section>
<footer>{_e(BOT_NAME)} · Private intelligence dossier · {_e(ts_str())}</footer></article></body></html>"""


def generate_html_all(sources_data: dict[str, dict | None], query: str) -> str:
    pairs: list[tuple[str, str]] = [("Target", f"<code>{_e(query)}</code>")]

    # KC-SOURCE-01
    row_main = sources_data.get("main")
    if row_main:
        pairs.append(("SECTION:KC-SOURCE-01 · CORE DIRECTORY", ""))
        addr = clean_addr(row_main.get("address"))
        loc = ", ".join(x for x in [sv(row_main.get("town")), sv(row_main.get("district")), sv(row_main.get("state"))] if x)
        for l, v in [
            ("Naam",       _e(sv(row_main.get("name")))),
            ("Father",     _e(sv(row_main.get("fathersName")))),
            ("Number",     _e(sv(row_main.get("phoneNumber")))),
            ("Alt Number", _e(sv(row_main.get("otherNumber")))),
            ("ID",         _e(sv(row_main.get("aadharNumber")))),
            ("Address",    _e(addr)),
            ("Location",   _e(loc)),
            ("Zip",        _e(sv(row_main.get("pincode")))),
        ]:
            if v:
                pairs.append((l, v))

    # KC-SOURCE-02
    row_umba = sources_data.get("umba")
    if row_umba:
        pairs.append(("SECTION:KC-SOURCE-02 · TELECOM ARCHIVE", ""))
        u_addr = clean_addr(row_umba.get("address"))
        for l, v in [
            ("Naam",    _e(sv(row_umba.get("name")))),
            ("Number",  _e(sv(row_umba.get("phoneNumber")))),
            ("Address", _e(u_addr)),
            ("Email",   _e(sv(row_umba.get("email")))),
            ("Gender",  _e(sv(row_umba.get("gender")))),
            ("Carrier", _e(sv(row_umba.get("carrier")))),
        ]:
            if v:
                pairs.append((l, v))

    # KC-SOURCE-03
    row_bsnl = sources_data.get("bsnl")
    if row_bsnl:
        pairs.append(("SECTION:KC-SOURCE-03 · BSNL MOBILE", ""))
        b_addr = clean_addr(row_bsnl.get("address"))
        for l, v in [
            ("Naam",      _e(sv(row_bsnl.get("name")))),
            ("Number",    _e(sv(row_bsnl.get("number")))),
            ("Carrier",   _e(sv(row_bsnl.get("carrier")))),
            ("Gender",    _e(sv(row_bsnl.get("gender")))),
            ("Address",   _e(b_addr)),
            ("Job Title", _e(sv(row_bsnl.get("job_title")))),
            ("Company",   _e(sv(row_bsnl.get("company_name")))),
            ("Email",     _e(sv(row_bsnl.get("email")))),
            ("Facebook",  _e(sv(row_bsnl.get("facebook")))),
            ("Twitter",   _e(sv(row_bsnl.get("twitter")))),
        ]:
            if v:
                pairs.append((l, v))

    # KC-SOURCE-04
    row_cars = sources_data.get("cars")
    if row_cars:
        pairs.append(("SECTION:KC-SOURCE-04 · VEHICLE REGISTRY", ""))
        c_addr = clean_addr(row_cars.get("address"))
        for l, v in [
            ("Naam",       _e(sv(row_cars.get("name")))),
            ("Mobile",     _e(sv(row_cars.get("mobile_no")))),
            ("Model",      _e(sv(row_cars.get("model")))),
            ("Submodel",   _e(sv(row_cars.get("submodel")))),
            ("Asset Type", _e(sv(row_cars.get("asset_type")))),
            ("Product",    _e(sv(row_cars.get("product")))),
            ("Address",    _e(c_addr)),
            ("City",       _e(sv(row_cars.get("city")))),
            ("Pin Code",   _e(sv(row_cars.get("pincode")))),
            ("EMI Amount", _e(sv(row_cars.get("emi_amt")))),
            ("Tenor",      _e(sv(row_cars.get("tenor")))),
            ("ROI",        _e(sv(row_cars.get("roi")))),
            ("Status",     _e(sv(row_cars.get("mis_status")))),
        ]:
            if v:
                pairs.append((l, v))

    # KC-SOURCE-05
    row_air = sources_data.get("air")
    if row_air:
        pairs.append(("SECTION:KC-SOURCE-05 · AIR CELLULAR", ""))
        a_addr = clean_addr(row_air.get("address"))
        for l, v in [
            ("Naam",      _e(sv(row_air.get("name")))),
            ("Number",    _e(sv(row_air.get("number")))),
            ("Carrier",   _e(sv(row_air.get("carrier")))),
            ("Gender",    _e(sv(row_air.get("gender")))),
            ("Address",   _e(a_addr)),
            ("Job Title", _e(sv(row_air.get("job_title")))),
            ("Company",   _e(sv(row_air.get("company_name")))),
            ("Email",     _e(sv(row_air.get("email")))),
            ("Facebook",  _e(sv(row_air.get("facebook")))),
            ("Twitter",   _e(sv(row_air.get("twitter")))),
        ]:
            if v:
                pairs.append((l, v))

    # KC-SOURCE-06
    row_flip = sources_data.get("flipkart")
    if row_flip:
        pairs.append(("SECTION:KC-SOURCE-06 · FLIPKART COMMERCE", ""))
        f_addr = clean_addr(row_flip.get("address"))
        f_loc = ", ".join(x for x in [sv(row_flip.get("city")), sv(row_flip.get("state")), sv(row_flip.get("pincode"))] if x)
        for l, v in [
            ("Customer Name", _e(sv(row_flip.get("name")))),
            ("Phone",         _e(sv(row_flip.get("phone")))),
            ("Email",         _e(sv(row_flip.get("email")))),
            ("Address",       _e(f_addr)),
            ("Location",      _e(f_loc)),
        ]:
            if v:
                pairs.append((l, v))

    # KC-SOURCE-07
    row_tg = sources_data.get("telegram")
    if row_tg:
        pairs.append(("SECTION:KC-SOURCE-07 · TELEGRAM MASTER", ""))
        tg_name = " ".join(x for x in [sv(row_tg.get("first_name")), sv(row_tg.get("last_name"))] if x)
        for l, v in [
            ("Name",          _e(tg_name)),
            ("Username",      _e(sv(row_tg.get("username")))),
            ("Telegram ID",   _e(sv(row_tg.get("user_id")))),
            ("Phone",         _e(sv(row_tg.get("phone")))),
            ("Email",         _e(sv(row_tg.get("email")))),
            ("Status",        _e(sv(row_tg.get("status")))),
            ("Linked ID",     _e(sv(row_tg.get("linked_id")))),
            ("Linked Name",   _e(sv(row_tg.get("linked_name")))),
            ("Linked Handle", _e(sv(row_tg.get("linked_handle")))),
        ]:
            if v:
                pairs.append((l, v))

    memo = f"{_e(BOT_NAME)} verified intelligence multi-source report."
    name = (
        sv((row_main or {}).get("name"))
        or sv((row_umba or {}).get("name"))
        or sv((row_bsnl or {}).get("name"))
        or sv((row_cars or {}).get("name"))
        or sv((row_air or {}).get("name"))
        or sv((row_flip or {}).get("name"))
        or sv((row_tg or {}).get("first_name"))
        or f"Report for {query}"
    )
    return _html_page(name, f"{BOT_NAME} · Multi-Source Intelligence Dossier", pairs, memo)


def generate_html(row: dict, query: str) -> str:
    return generate_html_all({"main": row}, query)


def generate_html_combined(row_main: dict | None, row_umba: dict | None, query: str) -> str:
    return generate_html_all({"main": row_main, "umba": row_umba}, query)


def generate_receipt_html(user: dict, plan: dict, when: str, expiry: str, utr: str = "", gateway: str = "", invoice_id: str = "") -> str:
    pairs = [
        ("Invoice", _e(invoice_id)),
        ("Gateway", _e(gateway)),
        ("Member", _e(user.get("name") or "Bhai")),
        ("Plan", _e(plan["name"])),
        ("Amount", f"₹{_e(str(plan['amount']))}"),
        ("Stars", _e(str(plan.get("stars") or ""))),
        ("Waqt", _e(when)),
        ("Expiry", _e(expiry)),
        ("Lookups ab tak", _e(str(int(user.get("searches") or 0)))),
        ("Reference", _e(utr)),
    ]
    memo = f"{_e(BOT_NAME)} confirm karta hai — membership settle ho gayi. Invoice <b>{_e(invoice_id or '—')}</b> save hai."
    return _html_page(f"{plan['name']} · {user.get('name') or 'Bhai'}", f"{BOT_NAME} · Membership memo", pairs, memo)


def send_html_doc(cid: int, row: dict, query: str) -> None:
    send_html_file(
        cid,
        f"{BOT_NAME}_Report_{sv(row.get('phoneNumber')) or 'note'}.html",
        generate_html(row, query),
        "📄 Le bhai, teri directory note attach hai.",
    )


def _save_receipt(inv: str, content: str) -> str:
    os.makedirs(_RECEIPT_DIR, exist_ok=True)
    path = os.path.join(_RECEIPT_DIR, f"{inv}.html")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    _kv_set(f"rcpt:{inv}", path)
    return path


def send_receipt(cid: int, user: dict, plan: dict, when: str, expiry: str, utr: str, gateway: str = "", invoice_id: str = "") -> None:
    html = generate_receipt_html(user, plan, when, expiry, utr, gateway, invoice_id)
    if invoice_id:
        _save_receipt(invoice_id, html)
    send_html_file(
        cid,
        f"{BOT_NAME}_Memo_{invoice_id or utr}.html",
        html,
        f"👑 <b>Premium on hai</b> {_e(expiry)} tak. Memo · <code>{_e(invoice_id or utr)}</code>",
    )

