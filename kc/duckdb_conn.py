"""DuckDB connection, view management, query helpers, and system configuration."""
import re
import threading
import duckdb

from kc import shared
from kc.config import (
    BASE_HF,
    BASE_HF_AIR,
    BASE_HF_BSNL,
    BASE_HF_CAR,
    BASE_HF_FLIP,
    BASE_HF_TG,
    BASE_HF_UMBA,
    HF_TOKEN,
    SYS_CONCURRENT_SOURCES_INIT,
    SYS_CPU_THREADS_INIT,
    SYS_HTTP_TIMEOUT_MS_INIT,
    SYS_MAX_QUEUE_INIT,
    SYS_RAM_LIMIT_INIT,
    SYS_SEARCH_TIMEOUT_INIT,
    _UA,
)
from kc.logging_setup import log

_sys_lock = threading.Lock()

_VIEW_DEFS = [
    (
        "people_phone",
        f"CREATE VIEW IF NOT EXISTS people_phone AS SELECT * FROM read_parquet('{BASE_HF}idx_phone.*.parquet')"
    ),
    (
        "people_umba",
        f"""CREATE VIEW IF NOT EXISTS people_umba AS
            SELECT "Number"   AS phoneNumber,
                   "Name"     AS name,
                   "Address"  AS address,
                   "Email"    AS email,
                   "Gender"   AS gender,
                   "Carrier"  AS carrier
            FROM   read_parquet('{BASE_HF_UMBA}idx_phone.*.parquet')"""
    ),
    (
        "people_bsnl",
        f"""CREATE VIEW IF NOT EXISTS people_bsnl AS
            SELECT "Number"      AS number,
                   "Carrier"     AS carrier,
                   "Name"        AS name,
                   "Gender"      AS gender,
                   "Address"     AS address,
                   "JobTitle"    AS job_title,
                   "CompanyName" AS company_name,
                   "Email"       AS email,
                   "Facebook"    AS facebook,
                   "Twitter"     AS twitter
            FROM   read_parquet('{BASE_HF_BSNL}')"""
    ),
    (
        "people_cars",
        f"""CREATE VIEW IF NOT EXISTS people_cars AS
            SELECT "Name"          AS name,
                   "Mobile No"     AS mobile_no,
                   "Address"       AS address,
                   "Pin Code"      AS pincode,
                   "City"          AS city,
                   "Submodel"      AS submodel,
                   "Model"         AS model,
                   "Assettype"     AS asset_type,
                   "Misstatus"     AS mis_status,
                   "Tenor"         AS tenor,
                   "Roi"           AS roi,
                   "Irr"           AS irr,
                   "Noadvemi"      AS no_adv_emi,
                   "Emiamt"        AS emi_amt,
                   "Netltv"        AS net_ltv,
                   "Mfrsubded"     AS mfr_sub_ded,
                   "Mfrsubnded"    AS mfr_sub_nded,
                   "Dlrsubded"     AS dlr_sub_ded,
                   "Dlrsubnded"    AS dlr_sub_nded,
                   "Dsasubded"     AS dsa_sub_ded,
                   "Promotiondesc" AS promotion_desc,
                   "Product"       AS product
            FROM   read_parquet('{BASE_HF_CAR}')"""
    ),
    (
        "people_air",
        f"""CREATE VIEW IF NOT EXISTS people_air AS
            SELECT "Number"      AS number,
                   "Carrier"     AS carrier,
                   "Name"        AS name,
                   "Gender"      AS gender,
                   "Address"     AS address,
                   "JobTitle"    AS job_title,
                   "CompanyName" AS company_name,
                   "Email"       AS email,
                   "Facebook"    AS facebook,
                   "Twitter"     AS twitter
            FROM   read_parquet('{BASE_HF_AIR}')"""
    ),
    (
        "people_flipkart",
        f"""CREATE VIEW IF NOT EXISTS people_flipkart AS
            SELECT "CustomerName"    AS name,
                   "CustomerPhone"   AS phone,
                   "CustomerEmail"   AS email,
                   "CustomerAddress" AS address,
                   "CustomerCity"    AS city,
                   "CustomerState"   AS state,
                   "CustomerPincode" AS pincode
            FROM   read_parquet('{BASE_HF_FLIP}')"""
    ),
    (
        "people_telegram",
        f"""CREATE VIEW IF NOT EXISTS people_telegram AS
            SELECT "user_id"       AS user_id,
                   "username"      AS username,
                   "first_name"    AS first_name,
                   "last_name"     AS last_name,
                   "phone"         AS phone,
                   "email"         AS email,
                   "status"        AS status,
                   "linked_id"     AS linked_id,
                   "linked_name"   AS linked_name,
                   "linked_handle" AS linked_handle
            FROM   read_parquet('{BASE_HF_TG}')"""
    ),
]

_VIEW_DICT = dict(_VIEW_DEFS)

_SRC_VIEW_MAP = {
    "main": "people_phone",
    "umba": "people_umba",
    "bsnl": "people_bsnl",
    "cars": "people_cars",
    "air": "people_air",
    "flipkart": "people_flipkart",
    "telegram": "people_telegram",
}


def recreate_view(vname: str) -> bool:
    """Drop and recreate view in recovery path (never uses CREATE VIEW IF NOT EXISTS)."""
    if vname not in _VIEW_DICT or shared.db is None:
        return False
    ddl = _VIEW_DICT[vname].replace("CREATE VIEW IF NOT EXISTS", "CREATE VIEW")
    try:
        shared.db.execute(f"DROP VIEW IF EXISTS {vname}; {ddl}")
        shared._view_health[vname] = True
        log.info("DuckDB view %s recovered to True via DROP and re-CREATE", vname)
        return True
    except Exception as _ex:
        log.warning("DuckDB view %s lazy recreate failed: %s", vname, _ex)
        return False


def _to_dicts(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    cols = [d[0] for d in conn.description]
    return [{c: (None if str(v) == "nan" else v) for c, v in zip(cols, row)} for row in conn.fetchall()]


def _phone_variants(raw_number: str) -> list[str]:
    """Generates standard Indian phone formatting variants for exact Parquet pushdown matching."""
    s = str(raw_number).strip()
    digits = re.sub(r"\D", "", s)
    if len(digits) == 12 and digits.startswith("91"):
        d10 = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        d10 = digits[1:]
    elif len(digits) >= 10:
        d10 = digits[-10:]
    else:
        d10 = digits

    variants = [
        d10,
        f"91{d10}",
        f"+91{d10}",
        f"0{d10}",
        f"+91 {d10}",
        f"{d10[:5]} {d10[5:]}",
        f"+91 {d10[:5]} {d10[5:]}",
    ]
    seen = set()
    ordered = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            ordered.append(v)
    return ordered


def _safe_query(sql: str, params: list, source_name: str = "") -> list[dict]:
    """Thread-safe query execution with cursor pooling and fallback lock."""
    if shared.db is None:
        return []
    try:
        cur = shared.db.cursor()
        try:
            res = _to_dicts(cur.execute(sql, params))
            if source_name:
                shared._source_query_error[source_name] = False
            return res
        finally:
            cur.close()
    except Exception as ex:
        ex_str = str(ex)
        if "TProtocolException" in ex_str or "Invalid data" in ex_str:
            if source_name:
                shared._source_query_error[source_name] = True
                for sid, vname in _SRC_VIEW_MAP.items():
                    if sid.lower() in source_name.lower():
                        shared._view_health[vname] = False
            log.error("DuckDB unrecoverable query error [%s]: %s (skipping retry)", source_name or "DB", ex)
            return []

        with shared._db_lock:
            try:
                cur = shared.db.cursor()
                try:
                    res = _to_dicts(cur.execute(sql, params))
                    if source_name:
                        shared._source_query_error[source_name] = False
                    return res
                finally:
                    cur.close()
            except Exception as ex2:
                if source_name:
                    shared._source_query_error[source_name] = True
                    for sid, vname in _SRC_VIEW_MAP.items():
                        if sid.lower() in source_name.lower():
                            shared._view_health[vname] = False
                log.error("DuckDB query failed [%s]: %s (initial: %s)", source_name or "DB", ex2, ex)
                return []


def configure_system_settings(
    ram: str | None = None,
    threads: int | None = None,
    concurrent_sources: int | None = None,
    timeout: float | None = None,
    max_queue: int | None = None,
    http_timeout_ms: int | None = None,
) -> dict:
    """Manual configuration function for system architecture settings."""
    with _sys_lock:
        if ram is not None:
            shared.SYS_RAM_LIMIT = str(ram).strip()
        if threads is not None:
            shared.SYS_CPU_THREADS = max(1, int(threads))
        if concurrent_sources is not None:
            shared.SYS_CONCURRENT_SOURCES = max(1, min(7, int(concurrent_sources)))
            shared._source_semaphore = threading.Semaphore(shared.SYS_CONCURRENT_SOURCES)
        if timeout is not None:
            shared.SYS_SEARCH_TIMEOUT = max(3.0, float(timeout))
        if max_queue is not None:
            shared.SYS_MAX_QUEUE = max(1, int(max_queue))
        if http_timeout_ms is not None:
            shared.SYS_HTTP_TIMEOUT_MS = max(1000, int(http_timeout_ms))

        target_cfg = {
            "ram": shared.SYS_RAM_LIMIT,
            "threads": shared.SYS_CPU_THREADS,
            "http_timeout": shared.SYS_HTTP_TIMEOUT_MS,
        }
        if shared.db is not None:
            if target_cfg != shared._duckdb_applied_cfg:
                try:
                    shared.db.execute(
                        f"SET memory_limit='{shared.SYS_RAM_LIMIT}'; "
                        f"SET threads={shared.SYS_CPU_THREADS}; "
                        f"SET http_timeout={shared.SYS_HTTP_TIMEOUT_MS};"
                    )
                    shared._duckdb_applied_cfg = dict(target_cfg)
                    log.info(
                        "✅ Manual system settings applied to DuckDB · RAM: %s, Threads: %d, Concurrent: %d",
                        shared.SYS_RAM_LIMIT,
                        shared.SYS_CPU_THREADS,
                        shared.SYS_CONCURRENT_SOURCES,
                    )
                except Exception as exc:
                    log.warning("Notice on manual DuckDB tuning: %s", exc)

    return {
        "ram_limit": shared.SYS_RAM_LIMIT,
        "cpu_threads": shared.SYS_CPU_THREADS,
        "concurrent_sources": shared.SYS_CONCURRENT_SOURCES,
        "search_timeout": shared.SYS_SEARCH_TIMEOUT,
        "max_queue_size": shared.SYS_MAX_QUEUE,
        "http_timeout_ms": shared.SYS_HTTP_TIMEOUT_MS,
    }


def bootstrap() -> None:
    """Initialize DuckDB engine, apply configuration, and register view definitions."""
    shared.SYS_RAM_LIMIT = SYS_RAM_LIMIT_INIT
    shared.SYS_CPU_THREADS = SYS_CPU_THREADS_INIT
    shared.SYS_CONCURRENT_SOURCES = SYS_CONCURRENT_SOURCES_INIT
    shared.SYS_SEARCH_TIMEOUT = SYS_SEARCH_TIMEOUT_INIT
    shared.SYS_MAX_QUEUE = SYS_MAX_QUEUE_INIT
    shared.SYS_HTTP_TIMEOUT_MS = SYS_HTTP_TIMEOUT_MS_INIT
    shared._source_semaphore = threading.Semaphore(max(1, min(7, int(shared.SYS_CONCURRENT_SOURCES))))

    log.info("⏳ Initialising DuckDB connection …")
    shared.db = duckdb.connect(config={"custom_user_agent": _UA})
    try:
        shared.db.execute(
            f"SET memory_limit='{shared.SYS_RAM_LIMIT}'; "
            f"SET threads={shared.SYS_CPU_THREADS}; "
            f"SET http_timeout={shared.SYS_HTTP_TIMEOUT_MS}; "
            f"SET enable_object_cache=true; "
            f"SET preserve_insertion_order=false; "
            f"SET enable_progress_bar=false;"
        )
    except Exception:
        shared.db.execute(
            f"SET memory_limit='{shared.SYS_RAM_LIMIT}'; "
            f"SET threads={shared.SYS_CPU_THREADS}; "
            f"SET http_timeout={shared.SYS_HTTP_TIMEOUT_MS}; "
            f"SET enable_progress_bar=false;"
        )
    shared._duckdb_applied_cfg = {
        "ram": shared.SYS_RAM_LIMIT,
        "threads": shared.SYS_CPU_THREADS,
        "http_timeout": shared.SYS_HTTP_TIMEOUT_MS,
    }
    shared.db.execute("INSTALL httpfs; LOAD httpfs;")
    if HF_TOKEN and HF_TOKEN != "hf_YOUR_READ_TOKEN_HERE":
        try:
            shared.db.execute(f"CREATE SECRET (TYPE HUGGINGFACE, TOKEN '{HF_TOKEN}');")
        except Exception as _sec_ex:
            log.warning("DuckDB CREATE SECRET notice: %s", _sec_ex)

    for vname, ddl in _VIEW_DEFS:
        try:
            shared.db.execute(ddl)
            shared._view_health[vname] = True
            log.info("✅ DuckDB view ready: %s", vname)
        except Exception as _ex:
            shared._view_health[vname] = False
            log.warning("DuckDB view init notice (%s): %s (marked unhealthy)", vname, _ex)

