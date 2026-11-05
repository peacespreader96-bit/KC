"""Intelligence sources registry and formatting assembly."""
from kc.duckdb_conn import _phone_variants
from .air_source import _format_air_block, db_search_air
from .bsnl_source import _format_bsnl_block, db_search_bsnl
from .cars_source import _format_cars_block, db_search_cars
from .flipkart_source import _format_flipkart_block, db_search_flipkart
from .main_source import _format_main_block, db_search_main
from .telegram_source import _format_telegram_block, db_search_telegram
from .umba_source import _format_umba_block, db_search_umba

SOURCES = [
    {"id": "main",     "name": "KC-SOURCE-01", "search_fn": db_search_main,     "format_fn": _format_main_block},
    {"id": "umba",     "name": "KC-SOURCE-02", "search_fn": db_search_umba,     "format_fn": _format_umba_block},
    {"id": "bsnl",     "name": "KC-SOURCE-03", "search_fn": db_search_bsnl,     "format_fn": _format_bsnl_block},
    {"id": "cars",     "name": "KC-SOURCE-04", "search_fn": db_search_cars,     "format_fn": _format_cars_block},
    {"id": "air",      "name": "KC-SOURCE-05", "search_fn": db_search_air,      "format_fn": _format_air_block},
    {"id": "flipkart", "name": "KC-SOURCE-06", "search_fn": db_search_flipkart, "format_fn": _format_flipkart_block},
    {"id": "telegram", "name": "KC-SOURCE-07", "search_fn": db_search_telegram, "format_fn": _format_telegram_block},
]

# Compatibility alias
db_search = db_search_main
