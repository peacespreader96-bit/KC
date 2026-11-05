"""Mutable global state. Import as `import shared`, access as `shared.X`."""
import threading
import queue as _queue_mod
from collections import deque

# --- Handles (reassigned by /restore, /initdb, /sysconfig) ---
db = None
_sql = None
_source_semaphore: threading.Semaphore | None = None
_duckdb_applied_cfg: dict = {}

# --- System config (mutated by /sysconfig) ---
MAINTENANCE: bool = True   # safe default; cleared by startup_health
SYS_RAM_LIMIT: str = "350MB"
SYS_CPU_THREADS: int = 4
SYS_CONCURRENT_SOURCES: int = 7
SYS_SEARCH_TIMEOUT: float = 120.0
SYS_MAX_QUEUE: int = 20
SYS_HTTP_TIMEOUT_MS: int = 120000

# --- View health ---
_view_health: dict[str, bool] = {}
_source_query_error: dict[str, bool] = {}

# --- User state (mutated in place) ---
user_data:      dict[int, dict] = {}
user_sessions:  dict[tuple[int, int], dict] = {}
active_users:   set[int]        = set()
global_stats:   dict[str, int]  = {"searches": 0, "results": 0}
flow:           dict[int, dict] = {}
support_routes: dict[int, int]  = {}
seen_day:       dict[int, str]  = {}
_desk:          dict[int, dict] = {}
_chan_fail:     set[str]        = set()

# --- Locks ---
_sl       = threading.RLock()
_db_lock  = threading.RLock()
_sql_lock = threading.RLock()
_ql       = threading.Lock()

# --- Queue ---
_search_q     = _queue_mod.Queue()
_queue_order: list[dict] = []

# --- Log ring buffer ---
_log_buffer: deque[str] = deque(maxlen=400)
_log_lock = threading.Lock()

# --- Late-bound callbacks (breaks circular imports) ---
# Modules that need "up-calls" register here at import time.
_poke_desks_fn = lambda: None
_show_dashboard_fn = lambda cid, mid=None: None

