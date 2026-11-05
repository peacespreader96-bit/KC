"""Pure constants and environment configuration for KaramChand bot."""
import json
import os

TOKEN       = os.getenv("BOT_TOKEN",  "8723126732:AAHVFOySgFaWN4_4InqtKrvkgN15crbPi0w")
ADMIN_ID    = int(os.getenv("ADMIN_ID", "5505679330"))
HF_TOKEN    = os.getenv("HF_TOKEN",   "hf_YOUR_READ_TOKEN_HERE")
BOT_LINK    = os.getenv("BOT_LINK",   "https://t.me/Karam_Chand_bot")
DEV_LINK    = os.getenv("DEV_LINK",   "https://t.me/ansafrozkhan")
DEV_HANDLE  = os.getenv("DEV_HANDLE", "@ansafrozkhan")
BOT_NAME    = os.getenv("BOT_NAME",   "KaramChand")
UPI_ID      = os.getenv("UPI_ID",     "admin@upi")
USDT_BEP20  = os.getenv("USDT_BEP20", "0x0000000000000000000000000000000000000000")
USDT_TRC20  = os.getenv("USDT_TRC20", "TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx")
BASE_HF          = os.getenv("BASE_HF",          "hf://buckets/CutehackX/icrm-hitek-full-db-mixed-bucket/")
BASE_HF_UMBA     = os.getenv("BASE_HF_UMBA",     "hf://buckets/Khanenterprises/umba-bucket/")
BASE_HF_BSNL     = os.getenv("BASE_HF_BSNL",     "hf://buckets/Khanenterprises/Bsnalumba/19M-BSNL_Mobile.parquet")
BASE_HF_CAR      = os.getenv("BASE_HF_CAR",      "hf://buckets/Khanenterprises/Carumba/Indian Cars Owner Details.parquet")
BASE_HF_AIR      = os.getenv("BASE_HF_AIR",      "hf://buckets/Khanenterprises/Umbaairbucket/data.parquet")
BASE_HF_FLIP     = os.getenv("BASE_HF_FLIP",     "hf://buckets/Khanenterprises/Umbaflip-bucket/flipkart.parquet")
BASE_HF_TG       = os.getenv("BASE_HF_TG",       "hf://buckets/Khanenterprises/Umbat-bucket/TELEGRAM_MASTER_DB.parquet")
DUCKDB_MEM_LIMIT = os.getenv("DUCKDB_MEM_LIMIT", "500MB")
DUCKDB_THREADS   = int(os.getenv("DUCKDB_THREADS", "4"))
BASE_URL         = f"https://api.telegram.org/bot{TOKEN}"

SYS_RAM_LIMIT_INIT          = os.getenv("SYS_RAM_LIMIT",          "250MB")
SYS_CPU_THREADS_INIT        = int(os.getenv("SYS_CPU_THREADS",    "2"))
SYS_CONCURRENT_SOURCES_INIT = int(os.getenv("SYS_CONCURRENT_SOURCES", "7"))
SYS_SEARCH_TIMEOUT_INIT     = float(os.getenv("SYS_SEARCH_TIMEOUT", "60.0"))
SYS_MAX_QUEUE_INIT          = int(os.getenv("SYS_MAX_QUEUE",      "20"))
SYS_HTTP_TIMEOUT_MS_INIT    = int(os.getenv("SYS_HTTP_TIMEOUT_MS", "60000"))
SYS_HTTP_POOL_CONN          = int(os.getenv("SYS_HTTP_POOL_CONN", "25"))
SYS_HTTP_POOL_MAX           = int(os.getenv("SYS_HTTP_POOL_MAX",  "50"))

FORCE_CHANNELS: list[str] = []
_fc_env = os.getenv("FORCE_CHANNELS", "").strip()
if _fc_env:
    try:
        FORCE_CHANNELS = json.loads(_fc_env) if _fc_env.startswith("[") else [x.strip() for x in _fc_env.split(",") if x.strip()]
    except Exception:
        FORCE_CHANNELS = [x.strip() for x in _fc_env.split(",") if x.strip()]

FREE_DAILY_LIMIT  = 3
SPIN_INTERVAL     = "daily"
SPIN_WINNER_LIMIT = 5
SPIN_JACKPOT      = 64
SPIN_WINS         = {64: "premium", 43: 5, 22: 3, 1: 1}
STREAK_INTERVAL   = 3
STREAK_BONUS      = 1
WELCOME_IMAGE_URL = "https://raw.githubusercontent.com/peacespreader96-bit/KC/refs/heads/main/banner.jpg"

PLANS: dict[str, dict] = {
    "day":   {"id": "day",   "name": "1 Din ka Pass",  "days": 1,  "amount": 29,  "stars": 29},
    "week":  {"id": "week",  "name": "7 Din ka Pass",  "days": 7,  "amount": 79,  "stars": 79},
    "month": {"id": "month", "name": "30 Din ka Pass", "days": 30, "amount": 150, "stars": 150},
}
SUPPORTED_LANGS = ["en"]

SEARCH_COOLDOWN    = 0
MSG_COOLDOWN       = 1
MAX_QUEUE          = SYS_MAX_QUEUE_INIT
SESSION_TTL        = 1800
QUEUE_BROADCAST_IV = 5
GC_INTERVAL        = 300
_STAGE_GAP         = 0.30
_PAY_TTL           = 1800
_ROLES_ASSIGNABLE  = ("FINANCE_MANAGER", "SUPPORT_AGENT", "USER")
_PAY_MODES         = {"checkout", "WAITING_FOR_UTR", "WAITING_FOR_TX", "stars", "pay_choose", "crypto"}
_GW_MODES          = {
    "upi":    {"checkout", "WAITING_FOR_UTR"},
    "crypto": {"crypto", "WAITING_FOR_TX"},
    "stars":  {"stars"},
}

_ROOT_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_HERE        = _ROOT_DIR
_DB_PATH     = os.path.join(_ROOT_DIR, "users.db")
_BACKUP_DIR  = os.path.join(_ROOT_DIR, "backups")
_RECEIPT_DIR = os.path.join(_ROOT_DIR, "receipts")
_BAN_TEXT    = "Bhai tera ID ban hai. Koi feature nahi chalega — desk band. Operator se baat kar, yahan rona band."
_MAINT_TEXT  = "Bhai system maintenance mein hai, thodi der baad aana."
_UA          = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
