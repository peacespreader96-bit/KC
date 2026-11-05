"""Utility helpers and formatters."""
import html as html_lib
import re
from datetime import datetime, timezone, timedelta
from kc.config import DEV_HANDLE

IST = timezone(timedelta(hours=5, minutes=30))
_e  = html_lib.escape
_D  = f"<i>Desk · {html_lib.escape(DEV_HANDLE)}</i>"

def now_ist() -> datetime: return datetime.now(IST)
def ts_str() -> str: return now_ist().strftime("%d-%m-%Y %I:%M:%S %p IST")
def fmt_until(ts: float) -> str: return datetime.fromtimestamp(ts, IST).strftime("%d %b %Y, %I:%M %p IST")
def fmt_ts(ts: float | None) -> str:
    if not ts: return "—"
    try: return datetime.fromtimestamp(float(ts), IST).strftime("%d %b %Y, %I:%M %p IST")
    except Exception: return "—"

def elapsed_label(secs: float) -> str:
    if secs < 0.5: return "⚡ Bijli se tez (<0.5s)"
    if secs < 1.5: return "🚀 Rocket speed (~1s)"
    if secs < 5:   return f"⚡ Tez tha ({secs:.1f}s)"
    if secs < 15:  return f"🔥 {secs:.1f}s"
    return f"⏱ {secs:.1f}s"

def sv(v) -> str:
    s = str(v).strip() if v is not None else ""
    return "" if s.lower() == "nan" else s

def q_bar(pos: int, cap: int = 10) -> str:
    pct = 100 if pos <= 1 else max(10, 100 - (pos - 1) * (100 // cap))
    filled = max(0, min(cap, int(round(cap * pct / 100))))
    return f"[{'█' * filled}{'░' * (cap - filled)}] {pct}%"

def clean_addr(addr) -> str:
    if not addr: return ""
    return re.sub(r"\s+", " ", re.sub(r"!+", ", ", str(addr))).strip(", ")

def fmt_td(secs: int) -> str:
    secs = max(0, int(secs))
    d, rem = divmod(secs, 86400); h, rem = divmod(rem, 3600); m, s = divmod(rem, 60)
    if d: return f"{d}d {h}h"
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def _today() -> str: return now_ist().strftime("%Y-%m-%d")
def _clip(text: str, n: int = 3800) -> str:
    return text if len(text) <= n else text[: n - 24] + "\n\n… kaat diya bhai"
