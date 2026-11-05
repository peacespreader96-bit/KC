"""Logging setup with ring buffer integrated with shared mutable state."""
import logging
from kc import shared

_FMT  = "%(asctime)s [%(levelname)-5s] %(message)s"
_TIME = "%I:%M:%S %p"

class _RingHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        formatted = self.format(record)
        with shared._log_lock:
            shared._log_buffer.append(formatted)

    def tail(self, n: int = 150) -> list[str]:
        with shared._log_lock:
            return list(shared._log_buffer)[-n:]

_ring = _RingHandler()
_ring.setFormatter(logging.Formatter(_FMT, _TIME))
logging.basicConfig(level=logging.INFO, format=_FMT, datefmt=_TIME)
if _ring not in logging.getLogger().handlers:
    logging.getLogger().addHandler(_ring)
log = logging.getLogger("bot")

