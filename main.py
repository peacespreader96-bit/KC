"""KaramChand bot — entrypoint. `uvicorn main:app`."""
import os
import threading

import uvicorn
from fastapi import FastAPI

from kc import shared, background, polling, worker as worker_mod
import kc.duckdb_conn as duckdb_conn
import kc.sqlite_db as sqlite_db
from kc.config import ADMIN_ID
from kc.logging_setup import log

app = FastAPI(title="KaramChand", docs_url=None, redoc_url=None)


@app.api_route("/", methods=["GET", "HEAD"])
def health():
    return {
        "status": "alive",
        "queue": shared._search_q.qsize(),
        "maintenance": shared.MAINTENANCE,
    }


# --- Bootstrap DBs (must run before threads start) ---
sqlite_db.bootstrap()
duckdb_conn.bootstrap()

# --- Background threads — health check runs in background, does not block $PORT ---
threading.Thread(target=background.startup_health, daemon=True).start()

for fn in [
    worker_mod.worker,
    polling.poll_telegram,
    background.broadcaster,
    background.session_gc,
    background.persist_loop,
    background.backup_loop,
    background.expiry_loop,
]:
    threading.Thread(target=fn, daemon=True).start()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), log_level="info")

