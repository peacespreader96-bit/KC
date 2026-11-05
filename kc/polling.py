"""Telegram long polling loop and update dispatcher."""
import json
import time
import urllib.parse
import requests

from kc.config import BASE_URL
from kc.handlers import handle_cb, handle_msg, handle_pre_checkout
from kc.logging_setup import log
from kc.telegram_api import _session


def poll_telegram() -> None:
    try:
        _session.get(f"{BASE_URL}/deleteWebhook?drop_pending_updates=false", timeout=5)
    except Exception:
        pass
    offset, backoff = None, 1
    while True:
        try:
            allowed = urllib.parse.quote(json.dumps(["message", "callback_query", "pre_checkout_query"]))
            url = f"{BASE_URL}/getUpdates?timeout=30&allowed_updates={allowed}" + (f"&offset={offset}" if offset else "")
            resp = _session.get(url, timeout=35).json()
            if not resp.get("ok"):
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            backoff = 1
            for upd in resp.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd:
                        handle_cb(upd["callback_query"])
                    elif "pre_checkout_query" in upd:
                        handle_pre_checkout(upd["pre_checkout_query"])
                    elif "message" in upd:
                        handle_msg(upd["message"])
                except Exception:
                    log.exception("Update handler crash")
        except requests.RequestException:
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception:
            time.sleep(3)

