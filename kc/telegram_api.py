"""Telegram API client and messaging helpers."""
import io
import time
import requests

from kc.config import BASE_URL, TOKEN, SYS_HTTP_POOL_CONN, SYS_HTTP_POOL_MAX
from kc.logging_setup import log

_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=SYS_HTTP_POOL_CONN,
    pool_maxsize=SYS_HTTP_POOL_MAX,
    max_retries=1,
)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def tg(method: str, payload: dict, *, retries: int = 2) -> dict:
    url = f"{BASE_URL}/{method}"
    for attempt in range(retries + 1):
        try:
            res = _session.post(url, json=payload, timeout=10)
            data = res.json()
            if not data.get("ok"):
                desc = str(data.get("description") or "")
                if "message is not modified" not in desc:
                    log.warning("TG/%s → %s", method, desc)
                if res.status_code == 429 or "Too Many Requests" in desc:
                    retry_after = int((data.get("parameters") or {}).get("retry_after") or 1)
                    if attempt < retries:
                        time.sleep(min(max(retry_after, 1), 30))
                        continue
            return data
        except requests.RequestException as exc:
            if attempt == retries:
                log.error("TG/%s failed (%d retries): %s", method, retries, exc)
                return {}
            time.sleep(0.3 * (attempt + 1))
    return {}


def send_msg(cid: int, text: str, kb=None) -> int | None:
    p: dict = {"chat_id": cid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendMessage", p).get("result", {}).get("message_id")


def send_photo(cid: int, photo: str, caption: str, kb=None) -> int | None:
    p: dict = {"chat_id": cid, "photo": photo, "caption": caption[:1024], "parse_mode": "HTML"}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    return tg("sendPhoto", p).get("result", {}).get("message_id")


def edit_msg(cid: int, mid: int | None, text: str, kb=None) -> None:
    if not mid:
        return
    p: dict = {"chat_id": cid, "message_id": mid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    tg("editMessageText", p)


def desk_edit(cid: int, mid: int | None, text: str, kb=None) -> None:
    if not mid:
        send_msg(cid, text, kb=kb)
        return
    p: dict = {"chat_id": cid, "message_id": mid, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    res = tg("editMessageText", p)
    desc = str(res.get("description") or "")
    if res.get("ok") or "message is not modified" in desc:
        return
    cap: dict = {"chat_id": cid, "message_id": mid, "caption": text[:1024], "parse_mode": "HTML"}
    if kb:
        cap["reply_markup"] = {"inline_keyboard": kb}
    res2 = tg("editMessageCaption", cap)
    desc2 = str(res2.get("description") or "")
    if res2.get("ok") or "message is not modified" in desc2:
        return
    tg("deleteMessage", {"chat_id": cid, "message_id": mid})
    send_msg(cid, text, kb=kb)


def answer_cb(cb_id: str, text: str | None = None, alert: bool = False) -> None:
    p: dict = {"callback_query_id": cb_id}
    if text:
        p["text"] = text[:180]
        p["show_alert"] = alert
    tg("answerCallbackQuery", p)


def send_html_file(cid: int, filename: str, content: str, caption: str) -> None:
    try:
        _session.post(
            f"{BASE_URL}/sendDocument",
            data={"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
            files={"document": (filename, io.BytesIO(content.encode("utf-8")), "text/html")},
            timeout=25,
        )
    except Exception:
        log.exception("HTML document failed for %s", cid)


def _download_tg_file(file_id: str) -> bytes | None:
    info = tg("getFile", {"file_id": file_id})
    path = ((info.get("result") or {}).get("file_path")) if info else None
    if not path:
        return None
    try:
        res = _session.get(f"https://api.telegram.org/file/bot{TOKEN}/{path}", timeout=120)
        return res.content if res.status_code == 200 else None
    except Exception:
        log.exception("telegram file download failed")
        return None

