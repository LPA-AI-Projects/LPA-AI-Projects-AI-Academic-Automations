"""
Bitrix24 Tasks: upload outline PDF to Drive, attach to task, notify via task comment.

Workflow:
1. disk.folder.uploadfile (folder auto-created under My Drive if needed)
2. tasks.task.files.attach
3. task.commentitem.add
"""
from __future__ import annotations

import base64
import os
from typing import Any

from app.core.config import settings
from app.services.bitrix_crm import bitrix_call, bitrix_configured
from app.utils.logger import get_logger

logger = get_logger(__name__)


def _api_webhook_base() -> str:
    """Insert /api/ segment for REST v3 task methods."""
    base = (settings.BITRIX_WEBHOOK_URL or "").strip().rstrip("/")
    if "/rest/api/" in base:
        return base
    if "/rest/" in base:
        return base.replace("/rest/", "/rest/api/", 1)
    return base


async def bitrix_call_api(method: str, params: dict[str, Any] | None = None) -> Any:
    """Call method on /rest/api/{user}/{secret}/ (v3)."""
    base = _api_webhook_base()
    method_name = (method or "").strip().strip("/")
    if method_name.endswith(".json"):
        method_name = method_name[:-5]
    url = f"{base}/{method_name}.json"
    import httpx

    logger.info("Bitrix REST API call | method=%s", method_name)
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url, json=params or {})
    raw = response.text or ""
    try:
        payload = response.json()
    except Exception:
        raise RuntimeError(f"Bitrix API non-JSON: HTTP {response.status_code}") from None
    if not isinstance(payload, dict):
        raise RuntimeError("Bitrix API unexpected response")
    if payload.get("error"):
        raise RuntimeError(
            f"Bitrix API error: {payload.get('error')} — {payload.get('error_description')}"
        )
    return payload.get("result")


def _extract_disk_file_id(upload_result: Any) -> int | None:
    if upload_result is None:
        return None
    if isinstance(upload_result, dict):
        for key in ("ID", "id", "FILE_ID", "fileId"):
            v = upload_result.get(key)
            if v is not None:
                try:
                    return int(v)
                except (TypeError, ValueError):
                    continue
    if isinstance(upload_result, int):
        return upload_result
    return None


def _folder_id_from_item(item: Any) -> str | None:
    if not isinstance(item, dict):
        return None
    for key in ("ID", "id"):
        v = item.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


async def ensure_outline_folder() -> str:
    """
    Find or create ``BITRIX_DRIVE_FOLDER_NAME`` under the portal's default Drive storage.
    """
    folder_name = (settings.BITRIX_DRIVE_FOLDER_NAME or "CourseOutlines").strip()
    storage = await bitrix_call("disk.storage.getlist", {})
    if not isinstance(storage, list) or not storage:
        raise RuntimeError("disk.storage.getlist returned no storages")

    first = storage[0]
    if not isinstance(first, dict):
        raise RuntimeError(f"Unexpected storage entry: {first!r}")
    storage_id = first.get("ID") or first.get("id")
    if storage_id is None:
        raise RuntimeError(f"Storage entry missing ID: {first!r}")

    children = await bitrix_call("disk.storage.getchildren", {"id": storage_id})
    if isinstance(children, list):
        for item in children:
            if isinstance(item, dict) and str(item.get("NAME") or "") == folder_name:
                fid = _folder_id_from_item(item)
                if fid:
                    logger.info(
                        "Bitrix Drive folder found | name=%s id=%s",
                        folder_name,
                        fid,
                    )
                    return fid

    created = await bitrix_call(
        "disk.storage.addfolder",
        {
            "id": storage_id,
            "data": {"NAME": folder_name},
        },
    )
    if isinstance(created, dict):
        fid = _folder_id_from_item(created)
        if fid:
            logger.info(
                "Bitrix Drive folder created | name=%s id=%s",
                folder_name,
                fid,
            )
            return fid
    raise RuntimeError(f"disk.storage.addfolder did not return folder id: {created!r}")


async def upload_pdf_to_drive_folder(pdf_path: str, *, folder_id: str | None = None) -> int:
    fid = (folder_id or "").strip()
    if not fid:
        fid = (settings.BITRIX_DRIVE_FOLDER_ID or "").strip()
    if not fid:
        fid = await ensure_outline_folder()

    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    filename = os.path.basename(pdf_path)
    with open(pdf_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")

    body = {
        "id": int(fid),
        "data": {"NAME": filename},
        "fileContent": [filename, b64],
        "generateUniqueName": True,
    }
    result = await bitrix_call("disk.folder.uploadfile", body)
    file_id = _extract_disk_file_id(result)
    if file_id is None:
        raise RuntimeError(f"disk.folder.uploadfile did not return file id: {result!r}")
    logger.info("Bitrix Drive upload ok | folder_id=%s file_id=%s name=%s", fid, file_id, filename)
    return file_id


async def attach_file_to_task(task_id: str | int, file_id: int) -> dict[str, Any]:
    body = {"taskId": int(task_id), "fileId": int(file_id)}
    result = await bitrix_call("tasks.task.files.attach", body)
    logger.info("Bitrix task file attached | task_id=%s file_id=%s", task_id, file_id)
    return result if isinstance(result, dict) else {"result": result}


async def send_task_comment(
    task_id: str | int,
    message: str,
    drive_file_id: int | None = None,
) -> Any:
    """
    Add a task comment per Bitrix docs: TASKID + FIELDS.POST_MESSAGE.

    Optional ``UF_FORUM_MESSAGE_DOC`` with Drive file ids prefixed by ``n`` (e.g. ``n7167800``).
    See https://apidocs.bitrix24.com/api-reference/tasks/comment-item/task-comment-item-add.html
    """
    text = (message or "").strip()
    if not text:
        return None

    fields: dict[str, Any] = {"POST_MESSAGE": text}
    if drive_file_id is not None:
        fields["UF_FORUM_MESSAGE_DOC"] = [f"n{int(drive_file_id)}"]

    result = await bitrix_call(
        "task.commentitem.add",
        {
            "TASKID": int(task_id),
            "FIELDS": fields,
        },
    )
    logger.info("Bitrix task comment added | task_id=%s drive_file_id=%s", task_id, drive_file_id)
    return result


async def fetch_task_item_data(task_id: str | int) -> dict[str, Any]:
    """Load task fields when webhook only sends id (task.item.getdata)."""
    result = await bitrix_call("task.item.getdata", {"taskId": int(task_id)})
    if isinstance(result, dict):
        return result
    raise RuntimeError(f"task.item.getdata returned unexpected type: {type(result)}")


def _message_text_from_im_row(row: dict[str, Any]) -> str:
    for key in ("text", "message", "MESSAGE", "TEXT"):
        v = row.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


async def _fetch_task_chat_id(task_id: str | int) -> int | None:
    """Resolve task chat id (old CHAT_ID or new chat.id)."""
    tid = int(task_id)
    try:
        result = await bitrix_call_api(
            "tasks.task.get",
            {"id": tid, "select": ["id", "chat.id", "chatId"]},
        )
    except Exception:
        logger.warning("tasks.task.get via /rest/api/ failed; trying standard /rest/")
        result = await bitrix_call("tasks.task.get", {"taskId": tid, "select": ["CHAT_ID", "chatId"]})

    if isinstance(result, dict):
        item = result.get("item") if isinstance(result.get("item"), dict) else result.get("task")
        if isinstance(item, dict):
            chat = item.get("chat")
            if isinstance(chat, dict):
                cid = chat.get("id")
                if cid is not None:
                    return int(cid)
            for key in ("chatId", "CHAT_ID", "chat_id"):
                v = item.get(key)
                if v is not None:
                    return int(v)
        for key in ("chatId", "CHAT_ID"):
            v = result.get(key)
            if v is not None:
                return int(v)
    return None


async def fetch_task_comment_text(task_id: str | int, message_id: str | int) -> str | None:
    """
    Load task chat / comment text for ONTASKCOMMENTADD.

    New task card: MESSAGE_ID + im.dialog.messages.get.
    Old card fallback: task.commentitem.get when comment id > 0.
    """
    mid = int(message_id)
    chat_id = await _fetch_task_chat_id(task_id)
    if chat_id is not None:
        dialog_id = f"chat{chat_id}"
        try:
            rows = await bitrix_call(
                "im.dialog.messages.get",
                {
                    "DIALOG_ID": dialog_id,
                    "LAST_ID": mid + 1,
                    "LIMIT": 20,
                },
            )
        except Exception as e:
            logger.warning(
                "im.dialog.messages.get failed | task_id=%s chat_id=%s error=%s",
                task_id,
                chat_id,
                e,
            )
            rows = None
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_id = row.get("id") or row.get("ID")
                try:
                    if row_id is not None and int(row_id) == mid:
                        text = _message_text_from_im_row(row)
                        if text:
                            return text
                except (TypeError, ValueError):
                    continue
            if rows:
                text = _message_text_from_im_row(rows[-1] if isinstance(rows[-1], dict) else {})
                if text:
                    return text

    comment_id = int(message_id)
    if comment_id > 0:
        try:
            result = await bitrix_call(
                "task.commentitem.get",
                {"TASKID": int(task_id), "ITEMID": comment_id},
            )
            if isinstance(result, dict):
                text = str(result.get("POST_MESSAGE") or result.get("postMessage") or "").strip()
                if text:
                    return text
        except Exception as e:
            logger.warning(
                "task.commentitem.get failed | task_id=%s item_id=%s error=%s",
                task_id,
                comment_id,
                e,
            )
    return None


async def deliver_outline_pdf_to_bitrix_task(
    *,
    task_id: str,
    pdf_path: str | None,
    pdf_url: str | None,
    course_name: str,
) -> dict[str, Any]:
    """
    Upload PDF to Drive, attach to task, post task comment. Failures are logged; partial success ok.
    """
    out: dict[str, Any] = {
        "task_id": task_id,
        "drive_file_id": None,
        "attachment_id": None,
        "comment_sent": False,
    }
    if not bitrix_configured():
        logger.info("Bitrix task delivery skipped: BITRIX_WEBHOOK_URL not set")
        return out
    if not settings.BITRIX_TASK_ATTACH_ENABLED:
        logger.info("Bitrix task delivery skipped: BITRIX_TASK_ATTACH_ENABLED=false")
        return out
    if not pdf_path or not os.path.isfile(pdf_path):
        logger.warning("Bitrix task delivery skipped: PDF path missing | task_id=%s", task_id)
        return out

    file_id: int | None = None
    try:
        file_id = await upload_pdf_to_drive_folder(pdf_path)
        out["drive_file_id"] = file_id
        attach_result = await attach_file_to_task(task_id, file_id)
        if isinstance(attach_result, dict):
            out["attachment_id"] = attach_result.get("attachmentId")
    except Exception:
        logger.exception("Bitrix task PDF upload/attach failed | task_id=%s", task_id)

    if file_id is not None:
        try:
            msg = f"""Course outline generated successfully.

Google Drive:
{pdf_url or "(link not available)"}

PDF uploaded and attached to this task."""
            await send_task_comment(
                task_id,
                msg,
                drive_file_id=file_id,
            )
            out["comment_sent"] = True
        except Exception:
            logger.exception("Bitrix task comment failed | task_id=%s", task_id)

    return out
