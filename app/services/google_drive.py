from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any

import httpx

from app.utils.logger import get_logger

logger = get_logger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
GOOGLE_DRIVE_PERMISSIONS_URL_TMPL = "https://www.googleapis.com/drive/v3/files/{file_id}/permissions"
GOOGLE_SLIDES_MIME = "application/vnd.google-apps.presentation"


class GoogleDriveUploadError(RuntimeError):
    """Raised when Google Drive upload/conversion fails."""


class GoogleSlidesMergeError(RuntimeError):
    """Raised when Google Apps Script merge fails."""


def _get_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise GoogleDriveUploadError(f"Missing required environment variable: {name}")
    return value


def _get_access_token() -> str:
    client_id = _get_env("GOOGLE_CLIENT_ID")
    client_secret = _get_env("GOOGLE_CLIENT_SECRET")
    refresh_token = _get_env("GOOGLE_REFRESH_TOKEN")

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.post(GOOGLE_TOKEN_URL, data=data)

    if response.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google OAuth token exchange failed: HTTP {response.status_code} body={(response.text or '')[:1000]}"
        )

    payload = response.json() if response.content else {}
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise GoogleDriveUploadError("Google OAuth token response missing access_token.")
    return access_token


def upload_ppt_to_google_drive(file_path: str, filename: str) -> dict[str, Any]:
    """
    Upload a PPT/PPTX file to Google Drive, convert it into Google Slides,
    then set public edit permission (anyone=writer).

    Required env vars:
    - GOOGLE_CLIENT_ID
    - GOOGLE_CLIENT_SECRET
    - GOOGLE_REFRESH_TOKEN
    """
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        raise GoogleDriveUploadError(f"File not found: {file_path}")

    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    metadata: dict[str, Any] = {
        "name": filename,
        # Upload as native Google Slides document (editable in browser)
        "mimeType": GOOGLE_SLIDES_MIME,
    }
    folder_id = (os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip()
    if folder_id:
        metadata["parents"] = [folder_id]

    with path.open("rb") as fh:
        files = {
            "metadata": ("metadata", json.dumps(metadata), "application/json; charset=UTF-8"),
            "file": (filename, fh, "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        }
        with httpx.Client(timeout=120.0) as client:
            upload_resp = client.post(GOOGLE_DRIVE_UPLOAD_URL, headers=headers, files=files)

    if upload_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive upload failed: HTTP {upload_resp.status_code} body={(upload_resp.text or '')[:2000]}"
        )

    uploaded = upload_resp.json() if upload_resp.content else {}
    file_id = uploaded.get("id")
    if not isinstance(file_id, str) or not file_id:
        raise GoogleDriveUploadError("Google Drive upload succeeded but file id was missing.")

    # Set permission: anyone can edit
    permission_payload = {"type": "anyone", "role": "writer"}
    permission_url = GOOGLE_DRIVE_PERMISSIONS_URL_TMPL.format(file_id=file_id)
    with httpx.Client(timeout=30.0) as client:
        perm_resp = client.post(permission_url, headers=headers, json=permission_payload)

    if perm_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive permission update failed: HTTP {perm_resp.status_code} body={(perm_resp.text or '')[:2000]}"
        )

    edit_link = f"https://docs.google.com/presentation/d/{file_id}/edit"
    logger.info("Google Drive upload success | file_id=%s filename=%s", file_id, filename)
    return {"file_id": file_id, "edit_link": edit_link}


def merge_google_slides_via_apps_script(presentation_ids: list[str]) -> dict[str, Any]:
    """
    Merge multiple Google Slides presentations into a single deck via Google Apps Script web app.

    Required env vars:
    - GOOGLE_SCRIPT_URL
    - GOOGLE_SCRIPT_KEY
    Optional:
    - GOOGLE_DRIVE_FOLDER_ID (forwarded to script as folderId)
    """
    ids = [str(x).strip() for x in presentation_ids if str(x).strip()]
    if not ids:
        raise GoogleSlidesMergeError("No presentation IDs provided for merge.")

    script_url = _get_env("GOOGLE_SCRIPT_URL")
    script_key = _get_env("GOOGLE_SCRIPT_KEY")
    folder_id = (os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip()

    payload: dict[str, Any] = {
        "key": script_key,
        "presentationIds": ids,
    }
    if folder_id:
        payload["folderId"] = folder_id

    with httpx.Client(timeout=120.0) as client:
        response = client.post(script_url, json=payload)

    if response.status_code >= 400:
        raise GoogleSlidesMergeError(
            f"Apps Script merge call failed: HTTP {response.status_code} body={(response.text or '')[:2000]}"
        )

    data = response.json() if response.content else {}
    if isinstance(data, dict) and data.get("ok") is False:
        raise GoogleSlidesMergeError(f"Apps Script merge error: {data.get('error')}")

    merged_file_id = data.get("presentationId") or data.get("file_id")
    merged_link = data.get("link") or data.get("edit_link")
    if not isinstance(merged_file_id, str) or not merged_file_id.strip():
        raise GoogleSlidesMergeError(f"Apps Script merge missing presentationId. payload={str(data)[:800]}")
    if not isinstance(merged_link, str) or not merged_link.strip():
        merged_link = f"https://docs.google.com/presentation/d/{merged_file_id.strip()}/edit"

    merged_file_id = merged_file_id.strip()
    merged_link = merged_link.strip()
    logger.info("Apps Script merge success | merged_file_id=%s", merged_file_id)
    return {"file_id": merged_file_id, "edit_link": merged_link}

