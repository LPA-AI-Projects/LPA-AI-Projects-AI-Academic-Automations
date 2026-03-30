from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Any

import httpx

from app.core.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
GOOGLE_DRIVE_RESUMABLE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable"
GOOGLE_DRIVE_PERMISSIONS_URL_TMPL = "https://www.googleapis.com/drive/v3/files/{file_id}/permissions"
GOOGLE_DRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"
GOOGLE_SLIDES_MIME = "application/vnd.google-apps.presentation"
DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


class GoogleDriveUploadError(RuntimeError):
    """Raised when Google Drive upload/conversion fails."""


class GoogleSlidesMergeError(RuntimeError):
    """Raised when Google Apps Script merge fails."""


def _get_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        value = str(getattr(settings, name, "") or "").strip()
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


def _set_public_edit_permission(file_id: str, access_token: str) -> None:
    permission_payload = {"type": "anyone", "role": "writer"}
    permission_url = GOOGLE_DRIVE_PERMISSIONS_URL_TMPL.format(file_id=file_id)
    headers = {"Authorization": f"Bearer {access_token}"}
    with httpx.Client(timeout=30.0) as client:
        perm_resp = client.post(permission_url, headers=headers, json=permission_payload)
    if perm_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive permission update failed: HTTP {perm_resp.status_code} body={(perm_resp.text or '')[:2000]}"
        )


def _sanitize_drive_name(name: str) -> str:
    cleaned = (name or "").strip()
    forbidden = '\\/:*?"<>|'
    for ch in forbidden:
        cleaned = cleaned.replace(ch, "_")
    return cleaned[:120] or "course"


def ensure_drive_folder(folder_name: str, *, parent_folder_id: str | None = None) -> dict[str, str]:
    """
    Create (or reuse) a Google Drive folder by name and return its id/link.
    """
    access_token = _get_access_token()
    safe_name = _sanitize_drive_name(folder_name)
    headers = {"Authorization": f"Bearer {access_token}"}

    safe_name_q = safe_name.replace("'", "\\'")
    q_parts = [
        f"name = '{safe_name_q}'",
        "mimeType = 'application/vnd.google-apps.folder'",
        "trashed = false",
    ]
    if parent_folder_id:
        q_parts.append(f"'{parent_folder_id}' in parents")
    query = " and ".join(q_parts)

    with httpx.Client(timeout=30.0) as client:
        search_resp = client.get(
            GOOGLE_DRIVE_FILES_URL,
            headers=headers,
            params={"q": query, "fields": "files(id,name)", "pageSize": 1},
        )
    if search_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive folder search failed: HTTP {search_resp.status_code} body={(search_resp.text or '')[:2000]}"
        )
    files = (search_resp.json() or {}).get("files") or []
    if files:
        fid = str(files[0].get("id") or "").strip()
        if fid:
            return {"folder_id": fid, "folder_link": f"https://drive.google.com/drive/folders/{fid}"}

    metadata: dict[str, Any] = {"name": safe_name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]
    with httpx.Client(timeout=30.0) as client:
        create_resp = client.post(
            GOOGLE_DRIVE_FILES_URL,
            headers={**headers, "Content-Type": "application/json"},
            json=metadata,
        )
    if create_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive folder create failed: HTTP {create_resp.status_code} body={(create_resp.text or '')[:2000]}"
        )
    created = create_resp.json() if create_resp.content else {}
    folder_id = str(created.get("id") or "").strip()
    if not folder_id:
        raise GoogleDriveUploadError("Google Drive folder creation succeeded but folder id was missing.")
    return {"folder_id": folder_id, "folder_link": f"https://drive.google.com/drive/folders/{folder_id}"}


def upload_ppt_bytes_to_google_drive(
    file_bytes: bytes,
    filename: str,
    *,
    parent_folder_id: str | None = None,
    convert_to_google_slides: bool = True,
) -> dict[str, Any]:
    if not file_bytes:
        raise GoogleDriveUploadError("Cannot upload empty PPT bytes to Google Drive.")

    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    target_mime = (
        GOOGLE_SLIDES_MIME
        if convert_to_google_slides
        else "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    metadata: dict[str, Any] = {
        "name": filename,
        "mimeType": target_mime,
    }
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]
    upload_headers = {
        **headers,
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "X-Upload-Content-Length": str(len(file_bytes)),
    }
    with httpx.Client(timeout=120.0) as client:
        start_resp = client.post(
            GOOGLE_DRIVE_RESUMABLE_UPLOAD_URL,
            headers=upload_headers,
            json=metadata,
        )
    if start_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive resumable init failed: HTTP {start_resp.status_code} body={(start_resp.text or '')[:2000]}"
        )
    resumable_url = (start_resp.headers.get("Location") or "").strip()
    if not resumable_url:
        raise GoogleDriveUploadError("Google Drive resumable init succeeded but Location header was missing.")

    with httpx.Client(timeout=300.0) as client:
        upload_resp = client.put(
            resumable_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            },
            content=file_bytes,
        )
    if upload_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive upload failed: HTTP {upload_resp.status_code} body={(upload_resp.text or '')[:2000]}"
        )
    uploaded = upload_resp.json() if upload_resp.content else {}
    file_id = uploaded.get("id")
    if not isinstance(file_id, str) or not file_id:
        raise GoogleDriveUploadError("Google Drive upload succeeded but file id was missing.")
    _set_public_edit_permission(file_id, access_token)
    if convert_to_google_slides:
        edit_link = f"https://docs.google.com/presentation/d/{file_id}/edit"
    else:
        edit_link = f"https://drive.google.com/file/d/{file_id}/view"
    logger.info("Google Drive bytes upload success | file_id=%s filename=%s", file_id, filename)
    return {"file_id": file_id, "edit_link": edit_link}


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

    with path.open("rb") as fh:
        file_bytes = fh.read()
    folder_id = str(getattr(settings, "GOOGLE_DRIVE_FOLDER_ID", "") or os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip() or None
    return upload_ppt_bytes_to_google_drive(file_bytes, filename, parent_folder_id=folder_id)


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
    folder_id = str(getattr(settings, "GOOGLE_DRIVE_FOLDER_ID", "") or os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip()

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


def upload_pdf_bytes_to_google_drive(
    file_bytes: bytes,
    filename: str,
    *,
    parent_folder_id: str | None = None,
) -> dict[str, Any]:
    """Upload a PDF to Drive with resumable upload; anyone can view (link usable for Zoho)."""
    if not file_bytes:
        raise GoogleDriveUploadError("Cannot upload empty PDF bytes to Google Drive.")

    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    metadata: dict[str, Any] = {
        "name": _sanitize_drive_name(filename),
        "mimeType": "application/pdf",
    }
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]
    upload_headers = {
        **headers,
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": "application/pdf",
        "X-Upload-Content-Length": str(len(file_bytes)),
    }
    with httpx.Client(timeout=120.0) as client:
        start_resp = client.post(
            GOOGLE_DRIVE_RESUMABLE_UPLOAD_URL,
            headers=upload_headers,
            json=metadata,
        )
    if start_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive PDF resumable init failed: HTTP {start_resp.status_code} body={(start_resp.text or '')[:2000]}"
        )
    resumable_url = (start_resp.headers.get("Location") or "").strip()
    if not resumable_url:
        raise GoogleDriveUploadError("Google Drive resumable init succeeded but Location header was missing.")

    with httpx.Client(timeout=300.0) as client:
        upload_resp = client.put(
            resumable_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/pdf",
            },
            content=file_bytes,
        )
    if upload_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive PDF upload failed: HTTP {upload_resp.status_code} body={(upload_resp.text or '')[:2000]}"
        )
    uploaded = upload_resp.json() if upload_resp.content else {}
    file_id = uploaded.get("id")
    if not isinstance(file_id, str) or not file_id:
        raise GoogleDriveUploadError("Google Drive PDF upload succeeded but file id was missing.")
    _set_public_edit_permission(file_id, access_token)
    view_link = f"https://drive.google.com/file/d/{file_id}/view"
    logger.info("Google Drive PDF upload success | file_id=%s filename=%s", file_id, filename)
    return {"file_id": file_id, "edit_link": view_link}


def upload_docx_bytes_to_google_drive(
    file_bytes: bytes,
    filename: str,
    *,
    parent_folder_id: str | None = None,
) -> dict[str, Any]:
    """Upload a Word .docx to Drive (resumable); anyone can view."""
    if not file_bytes:
        raise GoogleDriveUploadError("Cannot upload empty DOCX bytes to Google Drive.")

    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    metadata: dict[str, Any] = {
        "name": _sanitize_drive_name(filename),
        "mimeType": DOCX_MIME,
    }
    if parent_folder_id:
        metadata["parents"] = [parent_folder_id]
    upload_headers = {
        **headers,
        "Content-Type": "application/json; charset=UTF-8",
        "X-Upload-Content-Type": DOCX_MIME,
        "X-Upload-Content-Length": str(len(file_bytes)),
    }
    with httpx.Client(timeout=120.0) as client:
        start_resp = client.post(
            GOOGLE_DRIVE_RESUMABLE_UPLOAD_URL,
            headers=upload_headers,
            json=metadata,
        )
    if start_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive DOCX resumable init failed: HTTP {start_resp.status_code} body={(start_resp.text or '')[:2000]}"
        )
    resumable_url = (start_resp.headers.get("Location") or "").strip()
    if not resumable_url:
        raise GoogleDriveUploadError("Google Drive resumable init succeeded but Location header was missing.")

    with httpx.Client(timeout=300.0) as client:
        upload_resp = client.put(
            resumable_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": DOCX_MIME,
            },
            content=file_bytes,
        )
    if upload_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive DOCX upload failed: HTTP {upload_resp.status_code} body={(upload_resp.text or '')[:2000]}"
        )
    uploaded = upload_resp.json() if upload_resp.content else {}
    file_id = uploaded.get("id")
    if not isinstance(file_id, str) or not file_id:
        raise GoogleDriveUploadError("Google Drive DOCX upload succeeded but file id was missing.")
    _set_public_edit_permission(file_id, access_token)
    view_link = f"https://drive.google.com/file/d/{file_id}/view"
    logger.info("Google Drive DOCX upload success | file_id=%s filename=%s", file_id, filename)
    return {"file_id": file_id, "edit_link": view_link}


# Fixed hierarchy under parent: ai_automation / course_outline / {course}_{zoho} / files
_DRIVE_AI_AUTOMATION = "ai_automation"
_DRIVE_COURSE_OUTLINE = "course_outline"
_DRIVE_PRE_POST_ASSISTANCE = "pre_and_post_assistance"
# Google Drive API: use parent id "root" for the authenticated user's My Drive root (no env folder required).
_DRIVE_MY_DRIVE_ROOT_ID = "root"


def _resolve_course_outline_parent_folder_id() -> str:
    """Explicit folder from env, or My Drive root so folders are created under root like exist_ok."""
    explicit = (
        str(getattr(settings, "GOOGLE_DRIVE_COURSE_OUTLINES_PARENT_FOLDER_ID", "") or "").strip()
        or str(getattr(settings, "GOOGLE_DRIVE_FOLDER_ID", "") or "").strip()
        or (os.getenv("GOOGLE_DRIVE_COURSE_OUTLINES_PARENT_FOLDER_ID") or "").strip()
        or (os.getenv("GOOGLE_DRIVE_FOLDER_ID") or "").strip()
    )
    if explicit:
        return explicit
    return _DRIVE_MY_DRIVE_ROOT_ID


def _find_existing_outline_course_folder_by_zoho(
    *,
    parent_outline_folder_id: str,
    safe_zoho: str,
) -> dict[str, str] | None:
    """
    Reuse an existing outline folder for the same Zoho record id.
    Matches folders named like: "{any_prefix}_{safe_zoho}" under course_outline parent.
    """
    access_token = _get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    suffix = f"_{safe_zoho}"
    suffix_q = suffix.replace("'", "\\'")
    q_parts = [
        "mimeType = 'application/vnd.google-apps.folder'",
        "trashed = false",
        f"'{parent_outline_folder_id}' in parents",
        f"name contains '{suffix_q}'",
    ]
    query = " and ".join(q_parts)

    with httpx.Client(timeout=30.0) as client:
        search_resp = client.get(
            GOOGLE_DRIVE_FILES_URL,
            headers=headers,
            params={
                "q": query,
                "fields": "files(id,name,createdTime)",
                "orderBy": "createdTime asc",
                "pageSize": 50,
            },
        )
    if search_resp.status_code >= 400:
        raise GoogleDriveUploadError(
            f"Google Drive outline folder search failed: HTTP {search_resp.status_code} body={(search_resp.text or '')[:2000]}"
        )

    files = (search_resp.json() or {}).get("files") or []
    if not isinstance(files, list) or not files:
        return None

    # Prefer exact suffix match to avoid accidental partial id matches.
    for f in files:
        name = str(f.get("name") or "").strip()
        fid = str(f.get("id") or "").strip()
        if fid and name.endswith(suffix):
            return {"folder_id": fid, "folder_name": name}

    # Fallback: first folder returned by createdTime.
    first = files[0]
    fid = str(first.get("id") or "").strip()
    name = str(first.get("name") or "").strip()
    if fid:
        return {"folder_id": fid, "folder_name": name}
    return None


def upload_course_outline_pdf_to_drive(
    pdf_path: str,
    *,
    course_name: str,
    zoho_record_id: str,
    version_number: int,
) -> dict[str, Any]:
    """
    Under GOOGLE_DRIVE_COURSE_OUTLINES_PARENT_FOLDER_ID or GOOGLE_DRIVE_FOLDER_ID (if set), or else
    under **My Drive root**, ensure:

    ``{parent}/ai_automation/course_outline/{course_name}_{zoho_record_id}/``

    Each folder is created only if missing (reuse by name under the same parent). Then upload
    ``{course_name}_v{n}.pdf``. Requires Google OAuth (client id, secret, refresh token).
    """
    parent = _resolve_course_outline_parent_folder_id()
    if parent == _DRIVE_MY_DRIVE_ROOT_ID:
        logger.info(
            "Google Drive course outline: no GOOGLE_DRIVE_* parent set; using My Drive root (parent id=root)"
        )
    path = Path(pdf_path)
    if not path.exists() or not path.is_file():
        raise GoogleDriveUploadError(f"PDF not found: {pdf_path}")

    safe_course = _sanitize_drive_name(course_name or "course")
    safe_zoho = _sanitize_drive_name(zoho_record_id or "zoho")
    ai_folder = ensure_drive_folder(_DRIVE_AI_AUTOMATION, parent_folder_id=parent)
    outline_folder = ensure_drive_folder(_DRIVE_COURSE_OUTLINE, parent_folder_id=ai_folder["folder_id"])
    existing_folder = _find_existing_outline_course_folder_by_zoho(
        parent_outline_folder_id=outline_folder["folder_id"],
        safe_zoho=safe_zoho,
    )

    if existing_folder is not None:
        folder_id = existing_folder["folder_id"]
        existing_name = existing_folder.get("folder_name", "")
        if existing_name.endswith(f"_{safe_zoho}") and len(existing_name) > len(safe_zoho) + 1:
            safe_course = existing_name[: -(len(safe_zoho) + 1)] or safe_course
    else:
        per_course_name = f"{safe_course}_{safe_zoho}"
        course_folder = ensure_drive_folder(per_course_name, parent_folder_id=outline_folder["folder_id"])
        folder_id = course_folder["folder_id"]

    filename = f"{safe_course}_v{int(version_number)}.pdf"
    with path.open("rb") as fh:
        data = fh.read()
    return upload_pdf_bytes_to_google_drive(data, filename, parent_folder_id=folder_id)


def upload_assessment_docx_to_drive(
    docx_bytes: bytes,
    *,
    course_name: str,
    zoho_record_id: str,
    phase: str,
) -> dict[str, Any]:
    """
    ``{parent}/ai_automation/pre_and_post_assistance/{course_name}_{zoho_record_id}/{filename}.docx``

    Same parent resolution as course outlines (env folder or My Drive root). ``phase`` is ``pre`` or ``post``.
    """
    parent = _resolve_course_outline_parent_folder_id()
    if parent == _DRIVE_MY_DRIVE_ROOT_ID:
        logger.info(
            "Google Drive assessment DOCX: no GOOGLE_DRIVE_* parent set; using My Drive root (parent id=root)"
        )

    safe_course = _sanitize_drive_name(course_name or "course")
    safe_zoho = _sanitize_drive_name(zoho_record_id or "zoho")
    per_course_name = f"{safe_course}_{safe_zoho}"
    phase_clean = (phase or "pre").strip().lower()
    if phase_clean not in ("pre", "post"):
        phase_clean = "pre"

    ai_folder = ensure_drive_folder(_DRIVE_AI_AUTOMATION, parent_folder_id=parent)
    assistance_folder = ensure_drive_folder(_DRIVE_PRE_POST_ASSISTANCE, parent_folder_id=ai_folder["folder_id"])
    course_folder = ensure_drive_folder(per_course_name, parent_folder_id=assistance_folder["folder_id"])
    folder_id = course_folder["folder_id"]

    filename = f"{safe_course}_{phase_clean}_assessment.docx"
    return upload_docx_bytes_to_google_drive(docx_bytes, filename, parent_folder_id=folder_id)

