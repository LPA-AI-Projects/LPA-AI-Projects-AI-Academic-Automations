"""
Runtime file storage layout (created at startup; contents gitignored).

Override root with env: COURSE_AI_STORAGE_ROOT=/path/to/data
"""
from __future__ import annotations

import os


def storage_root() -> str:
    return os.path.normpath(
        os.environ.get("COURSE_AI_STORAGE_ROOT")
        or os.path.join(os.getcwd(), "storage")
    )


def pdfs_dir() -> str:
    return os.path.join(storage_root(), "pdfs")


def ppts_dir() -> str:
    return os.path.join(storage_root(), "ppts")


def uploads_dir() -> str:
    return os.path.join(storage_root(), "uploads")


def slides_upload_dir() -> str:
    return os.path.join(uploads_dir(), "slides")


def ensure_storage_dirs() -> None:
    os.makedirs(pdfs_dir(), exist_ok=True)
    os.makedirs(ppts_dir(), exist_ok=True)
    os.makedirs(slides_upload_dir(), exist_ok=True)
