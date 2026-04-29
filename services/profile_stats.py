from __future__ import annotations

import json
from typing import Any

from config import DATA_DIR
from services.metrics import get_reading_summary
from services.profile_store import count_user_favorites

PROGRESS_PATH = DATA_DIR / "miniapp_progress.json"


def _load_progress() -> dict[str, dict[str, Any]]:
    if not PROGRESS_PATH.exists():
        return {}
    try:
        data = json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _as_int(value: Any) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _user_progress_entries(user_id: int | str) -> list[dict[str, Any]]:
    user_key = str(user_id).strip()
    entries: list[dict[str, Any]] = []

    for key, item in _load_progress().items():
        if not isinstance(item, dict):
            continue
        item_user = str(item.get("user_id") or "").strip()
        if item_user:
            if item_user != user_key:
                continue
        elif not str(key).startswith(f"{user_key}:"):
            continue

        title_id = str(item.get("title_id") or "").strip()
        chapter_id = str(item.get("chapter_id") or "").strip()
        if title_id and chapter_id:
            entries.append(item)

    return entries


def get_webapp_profile_stats(user_id: int | str, recent_limit: int = 5) -> dict[str, Any]:
    reading = get_reading_summary(user_id, recent_limit=recent_limit)
    progress_entries = _user_progress_entries(user_id)

    progress_title_ids = {
        str(item.get("title_id") or "").strip()
        for item in progress_entries
        if str(item.get("title_id") or "").strip()
    }
    progress_chapter_ids = {
        str(item.get("chapter_id") or "").strip()
        for item in progress_entries
        if str(item.get("chapter_id") or "").strip()
    }

    opened_titles_count = max(len(progress_title_ids), int(reading.get("title_count") or 0))
    chapters_read_count = max(len(progress_chapter_ids), int(reading.get("chapter_count") or 0))
    pages_read_count = sum(_as_int(item.get("page_index")) for item in progress_entries)

    return {
        "favorites_count": count_user_favorites(user_id),
        "chapters_read_count": chapters_read_count,
        "opened_titles_count": opened_titles_count,
        "pages_read_count": pages_read_count,
        "recent_reads": reading.get("recent_reads") or [],
    }
