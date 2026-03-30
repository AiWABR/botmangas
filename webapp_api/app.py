from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import BASE_DIR, DATA_DIR, HOME_SECTION_LIMIT, PREFERRED_CHAPTER_LANG
from services.catalog_client import (
    flatten_chapters,
    get_chapter_reader_payload,
    get_home_payload,
    get_recent_chapters,
    get_title_bundle,
    get_title_search,
    search_titles,
)
from services.media_pipeline import resolve_telegraph_asset_path
from services.metrics import get_last_read_entry, mark_chapter_read

MINIAPP_DIR = BASE_DIR / "miniapp"
PROGRESS_PATH = Path(DATA_DIR) / "miniapp_progress.json"

app = FastAPI(
    title="Mangas Baltigo API",
    description="API do miniapp de mangas com catalogo, capitulos e leitor",
    version="1.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


class ProgressPayload(BaseModel):
    user_id: str
    title_id: str
    title_name: str = ""
    chapter_id: str
    chapter_number: str = ""
    chapter_url: str = ""
    page_index: int = 0
    total_pages: int = 0


def _load_progress() -> dict[str, dict[str, Any]]:
    if not PROGRESS_PATH.exists():
        return {}
    try:
        return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_progress(data: dict[str, dict[str, Any]]) -> None:
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _progress_key(user_id: str, title_id: str) -> str:
    return f"{user_id}:{title_id}"


def _public_last_read(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not entry:
        return None
    return {
        "title_id": entry.get("title_id") or "",
        "title_name": entry.get("title_name") or "",
        "chapter_id": entry.get("chapter_id") or "",
        "chapter_number": entry.get("chapter_number") or "",
        "updated_at": entry.get("updated_at") or "",
    }


def _public_chapter(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if not item:
        return None
    return {
        "chapter_id": item.get("chapter_id") or "",
        "chapter_number": item.get("chapter_number") or "",
        "chapter_language": item.get("chapter_language") or "",
        "chapter_volume": item.get("chapter_volume") or "",
        "group_name": item.get("group_name") or "",
        "updated_at": item.get("updated_at") or "",
    }


def _public_title_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "title_id": item.get("title_id") or "",
        "chapter_id": item.get("chapter_id") or "",
        "title": item.get("title") or "",
        "cover_url": item.get("cover_url") or "",
        "background_url": item.get("background_url") or "",
        "status": item.get("status") or "",
        "rating": item.get("rating") or "",
        "updated_at": item.get("updated_at") or "",
        "latest_chapter": item.get("latest_chapter") or "",
        "adult": bool(item.get("adult")),
    }


def _public_title_bundle(bundle: dict[str, Any], lang: str) -> dict[str, Any]:
    return {
        "title_id": bundle.get("title_id") or "",
        "title": bundle.get("title") or "",
        "preferred_title": bundle.get("preferred_title") or "",
        "alt_titles": bundle.get("alt_titles") or [],
        "description": bundle.get("description") or bundle.get("anilist_description") or "",
        "cover_url": bundle.get("cover_url") or "",
        "background_url": bundle.get("background_url") or "",
        "banner_url": bundle.get("banner_url") or bundle.get("background_url") or "",
        "cover_color": bundle.get("cover_color") or "",
        "status": bundle.get("status") or bundle.get("anilist_status") or "",
        "rating": bundle.get("rating") or "",
        "genres": bundle.get("genres") or [],
        "authors": bundle.get("authors") or [],
        "published": bundle.get("published") or "",
        "languages": bundle.get("languages") or [],
        "total_chapters": bundle.get("total_chapters") or 0,
        "anilist_url": bundle.get("anilist_url") or "",
        "anilist_score": bundle.get("anilist_score") or 0,
        "anilist_format": bundle.get("anilist_format") or "",
        "anilist_status": bundle.get("anilist_status") or "",
        "anilist_chapters": bundle.get("anilist_chapters") or 0,
        "anilist_volumes": bundle.get("anilist_volumes") or 0,
        "chapters": [_public_chapter(item) for item in flatten_chapters({"chapters": bundle.get("chapters") or []}, lang)],
        "latest_chapter": _public_chapter(bundle.get("latest_chapter")),
    }


def _public_reader_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "title_id": payload.get("title_id") or "",
        "title": payload.get("title") or "",
        "chapter_id": payload.get("chapter_id") or "",
        "chapter_number": payload.get("chapter_number") or "",
        "chapter_language": payload.get("chapter_language") or "",
        "chapter_volume": payload.get("chapter_volume") or "",
        "cover_url": payload.get("cover_url") or "",
        "image_count": payload.get("image_count") or 0,
        "images": payload.get("images") or [],
        "total_chapters": payload.get("total_chapters") or 0,
        "previous_chapter": _public_chapter(payload.get("previous_chapter")),
        "next_chapter": _public_chapter(payload.get("next_chapter")),
    }


@app.get("/api/ping")
async def ping():
    return {"ok": True}


@app.get("/api/home")
async def api_home(limit: int = Query(HOME_SECTION_LIMIT, ge=4, le=20)):
    payload, recent_chapters = await asyncio.gather(
        get_home_payload(limit=limit),
        get_recent_chapters(limit=min(limit, 10)),
    )
    return {
        "featured": [_public_title_item(item) for item in (payload.get("featured") or [])],
        "popular": [_public_title_item(item) for item in (payload.get("popular") or [])],
        "recent_titles": [_public_title_item(item) for item in (payload.get("recent_titles") or [])],
        "latest_titles": [_public_title_item(item) for item in (payload.get("latest_titles") or [])],
        "recent_chapters": [_public_title_item(item) for item in recent_chapters],
    }


@app.get("/api/search")
async def api_search(q: str = Query("", min_length=1), limit: int = Query(10, ge=1, le=20)):
    return {"query": q, "results": [_public_title_item(item) for item in await search_titles(q, limit=limit)]}


@app.get("/api/sections/{section_name}")
async def api_section(section_name: str, limit: int = Query(12, ge=1, le=20)):
    if section_name == "recent_chapters":
        return {"items": [_public_title_item(item) for item in await get_recent_chapters(limit=limit)]}

    section_map = {
        "featured": "getFeatured",
        "popular": "getPopular",
        "recent_titles": "getRecentRead",
        "latest_titles": "getLatestTable",
    }
    search_type = section_map.get(section_name)
    if not search_type:
        raise HTTPException(status_code=404, detail="Secao nao encontrada.")
    extra = {"search_time": "week"} if search_type == "getRecentRead" else {}
    items = await get_title_search(search_type, limit=limit, **extra)
    return {"items": [_public_title_item(item) for item in items]}


@app.get("/api/title/{title_id}")
async def api_title(title_id: str, user_id: str = Query(""), lang: str = Query(PREFERRED_CHAPTER_LANG)):
    try:
        bundle = await get_title_bundle(title_id, lang)
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error

    public_bundle = _public_title_bundle(bundle, lang)
    if user_id:
        public_bundle["last_read"] = _public_last_read(get_last_read_entry(user_id, bundle["title_id"]))
    return public_bundle


@app.get("/api/title/{title_id}/chapters")
async def api_title_chapters(title_id: str, lang: str = Query(PREFERRED_CHAPTER_LANG)):
    try:
        bundle = await get_title_bundle(title_id, lang)
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error

    return {
        "title_id": bundle["title_id"],
        "title": bundle.get("title") or "",
        "chapters": [_public_chapter(item) for item in flatten_chapters({"chapters": bundle.get("chapters") or []}, lang)],
    }


@app.get("/api/chapter/{chapter_id}")
async def api_chapter(chapter_id: str):
    try:
        return _public_reader_payload(await get_chapter_reader_payload(chapter_id, PREFERRED_CHAPTER_LANG))
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.get("/api/progress")
async def api_get_progress(user_id: str = Query(...), title_id: str = Query(...)):
    data = _load_progress()
    return _public_last_read(data.get(_progress_key(user_id, title_id))) or {}


@app.post("/api/progress")
async def api_save_progress(payload: ProgressPayload):
    data = _load_progress()
    key = _progress_key(payload.user_id, payload.title_id)
    stored = payload.model_dump()
    stored["chapter_url"] = ""
    data[key] = stored
    _save_progress(data)

    mark_chapter_read(
        user_id=payload.user_id,
        title_id=payload.title_id,
        chapter_id=payload.chapter_id,
        chapter_number=payload.chapter_number,
        title_name=payload.title_name,
        chapter_url="",
    )
    return {"ok": True}


@app.get("/api/media/telegraph/{asset_key}/{asset_name}")
async def api_telegraph_media(asset_key: str, asset_name: str):
    try:
        asset_path = resolve_telegraph_asset_path(asset_key, asset_name)
    except FileNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error

    return FileResponse(
        asset_path,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@app.get("/")
async def root():
    return FileResponse(MINIAPP_DIR / "index.html")


if MINIAPP_DIR.exists():
    app.mount("/miniapp", StaticFiles(directory=MINIAPP_DIR, html=True), name="miniapp")
