from __future__ import annotations

import asyncio
import hashlib
import html
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from config import (
    BASE_DIR,
    BOT_BRAND,
    BOT_TOKEN,
    CAKTO_NOTIFY_USERS,
    CAKTO_WEBHOOK_SECRET,
    DATA_DIR,
    HOME_SECTION_LIMIT,
    PREFERRED_CHAPTER_LANG,
)
from services.catalog_client import (
    flatten_chapters,
    get_cached_title_bundle,
    get_cached_title_summary,
    get_chapter_reader_payload,
    get_home_payload,
    get_recent_chapters,
    get_title_bundle,
    get_title_chapters_snapshot,
    get_title_search,
    search_titles,
)
from services.cakto_gateway import extract_webhook_secret_values, process_cakto_webhook
from services.media_pipeline import resolve_telegraph_asset_path
from services.metrics import get_last_read_entry, get_recently_read, mark_chapter_read
from services.offline_access import init_offline_access_db
from services.profile_store import (
    list_user_favorites,
    merge_user_favorites,
    remove_user_favorite,
    set_user_favorite,
)

MINIAPP_DIR = BASE_DIR / "miniapp"
PROGRESS_PATH = Path(DATA_DIR) / "miniapp_progress.json"

app = FastAPI(
    title="Mangas Baltigo API",
    description="API otimizada do miniapp de mangás",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

init_offline_access_db()


class ProgressPayload(BaseModel):
    user_id: str = Field(min_length=1)
    title_id: str = Field(min_length=1)
    title_name: str = ""
    chapter_id: str = Field(min_length=1)
    chapter_number: str = ""
    chapter_url: str = ""
    page_index: int = 0
    total_pages: int = 0


class FavoritePayload(BaseModel):
    user_id: str = Field(min_length=1)
    title_id: str = Field(min_length=1)
    title: str = ""
    display_title: str = ""
    cover_url: str = ""
    background_url: str = ""
    latest_chapter: Any = ""
    latest_chapter_id: Any = ""
    chapter_id: Any = ""
    chapter_number: Any = ""
    status: Any = ""
    anilist_score: Any = ""
    rating: Any = ""
    added_at: int | float | None = None
    updated_at: int | float | None = None
    favorite: bool = True


class FavoritesSyncPayload(BaseModel):
    user_id: str = Field(min_length=1)
    favorites: list[dict[str, Any]] = Field(default_factory=list)


_CACHE: dict[str, dict[str, Any]] = {}
_CACHE_LOCK = asyncio.Lock()
_RECENT_TTL = 20
_HOME_TTL = 25
_TITLE_TTL = 90
_CHAPTER_TTL = 90
_SECTIONS_TTL = 25
_SEARCH_TTL = 20
_TITLE_OPEN_TIMEOUT = 22.0


def _now() -> float:
    return time.time()


def _cache_key(namespace: str, **kwargs: Any) -> str:
    raw = json.dumps({"ns": namespace, **kwargs}, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


async def _cache_get(namespace: str, ttl: int, **kwargs: Any) -> Any | None:
    key = _cache_key(namespace, **kwargs)
    async with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        if entry["expires_at"] < _now():
            _CACHE.pop(key, None)
            return None
        return entry["value"]


async def _cache_set(namespace: str, value: Any, ttl: int, **kwargs: Any) -> Any:
    key = _cache_key(namespace, **kwargs)
    async with _CACHE_LOCK:
        _CACHE[key] = {
            "value": value,
            "expires_at": _now() + ttl,
        }
    return value


async def _cached(namespace: str, ttl: int, producer, **kwargs: Any) -> Any:
    cached = await _cache_get(namespace, ttl, **kwargs)
    if cached is not None:
        return cached
    value = await producer()
    return await _cache_set(namespace, value, ttl, **kwargs)


async def _stale_while_revalidate(namespace: str, ttl: int, stale_ttl: int, producer, **kwargs: Any) -> Any:
    key = _cache_key(namespace, **kwargs)
    async with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry and entry["soft_expires_at"] >= _now():
            return entry["value"]
        if entry and entry["hard_expires_at"] >= _now():
            if not entry.get("refreshing"):
                entry["refreshing"] = True
                asyncio.create_task(_refresh_cache_entry(key, producer, ttl, stale_ttl))
            return entry["value"]

    value = await producer()
    async with _CACHE_LOCK:
        _CACHE[key] = {
            "value": value,
            "soft_expires_at": _now() + ttl,
            "hard_expires_at": _now() + stale_ttl,
            "refreshing": False,
        }
    return value


async def _refresh_cache_entry(key: str, producer, ttl: int, stale_ttl: int) -> None:
    try:
        value = await producer()
        async with _CACHE_LOCK:
            _CACHE[key] = {
                "value": value,
                "soft_expires_at": _now() + ttl,
                "hard_expires_at": _now() + stale_ttl,
                "refreshing": False,
            }
    except Exception:
        async with _CACHE_LOCK:
            if key in _CACHE:
                _CACHE[key]["refreshing"] = False


async def _invalidate_prefix(namespace: str) -> None:
    async with _CACHE_LOCK:
        for key in list(_CACHE.keys()):
            _CACHE.pop(key, None)


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
        "page_index": int(entry.get("page_index") or 0),
        "total_pages": int(entry.get("total_pages") or 0),
    }


def _public_updated_at_ms(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return int(time.time() * 1000)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return int(datetime.strptime(text[:19], fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return int(time.time() * 1000)


def _public_history_item(user_id: str, item: dict[str, Any], progress_data: dict[str, dict[str, Any]]) -> dict[str, Any]:
    title_id = item.get("title_id") or ""
    progress = progress_data.get(_progress_key(user_id, title_id)) or {}
    page_index = int(progress.get("page_index") or 1)
    total_pages = int(progress.get("total_pages") or 0)

    return {
        "title_id": title_id,
        "title_name": item.get("title_name") or progress.get("title_name") or "",
        "chapter_id": item.get("chapter_id") or progress.get("chapter_id") or "",
        "chapter_number": item.get("chapter_number") or progress.get("chapter_number") or "",
        "chapter_url": item.get("chapter_url") or progress.get("chapter_url") or "",
        "page_index": page_index,
        "total_pages": total_pages,
        "cover_url": progress.get("cover_url") or "",
        "updated_at": _public_updated_at_ms(progress.get("updated_at") or item.get("updated_at")),
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


def _has_real_chapter(item: dict[str, Any]) -> bool:
    return bool((item.get("chapter_id") or "").strip())


def _public_title_item(item: dict[str, Any]) -> dict[str, Any]:
    latest_value = item.get("latest_chapter")
    if isinstance(latest_value, dict):
        latest_value = latest_value.get("chapter_number") or latest_value.get("chapter_id") or ""

    return {
        "title_id": item.get("title_id") or "",
        "chapter_id": item.get("chapter_id") or "",
        "title": item.get("display_title") or item.get("title") or "",
        "cover_url": item.get("cover_url") or "",
        "background_url": item.get("background_url") or item.get("cover_url") or "",
        "status": item.get("status") or "",
        "rating": item.get("rating") or "",
        "updated_at": item.get("updated_at") or "",
        "latest_chapter": latest_value or "",
        "chapter_number": item.get("chapter_number") or latest_value or "",
        "adult": bool(item.get("adult")),
    }


def _sorted_filtered_chapters(bundle: dict[str, Any], lang: str) -> list[dict[str, Any]]:
    chapters = flatten_chapters({"chapters": bundle.get("chapters") or []}, lang)
    clean = [c for c in chapters if _has_real_chapter(c)]

    def chapter_sort(item: dict[str, Any]) -> tuple[float, str]:
        raw = str(item.get("chapter_number") or "").strip()
        try:
            return (float(raw), item.get("updated_at") or "")
        except Exception:
            return (-1.0, item.get("updated_at") or "")

    clean.sort(key=chapter_sort, reverse=True)
    return clean


def _public_title_bundle(bundle: dict[str, Any], lang: str) -> dict[str, Any]:
    chapters = _sorted_filtered_chapters(bundle, lang)
    latest = next((item for item in chapters if item.get("chapter_id")), None)
    chapters_partial = bool(bundle.get("chapters_partial") or bundle.get("partial"))
    try:
        total_chapters = len(chapters) or int(bundle.get("total_chapters") or bundle.get("anilist_chapters") or 0)
    except (TypeError, ValueError):
        total_chapters = len(chapters)

    return {
        "title_id": bundle.get("title_id") or "",
        "title": bundle.get("display_title") or bundle.get("title") or "",
        "preferred_title": bundle.get("preferred_title") or "",
        "alt_titles": bundle.get("alt_titles") or [],
        "description": bundle.get("description") or bundle.get("anilist_description") or "",
        "cover_url": bundle.get("cover_url") or "",
        "background_url": bundle.get("background_url") or bundle.get("cover_url") or "",
        "banner_url": bundle.get("banner_url") or bundle.get("background_url") or bundle.get("cover_url") or "",
        "cover_color": bundle.get("cover_color") or "",
        "status": bundle.get("status") or bundle.get("anilist_status") or "",
        "rating": bundle.get("rating") or "",
        "genres": bundle.get("genres") or [],
        "authors": bundle.get("authors") or [],
        "published": bundle.get("published") or "",
        "languages": bundle.get("languages") or [],
        "total_chapters": total_chapters,
        "chapters_partial": chapters_partial,
        "metadata_partial": bool(bundle.get("metadata_partial")),
        "chapters_error": bundle.get("chapters_error") or bundle.get("error") or "",
        "anilist_url": bundle.get("anilist_url") or "",
        "anilist_score": bundle.get("anilist_score") or 0,
        "anilist_format": bundle.get("anilist_format") or "",
        "anilist_status": bundle.get("anilist_status") or "",
        "anilist_chapters": bundle.get("anilist_chapters") or 0,
        "anilist_volumes": bundle.get("anilist_volumes") or 0,
        "adult": bool(bundle.get("adult")),
        "chapters": [_public_chapter(item) for item in chapters],
        "latest_chapter": _public_chapter(latest or bundle.get("latest_chapter")),
    }


def _partial_title_payload(title_id: str, error: str = "") -> dict[str, Any]:
    summary = get_cached_title_summary(title_id) or {}
    latest = summary.get("latest_chapter")
    latest_chapter = None
    if isinstance(latest, dict):
        latest_chapter = latest
    elif summary.get("chapter_id"):
        latest_chapter = {
            "chapter_id": summary.get("chapter_id") or "",
            "chapter_number": str(latest or "").strip(),
            "chapter_language": summary.get("language") or PREFERRED_CHAPTER_LANG,
        }

    display_title = (
        summary.get("display_title")
        or summary.get("title")
        or "Manga"
    )
    cover_url = summary.get("cover_url") or ""
    try:
        total_chapters = int(
            summary.get("total_chapters")
            or summary.get("chapters_count")
            or summary.get("chapter_count")
            or summary.get("anilist_chapters")
            or 0
        )
    except (TypeError, ValueError):
        total_chapters = 0

    return _public_title_bundle(
        {
            "title_id": title_id,
            "title": display_title,
            "display_title": display_title,
            "cover_url": cover_url,
            "background_url": summary.get("background_url") or cover_url,
            "status": summary.get("status") or summary.get("anilist_status") or "carregando",
            "rating": summary.get("rating") or summary.get("anilist_score") or "",
            "genres": summary.get("genres") or summary.get("anilist_genres") or [],
            "chapters": [],
            "languages": [],
            "total_chapters": total_chapters,
            "latest_chapter": latest_chapter,
            "chapters_partial": True,
            "chapters_error": error,
        },
        PREFERRED_CHAPTER_LANG,
    )


def _public_reader_payload(payload: dict[str, Any]) -> dict[str, Any]:
    images = [img for img in (payload.get("images") or []) if str(img or "").strip()]
    return {
        "title_id": payload.get("title_id") or "",
        "title": payload.get("title") or "",
        "chapter_id": payload.get("chapter_id") or "",
        "chapter_number": payload.get("chapter_number") or "",
        "chapter_language": payload.get("chapter_language") or "",
        "chapter_volume": payload.get("chapter_volume") or "",
        "cover_url": payload.get("cover_url") or "",
        "image_count": len(images),
        "images": images,
        "total_chapters": payload.get("total_chapters") or 0,
        "previous_chapter": _public_chapter(payload.get("previous_chapter")),
        "next_chapter": _public_chapter(payload.get("next_chapter")),
    }


def _normalize_query(text: str) -> str:
    import re
    import unicodedata

    text = unicodedata.normalize("NFKD", (text or "").strip().lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _search_score(query: str, item: dict[str, Any]) -> tuple[int, int, int]:
    q = _normalize_query(query)
    title = _normalize_query(item.get("title") or item.get("preferred_title") or item.get("display_title") or "")
    tags = [_normalize_query(tag) for tag in (item.get("genres") or [])]

    if not q:
        return (0, 0, 0)
    if title == q:
        return (500, 0, -len(title))
    if title.startswith(q):
        return (400, 0, -len(title))
    if q in title:
        return (300, 0, -len(title))
    if any(q in tag for tag in tags):
        return (220, 0, -len(title))

    overlap = len(set(q.split()) & set(title.split()))
    return (100 + overlap * 10, 0, -len(title))


async def _search_with_suggestions(query: str, limit: int) -> dict[str, Any]:
    raw_results = await _cached(
        "search",
        _SEARCH_TTL,
        lambda: search_titles(query, limit=max(20, limit * 3)),
        query=query,
        limit=max(20, limit * 3),
    )

    candidates = []
    for item in raw_results:
        if not item.get("title_id"):
            continue
        candidates.append(item)

    ranked = sorted(candidates, key=lambda item: _search_score(query, item), reverse=True)
    ranked = ranked[:limit]

    if ranked:
        return {
            "query": query,
            "results": [_public_title_item(item) for item in ranked],
            "suggestions": [],
        }

    home = await _home_payload(limit=max(10, limit))
    pool = []
    for key in ("featured", "popular", "recent_titles", "latest_titles"):
        pool.extend(home.get(key) or [])

    seen: set[str] = set()
    dedup_pool = []
    for item in pool:
        title_id = item.get("title_id") or ""
        if not title_id or title_id in seen:
            continue
        seen.add(title_id)
        dedup_pool.append(item)

    suggestions = sorted(dedup_pool, key=lambda item: _search_score(query, item), reverse=True)[:6]
    return {
        "query": query,
        "results": [],
        "suggestions": [_public_title_item(item) for item in suggestions if item.get("title_id")],
    }


async def _home_payload(limit: int) -> dict[str, Any]:
    async def producer() -> dict[str, Any]:
        payload, recent_chapters = await asyncio.gather(
            get_home_payload(limit=limit),
            get_recent_chapters(limit=min(limit * 2, 24)),
        )

        featured = [_public_title_item(item) for item in (payload.get("featured") or []) if _has_real_chapter(item)]
        popular = [_public_title_item(item) for item in (payload.get("popular") or []) if _has_real_chapter(item)]
        recent_titles = [_public_title_item(item) for item in (payload.get("recent_titles") or []) if _has_real_chapter(item)]
        latest_titles = [_public_title_item(item) for item in (payload.get("latest_titles") or []) if _has_real_chapter(item)]

        public_recent_chapters = []
        seen_chapters: set[str] = set()
        for item in recent_chapters:
            chapter_id = item.get("chapter_id") or ""
            if not chapter_id or chapter_id in seen_chapters:
                continue
            seen_chapters.add(chapter_id)
            public_recent_chapters.append(_public_title_item(item))

        latest_titles.sort(
            key=lambda item: (item.get("updated_at") or "", item.get("latest_chapter") or ""),
            reverse=True,
        )
        public_recent_chapters.sort(
            key=lambda item: (
                item.get("updated_at") or "",
                item.get("chapter_number") or item.get("latest_chapter") or "",
            ),
            reverse=True,
        )

        return {
            "featured": featured[:limit],
            "popular": popular[:limit],
            "recent_titles": recent_titles[:limit],
            "latest_titles": latest_titles[:limit],
            "recent_chapters": public_recent_chapters[: max(limit, 12)],
        }

    return await _stale_while_revalidate("home", _HOME_TTL, _HOME_TTL * 3, producer, limit=limit)


async def _title_payload(title_id: str, lang: str, user_id: str = "") -> dict[str, Any]:
    cache_kwargs = {"title_id": title_id, "lang": lang, "user_id": user_id}
    cached = await _cache_get("title", _TITLE_TTL, **cache_kwargs)
    if cached is not None and not cached.get("chapters_partial") and not cached.get("metadata_partial"):
        return cached

    def attach_user_data(public_bundle: dict[str, Any]) -> dict[str, Any]:
        if user_id:
            public_bundle["last_read"] = _public_last_read(get_last_read_entry(user_id, public_bundle["title_id"]))
        return public_bundle

    def refresh_full_bundle() -> None:
        async def runner() -> None:
            try:
                await get_title_bundle(title_id, lang)
            except Exception as error:
                print("[WEBAPP][TITLE_REFRESH_FAIL]", title_id, repr(error))

        try:
            asyncio.create_task(runner())
        except RuntimeError:
            pass

    catalog_cached = get_cached_title_bundle(title_id, lang)
    if catalog_cached is not None and catalog_cached.get("chapters"):
        public_cached = attach_user_data(_public_title_bundle(catalog_cached, lang))
        if public_cached.get("metadata_partial"):
            refresh_full_bundle()
            return public_cached
        return await _cache_set("title", public_cached, _TITLE_TTL, **cache_kwargs)

    async def producer() -> dict[str, Any]:
        try:
            snapshot = await asyncio.wait_for(
                get_title_chapters_snapshot(title_id, lang),
                timeout=min(_TITLE_OPEN_TIMEOUT, 4.5),
            )
            if snapshot.get("chapters") or snapshot.get("total_chapters"):
                refresh_full_bundle()
                return attach_user_data(_public_title_bundle(snapshot, lang))
        except Exception as error:
            print("[WEBAPP][TITLE_SNAPSHOT_PARTIAL]", title_id, repr(error))

        try:
            bundle = await asyncio.wait_for(
                get_title_bundle(title_id, lang),
                timeout=_TITLE_OPEN_TIMEOUT,
            )
        except Exception as error:
            print("[WEBAPP][TITLE_PARTIAL]", title_id, repr(error))
            return _partial_title_payload(title_id, repr(error))

        return attach_user_data(_public_title_bundle(bundle, lang))

    value = await producer()
    if value.get("chapters_partial") or value.get("metadata_partial"):
        return value
    return await _cache_set("title", value, _TITLE_TTL, **cache_kwargs)


async def _chapter_payload(chapter_id: str, lang: str) -> dict[str, Any]:
    async def producer() -> dict[str, Any]:
        payload = await get_chapter_reader_payload(chapter_id, lang)
        return _public_reader_payload(payload)

    return await _cached("chapter", _CHAPTER_TTL, producer, chapter_id=chapter_id, lang=lang)


@app.get("/api/ping")
async def ping() -> dict[str, bool]:
    return {"ok": True}


def _cakto_secret_candidates(request: Request, payload: dict[str, Any]) -> list[str]:
    candidates: list[str] = []

    for key in ("secret", "token"):
        value = request.query_params.get(key)
        if value:
            candidates.append(value.strip())

    for header_name in (
        "x-cakto-secret",
        "x-webhook-secret",
        "x-secret",
        "x-cakto-token",
    ):
        value = request.headers.get(header_name)
        if value:
            candidates.append(value.strip())

    authorization = request.headers.get("authorization", "").strip()
    if authorization.lower().startswith("bearer "):
        candidates.append(authorization.split(" ", 1)[1].strip())
    elif authorization:
        candidates.append(authorization)

    candidates.extend(extract_webhook_secret_values(payload))
    return [item for item in candidates if item]


def _cakto_secret_is_valid(request: Request, payload: dict[str, Any]) -> bool:
    expected = (CAKTO_WEBHOOK_SECRET or "").strip()
    if not expected:
        return True
    return expected in _cakto_secret_candidates(request, payload)


async def _notify_cakto_user(result: dict[str, Any]) -> None:
    if not CAKTO_NOTIFY_USERS or not BOT_TOKEN:
        return

    access = result.get("access") or {}
    if access.get("duplicate_event"):
        return

    user_id = result.get("user_id")
    if not user_id:
        return

    action = result.get("action")
    brand = html.escape(BOT_BRAND or "Mangas Baltigo")

    if action == "granted":
        plan = html.escape(result.get("plan_label") or access.get("plan_label") or "plano")
        expires_at = access.get("expires_at") or "vitalício"
        text = (
            "✅ <b>Leitura offline liberada!</b>\n\n"
            f"» <b>Plano:</b> <i>{plan}</i>\n"
            f"» <b>Validade:</b> <i>{html.escape(str(expires_at))}</i>\n\n"
            f"Agora o envio de todos os capítulos em PDF está ativo no <b>{brand}</b>."
        )
    elif action == "revoked":
        text = (
            "🔒 <b>Leitura offline bloqueada</b>\n\n"
            "A Cakto avisou cancelamento, reembolso ou chargeback dessa assinatura."
        )
    else:
        return

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=3.0)) as client:
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": int(user_id),
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
    except Exception:
        pass


def _log_cakto_webhook_payload(payload: dict[str, Any], result: dict[str, Any] | None = None) -> None:
    path = Path(DATA_DIR) / "cakto_webhooks.jsonl"
    record = {
        "received_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "result": result or {},
        "payload": payload,
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


@app.post("/api/webhooks/cakto")
async def api_cakto_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception as error:
        raise HTTPException(status_code=400, detail="JSON inválido.") from error

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload do webhook precisa ser JSON object.")

    if not _cakto_secret_is_valid(request, payload):
        _log_cakto_webhook_payload(payload, {"action": "unauthorized"})
        raise HTTPException(status_code=401, detail="Webhook Cakto não autorizado.")

    result = process_cakto_webhook(payload)
    _log_cakto_webhook_payload(payload, result)
    if result.get("action") in {"granted", "revoked"}:
        asyncio.create_task(_notify_cakto_user(result))

    return result


@app.get("/api/home")
async def api_home(limit: int = Query(HOME_SECTION_LIMIT, ge=4, le=24)):
    return await _home_payload(limit=limit)


@app.get("/api/search")
async def api_search(q: str = Query("", min_length=1), limit: int = Query(12, ge=1, le=24)):
    return await _search_with_suggestions(q, limit)


@app.get("/api/sections/{section_name}")
async def api_section(section_name: str, limit: int = Query(12, ge=1, le=24)):
    async def producer() -> dict[str, Any]:
        if section_name == "recent_chapters":
            items = await get_recent_chapters(limit=max(limit, 12))
            clean = [_public_title_item(item) for item in items if _has_real_chapter(item)]
            clean.sort(
                key=lambda item: (
                    item.get("updated_at") or "",
                    item.get("chapter_number") or item.get("latest_chapter") or "",
                ),
                reverse=True,
            )
            return {"items": clean[:limit]}

        section_map = {
            "featured": "getFeatured",
            "popular": "getPopular",
            "recent_titles": "getRecentRead",
            "latest_titles": "getLatestTable",
        }
        search_type = section_map.get(section_name)
        if not search_type:
            raise HTTPException(status_code=404, detail="Seção não encontrada.")

        extra = {"search_time": "week"} if search_type == "getRecentRead" else {}
        items = await get_title_search(search_type, limit=max(limit, 16), **extra)
        clean = [_public_title_item(item) for item in items if _has_real_chapter(item)]
        if section_name in {"latest_titles", "recent_titles"}:
            clean.sort(
                key=lambda item: (item.get("updated_at") or "", item.get("latest_chapter") or ""),
                reverse=True,
            )
        return {"items": clean[:limit]}

    return await _stale_while_revalidate(
        "section",
        _SECTIONS_TTL,
        _SECTIONS_TTL * 3,
        producer,
        section_name=section_name,
        limit=limit,
    )


@app.get("/api/title/{title_id}")
async def api_title(title_id: str, user_id: str = Query(""), lang: str = Query(PREFERRED_CHAPTER_LANG)):
    try:
        return await _title_payload(title_id, lang, user_id)
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.get("/api/title/{title_id}/chapters")
async def api_title_chapters(title_id: str, lang: str = Query(PREFERRED_CHAPTER_LANG)):
    try:
        bundle = await _title_payload(title_id, lang)
        return {
            "title_id": bundle["title_id"],
            "title": bundle.get("title") or "",
            "chapters": bundle.get("chapters") or [],
        }
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.get("/api/chapter/{chapter_id}")
async def api_chapter(chapter_id: str, lang: str = Query(PREFERRED_CHAPTER_LANG)):
    try:
        return await _chapter_payload(chapter_id, lang)
    except Exception as error:
        raise HTTPException(status_code=404, detail=str(error)) from error


@app.get("/api/progress")
async def api_get_progress(user_id: str = Query(...), title_id: str = Query(...)):
    data = _load_progress()
    return _public_last_read(data.get(_progress_key(user_id, title_id))) or {}


@app.get("/api/history")
async def api_get_history(user_id: str = Query(...), limit: int = Query(80, ge=1, le=200)):
    progress_data = _load_progress()
    items = get_recently_read(user_id, limit=limit)
    return {
        "items": [
            _public_history_item(user_id, item, progress_data)
            for item in items
            if item.get("title_id") and item.get("chapter_id")
        ]
    }


@app.post("/api/progress")
async def api_save_progress(payload: ProgressPayload):
    data = _load_progress()
    key = _progress_key(payload.user_id, payload.title_id)
    stored = payload.model_dump()
    data[key] = stored
    _save_progress(data)

    mark_chapter_read(
        user_id=payload.user_id,
        title_id=payload.title_id,
        chapter_id=payload.chapter_id,
        chapter_number=payload.chapter_number,
        title_name=payload.title_name,
        chapter_url=payload.chapter_url,
    )

    await _invalidate_prefix("cache")
    return {"ok": True}


@app.get("/api/favorites")
async def api_get_favorites(user_id: str = Query(...)):
    return {"items": list_user_favorites(user_id, limit=200)}


@app.post("/api/favorites")
async def api_save_favorite(payload: FavoritePayload):
    if not payload.favorite:
        remove_user_favorite(payload.user_id, payload.title_id)
        return {"ok": True, "items": list_user_favorites(payload.user_id, limit=200)}

    favorite = payload.model_dump(exclude={"favorite", "user_id"})
    set_user_favorite(payload.user_id, favorite)
    return {"ok": True, "items": list_user_favorites(payload.user_id, limit=200)}


@app.post("/api/favorites/sync")
async def api_sync_favorites(payload: FavoritesSyncPayload):
    return {"ok": True, "items": merge_user_favorites(payload.user_id, payload.favorites)}


@app.post("/api/refresh")
async def api_refresh():
    await _invalidate_prefix("cache")
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


@app.middleware("http")
async def add_perf_headers(request: Request, call_next):
    start = time.perf_counter()
    no_cache_index = request.url.path in {"/", "/miniapp", "/miniapp/", "/miniapp/index.html"}
    if no_cache_index:
        request.scope["headers"] = [
            (key, value)
            for key, value in request.scope.get("headers", [])
            if key.lower() not in {b"if-none-match", b"if-modified-since"}
        ]

    response: Response = await call_next(request)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
    response.headers["X-Response-Time"] = f"{elapsed_ms}ms"
    if no_cache_index:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    else:
        response.headers["Cache-Control"] = response.headers.get("Cache-Control", "public, max-age=15")
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "path": str(request.url.path)},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Erro interno no miniapp.",
            "path": str(request.url.path),
            "error": str(exc),
        },
    )


if MINIAPP_DIR.exists():
    app.mount("/miniapp", StaticFiles(directory=MINIAPP_DIR, html=True), name="miniapp")
