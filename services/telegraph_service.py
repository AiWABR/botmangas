from __future__ import annotations

import asyncio
import json
from pathlib import Path
from threading import Lock

from telegraph import Telegraph

from config import (
    BOT_BRAND,
    DATA_DIR,
    DISTRIBUTION_TAG,
    PROMO_BANNER_URL,
    TELEGRAPH_AUTHOR,
    WEBAPP_BASE_URL,
)
from services.media_pipeline import get_telegraph_asset_files, resolve_telegraph_asset_path

TELEGRAPH_CACHE_PATH = Path(DATA_DIR) / "telegraph_pages.json"

_telegraph: Telegraph | None = None
_telegraph_lock = Lock()
_telegraph_cache: dict[str, str] | None = None
_telegraph_cache_lock = Lock()
_telegraph_inflight: dict[str, asyncio.Task] = {}
_upload_semaphore = asyncio.Semaphore(2)
TELEGRAPH_PAGE_CACHE_VERSION = "v5"


def _load_cache() -> dict[str, str]:
    global _telegraph_cache
    if _telegraph_cache is not None:
        return _telegraph_cache

    with _telegraph_cache_lock:
        if _telegraph_cache is not None:
            return _telegraph_cache
        if TELEGRAPH_CACHE_PATH.exists():
            try:
                _telegraph_cache = json.loads(TELEGRAPH_CACHE_PATH.read_text(encoding="utf-8"))
            except Exception:
                _telegraph_cache = {}
        else:
            _telegraph_cache = {}
        return _telegraph_cache


def _save_cache() -> None:
    cache = _load_cache()
    TELEGRAPH_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TELEGRAPH_CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _page_cache_key(chapter_id: str) -> str:
    return f"{TELEGRAPH_PAGE_CACHE_VERSION}:{str(chapter_id or '').strip()}"


def get_cached_chapter_page_url(chapter_id: str) -> str:
    return str(_load_cache().get(_page_cache_key(chapter_id)) or "").strip()


def _get_client() -> Telegraph:
    global _telegraph
    if _telegraph is not None:
        return _telegraph

    with _telegraph_lock:
        if _telegraph is None:
            client = Telegraph()
            client.create_account(short_name=BOT_BRAND[:32] or "MangasBaltigo")
            _telegraph = client
    return _telegraph


def _normalize_title(title: str) -> str:
    raw = (title or "").strip() or "Leitura"
    if DISTRIBUTION_TAG.lower() not in raw.lower():
        raw = f"{raw} | {DISTRIBUTION_TAG}"
    return raw[:256]


def _build_nodes(title: str, images: list[str], footer_text: str | None = None) -> list[dict]:
    nodes: list[dict] = [{"tag": "h3", "children": [title]}]
    if footer_text:
        nodes.append({"tag": "p", "children": [footer_text]})
    for image in images:
        nodes.append({"tag": "img", "attrs": {"src": image}})
    return nodes


def _public_media_base() -> str:
    base = (WEBAPP_BASE_URL or "").strip().rstrip("/")
    lowered = base.lower()
    if not lowered.startswith("https://"):
        return ""
    if "127.0.0.1" in lowered or "localhost" in lowered:
        return ""
    return base


def _fallback_remote_urls(images: list[str]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for raw in [PROMO_BANNER_URL, *(images or [])]:
        url = str(raw or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


async def _upload_asset_files(asset_key: str, file_names: list[str]) -> list[str]:
    if not file_names:
        return []

    paths = [str(resolve_telegraph_asset_path(asset_key, file_name)) for file_name in file_names]

    async with _upload_semaphore:
        def _runner() -> list[str]:
            client = _get_client()
            uploaded = client.upload_file(paths)
            urls = []
            for item in uploaded:
                src = str(item or "").strip()
                if not src:
                    continue
                if src.startswith("/"):
                    src = "https://telegra.ph" + src
                urls.append(src)
            return urls

        return await asyncio.to_thread(_runner)


async def _build_cached_asset_urls(chapter_id: str, images: list[str]) -> list[str]:
    base = _public_media_base()
    if not base:
        return []

    asset_key, file_names = await get_telegraph_asset_files(chapter_id, images)
    return [f"{base}/api/media/telegraph/{asset_key}/{file_name}" for file_name in file_names]


async def get_or_create_chapter_page(
    chapter_id: str,
    title: str,
    images: list[str],
    footer_text: str | None = None,
) -> str:
    cache = _load_cache()
    cache_key = _page_cache_key(chapter_id)
    cached = cache.get(cache_key)
    if cached:
        return cached

    task = _telegraph_inflight.get(cache_key)
    if task:
        return await task

    async def _runner() -> str:
        if not images:
            raise RuntimeError("Nenhuma imagem encontrada para criar a pagina do Telegraph.")

        image_urls: list[str] = []
        asset_key = ""
        asset_names: list[str] = []

        try:
            image_urls = await _build_cached_asset_urls(chapter_id, images)
        except Exception:
            image_urls = []

        if not image_urls:
            try:
                asset_key, asset_names = await get_telegraph_asset_files(chapter_id, images)
                image_urls = await _upload_asset_files(asset_key, asset_names)
            except Exception:
                image_urls = []

        if not image_urls:
            image_urls = _fallback_remote_urls(images)

        if not image_urls:
            raise RuntimeError("Nao consegui preparar as imagens do Telegraph.")

        page_title = _normalize_title(title)
        footer = footer_text or f"Leitura via {TELEGRAPH_AUTHOR} | {DISTRIBUTION_TAG}"
        nodes = _build_nodes(title=page_title, images=image_urls, footer_text=footer)

        def _create_page() -> str:
            client = _get_client()
            response = client.create_page(
                title=page_title,
                content=nodes,
                author_name=(f"{TELEGRAPH_AUTHOR} {DISTRIBUTION_TAG}").strip()[:128],
            )
            return "https://telegra.ph/" + response["path"]

        url = await asyncio.to_thread(_create_page)
        cache[cache_key] = url
        await asyncio.to_thread(_save_cache)
        return url

    task = asyncio.create_task(_runner())
    _telegraph_inflight[cache_key] = task
    try:
        return await task
    finally:
        _telegraph_inflight.pop(cache_key, None)
