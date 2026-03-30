from __future__ import annotations

import asyncio
import re
import time
import unicodedata
from typing import Any

import httpx

from config import ANILIST_API_URL, ANILIST_CACHE_TTL_SECONDS, HTTP_TIMEOUT

_CLIENT: httpx.AsyncClient | None = None
_CLIENT_LOCK = asyncio.Lock()
_CACHE: dict[str, dict[str, Any]] = {}
_INFLIGHT: dict[str, asyncio.Task] = {}

_QUERY = """
query SearchManga($search: String!) {
  Page(page: 1, perPage: 5) {
    media(search: $search, type: MANGA) {
      id
      siteUrl
      status
      format
      genres
      averageScore
      meanScore
      chapters
      volumes
      bannerImage
      countryOfOrigin
      description(asHtml: false)
      title {
        userPreferred
        romaji
        english
        native
      }
      synonyms
      coverImage {
        extraLarge
        large
        color
      }
    }
  }
}
""".strip()


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFKD", _clean(value).lower())
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = re.sub(r"[^a-z0-9\s-]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _cache_get(key: str) -> dict[str, Any] | None:
    item = _CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item["time"]) > ANILIST_CACHE_TTL_SECONDS:
        _CACHE.pop(key, None)
        return None
    return item["data"]


def _cache_set(key: str, data: dict[str, Any]) -> dict[str, Any]:
    _CACHE[key] = {"time": time.time(), "data": data}
    return data


async def _get_client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    async with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = httpx.AsyncClient(
                base_url=ANILIST_API_URL,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "MangasBaltigo/1.0",
                },
                follow_redirects=True,
                timeout=httpx.Timeout(float(HTTP_TIMEOUT), connect=10.0, read=float(HTTP_TIMEOUT)),
                http2=True,
            )
    return _CLIENT


def _candidate_titles(title: str, alt_titles: list[str] | None = None) -> list[str]:
    seen: set[str] = set()
    candidates: list[str] = []

    for raw in [title, *(alt_titles or [])]:
        clean = _clean(raw)
        normalized = _normalize(clean)
        if not clean or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(clean)
        if len(candidates) >= 4:
            break

    return candidates


def _media_titles(media: dict[str, Any]) -> list[str]:
    title_block = media.get("title") or {}
    values = [
        title_block.get("userPreferred"),
        title_block.get("romaji"),
        title_block.get("english"),
        title_block.get("native"),
        *(media.get("synonyms") or []),
    ]

    seen: set[str] = set()
    result: list[str] = []
    for raw in values:
        clean = _clean(raw)
        normalized = _normalize(clean)
        if not clean or not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(clean)
    return result


def _match_score(candidates: list[str], media: dict[str, Any]) -> float:
    source_titles = [_normalize(item) for item in candidates if _normalize(item)]
    target_titles = [_normalize(item) for item in _media_titles(media) if _normalize(item)]
    if not source_titles or not target_titles:
        return 0.0

    best = 0.0
    for source in source_titles:
        source_tokens = set(source.split())
        for target in target_titles:
            if source == target:
                return 1.0
            if source in target or target in source:
                best = max(best, 0.92)
            target_tokens = set(target.split())
            if source_tokens and target_tokens:
                overlap = len(source_tokens & target_tokens) / max(len(source_tokens), len(target_tokens))
                best = max(best, overlap)
    return best


async def _query_anilist(search: str) -> list[dict[str, Any]]:
    client = await _get_client()
    response = await client.post("", json={"query": _QUERY, "variables": {"search": search}})
    response.raise_for_status()
    payload = response.json()
    return (((payload.get("data") or {}).get("Page") or {}).get("media") or [])


async def enrich_title_metadata(title: str, alt_titles: list[str] | None = None) -> dict[str, Any]:
    candidates = _candidate_titles(title, alt_titles)
    if not candidates:
        return {}

    cache_key = f"anilist:{'|'.join(_normalize(item) for item in candidates)}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    task = _INFLIGHT.get(cache_key)
    if task:
        return await task

    async def _load() -> dict[str, Any]:
        best_media: dict[str, Any] | None = None
        best_score = 0.0

        for candidate in candidates:
            try:
                items = await _query_anilist(candidate)
            except Exception:
                continue

            for media in items:
                score = _match_score(candidates, media)
                if score > best_score:
                    best_score = score
                    best_media = media

            if best_score >= 0.92:
                break

        if not best_media or best_score < 0.45:
            return {}

        title_block = best_media.get("title") or {}
        cover = best_media.get("coverImage") or {}
        titles = _media_titles(best_media)

        return {
            "anilist_id": best_media.get("id"),
            "anilist_url": _clean(best_media.get("siteUrl")),
            "anilist_status": _clean(best_media.get("status")),
            "anilist_format": _clean(best_media.get("format")),
            "anilist_score": best_media.get("averageScore") or best_media.get("meanScore") or 0,
            "anilist_chapters": best_media.get("chapters") or 0,
            "anilist_volumes": best_media.get("volumes") or 0,
            "anilist_country": _clean(best_media.get("countryOfOrigin")),
            "banner_url": _clean(best_media.get("bannerImage")),
            "cover_url_anilist": _clean(cover.get("extraLarge") or cover.get("large")),
            "cover_color": _clean(cover.get("color")),
            "anilist_description": _clean(best_media.get("description")),
            "anilist_titles": titles,
            "anilist_genres": [item for item in (best_media.get("genres") or []) if _clean(item)],
            "anilist_match_score": round(best_score, 4),
            "preferred_title": _clean(title_block.get("userPreferred") or title_block.get("romaji") or title_block.get("english")),
        }

    task = asyncio.create_task(_load())
    _INFLIGHT[cache_key] = task
    try:
        return _cache_set(cache_key, await task)
    finally:
        _INFLIGHT.pop(cache_key, None)
