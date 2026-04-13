import asyncio
import html
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS, BOT_USERNAME, CANAL_POSTAGEM_MANGA, STICKER_DIVISOR
from core.channel_target import ensure_channel_target
from services.catalog_client import (
    get_cached_title_bundle,
    get_cached_title_overview,
    get_title_bundle,
    get_title_overview,
    search_titles,
)

STATUS_PT_MAP = {
    "ongoing": "Em andamento",
    "completed": "Finalizado",
    "hiatus": "Em hiato",
    "cancelled": "Cancelado",
    "dropped": "Cancelado",
    "releasing": "Em lançamento",
    "finished": "Finalizado",
}

FORMAT_PT_MAP = {
    "MANGA": "Mangá",
    "MANHWA": "Manhwa",
    "MANHUA": "Manhua",
    "ONE_SHOT": "One-shot",
    "NOVEL": "Novel",
}

BLOCKED_GENRE_EXACT = {
    "based on a korean novel",
    "based on a novel",
    "based on a web novel",
    "based on a light novel",
    "based on a webtoon",
    "based on a manhwa",
    "based on a manhua",
    "based on an anime",
    "based on a game",
    "based on a video game",
    "based on a movie",
    "based on a tv series",
    "adaptation",
}

BLOCKED_GENRE_PATTERNS = [
    r"^based on\b",
    r"\bnovel\b",
    r"\bweb novel\b",
    r"\bkorean novel\b",
    r"\blight novel\b",
    r"\badaptation\b",
]

CATALOG_SITE_BASE = os.getenv("CATALOG_SITE_BASE", "https://mangaball.net").rstrip("/")

POSTED_MANGAS_FILE = Path("data/mangas_postados.json")
SKIPPED_MANGAS_FILE = Path("data/mangas_pulados.json")

BULK_DELAY_SECONDS = float(os.getenv("MANGA_BULK_DELAY_SECONDS", "8"))
BULK_HTTP_TIMEOUT = 20.0
DIVIDER_FALLBACK_TEXT = "━━━━━━━━━━━━━━"

BOTDATA_BULK_RUNNING_KEY = "postallmangas_running"
BOTDATA_BULK_TASK_KEY = "postallmangas_task"

HTTPX_LIMITS = httpx.Limits(max_keepalive_connections=10, max_connections=20)


class SkipMangaPost(Exception):
    pass


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", (value or "").strip().lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return " ".join(value.split())


def _pick_main_title(manga: dict) -> str:
    return (
        manga.get("display_title")
        or manga.get("title")
        or manga.get("preferred_title")
        or manga.get("name")
        or "Sem título"
    )


def _translate_status(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "N/A"
    return STATUS_PT_MAP.get(raw.lower(), raw)


def _translate_format(value: str) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return "Mangá"
    return FORMAT_PT_MAP.get(raw, raw.title())


def _latest_chapter_summary(manga: dict) -> dict:
    latest = manga.get("latest_chapter")
    if isinstance(latest, dict) and latest.get("chapter_id"):
        return latest

    chapter_id = str(manga.get("chapter_id") or "").strip()
    if not chapter_id:
        return {}

    return {
        "chapter_id": chapter_id,
        "chapter_number": str(
            manga.get("latest_chapter")
            or manga.get("chapter_number")
            or manga.get("latest_chapter_number")
            or ""
        ).strip(),
    }


def _pick_best_candidate(query: str, results: list[dict]) -> dict | None:
    if not results:
        return None

    normalized_query = _normalize_text(query)

    def _score(item: dict) -> tuple[int, int, int]:
        display_title = _normalize_text(item.get("display_title") or "")
        title = _normalize_text(item.get("title") or "")
        raw_title = _normalize_text(item.get("raw_title") or "")
        best_text = display_title or title or raw_title

        if not best_text:
            return (-1, 0, 0)
        if best_text == normalized_query or title == normalized_query or raw_title == normalized_query:
            return (500, -len(best_text), 1 if item.get("chapter_id") else 0)
        if best_text.startswith(normalized_query) or title.startswith(normalized_query):
            return (400, -len(best_text), 1 if item.get("chapter_id") else 0)
        if normalized_query in best_text or normalized_query in raw_title:
            return (300, -len(best_text), 1 if item.get("chapter_id") else 0)

        overlap = len(set(normalized_query.split()) & set(best_text.split()))
        return (100 + overlap, -len(best_text), 1 if item.get("chapter_id") else 0)

    return max(results, key=_score)


def _flatten_strings(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, str):
        text = html.unescape(value).strip()
        if not text:
            return []
        parts = re.split(r"[|,/•]+", text)
        return [p.strip() for p in parts if p.strip()]

    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            out.extend(_flatten_strings(item))
        return out

    if isinstance(value, dict):
        out: list[str] = []
        for key in ("name", "title", "label", "genre", "tag"):
            if value.get(key):
                out.extend(_flatten_strings(value.get(key)))
        return out

    return []


def _unique_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []

    for item in items:
        clean = " ".join(str(item).strip().split())
        if not clean:
            continue
        norm = _normalize_text(clean)
        if norm in seen:
            continue
        seen.add(norm)
        output.append(clean)

    return output


def _prettify_tag(value: str) -> str:
    value = html.unescape(str(value or "").strip())
    value = value.replace("_", " ")
    value = re.sub(r"\s+", " ", value).strip(" -#")
    return value


def _is_valid_display_genre(value: str) -> bool:
    raw = _prettify_tag(value)
    norm = _normalize_text(raw)

    if not norm:
        return False

    if norm in BLOCKED_GENRE_EXACT:
        return False

    for pattern in BLOCKED_GENRE_PATTERNS:
        if re.search(pattern, norm, flags=re.I):
            return False

    return True


def _filter_display_genres(items: list[str], limit: int = 6) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()

    for item in items or []:
        pretty = _prettify_tag(item)
        norm = _normalize_text(pretty)

        if not _is_valid_display_genre(pretty):
            continue
        if norm in seen:
            continue

        seen.add(norm)
        output.append(pretty)

        if len(output) >= limit:
            break

    return output


def _extract_json_ld_images(raw_html: str) -> list[str]:
    urls: list[str] = []
    if not raw_html:
        return urls

    matches = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        raw_html,
        flags=re.I | re.S,
    )

    for block in matches:
        block = block.strip()
        if not block:
            continue

        try:
            payload = json.loads(block)
        except Exception:
            continue

        def walk(obj: Any) -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key in {"thumbnailUrl", "contentUrl", "url", "image", "og:image"} and isinstance(value, str):
                        if value.startswith("http"):
                            urls.append(value)
                    else:
                        walk(value)
            elif isinstance(obj, list):
                for item in obj:
                    walk(item)

        walk(payload)

    return urls


def _extract_meta_content(raw_html: str, prop: str) -> str:
    if not raw_html:
        return ""

    pattern = (
        r'<meta[^>]+(?:property|name)=["\']'
        + re.escape(prop)
        + r'["\'][^>]+content=["\']([^"\']+)["\']'
    )
    match = re.search(pattern, raw_html, flags=re.I)
    return html.unescape(match.group(1)).strip() if match else ""


def _extract_badges_from_html(raw_html: str) -> list[str]:
    if not raw_html:
        return []

    matches = re.findall(
        r'data-tag-id="[^"]+"[^>]*>([^<]+)</span>',
        raw_html,
        flags=re.I,
    )
    return _unique_keep_order([html.unescape(x).strip() for x in matches if x.strip()])


def _resolve_manga_genres(manga: dict) -> list[str]:
    candidates: list[str] = []

    for key in (
        "genres",
        "genre_names",
        "tags",
        "tag_names",
        "anilist_genres",
        "anilist_tags",
        "mangaball_genres",
        "mangaball_tags",
        "categories",
        "keywords",
    ):
        candidates.extend(_flatten_strings(manga.get(key)))

    raw_html = (
        manga.get("raw_html")
        or manga.get("html")
        or manga.get("page_html")
        or manga.get("title_html")
        or ""
    )
    candidates.extend(_extract_badges_from_html(raw_html))

    return _unique_keep_order(candidates)


def _resolve_origin_photo(manga: dict) -> str:
    raw_html = (
        manga.get("raw_html")
        or manga.get("html")
        or manga.get("page_html")
        or manga.get("title_html")
        or ""
    )

    candidates = [
        manga.get("origin_cover_url"),
        manga.get("site_cover_url"),
        manga.get("source_cover_url"),
        manga.get("og_image"),
        manga.get("seo_image"),
        manga.get("thumbnailUrl"),
        _extract_meta_content(raw_html, "og:image"),
        *_extract_json_ld_images(raw_html),
        manga.get("cover_url"),
        manga.get("banner_url"),
        manga.get("background_url"),
    ]

    for candidate in candidates:
        url = str(candidate or "").strip()
        if url.startswith("http"):
            return url
    return ""


def _resolve_year(manga: dict) -> str:
    for key in ("year", "release_year", "published_year", "start_year", "anilist_year"):
        value = str(manga.get(key) or "").strip()
        if value:
            return value

    raw_html = (
        manga.get("raw_html")
        or manga.get("html")
        or manga.get("page_html")
        or manga.get("title_html")
        or ""
    )
    match = re.search(r"Published:\s*<b>(\d{4})</b>", raw_html, flags=re.I)
    return match.group(1) if match else ""


def _merge_post_payload(overview: dict, search_item: dict, bundle: dict | None = None) -> dict:
    merged = dict(overview or {})
    if bundle:
        merged.update({key: value for key, value in bundle.items() if value not in (None, "", [], {})})

    if not merged.get("title_id"):
        merged["title_id"] = search_item.get("title_id") or ""
    if not merged.get("title"):
        merged["title"] = search_item.get("title") or ""
    if not merged.get("display_title"):
        merged["display_title"] = search_item.get("display_title") or merged.get("title") or ""

    if not merged.get("cover_url"):
        merged["cover_url"] = search_item.get("cover_url") or ""
    if not merged.get("background_url"):
        merged["background_url"] = search_item.get("background_url") or merged.get("cover_url") or ""
    if not merged.get("banner_url"):
        merged["banner_url"] = merged.get("background_url") or merged.get("cover_url") or ""

    origin_photo = _resolve_origin_photo(merged)
    if origin_photo:
        merged["origin_cover_url"] = origin_photo
        merged["cover_url"] = origin_photo
        if not merged.get("banner_url"):
            merged["banner_url"] = origin_photo

    genres = _resolve_manga_genres(merged)
    if genres:
        merged["genres"] = genres

    year = _resolve_year(merged)
    if year:
        merged["year"] = year

    if not merged.get("latest_chapter"):
        latest = _latest_chapter_summary(search_item)
        if latest:
            merged["latest_chapter"] = latest

    return merged


def _new_http_client(timeout: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0"},
        limits=HTTPX_LIMITS,
    )


def _load_posted_manga_ids() -> list[str]:
    try:
        if not POSTED_MANGAS_FILE.exists():
            return []
        data = json.loads(POSTED_MANGAS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    return []


def _save_posted_manga_ids(items: list[str]) -> None:
    POSTED_MANGAS_FILE.parent.mkdir(parents=True, exist_ok=True)
    POSTED_MANGAS_FILE.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_skipped_manga_map() -> dict[str, dict]:
    try:
        if not SKIPPED_MANGAS_FILE.exists():
            return {}
        data = json.loads(SKIPPED_MANGAS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _save_skipped_manga_map(items: dict[str, dict]) -> None:
    SKIPPED_MANGAS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SKIPPED_MANGAS_FILE.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _mark_manga_skipped(skipped_map: dict[str, dict], title_id: str, title_name: str, reason: str) -> None:
    title_id = str(title_id or "").strip()
    if not title_id:
        return

    skipped_map[title_id] = {
        "title": str(title_name or "").strip(),
        "reason": str(reason or "").strip(),
    }
    _save_skipped_manga_map(skipped_map)


def _bulk_running(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.application.bot_data.get(BOTDATA_BULK_RUNNING_KEY, False))


def _set_bulk_running(context: ContextTypes.DEFAULT_TYPE, value: bool) -> None:
    context.application.bot_data[BOTDATA_BULK_RUNNING_KEY] = value


def _is_missing(value: Any) -> bool:
    if value is None:
        return True

    text = str(value).strip()
    return text in {"", "?", "N/A", "None", "null", "0"}


def _clean_url(value: Any) -> str:
    return str(value or "").replace("\n", "").replace("\r", "").strip()


def _looks_like_http_url(url: str) -> bool:
    if not url or " " in url:
        return False

    try:
        parsed = urlparse(url)
    except Exception:
        return False

    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _require_title_id(manga: dict) -> str:
    value = str(manga.get("title_id") or "").strip()
    if not value:
        raise SkipMangaPost("Obra sem title_id.")
    return value


def _require_title(manga: dict) -> str:
    title = _pick_main_title(manga)
    if _is_missing(title) or title == "Sem título":
        raise SkipMangaPost("Obra sem título válido.")
    return title


def _require_genres(manga: dict) -> list[str]:
    genres = _filter_display_genres(_resolve_manga_genres(manga), limit=6)
    if not genres:
        raise SkipMangaPost("Obra sem gêneros válidos.")
    return genres


def _require_chapters(manga: dict) -> str:
    chapters = (
        manga.get("total_chapters")
        or manga.get("chapter_count")
        or manga.get("anilist_chapters")
        or ""
    )
    if _is_missing(chapters):
        raise SkipMangaPost("Obra sem total de capítulos.")
    return str(chapters).strip()


def _require_status(manga: dict) -> str:
    raw = manga.get("status") or manga.get("anilist_status") or ""
    value = _translate_status(raw)
    if _is_missing(raw) or value == "N/A":
        raise SkipMangaPost("Obra sem status.")
    return value


def _require_format(manga: dict) -> str:
    raw = manga.get("format") or manga.get("type") or manga.get("anilist_format") or ""
    value = _translate_format(raw)
    if _is_missing(raw):
        raise SkipMangaPost("Obra sem formato.")
    return value


def _require_year(manga: dict) -> str:
    year = _resolve_year(manga)
    if _is_missing(year):
        raise SkipMangaPost("Obra sem ano.")
    return year


def _pick_valid_photo(manga: dict) -> str:
    candidates = [
        manga.get("origin_cover_url"),
        manga.get("cover_url"),
        manga.get("banner_url"),
        manga.get("background_url"),
    ]

    for candidate in candidates:
        url = _clean_url(candidate)
        if url and _looks_like_http_url(url):
            return url

    raise SkipMangaPost("Foto inválida, ausente ou inacessível.")


async def _build_validated_post_data(manga: dict) -> dict:
    return {
        "title_id": _require_title_id(manga),
        "title": _require_title(manga),
        "genres": _require_genres(manga),
        "chapters": _require_chapters(manga),
        "status": _require_status(manga),
        "format_name": _require_format(manga),
        "year": _require_year(manga),
        "photo": _pick_valid_photo(manga),
    }


def _build_caption_from_validated(data: dict) -> str:
    full_title = html.escape(str(data["title"])).upper()
    genres_text = ", ".join(f"#{g.replace(' ', '_')}" for g in data["genres"])

    info_lines = [
        f"<b>Gêneros:</b> <i>{html.escape(genres_text)}</i>",
        f"<b>Formato:</b> <i>{html.escape(str(data['format_name']))}</i>",
        f"<b>Capítulos:</b> <i>{html.escape(str(data['chapters']))}</i>",
        f"<b>Ano:</b> <i>{html.escape(str(data['year']))}</i>",
        f"<b>Status:</b> <i>{html.escape(str(data['status']))}</i>",
    ]

    return (
        f"📚 <b>{full_title}</b>\n\n"
        + "\n".join(info_lines)
        + "\n\nMangás Brasil | @MangasBrasil"
    )


def _build_keyboard(manga: dict) -> InlineKeyboardMarkup:
    title_id = manga.get("title_id") or ""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📚 Ler obra", url=f"https://t.me/{BOT_USERNAME}?start=title_{title_id}")]]
    )


async def _safe_edit_status(message, text: str) -> None:
    try:
        await message.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


async def _send_divider(bot, destination) -> None:
    sticker = str(STICKER_DIVISOR or "").strip()
    sticker_error = None

    if sticker:
        for _ in range(3):
            try:
                await bot.send_sticker(chat_id=destination, sticker=sticker)
                return
            except Exception as error:
                sticker_error = error
                await asyncio.sleep(0.5)

        print("ERRO STICKER DIVISOR MANGÁ:", repr(sticker_error), sticker)

    try:
        await bot.send_message(chat_id=destination, text=DIVIDER_FALLBACK_TEXT)
    except Exception as fallback_error:
        print("ERRO DIVISOR FALLBACK MANGÁ:", repr(fallback_error))
        if sticker_error:
            raise sticker_error
        raise


async def _send_manga_post(bot, destination, manga: dict) -> None:
    data = await _build_validated_post_data(manga)
    caption = _build_caption_from_validated(data)
    keyboard = _build_keyboard({"title_id": data["title_id"]})

    try:
        await bot.send_photo(
            chat_id=destination,
            photo=data["photo"],
            caption=caption,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception as error:
        raise SkipMangaPost(f"Falha ao enviar foto válida: {error!r}") from error

    await _send_divider(bot, destination)


def _extract_xml_locs(xml_text: str) -> list[str]:
    if not xml_text:
        return []
    return [
        html.unescape(x.strip())
        for x in re.findall(r"<loc>\s*(.*?)\s*</loc>", xml_text, flags=re.I | re.S)
        if x.strip()
    ]


def _title_from_slug(slug: str) -> str:
    text = slug.replace("-", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text.title() if text else "Mangá"


def _title_ref_from_url(url: str) -> dict | None:
    clean = (url or "").strip()

    match = re.search(
        r"/title-detail/(?P<slug>.+)-(?P<title_id>[a-zA-Z0-9]{12,})/?$",
        clean,
        flags=re.I,
    )
    if not match:
        return None

    title_id = match.group("title_id").strip()
    slug = match.group("slug").strip()
    guessed_title = _title_from_slug(slug)

    return {
        "title_id": title_id,
        "title": guessed_title,
        "display_title": guessed_title,
        "raw_title": guessed_title,
        "url": clean,
    }


async def _fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url)
    response.raise_for_status()
    return response.text


async def _load_pending_title_refs(
    posted_ids: set[str] | None = None,
    skipped_ids: set[str] | None = None,
) -> list[dict]:
    posted = posted_ids or set()
    skipped = skipped_ids or set()
    pending: list[dict] = []
    seen: set[str] = set()

    index_url = f"{CATALOG_SITE_BASE}/storage/sitemaps/sitemap-title-index.xml"

    async with _new_http_client(BULK_HTTP_TIMEOUT) as client:
        index_xml = await _fetch_text(client, index_url)
        sitemap_urls = [u for u in _extract_xml_locs(index_xml) if u.lower().endswith(".xml")]

        if not sitemap_urls:
            sitemap_urls = [index_url]

        for sitemap_url in sitemap_urls:
            sitemap_xml = await _fetch_text(client, sitemap_url)
            title_urls = [u for u in _extract_xml_locs(sitemap_xml) if "/title-detail/" in u]

            for title_url in title_urls:
                ref = _title_ref_from_url(title_url)
                if not ref:
                    continue

                title_id = ref["title_id"]
                if title_id in seen or title_id in posted or title_id in skipped:
                    continue

                seen.add(title_id)
                pending.append(ref)

    return pending


async def _resolve_payload_from_ref(ref: dict) -> dict | None:
    title_id = str(ref.get("title_id") or "").strip()
    if not title_id:
        return None

    overview = get_cached_title_overview(title_id)
    if overview is None:
        try:
            overview = await get_title_overview(title_id)
        except Exception:
            overview = {}

    bundle = get_cached_title_bundle(title_id)
    if bundle is None:
        try:
            bundle = await asyncio.wait_for(get_title_bundle(title_id), timeout=10.0)
        except Exception:
            bundle = None

    return _merge_post_payload(overview or {}, ref, bundle)


async def _run_postallmangas(
    context: ContextTypes.DEFAULT_TYPE,
    admin_chat_id: int,
    reply_to_message_id: int | None,
    limit: int | None,
) -> None:
    _set_bulk_running(context, True)
    try:
        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_MANGA or admin_chat_id)

        posted_ids = _load_posted_manga_ids()
        posted_set = set(posted_ids)

        skipped_map = _load_skipped_manga_map()
        skipped_set = set(skipped_map.keys())

        refs = await _load_pending_title_refs(
            posted_ids=posted_set,
            skipped_ids=skipped_set,
        )

        if not refs:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text="✅ <b>Não há mangás pendentes para postar.</b>",
                parse_mode="HTML",
                reply_to_message_id=reply_to_message_id,
            )
            return

        target_valid = limit
        sent = 0
        skipped = 0
        failed = 0
        processed = 0
        total_refs = len(refs)

        status_message = await context.bot.send_message(
            chat_id=admin_chat_id,
            text=(
                "🚀 <b>Postagem em lote iniciada.</b>\n\n"
                f"<b>Referências pendentes:</b> <code>{total_refs}</code>\n"
                f"<b>Meta válida:</b> <code>{target_valid if target_valid is not None else 'todas'}</code>\n"
                f"<b>Intervalo:</b> <code>{BULK_DELAY_SECONDS:.0f}s</code>"
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to_message_id,
        )

        for ref in refs:
            if target_valid is not None and sent >= target_valid:
                break

            title_id = str(ref.get("title_id") or "").strip()
            title_name = str(ref.get("display_title") or ref.get("title") or "Mangá").strip()
            processed += 1
            sent_now = False

            try:
                payload = await _resolve_payload_from_ref(ref)
                if not payload:
                    raise SkipMangaPost("Não consegui montar o payload da obra.")

                await _send_manga_post(context.bot, destination, payload)

                if title_id and title_id not in posted_set:
                    posted_ids.append(title_id)
                    posted_set.add(title_id)
                    _save_posted_manga_ids(posted_ids)

                sent += 1
                sent_now = True

            except SkipMangaPost as skip_error:
                skipped += 1
                _mark_manga_skipped(skipped_map, title_id, title_name, str(skip_error))
                skipped_set.add(title_id)
                print("POSTALLMANGAS PULADO:", repr(skip_error), title_id, title_name)

            except Exception as error:
                failed += 1
                print("ERRO POSTALLMANGAS:", repr(error), title_id, title_name)

            await _safe_edit_status(
                status_message,
                (
                    "🚀 <b>Postagem em lote em andamento.</b>\n\n"
                    f"<b>Enviados:</b> <code>{sent}</code>\n"
                    f"<b>Pulados:</b> <code>{skipped}</code>\n"
                    f"<b>Falhas:</b> <code>{failed}</code>\n"
                    f"<b>Analisados:</b> <code>{processed}/{total_refs}</code>\n"
                    f"<b>Meta válida:</b> <code>{target_valid if target_valid is not None else 'todas'}</code>\n"
                    f"<b>Atual:</b> <code>{html.escape(title_name)}</code>"
                ),
            )

            if target_valid is not None and sent >= target_valid:
                break

            if sent_now and processed < total_refs:
                await asyncio.sleep(BULK_DELAY_SECONDS)

        if target_valid is not None and sent < target_valid:
            final_text = (
                "⚠️ <b>Postagem em lote finalizada sem atingir a meta.</b>\n\n"
                f"<b>Meta pedida:</b> <code>{target_valid}</code>\n"
                f"<b>Enviados:</b> <code>{sent}</code>\n"
                f"<b>Pulados:</b> <code>{skipped}</code>\n"
                f"<b>Falhas:</b> <code>{failed}</code>\n"
                f"<b>Analisados:</b> <code>{processed}/{total_refs}</code>"
            )
        else:
            final_text = (
                "✅ <b>Postagem em lote finalizada.</b>\n\n"
                f"<b>Enviados:</b> <code>{sent}</code>\n"
                f"<b>Pulados:</b> <code>{skipped}</code>\n"
                f"<b>Falhas:</b> <code>{failed}</code>\n"
                f"<b>Analisados:</b> <code>{processed}/{total_refs}</code>"
            )

        await _safe_edit_status(status_message, final_text)

    finally:
        _set_bulk_running(context, False)
        context.application.bot_data.pop(BOTDATA_BULK_TASK_KEY, None)


async def postmanga(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text(
            "❌ <b>Você não tem permissão para usar este comando.</b>",
            parse_mode="HTML",
        )
        return

    if not context.args:
        await message.reply_text(
            "❌ <b>Faltou o nome do mangá.</b>\n\n"
            "Use assim:\n"
            "<code>/postmanga nome do mangá</code>\n\n"
            "📌 <b>Exemplo:</b>\n"
            "<code>/postmanga solo leveling</code>",
            parse_mode="HTML",
        )
        return

    query = " ".join(context.args).strip()
    status_message = await message.reply_text(
        "📤 <b>Montando postagem...</b>\nAguarde um instante.",
        parse_mode="HTML",
    )

    try:
        results = await search_titles(query, limit=8)
        if not results:
            await status_message.edit_text(
                "❌ <b>Não encontrei esse mangá.</b>",
                parse_mode="HTML",
            )
            return

        search_item = _pick_best_candidate(query, results)
        if not search_item or not search_item.get("title_id"):
            await status_message.edit_text(
                "❌ <b>Não consegui identificar a obra certa.</b>",
                parse_mode="HTML",
            )
            return

        await status_message.edit_text(
            "📤 <b>Montando postagem...</b>\nResolvi a obra e estou preparando o card do canal.",
            parse_mode="HTML",
        )

        title_id = search_item["title_id"]

        overview = get_cached_title_overview(title_id)
        if overview is None:
            try:
                overview = await get_title_overview(title_id)
            except Exception:
                overview = {}

        bundle = get_cached_title_bundle(title_id)
        if bundle is None:
            try:
                bundle = await asyncio.wait_for(get_title_bundle(title_id), timeout=10.0)
            except Exception:
                bundle = None

        manga = _merge_post_payload(overview, search_item, bundle)
        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_MANGA or message.chat_id)

        await _send_manga_post(context.bot, destination, manga)

        await status_message.edit_text(
            f"✅ <b>Postagem enviada com sucesso.</b>\n\n<code>{html.escape(manga.get('title') or query)}</code>",
            parse_mode="HTML",
        )

    except SkipMangaPost as skip_error:
        print("POSTMANGA PULADO:", repr(skip_error), query)
        await status_message.edit_text(
            f"⚠️ <b>Obra pulada.</b>\n\n{html.escape(str(skip_error))}",
            parse_mode="HTML",
        )

    except Exception as error:
        print("ERRO POSTMANGA:", repr(error))
        await status_message.edit_text(
            f"❌ <b>Não consegui postar esse mangá.</b>\n\n{html.escape(str(error) or 'Tente novamente em instantes.')}",
            parse_mode="HTML",
        )


async def postallmangas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text(
            "❌ <b>Você não tem permissão para usar este comando.</b>",
            parse_mode="HTML",
        )
        return

    if _bulk_running(context):
        await message.reply_text(
            "⏳ <b>Já existe uma postagem em lote rodando agora.</b>",
            parse_mode="HTML",
        )
        return

    limit: int | None = None
    if context.args:
        raw = str(context.args[0]).strip()
        if not raw.isdigit():
            await message.reply_text(
                "❌ <b>Quantidade inválida.</b>\n\n"
                "Use:\n"
                "<code>/postallmangas</code>\n"
                "ou\n"
                "<code>/postallmangas 100</code>",
                parse_mode="HTML",
            )
            return

        limit = int(raw)
        if limit <= 0:
            await message.reply_text(
                "❌ <b>A quantidade precisa ser maior que zero.</b>",
                parse_mode="HTML",
            )
            return

    task = context.application.create_task(
        _run_postallmangas(
            context=context,
            admin_chat_id=message.chat_id,
            reply_to_message_id=message.message_id,
            limit=limit,
        )
    )
    context.application.bot_data[BOTDATA_BULK_TASK_KEY] = task

    if limit is None:
        await message.reply_text(
            "🚀 <b>Fila de postagem em lote iniciada.</b>\n\n"
            "Vou começar a postar os mangás pendentes agora.",
            parse_mode="HTML",
        )
    else:
        await message.reply_text(
            "🚀 <b>Fila de postagem em lote iniciada.</b>\n\n"
            f"Vou continuar até postar <code>{limit}</code> mangás válidos.",
            parse_mode="HTML",
        )
