import asyncio
import hashlib
import html
import time

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Update,
)
from telegram.ext import ContextTypes

from config import BOT_BRAND, BOT_USERNAME
from services.catalog_client import get_cached_search_titles, search_titles

INLINE_LIMIT = 8
INLINE_QUERY_TTL = 120
INLINE_SEARCH_TIMEOUT = 2.2
INLINE_ANSWER_CACHE = 8

INLINE_CACHE: dict[str, tuple[float, list[dict]]] = {}
_INLINE_INFLIGHT: dict[str, asyncio.Task] = {}

STATUS_PT_MAP = {
    "ongoing": "Em andamento",
    "completed": "Finalizado",
    "hiatus": "Em hiato",
    "cancelled": "Cancelado",
    "dropped": "Cancelado",
    "releasing": "Em lancamento",
    "finished": "Finalizado",
}


def _result_id(title_id: str, index: int) -> str:
    return hashlib.md5(f"{title_id}:{index}".encode("utf-8")).hexdigest()


def _normalize_query(text: str) -> str:
    return " ".join(str(text or "").strip().split())


def _translate_status(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "N/A"
    return STATUS_PT_MAP.get(raw.lower(), raw)


def _cache_get(query: str) -> list[dict] | None:
    item = INLINE_CACHE.get(query)
    if not item:
        return None
    created_at, results = item
    if time.time() - created_at > INLINE_QUERY_TTL:
        INLINE_CACHE.pop(query, None)
        return None
    return results


def _cache_set(query: str, results: list[dict]) -> list[dict]:
    INLINE_CACHE[query] = (time.time(), results)
    return results


def _safe_results(results: list[dict] | None) -> list[dict]:
    if not results:
        return []
    clean = []
    seen: set[str] = set()

    for item in results:
        if not isinstance(item, dict):
            continue

        title_id = str(item.get("title_id") or "").strip()
        if not title_id or title_id in seen:
            continue

        seen.add(title_id)
        clean.append(item)

        if len(clean) >= INLINE_LIMIT:
            break

    return clean


async def _search_inline(query: str) -> list[dict]:
    normalized = _normalize_query(query)
    if not normalized:
        return []

    cached = _cache_get(normalized)
    if cached is not None:
        return cached

    cached_catalog = get_cached_search_titles(normalized, limit=INLINE_LIMIT)
    if cached_catalog is not None:
        return _cache_set(normalized, _safe_results(cached_catalog))

    inflight_key = normalized.lower()
    existing_task = _INLINE_INFLIGHT.get(inflight_key)
    if existing_task:
        try:
            return await existing_task
        except Exception:
            return []

    async def _runner() -> list[dict]:
        try:
            results = await asyncio.wait_for(
                search_titles(normalized, limit=INLINE_LIMIT),
                timeout=INLINE_SEARCH_TIMEOUT,
            )
            return _cache_set(normalized, _safe_results(results))
        except Exception as e:
            print("INLINE SEARCH ERROR:", repr(e))
            return []

    task = asyncio.create_task(_runner())
    _INLINE_INFLIGHT[inflight_key] = task
    try:
        return await task
    finally:
        _INLINE_INFLIGHT.pop(inflight_key, None)


def _build_description(item: dict) -> str:
    parts = []
    status = _translate_status(item.get("status") or "")
    if status and status != "N/A":
        parts.append(status)

    latest_chapter = item.get("latest_chapter") or item.get("chapter_number") or ""
    if latest_chapter:
        parts.append(f"Cap. {latest_chapter}")
    elif item.get("rating"):
        parts.append(f"Nota {item['rating']}")

    return " • ".join(parts) or "Abrir obra no bot"


def _build_message_text(item: dict) -> str:
    title = html.escape(item.get("display_title") or item.get("title") or "Manga")
    status = html.escape(_translate_status(item.get("status") or ""))
    latest = html.escape(str(item.get("latest_chapter") or item.get("chapter_number") or "N/A"))
    rating = html.escape(str(item.get("rating") or "N/A"))
    image_url = str(item.get("cover_url") or "").strip()

    if image_url:
        title_line = f'<b><a href="{html.escape(image_url, quote=True)}">📚</a> {title}</b>'
    else:
        title_line = f"<b>📚 {title}</b>"

    meta_lines = [f"» <b>Status:</b> <i>{status}</i>"]
    if latest != "N/A":
        meta_lines.append(f"» <b>Ultimo capitulo:</b> <i>{latest}</i>")
    if rating != "N/A":
        meta_lines.append(f"» <b>Nota:</b> <i>{rating}</i>")

    text = (
        f"{title_line}\n\n"
        f"{chr(10).join(meta_lines)}\n\n"
        f"✨ <i>Abra no @{html.escape(BOT_USERNAME)} e continue a leitura pelo {html.escape(BOT_BRAND)}.</i>"
    )

    if image_url:
        text += f'<a href="{html.escape(image_url, quote=True)}">\u200b</a>'

    return text


async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.inline_query
    if not query:
        return

    text = _normalize_query(query.query or "")
    print(f"INLINE QUERY: {text!r}")

    if len(text) < 2:
        await query.answer([], cache_time=2, is_personal=True)
        return

    # resposta imediata via cache local/catálogo, se existir
    cached = _cache_get(text)
    if cached is None:
        cached = get_cached_search_titles(text, limit=INLINE_LIMIT)

    if cached:
        results = _safe_results(cached)
    else:
        results = await _search_inline(text)

    print(f"INLINE RESULTS: {len(results)}")

    if not results:
        await query.answer([], cache_time=3, is_personal=True)
        return

    articles = []
    for index, item in enumerate(results[:INLINE_LIMIT]):
        title = item.get("display_title") or item.get("title") or "Manga"
        title_id = str(item.get("title_id") or "").strip()
        if not title_id:
            continue

        rows = [
            [InlineKeyboardButton("📚 Abrir obra", url=f"https://t.me/{BOT_USERNAME}?start=title_{title_id}")]
        ]

        chapter_id = str(item.get("chapter_id") or "").strip()
        if chapter_id:
            rows.append(
                [InlineKeyboardButton("🆕 Ultimo capitulo", url=f"https://t.me/{BOT_USERNAME}?start=ch_{chapter_id}")]
            )

        try:
            article = InlineQueryResultArticle(
                id=_result_id(title_id, index),
                title=title[:64],
                description=_build_description(item),
                thumbnail_url=item.get("cover_url") or None,
                input_message_content=InputTextMessageContent(
                    _build_message_text(item),
                    parse_mode="HTML",
                ),
                reply_markup=InlineKeyboardMarkup(rows),
            )
            articles.append(article)
        except Exception as e:
            print("INLINE ARTICLE ERROR:", repr(e))

    print(f"INLINE ARTICLES: {len(articles)}")

    await query.answer(
        articles,
        cache_time=INLINE_ANSWER_CACHE,
        is_personal=True,
    )
