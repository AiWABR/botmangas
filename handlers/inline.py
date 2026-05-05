import asyncio
import hashlib
import html
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Any

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Update,
)
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes

from config import BOT_BRAND, BOT_USERNAME
from services.catalog_client import (
    get_cached_search_titles,
    get_search_fallback_titles,
    search_titles_fast,
)

INLINE_LIMIT = 8
INLINE_QUERY_TTL = 90
INLINE_SEARCH_TIMEOUT = 4.8
INLINE_ANSWER_CACHE = 6

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


def _inline_log(event: str, **payload: Any) -> None:
    data = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "event": event,
        **payload,
    }
    try:
        print("[INLINE_DEBUG]", json.dumps(data, ensure_ascii=True, default=str), flush=True)
    except Exception:
        print("[INLINE_DEBUG_FALLBACK]", event, repr(payload), flush=True)


def _inline_exception(event: str, error: BaseException, **payload: Any) -> None:
    _inline_log(
        event,
        error_type=type(error).__name__,
        error_repr=repr(error),
        traceback=traceback.format_exc(),
        **payload,
    )


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
        _inline_log("cache_miss", query=query)
        return None
    created_at, results = item
    if time.time() - created_at > INLINE_QUERY_TTL:
        INLINE_CACHE.pop(query, None)
        _inline_log("cache_expired", query=query, age_seconds=round(time.time() - created_at, 3), count=len(results))
        return None
    _inline_log("cache_hit", query=query, age_seconds=round(time.time() - created_at, 3), count=len(results))
    return results


def _cache_set(query: str, results: list[dict]) -> list[dict]:
    INLINE_CACHE[query] = (time.time(), results)
    _inline_log("cache_set", query=query, count=len(results))
    return results


def _fallback_search(query: str) -> list[dict]:
    started = time.perf_counter()
    try:
        results = get_search_fallback_titles(query, limit=INLINE_LIMIT)
        _inline_log("fallback_search_ok", query=query, count=len(results), elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
        return results
    except Exception as error:
        _inline_exception("fallback_search_error", error, query=query, elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
        return []


async def _search_inline(query: str) -> list[dict]:
    started = time.perf_counter()
    normalized = _normalize_query(query)
    _inline_log("search_start", raw_query=query, normalized=normalized)
    if not normalized:
        _inline_log("search_empty_query", elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
        return []

    cached = _cache_get(normalized)
    if cached is not None:
        _inline_log("search_return_cache", query=normalized, count=len(cached), elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
        return cached

    cached_catalog = get_cached_search_titles(normalized, limit=INLINE_LIMIT)
    if cached_catalog is not None:
        _inline_log("search_return_catalog_cache", query=normalized, count=len(cached_catalog), elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
        return _cache_set(normalized, cached_catalog[:INLINE_LIMIT])

    fallback_catalog = _fallback_search(normalized)
    if fallback_catalog:
        _inline_log("search_return_fallback_before_network", query=normalized, count=len(fallback_catalog), elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
        return _cache_set(normalized, fallback_catalog[:INLINE_LIMIT])

    task = _INLINE_INFLIGHT.get(normalized.lower())
    if task:
        _inline_log("search_join_inflight", query=normalized)
        return await task

    async def _runner() -> list[dict]:
        runner_started = time.perf_counter()
        try:
            results = await asyncio.wait_for(
                search_titles_fast(normalized, limit=INLINE_LIMIT),
                timeout=INLINE_SEARCH_TIMEOUT,
            )
        except Exception as error:
            _inline_exception("network_search_error", error, query=normalized, elapsed_ms=round((time.perf_counter() - runner_started) * 1000, 2))
            fallback_results = _fallback_search(normalized)
            return _cache_set(normalized, fallback_results[:INLINE_LIMIT])

        _inline_log("network_search_ok", query=normalized, count=len(results), elapsed_ms=round((time.perf_counter() - runner_started) * 1000, 2))
        return _cache_set(normalized, results[:INLINE_LIMIT])

    task = asyncio.create_task(_runner())
    _INLINE_INFLIGHT[normalized.lower()] = task
    try:
        results = await task
        _inline_log("search_done", query=normalized, count=len(results), elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
        return results
    finally:
        _INLINE_INFLIGHT.pop(normalized.lower(), None)


def _build_description(item: dict) -> str:
    parts = []
    status = _translate_status(item.get("status") or "")
    if status and status != "N/A":
        parts.append(status)
    if item.get("latest_chapter"):
        parts.append(f"Cap. {item['latest_chapter']}")
    elif item.get("rating"):
        parts.append(f"Nota {item['rating']}")
    return " • ".join(parts) or "Abrir obra no bot"


def _deep_link(payload: str) -> str:
    username = (BOT_USERNAME or "").strip().lstrip("@")
    if not username:
        return ""
    return f"https://t.me/{username}?start={payload}"


def _bot_url() -> str:
    username = (BOT_USERNAME or "").strip().lstrip("@")
    if not username:
        return "https://t.me/"
    return f"https://t.me/{username}"


def _inline_keyboard(item: dict) -> InlineKeyboardMarkup:
    title_id = str(item.get("title_id") or "").strip()
    chapter_id = str(item.get("chapter_id") or "").strip()
    rows: list[list[InlineKeyboardButton]] = []

    if title_id:
        rows.append(
            [InlineKeyboardButton("📚 Abrir obra", url=_deep_link(f"title_{title_id}"))]
        )
        rows.append(
            [InlineKeyboardButton("📖 Lista de capítulos", url=_deep_link(f"chapters_{title_id}"))]
        )

    if chapter_id:
        rows.append(
            [InlineKeyboardButton("🆕 Último capítulo", url=_deep_link(f"ch_{chapter_id}"))]
        )

    if not rows:
        rows.append(
            [InlineKeyboardButton("🔎 Abrir bot", url=f"https://t.me/{(BOT_USERNAME or '').strip().lstrip('@')}")]
        )

    return InlineKeyboardMarkup(rows)


def _build_message_text(item: dict, *, include_image_preview: bool = True) -> str:
    title = html.escape(item.get("display_title") or item.get("title") or "Manga")
    status = html.escape(_translate_status(item.get("status") or ""))
    latest = html.escape(item.get("latest_chapter") or "N/A")
    rating = html.escape(str(item.get("rating") or "N/A"))
    image_url = str(item.get("cover_url") or "").strip() if include_image_preview else ""

    if image_url:
        title_line = f'<b><a href="{html.escape(image_url, quote=True)}">📚</a> {title}</b>'
    else:
        title_line = f"<b>📚 {title}</b>"

    meta_lines = [f"» <b>Status:</b> <i>{status}</i>"]
    if latest != "N/A":
        meta_lines.append(f"» <b>Último capítulo:</b> <i>{latest}</i>")
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


def _build_article(item: dict, index: int, *, include_thumbnail: bool = True, include_image_preview: bool = True) -> InlineQueryResultArticle | None:
    title = item.get("display_title") or item.get("title") or "Manga"
    title_id = item.get("title_id") or ""
    if not title_id:
        return None

    return InlineQueryResultArticle(
        id=_result_id(title_id, index),
        title=title[:64],
        description=_build_description(item),
        thumbnail_url=(item.get("cover_url") or None) if include_thumbnail else None,
        input_message_content=InputTextMessageContent(
            _build_message_text(item, include_image_preview=include_image_preview),
            parse_mode="HTML",
        ),
        reply_markup=_inline_keyboard(item),
    )


def _article_debug(article: InlineQueryResultArticle) -> dict[str, Any]:
    data = article.to_dict()
    content = data.get("input_message_content") or {}
    keyboard = data.get("reply_markup") or {}
    return {
        "id": data.get("id"),
        "type": data.get("type"),
        "title": data.get("title"),
        "description": data.get("description"),
        "thumbnail_url": data.get("thumbnail_url"),
        "message_len": len(str(content.get("message_text") or "")),
        "parse_mode": content.get("parse_mode"),
        "button_count": sum(len(row) for row in keyboard.get("inline_keyboard") or []),
        "buttons": keyboard.get("inline_keyboard") or [],
    }


def _helper_article(query_text: str, *, kind: str) -> InlineQueryResultArticle:
    escaped_query = html.escape(query_text or "")

    if kind == "short":
        title = "Digite o nome do mangá"
        description = "Use pelo menos 2 letras para buscar no acervo."
        message = (
            "🔎 <b>Busca de mangás</b>\n\n"
            "Digite o nome da obra depois do usuário do bot para procurar no acervo."
        )
    else:
        title = "Nenhum mangá encontrado"
        description = "Abra o bot ou tente outro nome para buscar no acervo."
        message = (
            f"🔎 <b>Nenhum resultado encontrado</b>\n\n"
            f"» <b>Busca:</b> <i>{escaped_query or 'sem texto'}</i>\n\n"
            "Tente pesquisar por outro nome da obra."
        )

    return InlineQueryResultArticle(
        id=_result_id(f"{kind}:{query_text}", 0),
        title=title,
        description=description,
        input_message_content=InputTextMessageContent(message, parse_mode="HTML"),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔎 Abrir bot", url=_bot_url())]]
        ),
    )


async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.inline_query
    if not query:
        _inline_log("handler_without_inline_query", update=update.to_dict() if update else None)
        return

    text = _normalize_query(query.query or "")
    _inline_log(
        "update_received",
        update_id=update.update_id,
        inline_query_id=query.id,
        from_user=(query.from_user.to_dict() if query.from_user else None),
        raw_query=query.query,
        normalized_query=text,
        offset=query.offset,
        chat_type=query.chat_type,
        location=query.location.to_dict() if query.location else None,
    )
    if len(text) < 2:
        helper = _helper_article(text, kind="short")
        _inline_log("answer_short_query_prepare", inline_query_id=query.id, article=_article_debug(helper))
        try:
            await query.answer([helper], cache_time=2, is_personal=True)
            _inline_log("answer_short_query_ok", inline_query_id=query.id)
        except Exception as error:
            _inline_exception("answer_short_query_failed", error, inline_query_id=query.id)
        return

    try:
        results = await _search_inline(text)
    except Exception as error:
        _inline_exception("search_unhandled_error", error, inline_query_id=query.id, query=text)
        helper = _helper_article(text, kind="empty")
        try:
            await query.answer([helper], cache_time=2, is_personal=True)
            _inline_log("answer_search_error_helper_ok", inline_query_id=query.id)
        except Exception as answer_error:
            _inline_exception("answer_search_error_helper_failed", answer_error, inline_query_id=query.id)
        return

    if not results:
        helper = _helper_article(text, kind="empty")
        _inline_log("answer_empty_prepare", inline_query_id=query.id, query=text, article=_article_debug(helper))
        try:
            await query.answer([helper], cache_time=4, is_personal=True)
            _inline_log("answer_empty_ok", inline_query_id=query.id, query=text)
        except Exception as error:
            _inline_exception("answer_empty_failed", error, inline_query_id=query.id, query=text)
        return

    _inline_log(
        "search_results",
        inline_query_id=query.id,
        query=text,
        count=len(results),
        items=[
            {
                "title_id": item.get("title_id"),
                "chapter_id": item.get("chapter_id"),
                "title": item.get("display_title") or item.get("title"),
                "cover_url": item.get("cover_url"),
                "latest_chapter": item.get("latest_chapter"),
                "status": item.get("status"),
                "rating": item.get("rating"),
            }
            for item in results[:INLINE_LIMIT]
        ],
    )

    articles = []
    for index, item in enumerate(results[:INLINE_LIMIT]):
        article = _build_article(item, index)
        if article is not None:
            articles.append(article)

    if not articles:
        articles = [_helper_article(text, kind="empty")]

    _inline_log(
        "answer_prepare",
        inline_query_id=query.id,
        query=text,
        count=len(articles),
        cache_time=INLINE_ANSWER_CACHE,
        is_personal=True,
        articles=[_article_debug(article) for article in articles],
    )
    try:
        await query.answer(articles, cache_time=INLINE_ANSWER_CACHE, is_personal=True)
        _inline_log("answer_ok", inline_query_id=query.id, query=text, count=len(articles))
    except BadRequest as error:
        _inline_exception("answer_bad_request", error, inline_query_id=query.id, query=text, count=len(articles))
        safe_articles = [
            article
            for index, item in enumerate(results[:INLINE_LIMIT])
            if (article := _build_article(item, index, include_thumbnail=False, include_image_preview=False)) is not None
        ] or [_helper_article(text, kind="empty")]
        _inline_log(
            "answer_retry_prepare",
            inline_query_id=query.id,
            query=text,
            count=len(safe_articles),
            articles=[_article_debug(article) for article in safe_articles],
        )
        try:
            await query.answer(safe_articles, cache_time=2, is_personal=True)
            _inline_log("answer_retry_ok", inline_query_id=query.id, query=text, count=len(safe_articles))
        except TelegramError as retry_error:
            _inline_exception("answer_retry_failed", retry_error, inline_query_id=query.id, query=text, count=len(safe_articles))
    except TelegramError as error:
        _inline_exception("answer_telegram_error", error, inline_query_id=query.id, query=text, count=len(articles))
    except Exception as error:
        _inline_exception("answer_unexpected_error", error, inline_query_id=query.id, query=text, count=len(articles))


async def chosen_inline_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chosen = update.chosen_inline_result
    if not chosen:
        return
    _inline_log(
        "chosen_inline_result",
        update_id=update.update_id,
        result_id=chosen.result_id,
        from_user=chosen.from_user.to_dict() if chosen.from_user else None,
        query=chosen.query,
        inline_message_id=chosen.inline_message_id,
        location=chosen.location.to_dict() if chosen.location else None,
    )
