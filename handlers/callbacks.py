import asyncio
import html
import time
from urllib.parse import urlencode

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Update, WebAppInfo
from telegram.ext import ContextTypes

from core.background import fire_and_forget, fire_and_forget_sync, run_sync
from config import BOT_BRAND, CHAPTERS_PER_PAGE, PDF_BULK_SUBSCRIBE_URL, PREFERRED_CHAPTER_LANG, WEBAPP_BASE_URL
from core.pdf_queue import PdfJob, enqueue_pdf_job
from handlers.pdf_bulk import (
    can_use_pdf_bulk,
    normalize_pdf_bulk_order,
    request_pdf_bulk_for_title,
    stop_pdf_bulk,
)
from handlers.search import edit_search_page, render_search_page
from services.catalog_client import (
    flatten_chapters,
    get_cached_chapter_reader_payload,
    get_cached_title_overview,
    get_cached_title_bundle,
    get_cached_title_summary,
    get_chapter_reader_payload,
    get_title_overview,
    get_title_bundle,
    prefetch_reader_payloads,
    prefetch_title_bundles,
)
from services.cakto_gateway import get_checkout_options
from services.cakto_api import cakto_api_configured, verify_cakto_payment_for_user
from services.metrics import (
    get_last_read_entry,
    get_read_chapter_ids,
    log_event,
    mark_chapter_read,
)
from services.telegraph_service import get_cached_chapter_page_url, get_or_create_chapter_page

CALLBACK_COOLDOWN = 0.8
TELEGRAPH_INLINE_WAIT = 1.15
TITLE_PANEL_FAST_TIMEOUT = 8.0
CHAPTER_PANEL_FAST_TIMEOUT = 16.0
SUPPORT_BOT_URL = "https://t.me/QGSuporteBot"

_USER_CALLBACK_LOCKS: dict[int, asyncio.Lock] = {}
_MESSAGE_EDIT_LOCKS: dict[str, asyncio.Lock] = {}
_MESSAGE_INFLIGHT_ACTIONS: dict[str, str] = {}
_MESSAGE_PANEL_STATE: dict[str, tuple[str, str]] = {}


def _now() -> float:
    return time.monotonic()


def _callback_last_key(user_id: int) -> str:
    return f"callback_last:{user_id}"


def _callback_data_last_key(user_id: int) -> str:
    return f"callback_data_last:{user_id}"


def _user_lock(user_id: int) -> asyncio.Lock:
    lock = _USER_CALLBACK_LOCKS.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        _USER_CALLBACK_LOCKS[user_id] = lock
    return lock


def _message_lock(chat_id: int, message_id: int) -> asyncio.Lock:
    key = f"{chat_id}:{message_id}"
    lock = _MESSAGE_EDIT_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _MESSAGE_EDIT_LOCKS[key] = lock
    return lock


def _action_signature(data: str) -> str:
    parts = (data or "").split("|")
    if len(parts) >= 3:
        return "|".join(parts[:3])
    return data or ""


def _message_action_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}:{message_id}"


def _get_inflight_action(chat_id: int, message_id: int) -> str:
    return _MESSAGE_INFLIGHT_ACTIONS.get(_message_action_key(chat_id, message_id), "")


def _set_inflight_action(chat_id: int, message_id: int, action: str) -> None:
    _MESSAGE_INFLIGHT_ACTIONS[_message_action_key(chat_id, message_id)] = action


def _clear_inflight_action(chat_id: int, message_id: int) -> None:
    _MESSAGE_INFLIGHT_ACTIONS.pop(_message_action_key(chat_id, message_id), None)


def _set_panel_state(chat_id: int, message_id: int, kind: str, ref: str) -> None:
    _MESSAGE_PANEL_STATE[_message_action_key(chat_id, message_id)] = (kind, ref)


def _get_panel_state(chat_id: int, message_id: int) -> tuple[str, str]:
    return _MESSAGE_PANEL_STATE.get(_message_action_key(chat_id, message_id), ("", ""))


def _is_callback_cooldown(context: ContextTypes.DEFAULT_TYPE, user_id: int, data: str) -> bool:
    last_ts = context.user_data.get(_callback_last_key(user_id), 0.0)
    last_data = context.user_data.get(_callback_data_last_key(user_id), "")
    now = _now()

    if data and last_data == data and (now - last_ts) < CALLBACK_COOLDOWN:
        return True

    context.user_data[_callback_last_key(user_id)] = now
    context.user_data[_callback_data_last_key(user_id)] = data
    return False


async def _safe_answer_query(query, text: str | None = None, show_alert: bool = False) -> None:
    try:
        if text is None:
            await query.answer()
        else:
            await query.answer(text, show_alert=show_alert)
    except Exception:
        pass


def _pick_bundle_image(bundle: dict) -> str:
    return (
        bundle.get("cover_url")
        or bundle.get("background_url")
        or ""
    ).strip()


def _summary_latest_chapter(summary: dict) -> dict | None:
    latest = summary.get("latest_chapter")
    if isinstance(latest, dict):
        return latest

    chapter_id = str(summary.get("chapter_id") or "").strip()
    if not chapter_id:
        return None

    return {
        "chapter_id": chapter_id,
        "chapter_number": str(latest or "").strip(),
        "chapter_language": summary.get("language") or PREFERRED_CHAPTER_LANG,
    }


def _fallback_title_bundle(title_id: str, *, title: str = "Manga", summary: dict | None = None) -> dict:
    title_id = str(title_id or "").strip()
    summary = summary or get_cached_title_summary(title_id) or {}
    display_title = (
        summary.get("display_title")
        or summary.get("title")
        or title
        or "Manga"
    )
    cover_url = summary.get("cover_url") or ""

    return {
        "title_id": title_id,
        "title": display_title,
        "display_title": display_title,
        "cover_url": cover_url,
        "background_url": summary.get("background_url") or cover_url,
        "status": summary.get("status") or "carregando",
        "rating": summary.get("rating") or "",
        "chapters": [],
        "languages": [],
        "total_chapters": 0,
        "latest_chapter": _summary_latest_chapter(summary),
        "genres": [],
        "chapters_partial": True,
    }


async def _load_title_panel_bundle(title_id: str) -> dict:
    cached = get_cached_title_bundle(title_id) or get_cached_title_overview(title_id)
    if cached is not None:
        return cached

    summary = get_cached_title_summary(title_id)
    if summary is not None:
        prefetch_title_bundles([title_id], limit=1)
        return _fallback_title_bundle(title_id, summary=summary)

    try:
        return await asyncio.wait_for(get_title_overview(title_id), timeout=TITLE_PANEL_FAST_TIMEOUT)
    except Exception as error:
        print("[TITLE_PANEL][FAST_FALLBACK]", title_id, repr(error))
        prefetch_title_bundles([title_id], limit=1)
        return _fallback_title_bundle(title_id)


def _pick_chapter_image(chapter: dict) -> str:
    return (
        chapter.get("cover_url")
        or chapter.get("background_url")
        or ""
    ).strip()


def _truncate(text: str, limit: int = 320) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _display_score(bundle: dict) -> str:
    score = bundle.get("anilist_score") or bundle.get("rating") or ""
    if score in ("", None, 0):
        return ""
    return str(score)


def _miniapp_url(
    *,
    title_id: str = "",
    chapter_id: str = "",
    page: str = "",
    route: str = "",
    source: str = "bot",
) -> str:
    params: dict[str, str] = {"source": source}

    if title_id:
        tid = str(title_id).strip()
        params["title_id"] = tid
        params["manga_id"] = tid
        params["id"] = tid
        params["title"] = tid

    if chapter_id:
        cid = str(chapter_id).strip()
        params["chapter_id"] = cid
        params["cap"] = cid
        params["read"] = cid
        params["chapter"] = cid

    if page:
        pg = str(page).strip()
        params["page"] = pg
        params["view"] = pg

    resolved_route = route.strip() if route else ""
    if not resolved_route:
        if chapter_id:
            resolved_route = "reader"
        elif title_id and page == "chapters":
            resolved_route = "chapters"
        elif title_id:
            resolved_route = "detail"
        else:
            resolved_route = "home"

    params["route"] = resolved_route

    base = WEBAPP_BASE_URL.rstrip("/")
    query = urlencode(params)
    return f"{base}/miniapp/index.html?{query}" if query else f"{base}/miniapp/index.html"


def _title_text(bundle: dict, last_read: dict | None = None) -> str:
    title = html.escape(bundle.get("title") or "Manga")
    status = html.escape(bundle.get("status") or bundle.get("anilist_status") or "N/A")
    chapters = html.escape(str(bundle.get("total_chapters") or bundle.get("anilist_chapters") or "?"))
    score = _display_score(bundle)
    genres = bundle.get("genres") or []
    genres_text = html.escape(", ".join(str(item) for item in genres[:4])) if genres else "N/A"

    meta = [
        f"» <b>Status:</b> <i>{status}</i>",
        f"» <b>Capítulos:</b> <i>{chapters}</i>",
    ]
    if score:
        meta.append(f"» <b>Nota:</b> <i>{html.escape(score)}</i>")
    if last_read and last_read.get("chapter_number"):
        meta.append(f"» <b>Continuar de:</b> <i>Capítulo {html.escape(last_read['chapter_number'])}</i>")

    footer = "✨ <i>Escolha abaixo como quer continuar.</i>"
    if bundle.get("chapters_partial"):
        footer = "⏳ <i>Abri a obra. A lista completa ainda está carregando.</i>"

    return (
        f"📚 <b>{title}</b>\n\n"
        f"{chr(10).join(meta)}\n"
        f"» <b>Generos:</b> <i>{genres_text}</i>\n\n"
        f"{footer}"
    )


def _title_keyboard(bundle: dict, last_read: dict | None = None, user_id: int | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    title_id = str(bundle.get("title_id") or "").strip()
    latest_chapter = bundle.get("latest_chapter") or {}

    primary_row: list[InlineKeyboardButton] = []

    if last_read and last_read.get("chapter_id"):
        primary_row.append(
            InlineKeyboardButton(
                "⏱ Continuar",
                web_app=WebAppInfo(
                    url=_miniapp_url(
                        title_id=title_id,
                        chapter_id=last_read["chapter_id"],
                        route="reader",
                    )
                ),
            )
        )

    if latest_chapter.get("chapter_id"):
        primary_row.append(
            InlineKeyboardButton(
                "🆕 Último capítulo",
                web_app=WebAppInfo(
                    url=_miniapp_url(
                        title_id=title_id,
                        chapter_id=latest_chapter["chapter_id"],
                        route="reader",
                    )
                ),
            )
        )

    if primary_row:
        rows.append(primary_row[:2])

    rows.append(
        [
            InlineKeyboardButton(
                "📚 Lista de capítulos",
                web_app=WebAppInfo(
                    url=_miniapp_url(
                        title_id=title_id,
                        page="chapters",
                        route="chapters",
                    )
                ),
            )
        ]
    )

    if bundle.get("anilist_url"):
        rows.append([InlineKeyboardButton("📖 Descrição", url=bundle["anilist_url"])])

    if title_id:
        rows.append([InlineKeyboardButton("📥 Ler offline", callback_data=f"mb|offline|{title_id}")])

    return InlineKeyboardMarkup(rows)


def _offline_text(bundle: dict) -> str:
    title = html.escape(bundle.get("title") or "Manga")
    chapters = html.escape(str(bundle.get("total_chapters") or "?"))
    return (
        f"📥 <b>Ler offline</b>\n\n"
        f"» <b>Obra:</b> <i>{title}</i>\n"
        f"» <b>Capítulos:</b> <i>{chapters}</i>\n\n"
        "✨ <i>Escolha como quer receber os PDFs.</i>"
    )


def _offline_keyboard(bundle: dict) -> InlineKeyboardMarkup:
    title_id = str(bundle.get("title_id") or "").strip()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Primeiro ao último", callback_data=f"mb|pdfall|{title_id}|asc")],
            [InlineKeyboardButton("📥 Último ao primeiro", callback_data=f"mb|pdfall|{title_id}|desc")],
            [InlineKeyboardButton("🔙 Voltar para a obra", callback_data=f"mb|title|{title_id}")],
        ]
    )


def _normalize_url(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://", "tg://")):
        return url
    return f"https://{url}"


def _offline_locked_text(bundle: dict) -> str:
    title = html.escape(bundle.get("title") or "Manga")
    brand = html.escape(BOT_BRAND or "Mangas Baltigo")
    return (
        f"🔒 <b>Conteúdo exclusivo para assinantes do {brand}</b>\n\n"
        f"» <b>Obra:</b> <i>{title}</i>\n\n"
        "A leitura offline com todos os capítulos em PDF está bloqueada aqui no bot.\n\n"
        "Escolha um plano abaixo. Assim que a Cakto aprovar o pagamento, "
        "o bot libera seu ID automaticamente."
    )


def _offline_locked_keyboard(bundle: dict, user_id: int | None) -> InlineKeyboardMarkup | None:
    options = get_checkout_options(user_id)
    title_id = str(bundle.get("title_id") or "").strip()
    if options:
        rows = [[InlineKeyboardButton(option["label"], url=option["url"])] for option in options]
        if title_id:
            rows.append([InlineKeyboardButton("🔄 Já paguei / verificar", callback_data=f"mb|paycheck|{title_id}")])
        rows.append([InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)])
        return InlineKeyboardMarkup(rows)

    subscribe_url = _normalize_url(PDF_BULK_SUBSCRIBE_URL)
    if not subscribe_url:
        if title_id:
            return InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 Já paguei / verificar", callback_data=f"mb|paycheck|{title_id}")],
                    [InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)],
                ]
            )
        return None
    brand = BOT_BRAND or "Mangas Baltigo"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"✨ Assinar {brand}", url=subscribe_url)],
            [InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)],
        ]
    )


async def _send_offline_locked(target, bundle: dict, user_id: int | None) -> None:
    text = _offline_locked_text(bundle)
    keyboard = _offline_locked_keyboard(bundle, user_id)

    message = getattr(target, "message", None)
    if message:
        try:
            await message.reply_text(
                text,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
            return
        except Exception:
            pass

    try:
        await target.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception:
        pass


def _chapter_list_text(bundle: dict, page: int, total_items: int) -> str:
    total_pages = max(1, ((total_items - 1) // CHAPTERS_PER_PAGE) + 1)
    if bundle.get("chapters_partial") and total_items <= 0:
        return (
            f"📖 <b>{html.escape(bundle.get('title') or 'Manga')}</b>\n\n"
            "» <b>Status:</b> <i>carregando capítulos</i>\n\n"
            "A fonte demorou para responder. Tente novamente em alguns segundos."
        )

    return (
        f"📖 <b>{html.escape(bundle.get('title') or 'Manga')}</b>\n\n"
        f"» <b>Página:</b> <i>{page}/{total_pages}</i>\n"
        f"» <b>Capítulos disponíveis:</b> <i>{total_items}</i>\n\n"
        "Toque em um capítulo abaixo.\n"
        "✅ = capítulo já lido"
    )


def _chapter_button_label(item: dict, read_ids: set[str]) -> str:
    number = str(item.get("chapter_number") or "?").strip()
    return f"✅ {number}" if item.get("chapter_id") in read_ids else number


def _chapter_list_keyboard(bundle: dict, chapters: list[dict], page: int, read_ids: set[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    total_items = len(chapters)
    total_pages = max(1, ((total_items - 1) // CHAPTERS_PER_PAGE) + 1)
    start = (page - 1) * CHAPTERS_PER_PAGE
    end = min(start + CHAPTERS_PER_PAGE, total_items)
    page_items = chapters[start:end]

    line: list[InlineKeyboardButton] = []
    for item in page_items:
        line.append(
            InlineKeyboardButton(
                _chapter_button_label(item, read_ids),
                web_app=WebAppInfo(
                    url=_miniapp_url(
                        title_id=bundle["title_id"],
                        chapter_id=item["chapter_id"],
                        route="reader",
                    )
                ),
            )
        )
        if len(line) == 3:
            rows.append(line)
            line = []
    if line:
        rows.append(line)

    if not chapters and bundle.get("chapters_partial"):
        rows.append([InlineKeyboardButton("⏳ Tentar novamente", callback_data=f"mb|chap|{bundle['title_id']}|1")])

    rows.append([InlineKeyboardButton(f"{page}/{total_pages}", callback_data="mb|noop")])

    nav: list[InlineKeyboardButton] = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪", callback_data=f"mb|chap|{bundle['title_id']}|1"))
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"mb|chap|{bundle['title_id']}|{page - 1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"mb|chap|{bundle['title_id']}|{page + 1}"))
        nav.append(InlineKeyboardButton("⏩", callback_data=f"mb|chap|{bundle['title_id']}|{total_pages}"))
    if nav:
        rows.append(nav)

    rows.append(
        [
            InlineKeyboardButton(
                "🔙 Voltar para a obra",
                web_app=WebAppInfo(
                    url=_miniapp_url(title_id=bundle["title_id"], route="detail")
                ),
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


def _chapter_text(chapter: dict) -> str:
    title = html.escape(chapter.get("title") or "Manga")
    chapter_number = html.escape(chapter.get("chapter_number") or "?")
    lang = html.escape((chapter.get("chapter_language") or PREFERRED_CHAPTER_LANG).upper())
    image_count = html.escape(str(chapter.get("image_count") or 0))

    return (
        f"📖 <b>{title}</b>\n\n"
        f"» <b>Capítulo:</b> <i>{chapter_number}</i>\n"
        f"» <b>Idioma:</b> <i>{lang}</i>\n"
        f"» <b>Páginas:</b> <i>{image_count}</i>\n\n"
        "✨ <i>Escolha abaixo se quer leitura rápida ou PDF.</i>"
    )


def _chapter_keyboard(chapter: dict, telegraph_url: str = "", *, telegraph_pending: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    rows.append(
        [
            InlineKeyboardButton(
                "📰 Abrir leitura rápida" if telegraph_url else ("⏳ Preparando leitura rápida" if telegraph_pending else "📰 Leitura rápida"),
                web_app=WebAppInfo(
                    url=_miniapp_url(
                        title_id=chapter["title_id"],
                        chapter_id=chapter["chapter_id"],
                        route="reader",
                    )
                ),
            )
        ]
    )
    rows.append([InlineKeyboardButton("📄 Baixar PDF", callback_data=f"mb|pdf|{chapter['chapter_id']}")])

    nav: list[InlineKeyboardButton] = []
    if chapter.get("previous_chapter"):
        nav.append(
            InlineKeyboardButton(
                "⬅️ Anterior",
                web_app=WebAppInfo(
                    url=_miniapp_url(
                        title_id=chapter["title_id"],
                        chapter_id=chapter["previous_chapter"]["chapter_id"],
                        route="reader",
                    )
                ),
            )
        )
    if chapter.get("next_chapter"):
        nav.append(
            InlineKeyboardButton(
                "Próximo ➡️",
                web_app=WebAppInfo(
                    url=_miniapp_url(
                        title_id=chapter["title_id"],
                        chapter_id=chapter["next_chapter"]["chapter_id"],
                        route="reader",
                    )
                ),
            )
        )
    if nav:
        rows.append(nav)

    rows.append(
        [
            InlineKeyboardButton(
                "📚 Ver capítulos",
                web_app=WebAppInfo(
                    url=_miniapp_url(title_id=chapter["title_id"], page="chapters", route="chapters")
                ),
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                "🔙 Voltar para a obra",
                web_app=WebAppInfo(
                    url=_miniapp_url(title_id=chapter["title_id"], route="detail")
                ),
            )
        ]
    )
    return InlineKeyboardMarkup(rows)


def _loading_keyboard(label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="mb|noop")]])


def _payment_check_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⏳ Verificando pagamento", callback_data="mb|noop")],
            [InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)],
        ]
    )


async def _show_loading_markup(query, label: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    try:
        await query.edit_message_reply_markup(reply_markup=reply_markup or _loading_keyboard(label))
    except Exception:
        pass


async def _restore_reply_markup(query, reply_markup) -> None:
    if reply_markup is None:
        return
    try:
        await query.edit_message_reply_markup(reply_markup=reply_markup)
    except Exception:
        pass


async def _render_panel(target, text: str, keyboard: InlineKeyboardMarkup, photo: str = "", *, edit: bool):
    if edit:
        if photo:
            media = InputMediaPhoto(media=photo, caption=text, parse_mode="HTML")
            try:
                await target.edit_message_media(media=media, reply_markup=keyboard)
                return target.message
            except Exception:
                pass
        try:
            await target.edit_message_caption(caption=text, parse_mode="HTML", reply_markup=keyboard)
            return target.message
        except Exception:
            pass
        try:
            await target.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )
            return target.message
        except Exception:
            pass
        if photo:
            try:
                return await target.message.reply_photo(photo=photo, caption=text, parse_mode="HTML", reply_markup=keyboard)
            except Exception:
                pass
        return await target.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)

    if photo:
        try:
            return await target.reply_photo(photo=photo, caption=text, parse_mode="HTML", reply_markup=keyboard)
        except Exception:
            pass
    return await target.reply_text(text, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)


async def _render_panel_to_message(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    message_id: int,
    text: str,
    keyboard: InlineKeyboardMarkup,
    photo: str = "",
) -> None:
    bot = context.bot

    if photo:
        media = InputMediaPhoto(media=photo, caption=text, parse_mode="HTML")
        try:
            await bot.edit_message_media(chat_id=chat_id, message_id=message_id, media=media, reply_markup=keyboard)
            return
        except Exception:
            pass

    try:
        await bot.edit_message_caption(
            chat_id=chat_id,
            message_id=message_id,
            caption=text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        return
    except Exception:
        pass

    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
    except Exception:
        pass


def _chapter_telegraph_title(chapter: dict) -> str:
    return f"{chapter.get('title') or 'Manga'} - Capítulo {chapter.get('chapter_number') or '?'}"


async def _auto_finalize_telegraph_panel(
    context: ContextTypes.DEFAULT_TYPE,
    panel_message,
    chapter: dict,
    telegraph_task: asyncio.Task,
) -> None:
    if not panel_message:
        return

    try:
        url = await telegraph_task
    except Exception:
        return

    chat_id = getattr(getattr(panel_message, "chat", None), "id", None)
    message_id = getattr(panel_message, "message_id", None)
    if chat_id is None or message_id is None:
        return

    async with _message_lock(chat_id, message_id):
        kind, ref = _get_panel_state(chat_id, message_id)
        if kind != "chapter" or ref != (chapter.get("chapter_id") or ""):
            return

        await _render_panel_to_message(
            context,
            chat_id=chat_id,
            message_id=message_id,
            text=_chapter_text(chapter),
            keyboard=_chapter_keyboard(chapter, telegraph_url=url),
            photo=_pick_chapter_image(chapter),
        )


async def send_title_panel(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, user_id: int | None, *, edit: bool):
    bundle = await _load_title_panel_bundle(title_id)

    fire_and_forget(get_title_bundle(title_id))

    last_read = await run_sync(get_last_read_entry, user_id, bundle["title_id"]) if user_id else None

    chapters = flatten_chapters({"chapters": bundle.get("chapters") or []}, PREFERRED_CHAPTER_LANG)
    if not bundle.get("chapters"):
        prefetch_title_bundles([title_id], limit=1)
    else:
        latest = bundle.get("latest_chapter") or {}
        chapter_ids = [latest.get("chapter_id") or ""]
        chapter_ids.extend(item.get("chapter_id") or "" for item in chapters[:3])
        prefetch_reader_payloads(chapter_ids, limit=4)

    if user_id:
        fire_and_forget_sync(
            log_event,
            event_type="title_open",
            user_id=user_id,
            title_id=bundle["title_id"],
            title_name=bundle.get("title") or "",
        )

    panel_message = await _render_panel(
        target,
        _title_text(bundle, last_read),
        _title_keyboard(bundle, last_read, user_id),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "title", bundle["title_id"])


async def send_offline_panel(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, user_id: int | None, *, edit: bool):
    bundle = get_cached_title_bundle(title_id)
    if bundle is None:
        bundle = await get_title_bundle(title_id)

    if not can_use_pdf_bulk(user_id):
        await _send_offline_locked(target, bundle, user_id)
        return

    panel_message = await _render_panel(
        target,
        _offline_text(bundle),
        _offline_keyboard(bundle),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "offline", bundle["title_id"])


async def verify_offline_payment_panel(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, user_id: int):
    if not cakto_api_configured():
        await _send_offline_locked(target, await get_title_bundle(title_id), user_id)
        message = getattr(target, "message", None)
        try:
            await (message.reply_text if message else target.reply_text)(
                (
                    "⚠️ <b>Não consegui verificar pela API da Cakto.</b>\n\n"
                    "O bot ainda precisa de <code>CAKTO_CLIENT_ID</code> e "
                    "<code>CAKTO_CLIENT_SECRET</code> no ambiente para consultar pedidos.\n\n"
                    "Se você já pagou, chame o suporte para a liberação manual."
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)]]),
            )
        except Exception:
            pass
        return

    try:
        result = await verify_cakto_payment_for_user(user_id)
    except Exception:
        result = {"ok": False, "reason": "api_error"}

    if result.get("ok"):
        await send_offline_panel(target, context, title_id, user_id, edit=True)
        return

    reason = result.get("reason") or "not_found"
    if reason == "not_found":
        text = "Ainda não encontrei uma compra aprovada vinculada ao seu Telegram."
    elif reason == "order_not_paid":
        text = "Encontrei seu pedido, mas ele ainda não aparece como pago na Cakto."
    else:
        text = "Não consegui confirmar o pagamento agora."

    message = getattr(target, "message", None)
    try:
        await (message.reply_text if message else target.reply_text)(
            f"⏳ <b>Pagamento ainda não confirmado</b>\n\n{text}\n\n"
            "Se o Pix já saiu da sua conta, fale com o suporte para conferirmos manualmente.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛟 Suporte", url=SUPPORT_BOT_URL)]]),
        )
    except Exception:
        pass


async def send_chapters_page(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, page: int, user_id: int | None, *, edit: bool):
    bundle = get_cached_title_bundle(title_id)
    if bundle is None:
        try:
            bundle = await asyncio.wait_for(get_title_bundle(title_id), timeout=CHAPTER_PANEL_FAST_TIMEOUT)
        except Exception as error:
            print("[CHAPTER_PANEL][FAST_FALLBACK]", title_id, repr(error))
            prefetch_title_bundles([title_id], limit=1)
            bundle = _fallback_title_bundle(title_id)

    chapters = flatten_chapters({"chapters": bundle.get("chapters") or []}, PREFERRED_CHAPTER_LANG)
    read_ids = set(await run_sync(get_read_chapter_ids, user_id, bundle["title_id"])) if user_id else set()

    total_pages = max(1, ((len(chapters) - 1) // CHAPTERS_PER_PAGE) + 1)
    page = max(1, min(page, total_pages))

    start = (page - 1) * CHAPTERS_PER_PAGE
    end = min(start + CHAPTERS_PER_PAGE, len(chapters))
    visible_ids = [(item.get("chapter_id") or "") for item in chapters[start:end]]
    if visible_ids:
        prefetch_reader_payloads(visible_ids, limit=len(visible_ids))

    panel_message = await _render_panel(
        target,
        _chapter_list_text(bundle, page, len(chapters)),
        _chapter_list_keyboard(bundle, chapters, page, read_ids),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "chapters", bundle["title_id"])


async def send_chapter_panel(target, context: ContextTypes.DEFAULT_TYPE, chapter_id: str, user_id: int | None, *, edit: bool):
    chapter = get_cached_chapter_reader_payload(chapter_id) or await get_chapter_reader_payload(chapter_id)

    adjacent_refs = [
        (chapter.get("previous_chapter") or {}).get("chapter_id") or "",
        (chapter.get("next_chapter") or {}).get("chapter_id") or "",
    ]
    prefetch_reader_payloads(adjacent_refs, limit=2)

    fire_and_forget(get_title_bundle(chapter["title_id"]))

    if user_id:
        fire_and_forget_sync(
            mark_chapter_read,
            user_id=user_id,
            title_id=chapter["title_id"],
            chapter_id=chapter["chapter_id"],
            chapter_number=chapter.get("chapter_number") or "",
            title_name=chapter.get("title") or "",
            chapter_url="",
        )
        fire_and_forget_sync(
            log_event,
            event_type="chapter_open",
            user_id=user_id,
            title_id=chapter["title_id"],
            title_name=chapter.get("title") or "",
            chapter_id=chapter["chapter_id"],
            chapter_number=chapter.get("chapter_number") or "",
        )

    telegraph_url = get_cached_chapter_page_url(chapter["chapter_id"])
    telegraph_task: asyncio.Task | None = None

    if not telegraph_url:
        telegraph_task = asyncio.create_task(
            get_or_create_chapter_page(
                chapter_id=chapter["chapter_id"],
                title=_chapter_telegraph_title(chapter),
                images=chapter.get("images") or [],
            )
        )
        try:
            telegraph_url = await asyncio.wait_for(asyncio.shield(telegraph_task), timeout=TELEGRAPH_INLINE_WAIT)
        except asyncio.TimeoutError:
            telegraph_url = ""
        except Exception:
            telegraph_task = None

    panel_message = await _render_panel(
        target,
        _chapter_text(chapter),
        _chapter_keyboard(chapter, telegraph_url=telegraph_url, telegraph_pending=not bool(telegraph_url)),
        _pick_chapter_image(chapter),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "chapter", chapter["chapter_id"])

    if panel_message and not telegraph_url and telegraph_task is not None:
        fire_and_forget(_auto_finalize_telegraph_panel(context, panel_message, chapter, telegraph_task))


async def _send_telegraph(query, chapter_id: str):
    chapter = get_cached_chapter_reader_payload(chapter_id) or await get_chapter_reader_payload(chapter_id)
    url = await get_or_create_chapter_page(
        chapter_id=chapter["chapter_id"],
        title=_chapter_telegraph_title(chapter),
        images=chapter.get("images") or [],
    )
    panel_message = await _render_panel(
        query,
        _chapter_text(chapter),
        _chapter_keyboard(chapter, telegraph_url=url),
        _pick_chapter_image(chapter),
        edit=True,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "chapter", chapter["chapter_id"])


async def _enqueue_pdf(query, context: ContextTypes.DEFAULT_TYPE, chapter_id: str):
    chapter = get_cached_chapter_reader_payload(chapter_id) or await get_chapter_reader_payload(chapter_id)
    await enqueue_pdf_job(
        context.application,
        PdfJob(
            chat_id=query.message.chat_id,
            chapter_id=chapter["chapter_id"],
            chapter_number=chapter.get("chapter_number") or "",
            title_name=chapter.get("title") or "Manga",
            images=chapter.get("images") or [],
            caption=(
                f"📄 <b>{html.escape(chapter.get('title') or 'Manga')}</b>\n"
                f"Capítulo <code>{html.escape(chapter.get('chapter_number') or '?')}</code>\n"
                "@MangasBrasil"
            ),
        ),
    )
    return chapter


async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user

    if not query or not query.data:
        return
    if not query.data.startswith("mb|"):
        return

    if query.data == "mb|noop":
        await _safe_answer_query(query)
        return

    if not user:
        await _safe_answer_query(query, "Não consegui identificar seu usuário agora.", show_alert=True)
        return

    if _is_callback_cooldown(context, user.id, query.data):
        await _safe_answer_query(query, "⏳ Aguarde um instante antes de apertar de novo.", show_alert=False)
        return

    message = query.message
    user_lock = _user_lock(user.id)
    msg_lock = _message_lock(message.chat.id, message.message_id) if message else asyncio.Lock()

    current_action = _action_signature(query.data)
    if message and _get_inflight_action(message.chat.id, message.message_id) == current_action:
        await _safe_answer_query(query, "⏳ Essa ação já está sendo processada.", show_alert=False)
        return

    parts = query.data.split("|")
    action = parts[1] if len(parts) > 1 else ""
    user_id = user.id
    original_reply_markup = getattr(message, "reply_markup", None)

    async with user_lock:
        async with msg_lock:
            if message:
                if _get_inflight_action(message.chat.id, message.message_id) == current_action:
                    await _safe_answer_query(query, "⏳ Essa ação já está sendo processada.", show_alert=False)
                    return
                _set_inflight_action(message.chat.id, message.message_id, current_action)

            try:
                if action == "stopbulk" and len(parts) >= 3:
                    stopped = await stop_pdf_bulk(context, job_id=parts[2], user_id=user_id)
                    if stopped:
                        await _safe_answer_query(query, "Parando o download offline.", show_alert=False)
                    else:
                        await _safe_answer_query(query, "Esse lote já terminou ou expirou.", show_alert=True)
                    return

                if action == "title" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Abrindo obra")
                    await send_title_panel(query, context, parts[2], user_id, edit=True)
                    return

                if action == "offline" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    if can_use_pdf_bulk(user_id):
                        await _show_loading_markup(query, "⏳ Carregando offline")
                    await send_offline_panel(query, context, parts[2], user_id, edit=True)
                    return

                if action == "paycheck" and len(parts) >= 3:
                    await _safe_answer_query(query, "Verificando pagamento na Cakto...", show_alert=False)
                    await _show_loading_markup(query, "⏳ Verificando pagamento", reply_markup=_payment_check_keyboard())
                    await verify_offline_payment_panel(query, context, parts[2], user_id)
                    return

                if action == "chap" and len(parts) >= 4:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Carregando capítulos")
                    await send_chapters_page(query, context, parts[2], int(parts[3]), user_id, edit=True)
                    return

                if action == "read" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Abrindo capítulo")
                    await send_chapter_panel(query, context, parts[2], user_id, edit=True)
                    return

                if action == "sp" and len(parts) >= 4:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Carregando página")
                    rendered = render_search_page(context, parts[2], int(parts[3]))
                    if not rendered:
                        await _safe_answer_query(query, "Essa busca expirou. Faz outra busca pra continuar.", show_alert=True)
                        return
                    await edit_search_page(query, rendered)
                    return

                if action == "tg" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Preparando leitura rápida")
                    await _send_telegraph(query, parts[2])
                    return

                if action == "pdfall" and len(parts) >= 3:
                    if not can_use_pdf_bulk(user_id):
                        await _safe_answer_query(query, "Função liberada só para assinantes.", show_alert=True)
                        return
                    await _safe_answer_query(query, "Pedido recebido. Vou preparar os PDFs.", show_alert=False)
                    await request_pdf_bulk_for_title(
                        context,
                        chat_id=message.chat.id if message else user_id,
                        user_id=user_id,
                        title_ref=parts[2],
                        order=normalize_pdf_bulk_order(parts[3] if len(parts) >= 4 else "asc"),
                    )
                    return

                if action == "pdf" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Preparando PDF")
                    chapter = await _enqueue_pdf(query, context, parts[2])
                    await _render_panel(
                        query,
                        _chapter_text(chapter),
                        _chapter_keyboard(
                            chapter,
                            telegraph_url=get_cached_chapter_page_url(chapter["chapter_id"]),
                            telegraph_pending=not bool(get_cached_chapter_page_url(chapter["chapter_id"])),
                        ),
                        _pick_chapter_image(chapter),
                        edit=True,
                    )
                    return

                await _safe_answer_query(query, "Ação inválida ou expirada.", show_alert=False)

            except ValueError:
                await _safe_answer_query(query, "Dados inválidos nessa ação.", show_alert=True)
                await _restore_reply_markup(query, original_reply_markup)
            except Exception:
                await _safe_answer_query(query, "Ocorreu um erro ao processar essa ação.", show_alert=True)
                await _restore_reply_markup(query, original_reply_markup)
            finally:
                if message:
                    _clear_inflight_action(message.chat.id, message.message_id)
