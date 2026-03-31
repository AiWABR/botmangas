import asyncio
import html
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, Update, WebAppInfo
from telegram.ext import ContextTypes

from core.background import fire_and_forget
from config import CHAPTERS_PER_PAGE, PREFERRED_CHAPTER_LANG, WEBAPP_BASE_URL
from core.background import fire_and_forget_sync, run_sync
from core.pdf_queue import PdfJob, enqueue_pdf_job
from handlers.search import edit_search_page, render_search_page
from services.catalog_client import (
    flatten_chapters,
    get_cached_chapter_reader_payload,
    get_cached_title_overview,
    get_cached_title_bundle,
    get_chapter_reader_payload,
    get_title_overview,
    get_title_bundle,
    prefetch_reader_payloads,
    prefetch_title_bundles,
)
from services.metrics import (
    get_last_read_entry,
    get_read_chapter_ids,
    log_event,
    mark_chapter_read,
)
from services.telegraph_service import get_cached_chapter_page_url, get_or_create_chapter_page

CALLBACK_COOLDOWN = 0.8
TELEGRAPH_INLINE_WAIT = 1.6

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


def _title_text(bundle: dict, last_read: dict | None = None) -> str:
    title = html.escape(bundle.get("title") or "Manga")
    status = html.escape(bundle.get("status") or bundle.get("anilist_status") or "N/A")
    chapters = html.escape(str(bundle.get("total_chapters") or bundle.get("anilist_chapters") or "?"))
    score = _display_score(bundle)
    genres = bundle.get("genres") or []
    genres_text = html.escape(", ".join(str(item) for item in genres[:4])) if genres else "N/A"
    description = html.escape(_truncate(bundle.get("description") or bundle.get("anilist_description") or "Sem descricao disponivel.", 220))

    meta = [
        f"» <b>Status:</b> <i>{status}</i>",
        f"» <b>Capitulos:</b> <i>{chapters}</i>",
    ]
    if score:
        meta.append(f"» <b>Nota:</b> <i>{html.escape(score)}</i>")
    if last_read and last_read.get("chapter_number"):
        meta.append(f"» <b>Continuar de:</b> <i>Capitulo {html.escape(last_read['chapter_number'])}</i>")

    return (
        f"📚 <b>{title}</b>\n\n"
        f"{chr(10).join(meta)}\n"
        f"» <b>Generos:</b> <i>{genres_text}</i>\n\n"
        "✨ <i>Escolha abaixo como quer continuar.</i>"
    )


def _title_keyboard(bundle: dict, last_read: dict | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    title_id = bundle.get("title_id") or ""
    latest_chapter = bundle.get("latest_chapter") or {}

    primary_row = []
    if last_read and last_read.get("chapter_id"):
        primary_row.append(
            InlineKeyboardButton(
                "⏱ Continuar",
                web_app=WebAppInfo(
                    url=f"{WEBAPP_BASE_URL}/miniapp/index.html?title_id={title_id}&chapter_id={last_read['chapter_id']}"
                ),
            )
        )
    if latest_chapter.get("chapter_id"):
        primary_row.append(
            InlineKeyboardButton(
                "🆕 Ultimo capitulo",
                web_app=WebAppInfo(
                    url=f"{WEBAPP_BASE_URL}/miniapp/index.html?title_id={title_id}&chapter_id={latest_chapter['chapter_id']}"
                ),
            )
        )
    if primary_row:
        rows.append(primary_row[:2])

    rows.append([InlineKeyboardButton("📚 Lista de capitulos", callback_data=f"mb|chap|{title_id}|1")])

    if bundle.get("anilist_url"):
        rows.append([InlineKeyboardButton("📖 Descrição", url=bundle["anilist_url"])])

    return InlineKeyboardMarkup(rows)


def _chapter_list_text(bundle: dict, page: int, total_items: int) -> str:
    total_pages = max(1, ((total_items - 1) // CHAPTERS_PER_PAGE) + 1)
    return (
        f"📖 <b>{html.escape(bundle.get('title') or 'Manga')}</b>\n\n"
        f"» <b>Pagina:</b> <i>{page}/{total_pages}</i>\n"
        f"» <b>Capitulos disponiveis:</b> <i>{total_items}</i>\n\n"
        "Toque em um capitulo abaixo.\n"
        "✅ = capitulo ja lido"
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
                    url=f"{WEBAPP_BASE_URL}/miniapp/index.html?title_id={bundle['title_id']}&chapter_id={item['chapter_id']}"
                ),
            )
        )
        if len(line) == 3:
            rows.append(line)
            line = []
    if line:
        rows.append(line)

    rows.append([InlineKeyboardButton(f"{page}/{total_pages}", callback_data="mb|noop")])

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⏪", callback_data=f"mb|chap|{bundle['title_id']}|1"))
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"mb|chap|{bundle['title_id']}|{page - 1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"mb|chap|{bundle['title_id']}|{page + 1}"))
        nav.append(InlineKeyboardButton("⏩", callback_data=f"mb|chap|{bundle['title_id']}|{total_pages}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🔙 Voltar para a obra", callback_data=f"mb|title|{bundle['title_id']}")])
    return InlineKeyboardMarkup(rows)


def _chapter_text(chapter: dict) -> str:
    title = html.escape(chapter.get("title") or "Manga")
    chapter_number = html.escape(chapter.get("chapter_number") or "?")
    lang = html.escape((chapter.get("chapter_language") or PREFERRED_CHAPTER_LANG).upper())
    image_count = html.escape(str(chapter.get("image_count") or 0))

    return (
        f"📖 <b>{title}</b>\n\n"
        f"» <b>Capitulo:</b> <i>{chapter_number}</i>\n"
        f"» <b>Idioma:</b> <i>{lang}</i>\n"
        f"» <b>Paginas:</b> <i>{image_count}</i>\n\n"
        "✨ <i>Escolha abaixo se quer leitura rapida ou PDF.</i>"
    )


def _chapter_keyboard(chapter: dict, telegraph_url: str = "", *, telegraph_pending: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    rows.append(
        [
            InlineKeyboardButton(
                "📰 Abrir leitura rapida" if telegraph_url else ("⏳ Preparando leitura rapida" if telegraph_pending else "📰 Leitura rapida"),
                web_app=WebAppInfo(
                    url=f"{WEBAPP_BASE_URL}/miniapp/index.html?title_id={chapter['title_id']}&chapter_id={chapter['chapter_id']}"
                ),
            )
        ]
    )
    rows.append([InlineKeyboardButton("📄 Baixar PDF", callback_data=f"mb|pdf|{chapter['chapter_id']}")])

    nav = []
    if chapter.get("previous_chapter"):
        nav.append(
            InlineKeyboardButton(
                "⬅️ Anterior",
                web_app=WebAppInfo(
                    url=f"{WEBAPP_BASE_URL}/miniapp/index.html?title_id={chapter['title_id']}&chapter_id={chapter['previous_chapter']['chapter_id']}"
                ),
            )
        )
    if chapter.get("next_chapter"):
        nav.append(
            InlineKeyboardButton(
                "Proximo ➡️",
                web_app=WebAppInfo(
                    url=f"{WEBAPP_BASE_URL}/miniapp/index.html?title_id={chapter['title_id']}&chapter_id={chapter['next_chapter']['chapter_id']}"
                ),
            )
        )
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("📚 Ver capitulos", callback_data=f"mb|chap|{chapter['title_id']}|1")])
    rows.append([InlineKeyboardButton("🔙 Voltar para a obra", callback_data=f"mb|title|{chapter['title_id']}")])
    return InlineKeyboardMarkup(rows)


def _loading_keyboard(label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="mb|noop")]])


async def _show_loading_markup(query, label: str) -> None:
    try:
        await query.edit_message_reply_markup(reply_markup=_loading_keyboard(label))
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
    return f"{chapter.get('title') or 'Manga'} - Capitulo {chapter.get('chapter_number') or '?'}"


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
    bundle = get_cached_title_bundle(title_id) or get_cached_title_overview(title_id)
    if bundle is None:
        bundle = await get_title_overview(title_id)

    last_read = await run_sync(get_last_read_entry, user_id, bundle["title_id"]) if user_id else None

    if not bundle.get("chapters"):
        prefetch_title_bundles([title_id], limit=1)
    else:
        latest = bundle.get("latest_chapter") or {}
        chapter_ids = [latest.get("chapter_id") or ""]
        chapter_ids.extend(
            item.get("chapter_id") or ""
            for item in flatten_chapters({"chapters": bundle.get("chapters") or []}, PREFERRED_CHAPTER_LANG)[:2]
        )
        prefetch_reader_payloads(chapter_ids, limit=3)

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
        _title_keyboard(bundle, last_read),
        _pick_bundle_image(bundle),
        edit=edit,
    )
    if panel_message:
        _set_panel_state(panel_message.chat.id, panel_message.message_id, "title", bundle["title_id"])


async def send_chapters_page(target, context: ContextTypes.DEFAULT_TYPE, title_id: str, page: int, user_id: int | None, *, edit: bool):
    bundle = get_cached_title_bundle(title_id) or await get_title_bundle(title_id)
    chapters = flatten_chapters({"chapters": bundle.get("chapters") or []}, PREFERRED_CHAPTER_LANG)
    read_ids = set(await run_sync(get_read_chapter_ids, user_id, bundle["title_id"])) if user_id else set()

    total_pages = max(1, ((len(chapters) - 1) // CHAPTERS_PER_PAGE) + 1)
    page = max(1, min(page, total_pages))

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
                f"Capitulo <code>{html.escape(chapter.get('chapter_number') or '?')}</code>\n"
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
        await _safe_answer_query(query, "Nao consegui identificar seu usuario agora.", show_alert=True)
        return

    if _is_callback_cooldown(context, user.id, query.data):
        await _safe_answer_query(query, "⏳ Aguarde um instante antes de apertar de novo.", show_alert=False)
        return

    message = query.message
    user_lock = _user_lock(user.id)
    msg_lock = _message_lock(message.chat.id, message.message_id) if message else asyncio.Lock()

    current_action = _action_signature(query.data)
    if message and _get_inflight_action(message.chat.id, message.message_id) == current_action:
        await _safe_answer_query(query, "⏳ Essa acao ja esta sendo processada...", show_alert=False)
        return

    parts = query.data.split("|")
    action = parts[1] if len(parts) > 1 else ""
    user_id = user.id
    original_reply_markup = getattr(message, "reply_markup", None)

    async with user_lock:
        async with msg_lock:
            if message:
                if _get_inflight_action(message.chat.id, message.message_id) == current_action:
                    await _safe_answer_query(query, "⏳ Essa acao ja esta sendo processada...", show_alert=False)
                    return
                _set_inflight_action(message.chat.id, message.message_id, current_action)

            try:
                if action == "title" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Abrindo obra")
                    await send_title_panel(query, context, parts[2], user_id, edit=True)
                    return

                if action == "chap" and len(parts) >= 4:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Carregando capitulos")
                    await send_chapters_page(query, context, parts[2], int(parts[3]), user_id, edit=True)
                    return

                if action == "read" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Abrindo capitulo")
                    await send_chapter_panel(query, context, parts[2], user_id, edit=True)
                    return

                if action == "sp" and len(parts) >= 4:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Carregando pagina")
                    rendered = render_search_page(context, parts[2], int(parts[3]))
                    if not rendered:
                        await _safe_answer_query(query, "Essa busca expirou. Faz outra busca pra continuar.", show_alert=True)
                        return
                    await edit_search_page(query, rendered)
                    return

                if action == "tg" and len(parts) >= 3:
                    await _safe_answer_query(query)
                    await _show_loading_markup(query, "⏳ Preparando leitura rapida")
                    await _send_telegraph(query, parts[2])
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

                await _safe_answer_query(query, "Acao desconhecida.", show_alert=True)
            except asyncio.TimeoutError:
                await _restore_reply_markup(query, original_reply_markup)
                await _safe_answer_query(query, "⏳ Demorou demais para carregar. Tente de novo.", show_alert=True)
            except Exception as error:
                print("ERRO CALLBACK MANGA:", repr(error))
                await _restore_reply_markup(query, original_reply_markup)
                await _safe_answer_query(query, "Nao consegui concluir essa acao agora.", show_alert=True)
                try:
                    await query.message.reply_text(
                        "❌ Nao consegui concluir essa acao agora.\n\nTente novamente em instantes.",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            finally:
                if message:
                    _clear_inflight_action(message.chat.id, message.message_id)
