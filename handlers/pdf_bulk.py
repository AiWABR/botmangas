from __future__ import annotations

import asyncio
import html
import re
import secrets
from dataclasses import dataclass, field
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import (
    DISTRIBUTION_TAG,
    PDF_BULK_ALLOWED_IDS,
    PDF_BULK_DELAY_SECONDS,
    PDF_BULK_MAX_CHAPTERS,
    PREFERRED_CHAPTER_LANG,
)
from core.background import fire_and_forget
from core.pdf_queue import PdfJob, enqueue_pdf_job
from services.catalog_client import (
    flatten_chapters,
    get_cached_title_bundle,
    get_chapter_reader_payload,
    get_title_bundle,
    search_titles,
)
from utils.gatekeeper import ensure_channel_membership

ORDER_ASC = "asc"
ORDER_DESC = "desc"

_ALLOWED_BULK_USERS = set(PDF_BULK_ALLOWED_IDS)
_ACTIVE_BULK_KEYS: dict[str, str] = {}
_BULK_STATES: dict[str, "PdfBulkState"] = {}
_TITLE_REF_RE = re.compile(r"(?:title-detail/[^/\s]*-)?([a-f0-9]{20,32})", re.IGNORECASE)


@dataclass
class PdfBulkState:
    job_id: str
    active_key: str
    chat_id: int
    user_id: int
    title_ref: str
    order: str
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None
    status_message: Any | None = None
    title_name: str = "Manga"
    total: int = 0
    enqueued: int = 0
    failed: int = 0
    failed_numbers: list[str] = field(default_factory=list)


def can_use_pdf_bulk(user_id: int | None) -> bool:
    return bool(user_id and int(user_id) in _ALLOWED_BULK_USERS)


def normalize_pdf_bulk_order(order: str | None) -> str:
    return ORDER_DESC if str(order or "").strip().lower() in {"desc", "reverse", "latest"} else ORDER_ASC


def _extract_title_ref(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    match = _TITLE_REF_RE.search(text)
    if match:
        return match.group(1)
    return ""


def _bulk_key(user_id: int, title_ref: str) -> str:
    return f"{int(user_id)}:{title_ref.strip().lower()}"


def _stop_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Parar download", callback_data=f"mb|stopbulk|{job_id}")]]
    )


def _limited_chapters(chapters: list[dict]) -> tuple[list[dict], bool]:
    max_items = max(0, int(PDF_BULK_MAX_CHAPTERS or 0))
    if not max_items:
        return chapters, False
    return chapters[:max_items], len(chapters) > max_items


def _order_label(order: str) -> str:
    if normalize_pdf_bulk_order(order) == ORDER_DESC:
        return "do ultimo para o primeiro"
    return "do primeiro para o ultimo"


def _chapter_caption(chapter: dict) -> str:
    title = html.escape(chapter.get("title") or "Manga")
    number = html.escape(str(chapter.get("chapter_number") or "?"))
    tag = html.escape(DISTRIBUTION_TAG or "")
    return (
        f"<b>{title}</b>\n"
        f"Capitulo <code>{number}</code>\n"
        f"{tag}"
    ).strip()


async def _safe_edit(message, text: str, reply_markup=None) -> None:
    if not message:
        return
    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
    except Exception:
        pass


async def _safe_delete(message) -> None:
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


def _ordered_chapters(bundle: dict, title_id: str, order: str) -> list[dict]:
    return flatten_chapters(
        {"title_id": title_id, "chapters": bundle.get("chapters") or []},
        PREFERRED_CHAPTER_LANG,
        ascending=normalize_pdf_bulk_order(order) == ORDER_ASC,
    )


async def _edit_preparing_status(state: PdfBulkState, current_index: int = 0, current_chapter: str = "") -> None:
    lines = [
        "<b>Preparando PDFs offline</b>",
        "",
        f"<b>Obra:</b> {html.escape(state.title_name)}",
        f"<b>Ordem:</b> {_order_label(state.order)}",
    ]
    if state.total:
        lines.append(f"<b>Fila:</b> {current_index}/{state.total}")
    if current_chapter:
        lines.append(f"<b>Capitulo atual:</b> {html.escape(current_chapter)}")
    else:
        lines.append("<b>Status:</b> enfileirando...")

    await _safe_edit(state.status_message, "\n".join(lines), reply_markup=_stop_keyboard(state.job_id))


async def _run_pdf_bulk(app, state: PdfBulkState) -> None:
    was_limited = False
    stopped = False

    try:
        state.status_message = await app.bot.send_message(
            state.chat_id,
            (
                "<b>Lote de PDFs recebido</b>\n\n"
                "Vou carregar a obra e enfileirar os capitulos para leitura offline."
            ),
            parse_mode="HTML",
            reply_markup=_stop_keyboard(state.job_id),
        )

        bundle = get_cached_title_bundle(state.title_ref) or await get_title_bundle(state.title_ref)
        title_id = str(bundle.get("title_id") or state.title_ref).strip()
        state.title_name = bundle.get("title") or "Manga"
        chapters = _ordered_chapters(bundle, title_id, state.order)
        chapters, was_limited = _limited_chapters(chapters)
        state.total = len(chapters)

        if not chapters:
            await _safe_edit(
                state.status_message,
                f"<b>Nenhum capitulo encontrado</b>\n\nObra: {html.escape(state.title_name)}",
            )
            return

        await _edit_preparing_status(state)

        for index, item in enumerate(chapters, start=1):
            if state.cancel_event.is_set():
                stopped = True
                break

            chapter_number = str(item.get("chapter_number") or "?")
            if index == 1 or index == state.total or index % 5 == 0:
                await _edit_preparing_status(state, index, chapter_number)

            try:
                chapter = await get_chapter_reader_payload(
                    item.get("chapter_id") or "",
                    title_hint=title_id,
                )
                images = chapter.get("images") or []
                if not images:
                    raise RuntimeError("capitulo sem imagens")

                await enqueue_pdf_job(
                    app,
                    PdfJob(
                        chat_id=state.chat_id,
                        chapter_id=chapter["chapter_id"],
                        chapter_number=chapter.get("chapter_number") or chapter_number,
                        title_name=chapter.get("title") or state.title_name,
                        images=images,
                        caption=_chapter_caption(chapter),
                        is_bulk=True,
                        send_status=False,
                    ),
                )
                state.enqueued += 1
            except asyncio.CancelledError:
                stopped = True
                break
            except Exception:
                state.failed += 1
                if len(state.failed_numbers) < 8:
                    state.failed_numbers.append(chapter_number)

            delay = max(0.0, float(PDF_BULK_DELAY_SECONDS or 0.0))
            if delay:
                try:
                    await asyncio.wait_for(state.cancel_event.wait(), timeout=delay)
                    stopped = True
                    break
                except asyncio.TimeoutError:
                    pass

        if state.cancel_event.is_set():
            stopped = True

        lines = [
            "<b>Download offline parado</b>" if stopped else "<b>Lote enviado para a fila</b>",
            "",
            f"<b>Obra:</b> {html.escape(state.title_name)}",
            f"<b>Ordem:</b> {_order_label(state.order)}",
            f"<b>PDFs na fila:</b> {state.enqueued}/{state.total}",
        ]
        if stopped:
            lines.append("Os PDFs que ja estavam na fila ainda podem chegar.")
        if state.failed:
            lines.append(f"<b>Falharam ao preparar:</b> {state.failed}")
            if state.failed_numbers:
                lines.append(f"<b>Caps com falha:</b> {html.escape(', '.join(state.failed_numbers))}")
        if was_limited:
            lines.append("<b>Obs:</b> o limite PDF_BULK_MAX_CHAPTERS cortou o lote.")
        if not stopped:
            lines.extend(["", "Agora o bot vai enviar cada PDF aqui assim que ficar pronto."])
        await _safe_edit(state.status_message, "\n".join(lines))
    except asyncio.CancelledError:
        state.cancel_event.set()
        await _safe_edit(
            state.status_message,
            (
                "<b>Download offline parado</b>\n\n"
                f"<b>Obra:</b> {html.escape(state.title_name)}\n"
                f"<b>PDFs na fila:</b> {state.enqueued}/{state.total or '?'}\n"
                "Os PDFs que ja estavam na fila ainda podem chegar."
            ),
        )
    except Exception as error:
        text = f"<b>Nao consegui iniciar o lote de PDFs.</b>\n\n<code>{html.escape(str(error))}</code>"
        if state.status_message:
            await _safe_edit(state.status_message, text)
        else:
            try:
                await app.bot.send_message(state.chat_id, text, parse_mode="HTML")
            except Exception:
                pass
    finally:
        _ACTIVE_BULK_KEYS.pop(state.active_key, None)
        _BULK_STATES.pop(state.job_id, None)


async def request_pdf_bulk_for_title(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    user_id: int,
    title_ref: str,
    order: str = ORDER_ASC,
) -> bool:
    if not can_use_pdf_bulk(user_id):
        await context.bot.send_message(
            chat_id,
            "Esse envio em lote de PDFs e liberado so para membros autorizados.",
        )
        return False

    active_key = _bulk_key(user_id, title_ref)
    if active_key in _ACTIVE_BULK_KEYS:
        await context.bot.send_message(
            chat_id,
            "Ja existe um lote de PDFs dessa obra sendo preparado para voce.",
        )
        return False

    job_id = secrets.token_hex(4)
    state = PdfBulkState(
        job_id=job_id,
        active_key=active_key,
        chat_id=chat_id,
        user_id=user_id,
        title_ref=title_ref,
        order=normalize_pdf_bulk_order(order),
    )
    _ACTIVE_BULK_KEYS[active_key] = job_id
    _BULK_STATES[job_id] = state
    state.task = fire_and_forget(_run_pdf_bulk(context.application, state))
    return True


async def stop_pdf_bulk(context: ContextTypes.DEFAULT_TYPE, *, job_id: str, user_id: int) -> bool:
    state = _BULK_STATES.get(str(job_id or ""))
    if not state:
        return False
    if int(user_id) != int(state.user_id):
        await context.bot.send_message(
            state.chat_id,
            "So quem iniciou esse lote pode parar o download.",
        )
        return True

    state.cancel_event.set()
    await _safe_edit(
        state.status_message,
        (
            "<b>Parando download offline...</b>\n\n"
            f"<b>Obra:</b> {html.escape(state.title_name)}\n"
            f"<b>PDFs ja colocados na fila:</b> {state.enqueued}/{state.total or '?'}"
        ),
    )
    if state.task and not state.task.done():
        state.task.cancel()
    return True


async def pdfmanga(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.effective_message
    chat = update.effective_chat
    if not user or not message or not chat:
        return

    if chat.type != "private":
        await message.reply_text(
            "Esse comando envia muitos arquivos. Use no privado do bot.",
        )
        return

    if not can_use_pdf_bulk(user.id):
        await message.reply_text(
            "Esse envio em lote de PDFs e liberado so para membros autorizados.",
        )
        return

    args = list(context.args or [])
    order = ORDER_ASC
    if args and args[-1].lower() in {"asc", "desc"}:
        order = normalize_pdf_bulk_order(args.pop())

    query = " ".join(args).strip()
    if not query:
        await message.reply_text(
            (
                "Use <code>/pdfmanga nome do manga</code>.\n\n"
                "Para inverter a ordem, use <code>/pdfmanga nome desc</code>."
            ),
            parse_mode="HTML",
        )
        return

    title_ref = _extract_title_ref(query)
    loading = None

    if not title_ref:
        loading = await message.reply_text("Buscando a obra para preparar os PDFs...")
        try:
            results = await search_titles(query, limit=1)
        except Exception:
            results = []
        if not results:
            await _safe_edit(loading, "Nao encontrei essa obra. Tente outro nome ou envie o ID/link da obra.")
            return
        title_ref = str(results[0].get("title_id") or "").strip()

    await _safe_delete(loading)
    await request_pdf_bulk_for_title(
        context,
        chat_id=chat.id,
        user_id=user.id,
        title_ref=title_ref,
        order=order,
    )
