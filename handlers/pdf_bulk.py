from __future__ import annotations

import asyncio
import html
import re

from telegram import Update
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

_ALLOWED_BULK_USERS = set(PDF_BULK_ALLOWED_IDS)
_ACTIVE_BULK_REQUESTS: set[str] = set()
_TITLE_REF_RE = re.compile(r"(?:title-detail/[^/\s]*-)?([a-f0-9]{20,32})", re.IGNORECASE)


def can_use_pdf_bulk(user_id: int | None) -> bool:
    return bool(user_id and int(user_id) in _ALLOWED_BULK_USERS)


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


def _limited_chapters(chapters: list[dict]) -> tuple[list[dict], bool]:
    max_items = max(0, int(PDF_BULK_MAX_CHAPTERS or 0))
    if not max_items:
        return chapters, False
    return chapters[:max_items], len(chapters) > max_items


def _chapter_caption(chapter: dict) -> str:
    title = html.escape(chapter.get("title") or "Manga")
    number = html.escape(str(chapter.get("chapter_number") or "?"))
    tag = html.escape(DISTRIBUTION_TAG or "")
    return (
        f"<b>{title}</b>\n"
        f"Capitulo <code>{number}</code>\n"
        f"{tag}"
    ).strip()


async def _safe_edit(message, text: str) -> None:
    if not message:
        return
    try:
        await message.edit_text(text, parse_mode="HTML")
    except Exception:
        pass


async def _safe_delete(message) -> None:
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


async def _run_pdf_bulk(app, chat_id: int, user_id: int, title_ref: str, active_key: str) -> None:
    status_message = None
    try:
        status_message = await app.bot.send_message(
            chat_id,
            "<b>Lote de PDFs recebido</b>\n\nVou carregar a obra e enfileirar os capitulos para envio.",
            parse_mode="HTML",
        )

        bundle = get_cached_title_bundle(title_ref) or await get_title_bundle(title_ref)
        title_id = str(bundle.get("title_id") or title_ref).strip()
        title_name = bundle.get("title") or "Manga"
        chapters = flatten_chapters({"title_id": title_id, "chapters": bundle.get("chapters") or []}, PREFERRED_CHAPTER_LANG, ascending=True)
        chapters, was_limited = _limited_chapters(chapters)

        if not chapters:
            await _safe_edit(
                status_message,
                f"<b>Nenhum capitulo encontrado</b>\n\nObra: {html.escape(title_name)}",
            )
            return

        total = len(chapters)
        await _safe_edit(
            status_message,
            (
                "<b>Preparando PDFs offline</b>\n\n"
                f"<b>Obra:</b> {html.escape(title_name)}\n"
                f"<b>Capitulos:</b> {total}\n"
                "<b>Status:</b> enfileirando..."
            ),
        )

        enqueued = 0
        failed = 0
        failed_numbers: list[str] = []

        for index, item in enumerate(chapters, start=1):
            chapter_number = str(item.get("chapter_number") or "?")
            if index == 1 or index == total or index % 5 == 0:
                await _safe_edit(
                    status_message,
                    (
                        "<b>Preparando PDFs offline</b>\n\n"
                        f"<b>Obra:</b> {html.escape(title_name)}\n"
                        f"<b>Fila:</b> {index}/{total}\n"
                        f"<b>Capitulo atual:</b> {html.escape(chapter_number)}"
                    ),
                )

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
                        chat_id=chat_id,
                        chapter_id=chapter["chapter_id"],
                        chapter_number=chapter.get("chapter_number") or chapter_number,
                        title_name=chapter.get("title") or title_name,
                        images=images,
                        caption=_chapter_caption(chapter),
                        is_bulk=True,
                        send_status=False,
                    ),
                )
                enqueued += 1
            except Exception:
                failed += 1
                if len(failed_numbers) < 8:
                    failed_numbers.append(chapter_number)

            delay = max(0.0, float(PDF_BULK_DELAY_SECONDS or 0.0))
            if delay:
                await asyncio.sleep(delay)

        lines = [
            "<b>Lote enviado para a fila</b>",
            "",
            f"<b>Obra:</b> {html.escape(title_name)}",
            f"<b>PDFs na fila:</b> {enqueued}/{total}",
        ]
        if failed:
            lines.append(f"<b>Falharam ao preparar:</b> {failed}")
            if failed_numbers:
                lines.append(f"<b>Caps com falha:</b> {html.escape(', '.join(failed_numbers))}")
        if was_limited:
            lines.append("<b>Obs:</b> o limite PDF_BULK_MAX_CHAPTERS cortou o lote.")
        lines.append("")
        lines.append("Agora o bot vai enviar cada PDF aqui assim que ficar pronto.")
        await _safe_edit(status_message, "\n".join(lines))
    except Exception as error:
        text = f"<b>Nao consegui iniciar o lote de PDFs.</b>\n\n<code>{html.escape(str(error))}</code>"
        if status_message:
            await _safe_edit(status_message, text)
        else:
            await app.bot.send_message(chat_id, text, parse_mode="HTML")
    finally:
        _ACTIVE_BULK_REQUESTS.discard(active_key)


async def request_pdf_bulk_for_title(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    user_id: int,
    title_ref: str,
) -> bool:
    if not can_use_pdf_bulk(user_id):
        await context.bot.send_message(
            chat_id,
            "Esse envio em lote de PDFs e liberado so para membros autorizados.",
        )
        return False

    active_key = _bulk_key(user_id, title_ref)
    if active_key in _ACTIVE_BULK_REQUESTS:
        await context.bot.send_message(
            chat_id,
            "Ja existe um lote de PDFs dessa obra sendo preparado para voce.",
        )
        return False

    _ACTIVE_BULK_REQUESTS.add(active_key)
    fire_and_forget(_run_pdf_bulk(context.application, chat_id, user_id, title_ref, active_key))
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

    query = " ".join(context.args or []).strip()
    if not query:
        await message.reply_text(
            "Use <code>/pdfmanga nome do manga</code> ou abra uma obra pelo /buscar e toque em <b>Baixar todos PDFs</b>.",
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
    )
