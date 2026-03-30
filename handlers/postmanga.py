import asyncio
import html
import unicodedata

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS, BOT_BRAND, BOT_USERNAME, CANAL_POSTAGEM, STICKER_DIVISOR
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
    "releasing": "Em lancamento",
    "finished": "Finalizado",
}

FORMAT_PT_MAP = {
    "MANGA": "Manga",
    "MANHWA": "Manhwa",
    "MANHUA": "Manhua",
    "ONE_SHOT": "One-shot",
    "NOVEL": "Novel",
}


def _truncate_text(text: str, limit: int = 320) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", (value or "").strip().lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return " ".join(value.split())


def _pick_main_title(manga: dict) -> str:
    return manga.get("display_title") or manga.get("title") or manga.get("preferred_title") or "Sem titulo"


def _clean_description(description: str) -> str:
    return (description or "").strip()


def _translate_status(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "N/A"
    return STATUS_PT_MAP.get(raw.lower(), raw)


def _translate_format(value: str) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return "Manga"
    return FORMAT_PT_MAP.get(raw, raw.title())


def _latest_chapter_summary(manga: dict) -> dict:
    latest = manga.get("latest_chapter")
    if isinstance(latest, dict) and latest.get("chapter_id"):
        return latest

    chapter_id = (manga.get("chapter_id") or "").strip()
    if not chapter_id:
        return {}

    return {
        "chapter_id": chapter_id,
        "chapter_number": (manga.get("latest_chapter") or manga.get("chapter_number") or "").strip(),
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
    if not merged.get("latest_chapter"):
        latest = _latest_chapter_summary(search_item)
        if latest:
            merged["latest_chapter"] = latest
    return merged


def _build_caption(manga: dict) -> str:
    title = html.escape(_pick_main_title(manga)).upper()
    genres = manga.get("genres") or []
    genres_text = ", ".join(f"#{genre}" for genre in genres[:4]) if genres else "N/A"
    genres_text = html.escape(genres_text)

    chapters = manga.get("total_chapters") or manga.get("anilist_chapters") or "?"
    volumes = manga.get("anilist_volumes") or ""
    status = _translate_status(manga.get("status") or manga.get("anilist_status") or "N/A")
    fmt = _translate_format(manga.get("anilist_format") or "MANGA")
    score = manga.get("anilist_score") or manga.get("rating") or ""
    description = html.escape(
        _truncate_text(_clean_description(manga.get("description") or manga.get("anilist_description") or ""), 180)
    )

    meta_lines = [
        f"» <b>Status:</b> <i>{html.escape(str(status))}</i>",
        f"» <b>Formato:</b> <i>{html.escape(str(fmt))}</i>",
        f"» <b>Capitulos:</b> <i>{html.escape(str(chapters))}</i>",
        f"» <b>Generos:</b> <i>{genres_text}</i>",
    ]
    if volumes:
        meta_lines.append(f"» <b>Volumes:</b> <i>{html.escape(str(volumes))}</i>")
    if score:
        meta_lines.append(f"» <b>Nota:</b> <i>{html.escape(str(score))}</i>")

    body = f"📚 <b>{title}</b>\n\n{chr(10).join(meta_lines)}"
    if description:
        body += f"\n\n💬 <i>{description}</i>"
    body += f"\n\n✨ <i>Abra no {html.escape(BOT_BRAND)} e continue a leitura com poucos toques.</i>"
    return body


def _build_keyboard(manga: dict) -> InlineKeyboardMarkup:
    title_id = manga.get("title_id") or ""
    latest = _latest_chapter_summary(manga)
    rows = [
        [
            InlineKeyboardButton(
                "📚 Abrir obra",
                url=f"https://t.me/{BOT_USERNAME}?start=title_{title_id}",
            )
        ]
    ]

    if latest.get("chapter_id"):
        rows.append(
            [
                InlineKeyboardButton(
                    "🆕 Ultimo capitulo",
                    url=f"https://t.me/{BOT_USERNAME}?start=ch_{latest['chapter_id']}",
                )
            ]
        )

    if manga.get("anilist_url"):
        rows.append([InlineKeyboardButton("⭐ AniList", url=manga["anilist_url"])])

    return InlineKeyboardMarkup(rows)


async def postmanga(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text(
            "❌ <b>Voce nao tem permissao para usar este comando.</b>",
            parse_mode="HTML",
        )
        return

    if not context.args:
        await message.reply_text(
            "❌ <b>Faltou o nome do manga.</b>\n\n"
            "Use assim:\n"
            "<code>/postmanga nome do manga</code>\n\n"
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
            await status_message.edit_text("❌ <b>Nao encontrei esse manga.</b>", parse_mode="HTML")
            return

        search_item = _pick_best_candidate(query, results)
        if not search_item or not search_item.get("title_id"):
            await status_message.edit_text("❌ <b>Nao consegui identificar a obra certa.</b>", parse_mode="HTML")
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
        photo = manga.get("banner_url") or manga.get("cover_url") or manga.get("background_url") or None
        caption = _build_caption(manga)
        keyboard = _build_keyboard(manga)
        destination = CANAL_POSTAGEM or message.chat_id

        if photo:
            try:
                await context.bot.send_photo(
                    chat_id=destination,
                    photo=photo,
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as photo_error:
                print("ERRO POSTMANGA FOTO:", repr(photo_error))
                await context.bot.send_message(
                    chat_id=destination,
                    text=caption,
                    parse_mode="HTML",
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                )
        else:
            await context.bot.send_message(
                chat_id=destination,
                text=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        if STICKER_DIVISOR:
            await context.bot.send_sticker(chat_id=destination, sticker=STICKER_DIVISOR)

        await status_message.edit_text(
            f"✅ <b>Postagem enviada com sucesso.</b>\n\n<code>{html.escape(manga.get('title') or query)}</code>",
            parse_mode="HTML",
        )
    except Exception as error:
        print("ERRO POSTMANGA:", repr(error))
        await status_message.edit_text(
            "❌ <b>Nao consegui postar esse manga.</b>\n\nTente novamente em instantes.",
            parse_mode="HTML",
        )
