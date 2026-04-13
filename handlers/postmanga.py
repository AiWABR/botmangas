import asyncio
import html
import json
import re
import unicodedata
from typing import Any

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
    return (
        manga.get("display_title")
        or manga.get("title")
        or manga.get("preferred_title")
        or manga.get("name")
        or "Sem titulo"
    )


def _clean_description(description: str) -> str:
    description = (description or "").strip()
    description = re.sub(r"<br\s*/?>", "\n", description, flags=re.I)
    description = re.sub(r"</p\s*>", "\n", description, flags=re.I)
    description = re.sub(r"<[^>]+>", " ", description)
    description = html.unescape(description)
    return " ".join(description.split())


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
        * _extract_json_ld_images(raw_html),
        manga.get("cover_url"),
        manga.get("banner_url"),
        manga.get("background_url"),
    ]

    for candidate in candidates:
        url = str(candidate or "").strip()
        if url.startswith("http"):
            return url
    return ""


def _resolve_description(manga: dict) -> str:
    raw_html = (
        manga.get("raw_html")
        or manga.get("html")
        or manga.get("page_html")
        or manga.get("title_html")
        or ""
    )

    description = (
        manga.get("description")
        or manga.get("synopsis")
        or manga.get("anilist_description")
        or manga.get("seo_description")
        or _extract_meta_content(raw_html, "description")
        or _extract_meta_content(raw_html, "og:description")
        or ""
    )

    return _clean_description(description)


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

    description = _resolve_description(merged)
    if description:
        merged["description"] = description

    year = _resolve_year(merged)
    if year:
        merged["year"] = year

    if not merged.get("latest_chapter"):
        latest = _latest_chapter_summary(search_item)
        if latest:
            merged["latest_chapter"] = latest

    return merged


def _build_caption(manga: dict) -> str:
    full_title = html.escape(_pick_main_title(manga)).upper()

    genres = _resolve_manga_genres(manga)
    genres_text = ", ".join(f"#{g.replace(' ', '_')}" for g in genres[:6]) if genres else "N/A"

    chapters = (
        manga.get("total_chapters")
        or manga.get("chapter_count")
        or manga.get("anilist_chapters")
        or "?"
    )
    status = _translate_status(manga.get("status") or manga.get("anilist_status") or "N/A")
    format_name = _translate_format(manga.get("format") or manga.get("type") or manga.get("anilist_format") or "")
    year = _resolve_year(manga)
    description = html.escape(_truncate_text(_resolve_description(manga), 320))

    info_lines = [
        f"<b>Generos:</b> <i>{html.escape(genres_text)}</i>",
        f"<b>Formato:</b> <i>{html.escape(format_name)}</i>",
        f"<b>Capitulos:</b> <i>{html.escape(str(chapters))}</i>",
        f"<b>Status:</b> <i>{html.escape(str(status))}</i>",
    ]

    if year:
        info_lines.insert(3, f"<b>Ano:</b> <i>{html.escape(year)}</i>")

    return (
        f"📚 <b>{full_title}</b>\n\n"
        + "\n".join(info_lines)
        + f"\n\n 💬 <i>Leia pelo bot, do jeito mais simples e completo.</i>"
    )


def _build_keyboard(manga: dict) -> InlineKeyboardMarkup:
    title_id = manga.get("title_id") or ""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📚 Ler obra", url=f"https://t.me/{BOT_USERNAME}?start=title_{title_id}")]]
    )


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

        photo = (
            manga.get("origin_cover_url")
            or manga.get("cover_url")
            or manga.get("banner_url")
            or manga.get("background_url")
            or None
        )

        caption = _build_caption(manga)
        keyboard = _build_keyboard(manga)
        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_MANGA or message.chat_id)

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
            f"❌ <b>Nao consegui postar esse manga.</b>\n\n{html.escape(str(error) or 'Tente novamente em instantes.')}",
            parse_mode="HTML",
        )
