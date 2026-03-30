import html
import json
from pathlib import Path

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS, AUTO_POST_LIMIT, BOT_BRAND, BOT_USERNAME, CANAL_POSTAGEM_CAPITULOS, DATA_DIR
from core.channel_target import ensure_channel_target
from services.catalog_client import get_recent_chapters

POSTED_JSON_PATH = Path(DATA_DIR) / "capitulos_postados.json"


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in ADMIN_IDS


def _load_posted() -> list[str]:
    if not POSTED_JSON_PATH.exists():
        return []
    try:
        return json.loads(POSTED_JSON_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_posted(items: list[str]) -> None:
    POSTED_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    POSTED_JSON_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _deep_link(chapter_id: str, title_id: str = "") -> str:
    payload = f"{chapter_id}_{title_id}" if title_id else chapter_id
    return f"https://t.me/{BOT_USERNAME}?start=ch_{payload}"


def _title_link(title_id: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=title_{title_id}"


def _post_key(item: dict) -> str:
    return (item.get("chapter_id") or "").strip()


def _display_title(item: dict) -> str:
    return item.get("display_title") or item.get("title") or "Manga"


def _caption(item: dict) -> str:
    title = html.escape(_display_title(item))
    chapter_number = html.escape(str(item.get("chapter_number") or "?"))
    status = html.escape(item.get("status") or "Atualizado")
    updated_at = html.escape(item.get("updated_at") or "agora ha pouco")
    brand = html.escape(BOT_BRAND)

    lines = [
        "🆕 <b>Capitulo novo disponivel</b>",
        "",
        f"📚 <b>{title}</b>",
        f"» <b>Capitulo:</b> <i>{chapter_number}</i>",
        f"» <b>Status:</b> <i>{status}</i>",
        f"» <b>Atualizado:</b> <i>{updated_at}</i>",
        "",
        f"✨ <i>Abra no {brand} e continue a leitura.</i>",
    ]
    return "\n".join(lines)


def _keyboard(item: dict) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("📖 Ler capitulo", url=_deep_link(item["chapter_id"], item.get("title_id") or ""))]]
    if item.get("title_id"):
        rows.append([InlineKeyboardButton("📚 Abrir obra", url=_title_link(item["title_id"]))])
    return InlineKeyboardMarkup(rows)


async def _send_recent_chapter(bot, chat_id, item: dict) -> None:
    cover = item.get("cover_url") or item.get("background_url") or None
    caption = _caption(item)
    keyboard = _keyboard(item)

    if cover:
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=cover,
                caption=caption,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            return
        except Exception as error:
            print("ERRO POST NOVO CAP FOTO:", repr(error))

    await bot.send_message(
        chat_id=chat_id,
        text=caption,
        parse_mode="HTML",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def _post_recent_items(bot, destination, items: list[dict], posted: list[str]) -> tuple[int, int, list[str]]:
    posted_set = set(posted)
    sent = 0
    failed = 0

    for item in items:
        key = _post_key(item)
        if not key or key in posted_set:
            continue
        try:
            await _send_recent_chapter(bot, destination, item)
        except Exception as error:
            failed += 1
            print("ERRO POST NOVO CAP:", repr(error), item.get("chapter_id"), item.get("title"))
            continue

        posted.append(key)
        posted_set.add(key)
        sent += 1

    return sent, failed, posted


async def postnovoseps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user

    if not message or not user or not _is_admin(user.id):
        if message:
            await message.reply_text("❌ <b>Voce nao tem permissao para usar esse comando.</b>", parse_mode="HTML")
        return

    status_message = await message.reply_text(
        "📤 <b>Verificando atualizacoes recentes...</b>",
        parse_mode="HTML",
    )

    try:
        items = await get_recent_chapters(limit=AUTO_POST_LIMIT)
        if not items:
            await status_message.edit_text(
                "❌ <b>Nao encontrei capitulos recentes para postar.</b>",
                parse_mode="HTML",
            )
            return

        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_CAPITULOS or message.chat_id)
        posted = _load_posted()
        sent, failed, posted = await _post_recent_items(context.bot, destination, items, posted)
        _save_posted(posted[-300:])

        await status_message.edit_text(
            "✅ <b>Postagem concluida.</b>\n\n"
            f"<b>Novos capitulos enviados:</b> <code>{sent}</code>\n"
            f"<b>Falhas:</b> <code>{failed}</code>",
            parse_mode="HTML",
        )
    except Exception as error:
        print("ERRO POSTNOVOSEPS:", repr(error))
        await status_message.edit_text(
            f"❌ <b>Nao consegui concluir as atualizacoes agora.</b>\n\n{html.escape(str(error) or 'Tente novamente em instantes.')}",
            parse_mode="HTML",
        )


async def auto_post_new_eps_job(context: ContextTypes.DEFAULT_TYPE):
    if not CANAL_POSTAGEM_CAPITULOS:
        return

    try:
        destination = await ensure_channel_target(context.bot, CANAL_POSTAGEM_CAPITULOS)
        items = await get_recent_chapters(limit=AUTO_POST_LIMIT)
        if not items:
            return

        posted = _load_posted()
        sent, failed, posted = await _post_recent_items(context.bot, destination, items, posted)
        if sent or failed:
            _save_posted(posted[-300:])
    except Exception as error:
        print("ERRO AUTO POST NOVO CAP:", repr(error))
