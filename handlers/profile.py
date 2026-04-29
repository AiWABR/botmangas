from __future__ import annotations

import html
from io import BytesIO

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.error import TelegramError
from telegram.ext import ContextTypes

from config import BOT_BRAND, WEBAPP_BASE_URL
from core.background import run_sync
from services.metrics import get_reading_summary
from services.profile_store import count_user_favorites
from utils.gatekeeper import ensure_channel_membership
from utils.profile_card import build_profile_card


async def _download_user_avatar(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bytes | None:
    try:
        photos = await context.bot.get_user_profile_photos(user_id=user_id, limit=1)
        if not photos.total_count or not photos.photos:
            return None
        photo = photos.photos[0][-1]
        telegram_file = await context.bot.get_file(photo.file_id)
        buffer = BytesIO()
        await telegram_file.download_to_memory(out=buffer)
        return buffer.getvalue()
    except TelegramError:
        return None
    except Exception:
        return None


def _miniapp_url(route: str) -> str:
    base = (WEBAPP_BASE_URL or "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/miniapp/index.html?route={route}&page={route}&view={route}"


def _profile_keyboard() -> InlineKeyboardMarkup | None:
    history_url = _miniapp_url("history")
    favorites_url = _miniapp_url("lib")
    if not history_url or not favorites_url:
        return None

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Acompanhando",
                    web_app=WebAppInfo(url=history_url),
                ),
                InlineKeyboardButton(
                    "Favoritos",
                    web_app=WebAppInfo(url=favorites_url),
                ),
            ]
        ]
    )


def _caption(
    *,
    user_id: int,
    name: str,
    username: str,
    following_count: int,
    favorites_count: int,
    chapters_read_count: int,
    has_keyboard: bool,
) -> str:
    user_line = f"@{html.escape(username)}" if username else f"<code>{user_id}</code>"
    hint = (
        "Use os botões abaixo para abrir suas leituras e favoritos no WebApp."
        if has_keyboard
        else "No privado com o bot eu mostro os botões do WebApp."
    )
    return (
        "👤 <b>SEU PERFIL</b>\n\n"
        f"🆔 <b>ID:</b> <code>{user_id}</code>\n"
        f"👥 <b>Nome:</b> {html.escape(name or 'Usuario')}\n"
        f"🔗 <b>Telegram:</b> {user_line}\n\n"
        f"📖 <b>Acompanhando:</b> {following_count}\n"
        f"⭐ <b>Favoritos:</b> {favorites_count}\n"
        f"✅ <b>Caps lidos:</b> {chapters_read_count}\n\n"
        f"{hint}"
    )


async def mperfil(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.effective_message
    chat = update.effective_chat
    if not user or not message:
        return

    reading_summary = await run_sync(get_reading_summary, user.id, 5)
    favorites_count = await run_sync(count_user_favorites, user.id)
    avatar_bytes = await _download_user_avatar(context, user.id)

    full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
    full_name = full_name or user.username or "Usuario"

    card = await run_sync(
        build_profile_card,
        user_id=user.id,
        name=full_name,
        username=user.username or "",
        avatar_bytes=avatar_bytes,
        brand=BOT_BRAND or "Mangas Brasil",
        following_count=int(reading_summary.get("title_count") or 0),
        favorites_count=favorites_count,
        chapters_read_count=int(reading_summary.get("chapter_count") or 0),
        recent_reads=reading_summary.get("recent_reads") or [],
    )

    is_private = bool(chat and chat.type == "private")
    keyboard = _profile_keyboard() if is_private else None
    await message.reply_photo(
        photo=card,
        caption=_caption(
            user_id=user.id,
            name=full_name,
            username=user.username or "",
            following_count=int(reading_summary.get("title_count") or 0),
            favorites_count=favorites_count,
            chapters_read_count=int(reading_summary.get("chapter_count") or 0),
            has_keyboard=is_private and keyboard is not None,
        ),
        parse_mode="HTML",
        reply_markup=keyboard,
    )
