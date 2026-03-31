from urllib.parse import quote_plus

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes

from config import PROMO_BANNER_URL, WEBAPP_BASE_URL
from services.metrics import mark_user_seen
from utils.gatekeeper import ensure_channel_membership


async def buscar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat

    if not message or not user or not chat:
        return

    mark_user_seen(user.id, user.username or user.first_name or "")

    if chat.type != "private":
        await message.reply_text(
            "🔒 <b>Esse comando so funciona no privado.</b>\n\n"
            "Me chama no PV para abrir o catalogo e buscar por la.",
            parse_mode="HTML",
        )
        return

    query = " ".join(context.args or []).strip()

    if query:
        encoded_query = quote_plus(query)
        url = f"{WEBAPP_BASE_URL}/miniapp/index.html?q={encoded_query}"

        text = (
            "🔎 <b>Buscando seu manga...</b>\n\n"
            f"📚 <b>Pesquisa:</b> {query}\n\n"
            "✨ <i>Toque abaixo para abrir no miniapp.</i>"
        )
    else:
        url = f"{WEBAPP_BASE_URL}/miniapp/index.html"

        text = (
            "🔎 <b>Buscar manga</b>\n\n"
            "✨ <i>Toque abaixo para abrir o miniapp e pesquisar.</i>"
        )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📚 Abrir no miniapp",
                    web_app=WebAppInfo(url=url),
                )
            ]
        ]
    )

    try:
        await message.reply_photo(
            photo=PROMO_BANNER_URL,
            caption=text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception:
        await message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=keyboard,
            disable_web_page_preview=True,
        )
