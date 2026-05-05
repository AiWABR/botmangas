from __future__ import annotations

import html

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes

from services.i18n import t_user
from services.language_prefs import (
    get_user_interface_language,
    set_user_interface_language,
)
from utils.gatekeeper import ensure_channel_membership

LOCALE_LABELS = {
    "pt-BR": "🇧🇷 Português",
    "en-US": "🇺🇸 English",
    "es-ES": "🇪🇸 Español",
}


def _keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(LOCALE_LABELS["pt-BR"], callback_data="mb|uilang|pt-BR")],
            [InlineKeyboardButton(LOCALE_LABELS["en-US"], callback_data="mb|uilang|en-US")],
            [InlineKeyboardButton(LOCALE_LABELS["es-ES"], callback_data="mb|uilang|es-ES")],
        ]
    )


def language_panel_text(user_id: int | str | None) -> str:
    current = get_user_interface_language(user_id)
    return (
        f"{t_user(user_id, 'language.title')}\n\n"
        f"{t_user(user_id, 'language.body')}\n\n"
        f"{t_user(user_id, 'language.current', locale=html.escape(LOCALE_LABELS.get(current, current)))}"
    )


async def _replace_query_panel(query, text: str, markup: InlineKeyboardMarkup) -> None:
    try:
        if getattr(query.message, "photo", None):
            await query.edit_message_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=markup,
            )
        else:
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True,
            )
        return
    except BadRequest as error:
        # Photo messages without editable captions, inline edge cases, or stale messages.
        if "message is not modified" in str(error).lower():
            return
    except TelegramError:
        pass

    if query.message:
        await query.message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=markup,
            disable_web_page_preview=True,
        )


async def idioma(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return

    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return

    await message.reply_text(
        language_panel_text(user.id),
        parse_mode="HTML",
        reply_markup=_keyboard(),
        disable_web_page_preview=True,
    )


async def handle_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    query = update.callback_query
    user = update.effective_user
    if not query or not user or not query.data:
        return False
    if query.data == "mb|uilangmenu":
        await query.answer()
        await _replace_query_panel(query, language_panel_text(user.id), _keyboard())
        return True
    if not query.data.startswith("mb|uilang|"):
        return False

    locale = query.data.split("|", 2)[2]
    result = set_user_interface_language(user.id, locale)
    label = LOCALE_LABELS.get(result["interface_language"], result["interface_language"])
    text = t_user(user.id, "language.saved", locale=html.escape(label))
    try:
        await query.answer(label)
    except Exception:
        pass
    await _replace_query_panel(query, text, _keyboard())
    return True
