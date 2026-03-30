from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes

from config import BOT_BRAND, WEBAPP_BASE_URL


async def testminiapp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user = update.effective_user

    if message is None:
        return

    if not WEBAPP_BASE_URL:
        await message.reply_text("O miniapp ainda não está configurado. Ajusta `WEBAPP_BASE_URL` primeiro.")
        return

    first_name = user.first_name if user and user.first_name else "leitor"
    url = f"{WEBAPP_BASE_URL.rstrip('/')}/miniapp/index.html"

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(text="📱 Abrir miniapp", web_app=WebAppInfo(url=url))]]
    )

    await message.reply_text(
        (
            f"📱 <b>{BOT_BRAND} Miniapp</b>\n\n"
            f"{first_name}, toca no botão abaixo para abrir o leitor dentro do Telegram."
        ),
        parse_mode="HTML",
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )
