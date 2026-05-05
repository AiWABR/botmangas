from config import BOT_BRAND
from services.i18n import t_user
from utils.gatekeeper import ensure_channel_membership


async def ajuda(update, context):
    if not await ensure_channel_membership(update, context):
        return

    user_id = getattr(update.effective_user, "id", None)
    await update.effective_message.reply_text(
        t_user(user_id, "help.text", brand=BOT_BRAND),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
