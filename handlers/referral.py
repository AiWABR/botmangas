import html
from urllib.parse import quote

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes

from config import BOT_USERNAME, WEBAPP_BASE_URL
from services.affiliate_db import affiliate_summary, cents_to_money


def _affiliate_webapp_url(user_id: int) -> str:
    base = (WEBAPP_BASE_URL or "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/affiliate?user_id={int(user_id)}&bot={quote(BOT_USERNAME or '')}"


async def _send_panel(message, user_id: int):
    summary = affiliate_summary(user_id)
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
    app_url = _affiliate_webapp_url(user_id)

    text = (
        "<b>Programa de Afiliados</b>\n\n"
        f"Seu nivel: <b>{html.escape(str(summary['tier']))}</b>\n"
        f"Comissao direta: <b>{summary['direct_percent']}%</b>\n"
        f"Comissao indireta: <b>{summary['second_level_percent']}%</b>\n\n"
        f"Saldo disponivel: <b>{cents_to_money(summary['available_cents'])}</b>\n"
        f"Em garantia: <b>{cents_to_money(summary['pending_cents'])}</b>\n"
        f"Vendas validas: <b>{summary['valid_sales']}</b>\n\n"
        "Painel completo:\n"
        "veja historico, cadastre Pix, solicite saque e acompanhe tudo pelo webapp.\n\n"
        f"Seu link:\n<code>{html.escape(link)}</code>"
    )

    rows = []
    if app_url:
        rows.append([InlineKeyboardButton("Abrir painel de afiliados", web_app=WebAppInfo(url=app_url))])

    telegram_share = (
        "https://t.me/share/url?"
        f"url={quote(link)}"
        "&text=" + quote("Entra no bot de mangas comigo:")
    )
    whatsapp_share = "https://wa.me/?text=" + quote(f"Entra no bot de mangas comigo:\n{link}")
    rows.append([InlineKeyboardButton("Compartilhar no Telegram", url=telegram_share)])
    rows.append([InlineKeyboardButton("Compartilhar no WhatsApp", url=whatsapp_share)])

    if not app_url:
        text += "\n\nConfigure WEBAPP_BASE_URL para ativar o botao do painel."

    await message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(rows),
        disable_web_page_preview=True,
    )


async def indicacoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return
    await _send_panel(message, user.id)


async def referral_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    if query.data != "noop_indicar":
        return

    await query.answer()
    await _send_panel(query.message, user.id)
