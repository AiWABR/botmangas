from config import BOT_BRAND, BOT_USERNAME
from utils.gatekeeper import ensure_channel_membership


async def ajuda(update, context):
    if not await ensure_channel_membership(update, context):
        return

    inline_hint = f"@{BOT_USERNAME} one piece" if BOT_USERNAME else "nome do manga"
    text = (
        f"📚 <b>Ajuda - {BOT_BRAND}</b>\n\n"
        "🔎 <b>Como buscar uma obra</b>\n"
        "Use <code>/buscar nome do manga</code>\n\n"
        "📌 <b>Exemplos</b>\n"
        "• <code>/buscar solo leveling</code>\n"
        "• <code>/buscar one piece</code>\n"
        "• <code>/buscar vagabond</code>\n\n"
        "⚡ <b>Modo inline</b>\n"
        f"Use <code>{inline_hint}</code> em qualquer conversa para abrir resultados rapidos.\n\n"
        "📖 <b>Fluxo de leitura</b>\n"
        "• Pesquise a obra\n"
        "• Abra os detalhes\n"
        "• Escolha um capitulo\n"
        "• Leia pela leitura rapida ou gere PDF\n\n"
        "🎁 <b>Extras</b>\n"
        "• <code>/indicacoes</code> para seu link de convites\n"
        "• <code>/postnovoscaps</code> para admins postarem capitulos recentes\n"
        "• <code>/postmanga</code> para admins montarem uma postagem de destaque"
    )

    await update.effective_message.reply_text(text, parse_mode="HTML")
