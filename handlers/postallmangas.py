async def postallmangas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    message = update.effective_message

    if not message:
        return

    if not _is_admin(user_id):
        await message.reply_text(
            "❌ <b>Você não tem permissão para usar este comando.</b>",
            parse_mode="HTML",
        )
        return

    if _bulk_running(context):
        await message.reply_text(
            "⏳ <b>Já existe uma postagem em lote rodando.</b>",
            parse_mode="HTML",
        )
        return

    limit: int | None = None

    if context.args:
        raw = str(context.args[0]).strip()

        if not raw.isdigit():
            await message.reply_text(
                "❌ <b>Quantidade inválida.</b>\n\n"
                "Use assim:\n"
                "<code>/postallmangas</code>\n"
                "ou\n"
                "<code>/postallmangas 100</code>",
                parse_mode="HTML",
            )
            return

        limit = int(raw)
        if limit <= 0:
            await message.reply_text(
                "❌ <b>A quantidade precisa ser maior que zero.</b>",
                parse_mode="HTML",
            )
            return

    task = context.application.create_task(
        _run_bulk_post_mangas(
            context=context,
            admin_chat_id=message.chat_id,
            reply_to_message_id=message.message_id,
            limit=limit,
        )
    )
    _set_bulk_task(context, task)

    if limit is None:
        await message.reply_text(
            "🚀 <b>Fila de postagem em lote iniciada.</b>\n\n"
            "Vou postar todos os mangás pendentes, um por vez, com sticker divisor e intervalo entre eles.",
            parse_mode="HTML",
        )
    else:
        await message.reply_text(
            "🚀 <b>Fila de postagem em lote iniciada.</b>\n\n"
            f"Vou postar <code>{limit}</code> mangás pendentes, um por vez, com sticker divisor e intervalo entre eles.",
            parse_mode="HTML",
        )
