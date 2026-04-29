from __future__ import annotations

import html

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_IDS
from services.offline_access import (
    get_offline_access,
    grant_offline_access,
    normalize_plan,
    plan_label,
    revoke_offline_access,
)


def _is_admin(user_id: int | None) -> bool:
    return bool(user_id and int(user_id) in set(ADMIN_IDS))


async def offlineadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not message or not _is_admin(user.id if user else None):
        return

    args = list(context.args or [])
    if len(args) < 2 or not args[0].isdigit():
        await message.reply_text(
            (
                "Uso: <code>/offlineadd ID plano</code>\n\n"
                "Planos: <code>bronze</code>, <code>ouro</code>, "
                "<code>diamante</code>, <code>rubi</code>."
            ),
            parse_mode="HTML",
        )
        return

    target_id = int(args[0])
    plan = normalize_plan(args[1])
    if not plan:
        await message.reply_text("Plano inválido. Use bronze, ouro, diamante ou rubi.")
        return

    access = grant_offline_access(
        target_id,
        plan,
        event_id=f"manual:{target_id}:{plan}",
        event_type="manual_grant",
        source="manual",
        payload={"admin_id": user.id if user else None, "args": args},
    )

    expires_at = access.get("expires_at") or "vitalício"
    await message.reply_text(
        (
            "✅ <b>Offline liberado manualmente</b>\n\n"
            f"» <b>ID:</b> <code>{target_id}</code>\n"
            f"» <b>Plano:</b> <i>{html.escape(plan_label(plan))}</i>\n"
            f"» <b>Validade:</b> <i>{html.escape(str(expires_at))}</i>"
        ),
        parse_mode="HTML",
    )


async def offlinerevoke(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not message or not _is_admin(user.id if user else None):
        return

    args = list(context.args or [])
    if not args or not args[0].isdigit():
        await message.reply_text("Uso: <code>/offlinerevoke ID</code>", parse_mode="HTML")
        return

    target_id = int(args[0])
    revoke_offline_access(
        target_id,
        event_id=f"manual_revoke:{target_id}",
        event_type="manual_revoke",
        reason="manual",
        payload={"admin_id": user.id if user else None},
    )
    await message.reply_text(f"🔒 Offline removido do ID <code>{target_id}</code>.", parse_mode="HTML")


async def offlinecheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if not message or not _is_admin(user.id if user else None):
        return

    args = list(context.args or [])
    target_id = int(args[0]) if args and args[0].isdigit() else (user.id if user else 0)
    access = get_offline_access(target_id)
    if not access:
        await message.reply_text(f"ID <code>{target_id}</code> não tem assinatura offline salva.", parse_mode="HTML")
        return

    expires_at = access.get("expires_at") or "vitalício"
    await message.reply_text(
        (
            "📥 <b>Assinatura offline</b>\n\n"
            f"» <b>ID:</b> <code>{target_id}</code>\n"
            f"» <b>Status:</b> <i>{html.escape(str(access.get('status') or ''))}</i>\n"
            f"» <b>Ativa:</b> <i>{'sim' if access.get('is_active') else 'não'}</i>\n"
            f"» <b>Plano:</b> <i>{html.escape(str(access.get('plan_label') or access.get('plan') or ''))}</i>\n"
            f"» <b>Validade:</b> <i>{html.escape(str(expires_at))}</i>"
        ),
        parse_mode="HTML",
    )
