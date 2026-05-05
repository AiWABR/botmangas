from __future__ import annotations

import html
import time

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
from services.offline_messages import access_validity_label, offline_welcome_message


def _is_admin(user_id: int | None) -> bool:
    return bool(user_id and int(user_id) in set(ADMIN_IDS))


def _manual_plan(raw: str) -> str:
    value = str(raw or "").strip().lower().replace("_", "").replace("-", "")
    aliases = {
        "7d": "bronze",
        "7dias": "bronze",
        "semana": "bronze",
        "1m": "ouro",
        "30d": "ouro",
        "30dias": "ouro",
        "mes": "ouro",
        "1mes": "ouro",
        "mensal": "ouro",
        "1a": "diamante",
        "1ano": "diamante",
        "ano": "diamante",
        "anual": "diamante",
        "365d": "diamante",
        "vitalicio": "rubi",
        "vitalicia": "rubi",
        "vital": "rubi",
        "life": "rubi",
        "lifetime": "rubi",
    }
    return aliases.get(value) or normalize_plan(raw)


def _usage_liberar() -> str:
    return (
        "Uso: <code>/liberar ID tempo</code>\n\n"
        "Tempos aceitos:\n"
        "• <code>7d</code> = 7 dias\n"
        "• <code>1m</code> = 1 mês\n"
        "• <code>1a</code> = 1 ano\n"
        "• <code>vitalicio</code> = vitalício"
    )


async def _notify_user(context: ContextTypes.DEFAULT_TYPE, target_id: int, access: dict, source: str) -> str:
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=offline_welcome_message(access, source=source),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return ""
    except Exception as error:
        return str(error)


async def _grant_manual(update: Update, context: ContextTypes.DEFAULT_TYPE, *, notify: bool) -> None:
    user = update.effective_user
    message = update.effective_message
    if not message or not _is_admin(user.id if user else None):
        return

    args = list(context.args or [])
    if len(args) < 2 or not args[0].isdigit():
        await message.reply_text(_usage_liberar(), parse_mode="HTML")
        return

    target_id = int(args[0])
    plan = _manual_plan(args[1])
    if not plan:
        await message.reply_text(
            "⚠️ Tempo inválido. Use <code>7d</code>, <code>1m</code>, <code>1a</code> ou <code>vitalicio</code>.",
            parse_mode="HTML",
        )
        return

    access = grant_offline_access(
        target_id,
        plan,
        event_id=f"manual_grant:{target_id}:{plan}:{int(time.time())}",
        event_type="manual_grant",
        source="manual",
        payload={"admin_id": user.id if user else None, "args": args},
    )

    validity = access_validity_label(access)
    await message.reply_text(
        (
            "✅ <b>Membro liberado com sucesso</b>\n\n"
            f"» <b>ID:</b> <code>{target_id}</code>\n"
            f"» <b>Plano:</b> <i>{html.escape(plan_label(plan))}</i>\n"
            f"» <b>Validade:</b> <i>{html.escape(validity)}</i>"
        ),
        parse_mode="HTML",
    )

    if notify:
        error = await _notify_user(context, target_id, access, "manual")
        if error:
            await message.reply_text(
                f"⚠️ A liberação foi salva, mas não consegui avisar o usuário: <code>{html.escape(error)}</code>",
                parse_mode="HTML",
            )


async def liberar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _grant_manual(update, context, notify=True)


async def offlineadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _grant_manual(update, context, notify=False)


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
        event_id=f"manual_revoke:{target_id}:{int(time.time())}",
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

    await message.reply_text(
        (
            "📥 <b>Assinatura offline</b>\n\n"
            f"» <b>ID:</b> <code>{target_id}</code>\n"
            f"» <b>Status:</b> <i>{html.escape(str(access.get('status') or ''))}</i>\n"
            f"» <b>Ativa:</b> <i>{'sim' if access.get('is_active') else 'não'}</i>\n"
            f"» <b>Plano:</b> <i>{html.escape(str(access.get('plan_label') or access.get('plan') or ''))}</i>\n"
            f"» <b>Validade:</b> <i>{html.escape(access_validity_label(access))}</i>"
        ),
        parse_mode="HTML",
    )
