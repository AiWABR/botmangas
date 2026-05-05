from __future__ import annotations

import html
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from config import AI_TIMEZONE, BOT_BRAND
from services.cakto_gateway import get_checkout_options
from services.i18n import t_user
from services.offline_access import PLAN_DAYS, get_offline_access, normalize_plan, plan_label
from utils.gatekeeper import ensure_channel_membership

SUPPORT_BOT_URL = "https://t.me/QGSuporteBot"

PLAN_SHORT_LABELS = {
    "bronze": "Bronze",
    "ouro": "Ouro",
    "diamante": "Diamante",
    "rubi": "Rubi",
    "1m": "Ouro",
    "3m": "3 meses",
    "6m": "6 meses",
    "lifetime": "Rubi",
}


def _timezone() -> ZoneInfo:
    try:
        return ZoneInfo(AI_TIMEZONE or "America/Cuiaba")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _parse_utc_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    for candidate in (raw, raw.replace("T", " ").replace("Z", "+00:00"), raw.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            pass
    try:
        return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _format_local_datetime(value: str | None, user_id: int) -> str:
    parsed = _parse_utc_datetime(value)
    if parsed is None:
        return t_user(user_id, "plan.not_defined")
    return parsed.astimezone(_timezone()).strftime("%d/%m/%Y às %H:%M")


def _duration_label(plan: str | None, user_id: int) -> str:
    plan_key = normalize_plan(plan)
    days = PLAN_DAYS.get(plan_key)
    if days is None:
        return t_user(user_id, "plan.lifetime")
    if int(days) == 1:
        return t_user(user_id, "plan.one_day")
    return t_user(user_id, "plan.days", days=int(days))


def _remaining_label(expires_at: str | None, user_id: int) -> str:
    expires = _parse_utc_datetime(expires_at)
    if expires is None:
        return t_user(user_id, "plan.never_expires")
    delta = expires - datetime.now(timezone.utc)
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return t_user(user_id, "plan.expired")
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = max(1, remainder // 60)
    if days:
        return t_user(user_id, "plan.remaining_days", days=days, hours=hours)
    if hours:
        return t_user(user_id, "plan.remaining_hours", hours=hours, minutes=minutes)
    return t_user(user_id, "plan.remaining_minutes", minutes=minutes)


def _status_label(access: dict | None, user_id: int) -> str:
    if not access:
        return t_user(user_id, "plan.status_none")
    if access.get("is_active"):
        return t_user(user_id, "plan.status_active")
    status = str(access.get("status") or "").strip()
    if status == "expired":
        return t_user(user_id, "plan.status_expired")
    if status == "revoked":
        return t_user(user_id, "plan.status_revoked")
    return status or t_user(user_id, "plan.status_inactive")


def _renew_option(user_id: int, plan: str | None) -> dict[str, str] | None:
    plan_key = normalize_plan(plan)
    if not plan_key:
        return None
    for option in get_checkout_options(user_id):
        if option.get("plan") == plan_key:
            return option
    return None


def _plan_keyboard(user_id: int, access: dict | None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    plan_key = normalize_plan((access or {}).get("plan") or "")
    renew = _renew_option(user_id, plan_key)

    if renew:
        short_label = PLAN_SHORT_LABELS.get(plan_key, plan_label(plan_key))
        rows.append([InlineKeyboardButton(t_user(user_id, "plan.renew_button", plan=short_label), url=renew["url"])])
    else:
        for option in get_checkout_options(user_id):
            rows.append([InlineKeyboardButton(option["label"], url=option["url"])])

    rows.append([InlineKeyboardButton(t_user(user_id, "common.support"), url=SUPPORT_BOT_URL)])
    return InlineKeyboardMarkup(rows)


def _plan_text(user_id: int, access: dict | None) -> str:
    brand = html.escape(BOT_BRAND or "Mangas Baltigo")
    status = html.escape(_status_label(access, user_id))
    if not access:
        return t_user(user_id, "plan.no_plan_text", brand=brand, status=status, uid=user_id)

    plan = access.get("plan") or ""
    expires_at = access.get("expires_at") or ""
    expires_text = t_user(user_id, "plan.never_expires") if access.get("is_lifetime") else _format_local_datetime(expires_at, user_id)
    remaining = t_user(user_id, "plan.never_expires") if access.get("is_lifetime") else _remaining_label(expires_at, user_id)

    return t_user(
        user_id,
        "plan.active_text",
        status=status,
        plan=html.escape(plan_label(plan)),
        duration=html.escape(_duration_label(plan, user_id)),
        expires=html.escape(expires_text),
        remaining=html.escape(remaining),
        uid=user_id,
    )


async def send_plan_panel(target, user_id: int) -> None:
    access = get_offline_access(user_id)
    sender = getattr(target, "message", None) or target
    await sender.reply_text(
        _plan_text(user_id, access),
        parse_mode="HTML",
        reply_markup=_plan_keyboard(user_id, access),
        disable_web_page_preview=True,
    )


async def plano(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_channel_membership(update, context):
        return
    user = update.effective_user
    message = update.effective_message
    if not user or not message:
        return
    await send_plan_panel(message, user.id)
