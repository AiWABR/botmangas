from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Any

from config import BOT_BRAND


def _parse_dt(value: str | None) -> datetime | None:
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


def access_validity_label(access: dict[str, Any] | None) -> str:
    if not access:
        return "não definido"
    if access.get("is_lifetime") or not access.get("expires_at"):
        return "vitalício"
    parsed = _parse_dt(str(access.get("expires_at") or ""))
    if not parsed:
        return str(access.get("expires_at") or "não definido")
    return parsed.strftime("%d/%m/%Y às %H:%M UTC")


def offline_welcome_message(access: dict[str, Any], *, source: str = "payment") -> str:
    brand = html.escape(BOT_BRAND or "Mangas Baltigo")
    plan = html.escape(str(access.get("plan_label") or access.get("plan") or "plano offline"))
    validity = html.escape(access_validity_label(access))
    title = "✅ <b>Assinatura offline liberada!</b>"
    intro = "Seu pagamento foi confirmado e seu acesso premium já está ativo."
    if source == "manual":
        title = "✅ <b>Acesso offline liberado manualmente!</b>"
        intro = "Um administrador ativou seu acesso premium."

    return (
        f"{title}\n\n"
        f"{intro}\n\n"
        f"» <b>Plano:</b> <i>{plan}</i>\n"
        f"» <b>Validade:</b> <i>{validity}</i>\n\n"
        f"Agora você pode usar os recursos offline do <b>{brand}</b>, incluindo downloads quando disponíveis.\n\n"
        "Use /plano para acompanhar seu status a qualquer momento."
    )
