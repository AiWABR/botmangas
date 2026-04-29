from __future__ import annotations

import time
from typing import Any

import httpx

from config import (
    CAKTO_API_BASE_URL,
    CAKTO_CLIENT_ID,
    CAKTO_CLIENT_SECRET,
    CAKTO_ORDER_SYNC_LIMIT,
)
from services.cakto_gateway import extract_access_target, process_cakto_webhook

_TOKEN: str = ""
_TOKEN_EXPIRES_AT = 0.0

PAID_STATUSES = {"paid", "approved", "completed", "active"}
REVOKED_STATUSES = {"refunded", "chargeback", "canceled", "cancelled"}


def cakto_api_configured() -> bool:
    return bool(CAKTO_CLIENT_ID and CAKTO_CLIENT_SECRET and CAKTO_API_BASE_URL)


async def _get_token() -> str:
    global _TOKEN, _TOKEN_EXPIRES_AT

    if _TOKEN and _TOKEN_EXPIRES_AT > time.monotonic() + 60:
        return _TOKEN

    if not cakto_api_configured():
        raise RuntimeError("Configure CAKTO_CLIENT_ID e CAKTO_CLIENT_SECRET para consultar pedidos.")

    async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=6.0)) as client:
        response = await client.post(
            f"{CAKTO_API_BASE_URL}/public_api/token/",
            data={
                "client_id": CAKTO_CLIENT_ID,
                "client_secret": CAKTO_CLIENT_SECRET,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        payload = response.json()

    token = str(payload.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("A Cakto nao retornou access_token.")

    try:
        expires_in = int(payload.get("expires_in") or 3600)
    except (TypeError, ValueError):
        expires_in = 3600

    _TOKEN = token
    _TOKEN_EXPIRES_AT = time.monotonic() + max(300, expires_in - 30)
    return token


async def list_recent_cakto_orders(limit: int | None = None) -> list[dict[str, Any]]:
    token = await _get_token()
    limit = max(1, min(int(limit or CAKTO_ORDER_SYNC_LIMIT or 100), 200))

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=6.0)) as client:
        response = await client.get(
            f"{CAKTO_API_BASE_URL}/public_api/orders/",
            params={
                "limit": limit,
                "ordering": "-paidAt",
                "utm_source": "telegram",
                "utm_campaign": "offline_manga",
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        response.raise_for_status()
        payload = response.json()

    results = payload.get("results") if isinstance(payload, dict) else payload
    if isinstance(results, list):
        return [item for item in results if isinstance(item, dict)]
    return []


def _order_status(order: dict[str, Any]) -> str:
    return str(order.get("status") or "").strip().lower()


def _order_is_paid(order: dict[str, Any]) -> bool:
    status = _order_status(order)
    if status in REVOKED_STATUSES:
        return False
    return status in PAID_STATUSES or bool(order.get("paidAt") or order.get("paid_at"))


async def verify_cakto_payment_for_user(user_id: int | str) -> dict[str, Any]:
    uid = int(user_id)
    orders = await list_recent_cakto_orders()

    checked = 0
    for order in orders:
        checked += 1
        target = extract_access_target(order)
        if int(target.get("user_id") or 0) != uid:
            continue

        if not _order_is_paid(order):
            return {
                "ok": False,
                "reason": "order_not_paid",
                "checked": checked,
                "order_status": _order_status(order),
            }

        payload = {
            **order,
            "event": "purchase_approved",
            "status": _order_status(order) or "paid",
        }
        result = process_cakto_webhook(payload)
        return {
            "ok": result.get("action") == "granted",
            "reason": result.get("reason") or "",
            "checked": checked,
            "result": result,
        }

    return {
        "ok": False,
        "reason": "not_found",
        "checked": checked,
    }
