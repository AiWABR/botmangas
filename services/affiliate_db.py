from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from config import DATA_DIR
from services.referral_db import get_referrer_chain

DB_PATH = DATA_DIR / "affiliate_gateway.sqlite3"

DEFAULT_SETTINGS = {
    "bronze_percent": "30",
    "silver_percent": "60",
    "gold_percent": "70",
    "second_level_percent": "25",
    "silver_sales": "50",
    "gold_sales": "100",
    "min_withdraw_sales": "3",
    "guarantee_days": "7",
    "plan_bronze_cents": "799",
    "plan_ouro_cents": "1799",
    "plan_diamante_cents": "7999",
    "plan_rubi_cents": "24900",
    "currency": "BRL",
}

FINAL_COMMISSION_STATUSES = {"available", "withdrawal_pending", "paid"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_text(value: datetime | None = None) -> str:
    return (value or _utc_now()).astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


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


def cents_to_money(value: int | str | None) -> str:
    cents = int(value or 0)
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}R$ {cents // 100},{cents % 100:02d}"


@contextmanager
def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=30000;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_affiliate_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS affiliate_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS affiliate_profiles (
                user_id INTEGER PRIMARY KEY,
                pix_key TEXT DEFAULT '',
                blocked INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS affiliate_commissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                buyer_user_id INTEGER NOT NULL,
                affiliate_user_id INTEGER NOT NULL,
                level INTEGER NOT NULL,
                plan TEXT NOT NULL,
                sale_amount_cents INTEGER NOT NULL,
                commission_percent REAL NOT NULL,
                commission_amount_cents INTEGER NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                release_at TEXT NOT NULL,
                paid_withdrawal_id INTEGER,
                UNIQUE(event_id, affiliate_user_id, level)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_affiliate_status
            ON affiliate_commissions(affiliate_user_id, status)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_affiliate_commissions_release
            ON affiliate_commissions(status, release_at)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS affiliate_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount_cents INTEGER NOT NULL,
                pix_key TEXT NOT NULL,
                status TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                paid_at TEXT,
                paid_by_admin_id INTEGER,
                admin_note TEXT DEFAULT ''
            )
            """
        )
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO affiliate_settings(key, value, updated_at)
                VALUES (?, ?, ?)
                """,
                (key, value, _dt_text()),
            )


def get_settings() -> dict[str, str]:
    init_affiliate_db()
    with _connect() as conn:
        rows = conn.execute("SELECT key, value FROM affiliate_settings").fetchall()
    data = dict(DEFAULT_SETTINGS)
    data.update({row["key"]: row["value"] for row in rows})
    return data


def update_setting(key: str, value: str) -> None:
    if key not in DEFAULT_SETTINGS:
        raise ValueError("Configuracao invalida.")
    init_affiliate_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO affiliate_settings(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, str(value).strip(), _dt_text()),
        )


def ensure_profile(user_id: int | str) -> None:
    uid = int(user_id)
    now = _dt_text()
    init_affiliate_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO affiliate_profiles(user_id, pix_key, blocked, created_at, updated_at)
            VALUES (?, '', 0, ?, ?)
            """,
            (uid, now, now),
        )
        conn.execute("UPDATE affiliate_profiles SET updated_at = ? WHERE user_id = ?", (now, uid))


def set_pix_key(user_id: int | str, pix_key: str) -> dict[str, Any]:
    uid = int(user_id)
    pix = str(pix_key or "").strip()
    if len(pix) < 3:
        raise ValueError("Chave Pix invalida.")
    ensure_profile(uid)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE affiliate_profiles
            SET pix_key = ?, updated_at = ?
            WHERE user_id = ?
            """,
            (pix[:180], _dt_text(), uid),
        )
    return get_profile(uid)


def get_profile(user_id: int | str) -> dict[str, Any]:
    uid = int(user_id)
    ensure_profile(uid)
    with _connect() as conn:
        row = conn.execute(
            "SELECT user_id, pix_key, blocked, created_at, updated_at FROM affiliate_profiles WHERE user_id = ?",
            (uid,),
        ).fetchone()
    return dict(row) if row else {"user_id": uid, "pix_key": "", "blocked": 0}


def _valid_sales_count(conn: sqlite3.Connection, user_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT event_id)
        FROM affiliate_commissions
        WHERE affiliate_user_id = ?
          AND level = 1
          AND status IN ('available', 'withdrawal_pending', 'paid')
        """,
        (user_id,),
    ).fetchone()
    return int(row[0] or 0)


def _tier_for_sales(settings: dict[str, str], sales: int) -> tuple[str, float]:
    gold_sales = int(settings.get("gold_sales") or 100)
    silver_sales = int(settings.get("silver_sales") or 50)
    if sales >= gold_sales:
        return "Ouro", float(settings.get("gold_percent") or 70)
    if sales >= silver_sales:
        return "Prata", float(settings.get("silver_percent") or 60)
    return "Bronze", float(settings.get("bronze_percent") or 30)


def plan_price_cents(plan: str, settings: dict[str, str] | None = None) -> int:
    cfg = settings or get_settings()
    key = f"plan_{str(plan or '').strip().lower()}_cents"
    return int(cfg.get(key) or 0)


def create_commissions_for_sale(
    buyer_user_id: int | str,
    plan: str,
    *,
    event_id: str,
    sale_amount_cents: int | None = None,
) -> list[dict[str, Any]]:
    buyer_id = int(buyer_user_id)
    if not event_id:
        return []

    settings = get_settings()
    amount = int(sale_amount_cents or plan_price_cents(plan, settings) or 0)
    if amount <= 0:
        return []

    chain = get_referrer_chain(buyer_id, max_depth=2)
    if not chain:
        return []

    guarantee_days = int(settings.get("guarantee_days") or 7)
    now = _dt_text()
    release_at = _dt_text(_utc_now() + timedelta(days=max(0, guarantee_days)))
    created: list[dict[str, Any]] = []

    init_affiliate_db()
    with _connect() as conn:
        for level, affiliate_id in enumerate(chain[:2], start=1):
            if int(affiliate_id) == buyer_id:
                continue
            profile = conn.execute(
                "SELECT blocked FROM affiliate_profiles WHERE user_id = ?",
                (int(affiliate_id),),
            ).fetchone()
            if profile and int(profile["blocked"] or 0) == 1:
                continue

            if level == 1:
                sales = _valid_sales_count(conn, int(affiliate_id))
                _tier, percent = _tier_for_sales(settings, sales)
            else:
                percent = float(settings.get("second_level_percent") or 25)

            commission_cents = int(round(amount * (percent / 100.0)))
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO affiliate_commissions(
                    event_id, buyer_user_id, affiliate_user_id, level, plan,
                    sale_amount_cents, commission_percent, commission_amount_cents,
                    status, created_at, release_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending_guarantee', ?, ?)
                """,
                (
                    event_id,
                    buyer_id,
                    int(affiliate_id),
                    level,
                    str(plan or ""),
                    amount,
                    percent,
                    commission_cents,
                    now,
                    release_at,
                ),
            )
            if cursor.rowcount:
                created.append(
                    {
                        "affiliate_user_id": int(affiliate_id),
                        "level": level,
                        "amount_cents": commission_cents,
                        "percent": percent,
                        "release_at": release_at,
                    }
                )
    return created


def cancel_commissions_for_sale(event_id: str | None = None, buyer_user_id: int | str | None = None) -> int:
    init_affiliate_db()
    clauses = ["status IN ('pending_guarantee', 'available')"]
    params: list[Any] = []
    if event_id:
        clauses.append("event_id = ?")
        params.append(event_id)
    if buyer_user_id is not None:
        clauses.append("buyer_user_id = ?")
        params.append(int(buyer_user_id))
    if len(clauses) == 1:
        return 0

    with _connect() as conn:
        cursor = conn.execute(
            f"UPDATE affiliate_commissions SET status = 'canceled' WHERE {' AND '.join(clauses)}",
            params,
        )
        return int(cursor.rowcount or 0)


def release_due_commissions() -> int:
    init_affiliate_db()
    with _connect() as conn:
        cursor = conn.execute(
            """
            UPDATE affiliate_commissions
            SET status = 'available'
            WHERE status = 'pending_guarantee'
              AND release_at <= ?
            """,
            (_dt_text(),),
        )
        return int(cursor.rowcount or 0)


def affiliate_summary(user_id: int | str) -> dict[str, Any]:
    uid = int(user_id)
    release_due_commissions()
    profile = get_profile(uid)
    settings = get_settings()
    with _connect() as conn:
        sales = _valid_sales_count(conn, uid)
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS total, COALESCE(SUM(commission_amount_cents), 0) AS amount
            FROM affiliate_commissions
            WHERE affiliate_user_id = ?
            GROUP BY status
            """,
            (uid,),
        ).fetchall()
        direct = conn.execute(
            """
            SELECT COUNT(*)
            FROM affiliate_commissions
            WHERE affiliate_user_id = ? AND level = 1
            """,
            (uid,),
        ).fetchone()[0]
        indirect = conn.execute(
            """
            SELECT COUNT(*)
            FROM affiliate_commissions
            WHERE affiliate_user_id = ? AND level = 2
            """,
            (uid,),
        ).fetchone()[0]
        withdrawals = conn.execute(
            """
            SELECT status, COALESCE(SUM(amount_cents), 0) AS amount, COUNT(*) AS total
            FROM affiliate_withdrawals
            WHERE user_id = ?
            GROUP BY status
            """,
            (uid,),
        ).fetchall()

    by_status = {row["status"]: {"count": int(row["total"] or 0), "amount_cents": int(row["amount"] or 0)} for row in rows}
    withdrawal_status = {row["status"]: {"count": int(row["total"] or 0), "amount_cents": int(row["amount"] or 0)} for row in withdrawals}
    tier, percent = _tier_for_sales(settings, sales)
    min_sales = int(settings.get("min_withdraw_sales") or 3)
    available_cents = by_status.get("available", {}).get("amount_cents", 0)

    return {
        "user_id": uid,
        "profile": profile,
        "tier": tier,
        "direct_percent": percent,
        "second_level_percent": float(settings.get("second_level_percent") or 25),
        "valid_sales": sales,
        "min_withdraw_sales": min_sales,
        "can_withdraw": available_cents > 0 and sales >= min_sales and bool(profile.get("pix_key")) and not profile.get("blocked"),
        "direct_commissions": int(direct or 0),
        "indirect_commissions": int(indirect or 0),
        "pending_cents": by_status.get("pending_guarantee", {}).get("amount_cents", 0),
        "available_cents": available_cents,
        "withdrawal_pending_cents": by_status.get("withdrawal_pending", {}).get("amount_cents", 0),
        "paid_cents": by_status.get("paid", {}).get("amount_cents", 0),
        "canceled_cents": by_status.get("canceled", {}).get("amount_cents", 0),
        "withdrawals": withdrawal_status,
        "settings": settings,
    }


def list_commissions(user_id: int | str, limit: int = 60) -> list[dict[str, Any]]:
    uid = int(user_id)
    release_due_commissions()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM affiliate_commissions
            WHERE affiliate_user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (uid, max(1, min(int(limit or 60), 200))),
        ).fetchall()
    return [dict(row) for row in rows]


def list_withdrawals(user_id: int | str, limit: int = 40) -> list[dict[str, Any]]:
    uid = int(user_id)
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM affiliate_withdrawals
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (uid, max(1, min(int(limit or 40), 200))),
        ).fetchall()
    return [dict(row) for row in rows]


def request_withdrawal(user_id: int | str) -> dict[str, Any]:
    uid = int(user_id)
    release_due_commissions()
    summary = affiliate_summary(uid)
    if summary["profile"].get("blocked"):
        raise ValueError("Afiliado bloqueado.")
    if not summary["profile"].get("pix_key"):
        raise ValueError("Cadastre sua chave Pix antes de pedir saque.")
    if summary["valid_sales"] < summary["min_withdraw_sales"]:
        raise ValueError("Voce ainda nao atingiu o minimo de vendas validas.")
    if summary["available_cents"] <= 0:
        raise ValueError("Voce nao tem saldo disponivel para saque.")

    with _connect() as conn:
        pending = conn.execute(
            """
            SELECT id
            FROM affiliate_withdrawals
            WHERE user_id = ? AND status = 'pending'
            LIMIT 1
            """,
            (uid,),
        ).fetchone()
        if pending:
            raise ValueError("Voce ja tem um saque pendente.")

        now = _dt_text()
        cursor = conn.execute(
            """
            INSERT INTO affiliate_withdrawals(user_id, amount_cents, pix_key, status, requested_at)
            VALUES (?, ?, ?, 'pending', ?)
            """,
            (uid, int(summary["available_cents"]), summary["profile"].get("pix_key") or "", now),
        )
        withdrawal_id = int(cursor.lastrowid)
        conn.execute(
            """
            UPDATE affiliate_commissions
            SET status = 'withdrawal_pending',
                paid_withdrawal_id = ?
            WHERE affiliate_user_id = ?
              AND status = 'available'
            """,
            (withdrawal_id, uid),
        )
        row = conn.execute("SELECT * FROM affiliate_withdrawals WHERE id = ?", (withdrawal_id,)).fetchone()
    return dict(row)


def pay_withdrawal(withdrawal_id: int | str, admin_id: int | str | None = None) -> dict[str, Any]:
    wid = int(withdrawal_id)
    with _connect() as conn:
        row = conn.execute("SELECT * FROM affiliate_withdrawals WHERE id = ?", (wid,)).fetchone()
        if not row:
            raise ValueError("Saque nao encontrado.")
        if row["status"] != "pending":
            raise ValueError("Esse saque nao esta pendente.")
        now = _dt_text()
        conn.execute(
            """
            UPDATE affiliate_withdrawals
            SET status = 'paid', paid_at = ?, paid_by_admin_id = ?
            WHERE id = ?
            """,
            (now, int(admin_id) if admin_id else None, wid),
        )
        conn.execute(
            """
            UPDATE affiliate_commissions
            SET status = 'paid'
            WHERE paid_withdrawal_id = ?
              AND status = 'withdrawal_pending'
            """,
            (wid,),
        )
        updated = conn.execute("SELECT * FROM affiliate_withdrawals WHERE id = ?", (wid,)).fetchone()
    return dict(updated)


def refuse_withdrawal(withdrawal_id: int | str, admin_note: str = "") -> dict[str, Any]:
    wid = int(withdrawal_id)
    with _connect() as conn:
        row = conn.execute("SELECT * FROM affiliate_withdrawals WHERE id = ?", (wid,)).fetchone()
        if not row:
            raise ValueError("Saque nao encontrado.")
        if row["status"] != "pending":
            raise ValueError("Esse saque nao esta pendente.")
        conn.execute(
            """
            UPDATE affiliate_withdrawals
            SET status = 'refused', admin_note = ?
            WHERE id = ?
            """,
            (str(admin_note or "")[:240], wid),
        )
        conn.execute(
            """
            UPDATE affiliate_commissions
            SET status = 'available',
                paid_withdrawal_id = NULL
            WHERE paid_withdrawal_id = ?
              AND status = 'withdrawal_pending'
            """,
            (wid,),
        )
        updated = conn.execute("SELECT * FROM affiliate_withdrawals WHERE id = ?", (wid,)).fetchone()
    return dict(updated)


def admin_overview() -> dict[str, Any]:
    release_due_commissions()
    init_affiliate_db()
    with _connect() as conn:
        profile_count = conn.execute("SELECT COUNT(*) FROM affiliate_profiles").fetchone()[0]
        pending_withdrawals = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount_cents), 0) FROM affiliate_withdrawals WHERE status = 'pending'"
        ).fetchone()
        paid_withdrawals = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount_cents), 0) FROM affiliate_withdrawals WHERE status = 'paid'"
        ).fetchone()
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS total, COALESCE(SUM(commission_amount_cents), 0) AS amount
            FROM affiliate_commissions
            GROUP BY status
            """
        ).fetchall()
    return {
        "profiles": int(profile_count or 0),
        "pending_withdrawals": int(pending_withdrawals[0] or 0),
        "pending_withdrawal_cents": int(pending_withdrawals[1] or 0),
        "paid_withdrawals": int(paid_withdrawals[0] or 0),
        "paid_withdrawal_cents": int(paid_withdrawals[1] or 0),
        "commissions": {row["status"]: {"count": int(row["total"] or 0), "amount_cents": int(row["amount"] or 0)} for row in rows},
        "settings": get_settings(),
    }


def admin_list_withdrawals(status: str = "pending", limit: int = 100) -> list[dict[str, Any]]:
    init_affiliate_db()
    status = str(status or "pending").strip()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM affiliate_withdrawals
            WHERE (? = 'all' OR status = ?)
            ORDER BY id DESC
            LIMIT ?
            """,
            (status, status, max(1, min(int(limit or 100), 300))),
        ).fetchall()
    return [dict(row) for row in rows]


def admin_user_snapshot(user_id: int | str) -> dict[str, Any]:
    uid = int(user_id)
    return {
        "summary": affiliate_summary(uid),
        "commissions": list_commissions(uid, limit=80),
        "withdrawals": list_withdrawals(uid, limit=40),
    }
