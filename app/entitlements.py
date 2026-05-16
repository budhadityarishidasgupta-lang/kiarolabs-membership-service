from __future__ import annotations

from fastapi import HTTPException

from app.database import get_connection


ONLINE_PRACTICE_APP_CODES = {"math", "spelling", "general", "comprehension"}


def _normalize_codes(codes: str | list[str] | tuple[str, ...] | set[str]) -> set[str]:
    if isinstance(codes, str):
        return {codes.strip().lower()} if codes.strip() else set()
    return {str(code).strip().lower() for code in codes if str(code).strip()}


def _is_admin_user(user: dict | None) -> bool:
    return str((user or {}).get("role", "")).strip().lower() == "admin"


def _resolve_member_id(cur, user: dict | None) -> int | None:
    if not user:
        return None

    raw_member_id = user.get("member_id")
    email = str(user.get("sub") or user.get("email") or "").strip().lower()

    if raw_member_id:
        try:
            member_id = int(raw_member_id)
        except (TypeError, ValueError):
            member_id = None
        if member_id is not None:
            if not email:
                return member_id
            cur.execute(
                """
                SELECT id
                FROM kiaro_membership.members
                WHERE id = %s
                  AND LOWER(email) = LOWER(%s)
                LIMIT 1
                """,
                (member_id, email),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])

    if not email:
        return None

    cur.execute(
        """
        SELECT id
        FROM kiaro_membership.members
        WHERE LOWER(email) = LOWER(%s)
        ORDER BY id ASC
        LIMIT 1
        """,
        (email,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def get_member_app_codes_for_user(user: dict | None) -> set[str]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        member_id = _resolve_member_id(cur, user)
        if not member_id:
            return set()
        cur.execute(
            """
            SELECT app_code
            FROM kiaro_membership.member_apps
            WHERE member_id = %s
            """,
            (member_id,),
        )
        return {
            str(row[0]).strip().lower()
            for row in (cur.fetchall() or [])
            if row and row[0]
        }
    finally:
        cur.close()
        conn.close()


def user_has_member_app_access(user: dict | None, required_codes: str | list[str] | tuple[str, ...] | set[str], *, allow_admin: bool = True) -> bool:
    required = _normalize_codes(required_codes)
    if not required:
        return False
    if allow_admin and _is_admin_user(user):
        return True
    user_codes = get_member_app_codes_for_user(user)
    return bool(user_codes.intersection(required))


def email_has_member_app_access(user_email: str | None, required_codes: str | list[str] | tuple[str, ...] | set[str]) -> bool:
    email = str(user_email or "").strip().lower()
    required = _normalize_codes(required_codes)
    if not email or not required:
        return False

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT m.id
            FROM kiaro_membership.members m
            WHERE LOWER(m.email) = LOWER(%s)
            ORDER BY m.id ASC
            LIMIT 1
            """,
            (email,),
        )
        row = cur.fetchone()
        if not row:
            return False
        member_id = int(row[0])
        cur.execute(
            """
            SELECT 1
            FROM kiaro_membership.member_apps
            WHERE member_id = %s
              AND app_code = ANY(%s)
            LIMIT 1
            """,
            (member_id, list(required)),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def require_member_app_access(
    user: dict | None,
    required_codes: str | list[str] | tuple[str, ...] | set[str],
    *,
    allow_admin: bool = True,
    detail: str = "Access denied",
) -> None:
    if user_has_member_app_access(user, required_codes, allow_admin=allow_admin):
        return
    raise HTTPException(status_code=403, detail=detail)
