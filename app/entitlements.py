from __future__ import annotations

import re
from fastapi import HTTPException

from app.database import get_connection


ONLINE_PRACTICE_APP_CODES = {"math", "spelling", "general", "comprehension"}
ACTIVE_ONLINE_PRACTICE_PERMALINK_APP_CODE = {
    "ztwxby": "math",
    "gxvtls": "spelling",
    "sddokb": "general",
    "gckvb": "comprehension",
}

ACTIVE_MATH_MOCK_PERMALINK_TEST_ID = {
    "zqwlsf": "MATH_MOCK_1",
    "ohnryj": "MATH_MOCK_2",
    "edaol": "MATH_MOCK_3",
    "vrkrb": "MATH_MOCK_4",
    "etswx": "MATH_MOCK_5",
    "ptyyuo": "MATH_MOCK_6",
    "rwzwvf": "MATH_MOCK_7",
    "xgupvl": "MATH_MOCK_8",
    "enjhd": "MATH_MOCK_9",
    "gbveam": "MATH_MOCK_10",
    "wnqoqg": "MATH_MOCK_11",
    "xkgiqu": "MATH_MOCK_12",
}

ACTIVE_VR_PERMALINK_TO_KEY = {
    "qoipgs": "printable_vr_1",
    "hquiw": "printable_vr_2",
    "nsfah": "printable_vr_3",
    "fjzif": "printable_vr_4",
    "kgbqum": "printable_vr_5",
    "zwfglb": "printable_vr_6",
    "gsmpyn": "printable_vr_7",
    "efibzj": "printable_vr_8",
    "luiiv": "printable_vr_9",
}

ACTIVE_COMPREHENSION_PERMALINK_TO_KEY = {
    "exjlsl": "printable_comprehension_1",
    "rgznog": "printable_comprehension_2",
    "rbtolw": "printable_comprehension_3",
    "dtzldn": "printable_comprehension_4",
    "afjgni": "printable_comprehension_5",
    "ilgta": "printable_comprehension_6",
    "shixax": "printable_comprehension_7",
}

DISABLED_OR_IGNORED_PERMALINKS = {
    # Bundles / packs (disabled for V1)
    "akizdp",
    "rswci",
    "silvi",
    "nzoruy",
    # Maths printable historical overlap is intentionally not wired in V1.
}

VR_PAPER_CODE_TO_KEY = {
    "vr-p1": "printable_vr_1",
    "vr-p2": "printable_vr_2",
    "vr-p3": "printable_vr_3",
    "vr-p4": "printable_vr_4",
    "vr-p5": "printable_vr_5",
    "vr-p6": "printable_vr_6",
    "vr-p7": "printable_vr_7",
    "vr-p8": "printable_vr_8",
    "vr-p9": "printable_vr_9",
}


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


def _extract_permalink_from_url(url_value: str | None) -> str:
    normalized = (url_value or "").strip().rstrip("/")
    if not normalized:
        return ""
    match = re.search(r"/l/([A-Za-z0-9_-]+)(?:[/?#].*)?$", normalized)
    return (match.group(1).lower() if match else "")


def normalize_gumroad_identifier(value: str | None) -> str:
    token = (value or "").strip().lower().rstrip("/")
    if not token:
        return ""
    permalink_token = _extract_permalink_from_url(token)
    if permalink_token:
        return permalink_token
    # Keep slug-like identifiers only (must include at least one letter).
    # Raw numeric product IDs are not stable entitlement keys in this service.
    if re.fullmatch(r"[a-z0-9_-]+", token) and re.search(r"[a-z]", token):
        return token
    return ""


def _is_purchase_event_type(event_type: str | None) -> bool:
    normalized = str(event_type or "").strip().lower()
    return normalized in {"sale", "purchase", "sale.created", "purchase.created"}


def _is_refund_event_type(event_type: str | None) -> bool:
    normalized = str(event_type or "").strip().lower()
    return normalized in {"refund", "chargeback", "refund.created", "chargeback.created"}


def _resolve_printable_or_active_key(product_name: str, product_permalink: str, product_id: str) -> str | None:
    permalink = normalize_gumroad_identifier(product_permalink)
    pid = normalize_gumroad_identifier(product_id)
    name = (product_name or "").strip().lower()

    for token in (permalink, pid):
        if token in ACTIVE_VR_PERMALINK_TO_KEY:
            return ACTIVE_VR_PERMALINK_TO_KEY[token]
        if token in ACTIVE_COMPREHENSION_PERMALINK_TO_KEY:
            return ACTIVE_COMPREHENSION_PERMALINK_TO_KEY[token]
        if token in ACTIVE_ONLINE_PRACTICE_PERMALINK_APP_CODE:
            return f"module_{ACTIVE_ONLINE_PRACTICE_PERMALINK_APP_CODE[token]}"
        if token in ACTIVE_MATH_MOCK_PERMALINK_TEST_ID:
            return f"mock_{ACTIVE_MATH_MOCK_PERMALINK_TEST_ID[token]}"
        if token in DISABLED_OR_IGNORED_PERMALINKS:
            return "disabled_ignored_product"

    comp_match = re.search(r"comprehension.*\((\d+)\)", name)
    if comp_match:
        return f"printable_comprehension_{int(comp_match.group(1))}"

    vr_match = re.search(r"verbal reasoning.*\((\d+)\)", name)
    if vr_match:
        return f"printable_vr_{int(vr_match.group(1))}"

    return None


def get_printable_purchase_state_for_email(user_email: str | None) -> tuple[set[str], set[str]]:
    email = str(user_email or "").strip().lower()
    if not email:
        return set(), set()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT product_name, event_type, id
            FROM math_gumroad_events
            WHERE LOWER(email) = LOWER(%s)
            ORDER BY id ASC
            """,
            (email,),
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    is_active_by_key: dict[str, bool] = {}
    permalink_by_key: dict[str, str] = {}

    for product_name, event_type, _event_id in rows:
        payload = str(product_name or "")
        permalink_match = re.search(r"permalink=([A-Za-z0-9_-]+)", payload)
        permalink = (permalink_match.group(1).lower() if permalink_match else "")
        base_name = payload.split("|", 1)[0].strip()
        key = _resolve_printable_or_active_key(base_name, permalink, "")
        if not key:
            continue
        if _is_purchase_event_type(event_type):
            is_active_by_key[key] = True
            if permalink:
                permalink_by_key[key] = permalink
        elif _is_refund_event_type(event_type):
            is_active_by_key[key] = False

    purchased_keys = {key for key, active in is_active_by_key.items() if active}
    purchased_permalinks = {
        permalink_by_key[key]
        for key in purchased_keys
        if key.startswith("printable_") and permalink_by_key.get(key)
    }
    return purchased_keys, purchased_permalinks


def email_has_printable_key_access(user_email: str | None, product_key: str) -> bool:
    purchased_keys, _ = get_printable_purchase_state_for_email(user_email)
    return product_key in purchased_keys


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
