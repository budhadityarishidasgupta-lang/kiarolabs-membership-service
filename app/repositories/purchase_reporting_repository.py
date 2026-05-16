from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Any

from app.database import get_connection


def _normalize(value: Any) -> str:
    return str(value or "").strip()


def _parse_event_payload(payload: str) -> dict[str, str]:
    raw = _normalize(payload)
    base = raw.split("|", 1)[0].strip()
    permalink_match = re.search(r"permalink=([A-Za-z0-9_-]+)", raw)
    sale_id_match = re.search(r"sale_id=([^|]+)", raw)
    return {
        "product_title": base,
        "permalink": (permalink_match.group(1).lower() if permalink_match else ""),
        "sale_id": (sale_id_match.group(1).strip() if sale_id_match else ""),
    }


def _resolve_category(product_key: str | None) -> str:
    key = _normalize(product_key).lower()
    if not key:
        return "unknown"
    if key.startswith("module_"):
        return "online_practice"
    if key.startswith("mock_"):
        return "mock_exam"
    if key.startswith("printable_vr_"):
        return "vr_printable"
    if key.startswith("printable_comprehension_"):
        return "comprehension_printable"
    if key.startswith("disabled_"):
        return "disabled"
    return "unknown"


def _resolve_entitlement_target(product_key: str | None) -> str:
    key = _normalize(product_key)
    if not key:
        return ""
    if key.startswith("module_"):
        return f"kiaro_membership.member_apps:{key.replace('module_', '', 1)}"
    if key.startswith("mock_"):
        return f"math_user_test_access:{key.replace('mock_', '', 1)}"
    if key.startswith("printable_"):
        return f"math_gumroad_events:{key}"
    if key.startswith("disabled_"):
        return "none(disabled)"
    return ""


def _resolve_processing_status(*, processed: bool, member_id: int | None, product_key: str | None) -> str:
    key = _normalize(product_key)
    if not processed:
        return "pending_user_not_found" if member_id is None else "failed"
    if key == "disabled_ignored_product" or key.startswith("disabled_bundle_"):
        return "ignored_disabled_product"
    if not key:
        return "unknown_product"
    return "processed"


def _to_iso(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def list_purchase_events(
    *,
    resolve_product_key,
    email: str | None = None,
    category: str | None = None,
    status: str | None = None,
    permalink: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'math_gumroad_events'
            """
        )
        event_columns = {str(row[0]).strip().lower() for row in (cur.fetchall() or [])}
        created_col = "e.created_at" if "created_at" in event_columns else "NULL::timestamp"

        cur.execute(
            f"""
            SELECT
                e.id,
                e.email,
                e.product_name,
                e.event_type,
                e.test_id,
                e.processed,
                {created_col},
                m.id AS member_id,
                m.email AS member_email
            FROM math_gumroad_events e
            LEFT JOIN kiaro_membership.members m
              ON LOWER(m.email) = LOWER(e.email)
            ORDER BY e.id DESC
            """
        )
        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    items: list[dict] = []
    for row in rows:
        (
            event_id,
            buyer_email,
            product_payload,
            event_type,
            test_id,
            processed,
            created_at,
            member_id,
            member_email,
        ) = row

        parsed = _parse_event_payload(str(product_payload or ""))
        product_title = parsed["product_title"]
        product_permalink = parsed["permalink"]
        sale_id = parsed["sale_id"]
        product_key = resolve_product_key(product_title, product_permalink, "")
        category_value = _resolve_category(product_key)
        entitlement_target = _resolve_entitlement_target(product_key)
        processing_status = _resolve_processing_status(
            processed=bool(processed),
            member_id=member_id,
            product_key=product_key,
        )

        item = {
            "id": event_id,
            "purchase_datetime": _to_iso(created_at),
            "buyer_email": _normalize(buyer_email).lower(),
            "member_email": _normalize(member_email).lower(),
            "member_id": member_id,
            "gumroad_product_title": product_title,
            "gumroad_permalink": product_permalink,
            "sale_id": sale_id,
            "event_type": _normalize(event_type).lower(),
            "product_category": category_value,
            "price": "",
            "currency": "",
            "vat_tax": "",
            "total_paid": "",
            "entitlement_target": entitlement_target,
            "entitlement_granted": "yes" if processing_status == "processed" else "no",
            "processing_status": processing_status,
            "error_message": "" if processing_status == "processed" else processing_status,
            "test_id": _normalize(test_id),
            "raw_product_payload": _normalize(product_payload),
        }
        items.append(item)

    email_filter = _normalize(email).lower()
    category_filter = _normalize(category).lower()
    status_filter = _normalize(status).lower()
    permalink_filter = _normalize(permalink).lower()
    from_filter = _normalize(date_from)
    to_filter = _normalize(date_to)

    def _in_date_window(item: dict) -> bool:
        dt = _normalize(item.get("purchase_datetime"))
        if not dt:
            return True
        day = dt[:10]
        if from_filter and day < from_filter:
            return False
        if to_filter and day > to_filter:
            return False
        return True

    filtered = []
    for item in items:
        if email_filter and email_filter not in _normalize(item.get("buyer_email")).lower() and email_filter not in _normalize(item.get("member_email")).lower():
            continue
        if category_filter and category_filter != _normalize(item.get("product_category")).lower():
            continue
        if status_filter and status_filter != _normalize(item.get("processing_status")).lower():
            continue
        if permalink_filter and permalink_filter != _normalize(item.get("gumroad_permalink")).lower():
            continue
        if not _in_date_window(item):
            continue
        filtered.append(item)

    return filtered


def render_purchase_events_csv(rows: list[dict]) -> str:
    output = io.StringIO()
    fieldnames = [
        "purchase_datetime",
        "buyer_email",
        "member_email",
        "member_id",
        "gumroad_product_title",
        "gumroad_permalink",
        "sale_id",
        "event_type",
        "product_category",
        "price",
        "currency",
        "vat_tax",
        "total_paid",
        "entitlement_target",
        "entitlement_granted",
        "processing_status",
        "error_message",
        "test_id",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in fieldnames})
    return output.getvalue()
