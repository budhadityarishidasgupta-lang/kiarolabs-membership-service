from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.database import get_connection


@dataclass(frozen=True)
class SeedProduct:
    product_code: str
    page_section: str
    frontend_card_name: str
    provider_product_key: str
    provider_product_url: str
    provider_product_name: str
    provider_status: str = "Published"
    provider: str = "gumroad"


def _build_seed_product(
    *,
    product_code: str,
    page_section: str,
    frontend_card_name: str,
    provider_product_key: str,
    provider_product_name: str,
    provider_status: str = "Published",
) -> SeedProduct:
    key = str(provider_product_key or "").strip().lower()
    return SeedProduct(
        product_code=str(product_code or "").strip().upper(),
        page_section=page_section,
        frontend_card_name=frontend_card_name,
        provider_product_key=key,
        provider_product_url=f"https://kiarolabs.gumroad.com/l/{key}" if key else "",
        provider_product_name=provider_product_name,
        provider_status=provider_status,
    )


def _infer_product_family(product_code: str) -> str:
    code = str(product_code or "").strip().upper()
    if code.startswith("MME"):
        return "mock_exam"
    if code.endswith("SM") or code == "CHM":
        return "online_practice"
    if code.startswith(("MPP", "EPP", "CPP", "VRPP")):
        return "printable_paper"
    return "product"


def _infer_product_type(product_code: str) -> str:
    code = str(product_code or "").strip().upper()
    if code.startswith("MME"):
        return "single_mock_exam"
    if code.endswith("SM") or code == "CHM":
        return "module"
    if code.startswith(("MPP", "EPP", "CPP", "VRPP")):
        return "single_paper"
    return "product"


def _infer_subject(product_code: str) -> str:
    code = str(product_code or "").strip().upper()
    if code.startswith("MPP") or code == "MSM" or code.startswith("MME"):
        return "maths"
    if code.startswith("EPP"):
        return "english"
    if code.startswith("CPP") or code == "CHM":
        return "comprehension"
    if code.startswith("VRPP"):
        return "verbal-reasoning"
    if code == "WSM":
        return "words"
    if code == "SSM":
        return "spelling"
    return "general"


def _entitlement_for_product_code(product_code: str) -> tuple[str, str]:
    code = str(product_code or "").strip().upper()
    module_map = {
        "MSM": ("member_app", "math"),
        "SSM": ("member_app", "spelling"),
        "WSM": ("member_app", "general"),
        "GSM": ("member_app", "grammar"),
        "CHM": ("member_app", "comprehension"),
    }
    if code in module_map:
        return module_map[code]
    if code.startswith("MME"):
        suffix = code[3:]
        if suffix.isdigit():
            return ("mock_test_access", f"MATH_MOCK_{int(suffix)}")
    return ("product_access", code)


CATALOG_SEED_PRODUCTS: list[SeedProduct] = [
    _build_seed_product(product_code="MPP1", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 01", provider_product_key="snvrji", provider_product_name="11+ Maths Exam Practice Pack (1)"),
    _build_seed_product(product_code="MPP2", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 02", provider_product_key="dhylz", provider_product_name="11+ Maths Exam Practice Pack (2)"),
    _build_seed_product(product_code="MPP3", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 03", provider_product_key="kgqflr", provider_product_name="11+ Maths Exam Practice Pack (3)"),
    _build_seed_product(product_code="MPP4", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 04", provider_product_key="rbkiw", provider_product_name="11+ Maths Exam Practice Pack (4)"),
    _build_seed_product(product_code="MPP5", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 05", provider_product_key="uhlkh", provider_product_name="11+ Maths Exam Practice Pack (5)"),
    _build_seed_product(product_code="MPP6", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 06", provider_product_key="cjbvx", provider_product_name="11+ Maths Exam Practice Pack (6)"),
    _build_seed_product(product_code="MPP7", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 07", provider_product_key="ytldyf", provider_product_name="11+ Maths Exam Practice Pack (7)"),
    _build_seed_product(product_code="MPP8", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 08", provider_product_key="unnopn", provider_product_name="11+ Maths Exam Practice Pack (8)"),
    _build_seed_product(product_code="MPP9", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 09", provider_product_key="wehuf", provider_product_name="11+ Maths Exam Practice Pack (9)"),
    _build_seed_product(product_code="MPP10", page_section="Printable Papers", frontend_card_name="Maths Practice Paper 10", provider_product_key="pnmquw", provider_product_name="11+ Maths Exam Practice Pack (10)"),
    _build_seed_product(product_code="EPP1", page_section="Printable Papers", frontend_card_name="English Practice Pack 01", provider_product_key="phekgk", provider_product_name="English Practice Pack (1)"),
    _build_seed_product(product_code="EPP2", page_section="Printable Papers", frontend_card_name="English Practice Pack 02", provider_product_key="cclsi", provider_product_name="English Practice Pack (2)"),
    _build_seed_product(product_code="EPP3", page_section="Printable Papers", frontend_card_name="English Practice Pack 03", provider_product_key="urrvk", provider_product_name="English Practice Pack (3)"),
    _build_seed_product(product_code="EPP4", page_section="Printable Papers", frontend_card_name="English Practice Pack 04", provider_product_key="wuwrog", provider_product_name="English Practice Pack (4)"),
    _build_seed_product(product_code="EPP5", page_section="Printable Papers", frontend_card_name="English Practice Pack 05", provider_product_key="srvxaj", provider_product_name="English Practice Pack (5)"),
    _build_seed_product(product_code="EPP6", page_section="Printable Papers", frontend_card_name="English Practice Pack 06", provider_product_key="reesgh", provider_product_name="English Practice Pack (6)"),
    _build_seed_product(product_code="EPP7", page_section="Printable Papers", frontend_card_name="English Practice Pack 07", provider_product_key="zsioja", provider_product_name="English Practice Pack (7)"),
    _build_seed_product(product_code="EPP8", page_section="Printable Papers", frontend_card_name="English Practice Pack 08", provider_product_key="vhprd", provider_product_name="English Practice Pack (8)"),
    _build_seed_product(product_code="EPP9", page_section="Printable Papers", frontend_card_name="English Practice Pack 09", provider_product_key="aihlvo", provider_product_name="English Practice Pack (9)"),
    _build_seed_product(product_code="EPP10", page_section="Printable Papers", frontend_card_name="English Practice Pack 10", provider_product_key="bweqr", provider_product_name="English Practice Pack (10)"),
    _build_seed_product(product_code="CPP1", page_section="Printable Papers", frontend_card_name="Comprehension Set 1", provider_product_key="exjlsl", provider_product_name="English Comprehension (1)"),
    _build_seed_product(product_code="CPP2", page_section="Printable Papers", frontend_card_name="Comprehension Set 2", provider_product_key="rgznog", provider_product_name="English Comprehension (2)"),
    _build_seed_product(product_code="CPP3", page_section="Printable Papers", frontend_card_name="Comprehension Set 3", provider_product_key="rbtolw", provider_product_name="English Comprehension (3)"),
    _build_seed_product(product_code="CPP4", page_section="Printable Papers", frontend_card_name="Comprehension Set 4", provider_product_key="dtzldn", provider_product_name="English Comprehension (4)"),
    _build_seed_product(product_code="CPP5", page_section="Printable Papers", frontend_card_name="Comprehension Set 5", provider_product_key="afjgni", provider_product_name="English Comprehension (5)"),
    _build_seed_product(product_code="CPP6", page_section="Printable Papers", frontend_card_name="Comprehension Set 6", provider_product_key="ilgta", provider_product_name="English Comprehension (6)"),
    _build_seed_product(product_code="CPP7", page_section="Printable Papers", frontend_card_name="Comprehension Set 7", provider_product_key="shixax", provider_product_name="English Comprehension (7)"),
    _build_seed_product(product_code="VRPP1", page_section="Printable Papers", frontend_card_name="VR Practice Paper 01", provider_product_key="qoipgs", provider_product_name="VR Practice Paper 01"),
    _build_seed_product(product_code="VRPP2", page_section="Printable Papers", frontend_card_name="VR Practice Paper 02", provider_product_key="hquiw", provider_product_name="VR Practice Paper 02"),
    _build_seed_product(product_code="VRPP3", page_section="Printable Papers", frontend_card_name="VR Practice Paper 03", provider_product_key="nsfah", provider_product_name="VR Practice Paper 03"),
    _build_seed_product(product_code="VRPP4", page_section="Printable Papers", frontend_card_name="VR Practice Paper 04", provider_product_key="fjzif", provider_product_name="VR Practice Paper 04"),
    _build_seed_product(product_code="VRPP5", page_section="Printable Papers", frontend_card_name="VR Practice Paper 05", provider_product_key="kgbqum", provider_product_name="VR Practice Paper 05"),
    _build_seed_product(product_code="VRPP6", page_section="Printable Papers", frontend_card_name="VR Practice Paper 06", provider_product_key="zwfglb", provider_product_name="VR Practice Paper 06"),
    _build_seed_product(product_code="VRPP7", page_section="Printable Papers", frontend_card_name="VR Practice Paper 07", provider_product_key="gsmpyn", provider_product_name="VR Practice Paper 07"),
    _build_seed_product(product_code="VRPP8", page_section="Printable Papers", frontend_card_name="VR Practice Paper 08", provider_product_key="efibzj", provider_product_name="VR Practice Paper 08"),
    _build_seed_product(product_code="VRPP9", page_section="Printable Papers", frontend_card_name="VR Practice Paper 09", provider_product_key="luiiv", provider_product_name="VR Practice Paper 09"),
    _build_seed_product(product_code="MME1", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 1", provider_product_key="zqwlsf", provider_product_name="Maths Mock Exam 1"),
    _build_seed_product(product_code="MME2", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 2", provider_product_key="ohnryj", provider_product_name="Maths Mock Exam 2"),
    _build_seed_product(product_code="MME3", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 3", provider_product_key="edaol", provider_product_name="Maths Mock Exam 3"),
    _build_seed_product(product_code="MME4", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 4", provider_product_key="vrkrb", provider_product_name="Maths Mock Exam 4"),
    _build_seed_product(product_code="MME5", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 5", provider_product_key="etswx", provider_product_name="Maths Mock Exam 5"),
    _build_seed_product(product_code="MME6", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 6", provider_product_key="ptyyuo", provider_product_name="Maths Mock Exam 6"),
    _build_seed_product(product_code="MME7", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 7", provider_product_key="rwzwvf", provider_product_name="Maths Mock Exam 7"),
    _build_seed_product(product_code="MME8", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 8", provider_product_key="xkgiqu", provider_product_name="Maths Mock Exam 8"),
    _build_seed_product(product_code="MME9", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 9", provider_product_key="rswci", provider_product_name="Maths Mock Exam 9"),
    _build_seed_product(product_code="MME10", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 10", provider_product_key="akizdp", provider_product_name="Maths Mock Exam 10"),
    _build_seed_product(product_code="MME11", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 11", provider_product_key="enjhd", provider_product_name="Maths Mock Exam 11"),
    _build_seed_product(product_code="MME12", page_section="Mock Exams", frontend_card_name="Maths Mock Exam 12", provider_product_key="silvi", provider_product_name="Maths Mock Exam 12"),
    _build_seed_product(product_code="MSM", page_section="Online Practice / Packs", frontend_card_name="MathSprint Module", provider_product_key="ztwxby", provider_product_name="MathSprint Module"),
    _build_seed_product(product_code="SSM", page_section="Online Practice / Packs", frontend_card_name="SpellingSprint Module", provider_product_key="gxvtls", provider_product_name="SpellingSprint Module"),
    _build_seed_product(product_code="WSM", page_section="Online Practice / Packs", frontend_card_name="WordSprint Module", provider_product_key="sddokb", provider_product_name="WordSprint Module"),
    _build_seed_product(product_code="GSM", page_section="Online Practice / Packs", frontend_card_name="GrammarSprint Module", provider_product_key="gsm", provider_product_name="GrammarSprint Module"),
    _build_seed_product(product_code="CHM", page_section="Online Practice / Packs", frontend_card_name="ComprehensionSprint Module", provider_product_key="gckvb", provider_product_name="ComprehensionSprint"),
]


def init_product_catalog_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_products (
                product_code TEXT PRIMARY KEY,
                page_section TEXT NOT NULL,
                frontend_card_name TEXT NOT NULL,
                product_family TEXT NOT NULL,
                product_type TEXT NOT NULL,
                subject TEXT NOT NULL,
                entitlement_type TEXT NOT NULL,
                entitlement_value TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_provider_products (
                id SERIAL PRIMARY KEY,
                product_code TEXT NOT NULL REFERENCES catalog_products(product_code) ON DELETE CASCADE,
                provider TEXT NOT NULL,
                provider_product_key TEXT NOT NULL UNIQUE,
                provider_product_url TEXT NOT NULL,
                provider_product_name TEXT NOT NULL,
                provider_status TEXT NOT NULL DEFAULT 'Published',
                is_current BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS member_product_access (
                id SERIAL PRIMARY KEY,
                member_id INTEGER NOT NULL,
                product_code TEXT NOT NULL REFERENCES catalog_products(product_code) ON DELETE CASCADE,
                provider TEXT NOT NULL DEFAULT 'gumroad',
                provider_product_key TEXT,
                sale_id TEXT,
                purchase_email TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (member_id, product_code)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_member_product_access_member_status
            ON member_product_access (member_id, status)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_catalog_provider_products_code_current
            ON catalog_provider_products (product_code, is_current)
            """
        )

        for seed in CATALOG_SEED_PRODUCTS:
            entitlement_type, entitlement_value = _entitlement_for_product_code(seed.product_code)
            cur.execute(
                """
                INSERT INTO catalog_products (
                    product_code,
                    page_section,
                    frontend_card_name,
                    product_family,
                    product_type,
                    subject,
                    entitlement_type,
                    entitlement_value,
                    is_active,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (product_code) DO UPDATE
                SET
                    page_section = EXCLUDED.page_section,
                    frontend_card_name = EXCLUDED.frontend_card_name,
                    product_family = EXCLUDED.product_family,
                    product_type = EXCLUDED.product_type,
                    subject = EXCLUDED.subject,
                    entitlement_type = EXCLUDED.entitlement_type,
                    entitlement_value = EXCLUDED.entitlement_value,
                    is_active = TRUE,
                    updated_at = NOW()
                """,
                (
                    seed.product_code,
                    seed.page_section,
                    seed.frontend_card_name,
                    _infer_product_family(seed.product_code),
                    _infer_product_type(seed.product_code),
                    _infer_subject(seed.product_code),
                    entitlement_type,
                    entitlement_value,
                ),
            )
            cur.execute(
                """
                UPDATE catalog_provider_products
                SET is_current = FALSE, updated_at = NOW()
                WHERE product_code = %s
                  AND provider = %s
                  AND provider_product_key <> %s
                """,
                (seed.product_code, seed.provider, seed.provider_product_key),
            )
            cur.execute(
                """
                INSERT INTO catalog_provider_products (
                    product_code,
                    provider,
                    provider_product_key,
                    provider_product_url,
                    provider_product_name,
                    provider_status,
                    is_current,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (provider_product_key) DO UPDATE
                SET
                    product_code = EXCLUDED.product_code,
                    provider = EXCLUDED.provider,
                    provider_product_url = EXCLUDED.provider_product_url,
                    provider_product_name = EXCLUDED.provider_product_name,
                    provider_status = EXCLUDED.provider_status,
                    is_current = TRUE,
                    updated_at = NOW()
                """,
                (
                    seed.product_code,
                    seed.provider,
                    seed.provider_product_key,
                    seed.provider_product_url,
                    seed.provider_product_name,
                    seed.provider_status,
                ),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def resolve_product_by_provider_identifier(identifiers: set[str], *, provider: str = "gumroad", conn=None) -> dict[str, Any] | None:
    normalized = [str(identifier or "").strip().lower() for identifier in identifiers if str(identifier or "").strip()]
    if not normalized:
        return None
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                p.product_code,
                p.page_section,
                p.frontend_card_name,
                p.product_family,
                p.product_type,
                p.subject,
                p.entitlement_type,
                p.entitlement_value,
                cpp.provider,
                cpp.provider_product_key,
                cpp.provider_product_url,
                cpp.provider_product_name,
                cpp.provider_status,
                cpp.is_current
            FROM catalog_provider_products cpp
            JOIN catalog_products p
              ON p.product_code = cpp.product_code
            WHERE cpp.provider = %s
              AND cpp.provider_product_key = ANY(%s)
            ORDER BY cpp.is_current DESC, p.product_code ASC
            LIMIT 1
            """,
            (provider, normalized),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "product_code": str(row[0]),
            "page_section": str(row[1]),
            "frontend_card_name": str(row[2]),
            "product_family": str(row[3]),
            "product_type": str(row[4]),
            "subject": str(row[5]),
            "entitlement_type": str(row[6]),
            "entitlement_value": str(row[7]),
            "provider": str(row[8]),
            "provider_product_key": str(row[9]),
            "provider_product_url": str(row[10]),
            "provider_product_name": str(row[11]),
            "provider_status": str(row[12]),
            "is_current": bool(row[13]),
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def upsert_member_product_access(
    *,
    member_id: int,
    purchase_email: str,
    product_code: str,
    provider_product_key: str | None = None,
    provider: str = "gumroad",
    sale_id: str | None = None,
    status: str = "active",
    conn=None,
) -> None:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO member_product_access (
                member_id,
                product_code,
                provider,
                provider_product_key,
                sale_id,
                purchase_email,
                status,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (member_id, product_code) DO UPDATE
            SET
                provider = EXCLUDED.provider,
                provider_product_key = EXCLUDED.provider_product_key,
                sale_id = EXCLUDED.sale_id,
                purchase_email = EXCLUDED.purchase_email,
                status = EXCLUDED.status,
                updated_at = NOW()
            """,
            (
                int(member_id),
                str(product_code or "").strip().upper(),
                provider,
                str(provider_product_key or "").strip().lower() or None,
                str(sale_id or "").strip() or None,
                str(purchase_email or "").strip().lower() or None,
                str(status or "active").strip().lower(),
            ),
        )
        if owns_connection:
            conn.commit()
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_owned_product_codes_for_email(user_email: str | None, *, families: set[str] | None = None, conn=None) -> set[str]:
    email = str(user_email or "").strip().lower()
    if not email:
        return set()
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        if families:
            cur.execute(
                """
                SELECT DISTINCT mpa.product_code
                FROM member_product_access mpa
                JOIN kiaro_membership.members m
                  ON m.id = mpa.member_id
                JOIN catalog_products p
                  ON p.product_code = mpa.product_code
                WHERE LOWER(m.email) = LOWER(%s)
                  AND mpa.status = 'active'
                  AND p.product_family = ANY(%s)
                """,
                (email, list(families)),
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT mpa.product_code
                FROM member_product_access mpa
                JOIN kiaro_membership.members m
                  ON m.id = mpa.member_id
                WHERE LOWER(m.email) = LOWER(%s)
                  AND mpa.status = 'active'
                """,
                (email,),
            )
        return {str(row[0]).strip().upper() for row in (cur.fetchall() or []) if row and row[0]}
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def user_has_product_code_access(
    *,
    user_email: str,
    product_codes: list[str] | set[str] | tuple[str, ...],
    conn=None,
) -> bool:
    normalized = {str(code or "").strip().upper() for code in product_codes if str(code or "").strip()}
    if not normalized:
        return False
    owned_codes = get_owned_product_codes_for_email(user_email, conn=conn)
    return bool(owned_codes.intersection(normalized))


def user_has_product_prefix_access(
    *,
    user_email: str,
    prefixes: list[str] | set[str] | tuple[str, ...],
    conn=None,
) -> bool:
    normalized_prefixes = {str(prefix or "").strip().upper() for prefix in prefixes if str(prefix or "").strip()}
    if not normalized_prefixes:
        return False
    owned_codes = get_owned_product_codes_for_email(user_email, conn=conn)
    return any(any(code.startswith(prefix) for prefix in normalized_prefixes) for code in owned_codes)


def get_current_printable_catalog(*, conn=None) -> list[dict[str, Any]]:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                p.product_code,
                p.page_section,
                p.frontend_card_name,
                p.product_family,
                p.product_type,
                p.subject,
                cpp.provider_product_key,
                cpp.provider_product_url,
                cpp.provider_product_name,
                cpp.provider_status
            FROM catalog_products p
            JOIN catalog_provider_products cpp
              ON cpp.product_code = p.product_code
             AND cpp.is_current = TRUE
            WHERE p.product_family = 'printable_paper'
              AND p.is_active = TRUE
            ORDER BY p.subject, p.product_code
            """
        )
        rows = cur.fetchall() or []
        return [
            {
                "product_code": str(row[0]),
                "page_section": str(row[1]),
                "frontend_card_name": str(row[2]),
                "product_family": str(row[3]),
                "product_type": str(row[4]),
                "subject": str(row[5]),
                "provider_product_key": str(row[6]),
                "provider_product_url": str(row[7]),
                "provider_product_name": str(row[8]),
                "provider_status": str(row[9]),
            }
            for row in rows
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()
