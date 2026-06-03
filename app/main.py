import os
import re
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException, Depends, Body
#from fastapi.security import OAuth2PasswordBearer
from app.auth import get_current_user, resolve_verified_learning_user_id
from pydantic import BaseModel, EmailStr
from app.database import get_connection
from app.database_init_words import init_words_tables
from datetime import datetime, timedelta
from passlib.context import CryptContext
from jose import jwt, JWTError
from app.admin.ingestion_router import printable_router
from app.admin.ingestion_router import router as admin_ingestion_router
from app.admin.branding_router import router as admin_branding_router
from app.admin.curriculum_router import router as admin_curriculum_router
from app.practice.router import admin_router as practice_admin_router
from app.practice.router import router as practice_router
from app.practice.math_test_engine import init_math_submission_tables
from app.ingestion.english_printable.service import init_english_paper_printable_tables
from app.ingestion.verbal_reasoning.service import init_verbal_reasoning_printable_tables
from typing import Optional
from app.comprehension.router import router as comprehension_router
from app.auth_reset import init_password_reset_tables, router as auth_reset_router
from app.practice.synonym_engine import get_synonym_attempt_summary
from app.entitlements import (
    ACTIVE_MATH_MOCK_PERMALINK_TEST_ID,
    ACTIVE_ONLINE_PRACTICE_PERMALINK_APP_CODE,
    DISABLED_OR_IGNORED_PERMALINKS,
    normalize_gumroad_identifier,
    get_printable_purchase_state_for_email,
)
from app.product_catalog import (
    get_current_printable_catalog,
    get_owned_product_codes_for_email,
    init_product_catalog_tables,
    resolve_product_by_provider_identifier,
    upsert_member_product_access,
)


# =========================
# Config
# =========================
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 2

pwd_context = CryptContext(
    schemes=["bcrypt", "pbkdf2_sha256"],
    deprecated="auto"
)
#oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

app = FastAPI()


@app.on_event("startup")
def startup_event():
    try:
        init_words_tables()
        print("✅ words tables initialized")
    except Exception as e:
        print("❌ words init failed:", e)

    try:
        init_password_reset_tables()
        print("password reset tables initialized")
    except Exception as e:
        print("password reset init failed:", e)

    try:
        init_math_submission_tables()
        print("math submission tables initialized")
    except Exception as e:
        print("math submission init failed:", e)

    try:
        init_verbal_reasoning_printable_tables()
        print("verbal reasoning printable tables initialized")
    except Exception as e:
        print("verbal reasoning printable init failed:", e)

    try:
        init_english_paper_printable_tables()
        print("english paper printable tables initialized")
    except Exception as e:
        print("english paper printable init failed:", e)

    try:
        init_product_catalog_tables()
        print("product catalog tables initialized")
    except Exception as e:
        print("product catalog init failed:", e)

# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://kiarolabs.com",
        "https://www.kiarolabs.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# =========================
# Models
# =========================
class RegisterRequest(BaseModel):
    name: str | None = None
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class AdminUserAppsUpdateRequest(BaseModel):
    email: EmailStr
    apps: list[str]


AVAILABLE_APP_CATALOG = [
    {
        "app_code": "general",
        "label": "WordSprint",
        "description": "Core word learning access",
        "group": "core",
    },
    {
        "app_code": "spelling",
        "label": "SpellingSprint",
        "description": "Spelling lesson access",
        "group": "core",
    },
    {
        "app_code": "math",
        "label": "MathSprint",
        "description": "Maths lesson access",
        "group": "core",
    },
    {
        "app_code": "practice",
        "label": "Printable Papers",
        "description": "Printable practice paper access",
        "group": "products",
    },
    {
        "app_code": "vr_printables",
        "label": "VR Printables",
        "description": "Verbal reasoning printable access",
        "group": "products",
    },
    {
        "app_code": "vr_single_paper",
        "label": "VR Single Paper",
        "description": "Single verbal reasoning paper access",
        "group": "products",
    },
    {
        "app_code": "vr_starter_pack",
        "label": "VR Starter Pack",
        "description": "Starter pack verbal reasoning access",
        "group": "products",
    },
    {
        "app_code": "vr_complete_pack",
        "label": "VR Complete Pack",
        "description": "Complete verbal reasoning pack access",
        "group": "products",
    },
    {
        "app_code": "mock",
        "label": "Mock Exams",
        "description": "Mock exam access",
        "group": "products",
    },
    {
        "app_code": "comprehension",
        "label": "ComprehensionSprint",
        "description": "Comprehension passage access",
        "group": "products",
    },
    {
        "app_code": "nvr",
        "label": "NVRSprint",
        "description": "NVR access",
        "group": "future",
    },
]


# =========================
# Helpers
# =========================
def create_access_token(email: str, account_type: str, user_id: str | None = None):
    expire = datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)

    payload = {
        "sub": email,
        "user_id": user_id,
        "account_type": account_type,
        "exp": expire
    }

    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)

    return token, expire


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


#def get_current_user(token: str = Depends(oauth2_scheme)):
#    try:
#        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
#        email = payload.get("sub")
#        if not email:
#            raise HTTPException(status_code=401, detail="Invalid token")
#        return payload
#    except JWTError:
#        raise HTTPException(status_code=401, detail="Invalid or expired token")
#

def derive_subscription_state(subscription_status: str | None, subscription_end):
    now = datetime.utcnow()

    if subscription_end and subscription_end < now:
        return False, "subscription_expired"

    if subscription_status == "active":
        return True, "active"

    if subscription_status == "cancelled":
        return True, "cancelled"

    return False, subscription_status or "inactive"


def _get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cur.fetchall()}


def _fetch_attempt_accuracy(cur, table_name: str, user_id: int, *, user_columns: list[str], correct_columns: list[str]):
    columns = _get_table_columns(cur, table_name)
    matched_user_columns = [column for column in user_columns if column in columns]
    matched_correct_column = next((column for column in correct_columns if column in columns), None)

    if not matched_user_columns or not matched_correct_column:
        return 0, 0.0

    where_clause = " OR ".join(f"{column} = %s" for column in matched_user_columns)
    params = tuple(user_id for _ in matched_user_columns)

    cur.execute(
        f"""
        SELECT
            COUNT(*),
            COALESCE(AVG(CASE WHEN {matched_correct_column} THEN 1 ELSE 0 END) * 100, 0)
        FROM {table_name}
        WHERE {where_clause}
        """,
        params,
    )
    attempts, accuracy = cur.fetchone()
    return attempts or 0, round(accuracy or 0, 2)


def _safe_completion_percent(completed_lessons: int, total_lessons: int) -> int:
    if total_lessons <= 0:
        return 0
    return max(0, min(100, round((completed_lessons / total_lessons) * 100)))


def _first_matching_column(columns: set[str], candidates: list[str]) -> str | None:
    return next((candidate for candidate in candidates if candidate in columns), None)


def _fetch_spelling_completion(cur, user_id: int):
    cur.execute(
        """
        SELECT COUNT(*)
        FROM spelling_lessons
        WHERE COALESCE(is_active, TRUE) = TRUE
        """
    )
    total_lessons = cur.fetchone()[0] or 0

    cur.execute(
        """
        SELECT COUNT(DISTINCT sa.lesson_id)
        FROM spelling_attempts sa
        JOIN spelling_lessons l
          ON l.lesson_id = sa.lesson_id
        WHERE sa.user_id = %s
          AND sa.lesson_id IS NOT NULL
          AND COALESCE(l.is_active, TRUE) = TRUE
        """,
        (user_id,),
    )
    completed_lessons = cur.fetchone()[0] or 0
    return completed_lessons, total_lessons, _safe_completion_percent(completed_lessons, total_lessons)


def _fetch_words_completion(cur, user_id: int):
    words_lessons_columns = _get_table_columns(cur, "words_lessons")
    has_words_is_active = "is_active" in words_lessons_columns
    total_where = "WHERE COALESCE(is_active, TRUE) = TRUE" if has_words_is_active else ""
    completed_where = "AND COALESCE(l.is_active, TRUE) = TRUE" if has_words_is_active else ""
    attempt_table = None

    if _get_table_columns(cur, "synonym_attempts"):
        attempt_table = "synonym_attempts"
    elif _get_table_columns(cur, "words_attempts"):
        attempt_table = "words_attempts"

    cur.execute(f"SELECT COUNT(*) FROM words_lessons {total_where}")
    total_lessons = cur.fetchone()[0] or 0

    if not attempt_table:
        return 0, total_lessons, _safe_completion_percent(0, total_lessons)

    cur.execute(
        f"""
        SELECT COUNT(DISTINCT lw.lesson_id)
        FROM {attempt_table} wa
        JOIN words_lesson_words lw
          ON lw.word_id = wa.word_id
        JOIN words_lessons l
          ON l.id = lw.lesson_id
        WHERE wa.user_id = %s
        {completed_where}
        """,
        (user_id,),
    )
    completed_lessons = cur.fetchone()[0] or 0
    return completed_lessons, total_lessons, _safe_completion_percent(completed_lessons, total_lessons)


def _fetch_math_completion(cur, user_id: int):
    math_lessons_columns = _get_table_columns(cur, "math_lessons")
    math_attempts_columns = _get_table_columns(cur, "math_attempts")
    lesson_pk_column = _first_matching_column(math_lessons_columns, ["lesson_id", "id"])
    has_is_active = "is_active" in math_lessons_columns
    user_columns = [column for column in ("student_id", "user_id") if column in math_attempts_columns]

    if not math_lessons_columns:
        return 0, 0, 0

    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM math_lessons
        {"WHERE COALESCE(is_active, TRUE) = TRUE" if has_is_active else ""}
        """
    )
    total_lessons = cur.fetchone()[0] or 0

    if "lesson_id" not in math_attempts_columns or not user_columns:
        return 0, total_lessons, _safe_completion_percent(0, total_lessons)

    where_clause = " OR ".join(f"ma.{column} = %s" for column in user_columns)
    params = tuple(user_id for _ in user_columns)

    if lesson_pk_column:
        join_clause = f"JOIN math_lessons ml ON ml.{lesson_pk_column} = ma.lesson_id"
        active_clause = "AND COALESCE(ml.is_active, TRUE) = TRUE" if has_is_active else ""
    else:
        join_clause = ""
        active_clause = ""

    cur.execute(
        f"""
        SELECT COUNT(DISTINCT ma.lesson_id)
        FROM math_attempts ma
        {join_clause}
        WHERE ({where_clause})
          AND ma.lesson_id IS NOT NULL
          {active_clause}
        """,
        params,
    )
    completed_lessons = cur.fetchone()[0] or 0
    return completed_lessons, total_lessons, _safe_completion_percent(completed_lessons, total_lessons)


def _fetch_comprehension_completion(cur, user_id: int):
    cur.execute("SELECT COUNT(*) FROM comprehension_passages")
    total_lessons = cur.fetchone()[0] or 0

    cur.execute(
        """
        SELECT COUNT(DISTINCT passage_id)
        FROM comprehension_attempts
        WHERE user_id = %s
          AND passage_id IS NOT NULL
        """,
        (user_id,),
    )
    completed_lessons = cur.fetchone()[0] or 0
    return completed_lessons, total_lessons, _safe_completion_percent(completed_lessons, total_lessons)


def _fetch_legacy_dashboard_attempts(cur, user_id: int):
    cur.execute(
        """
        SELECT
            COUNT(*) as attempts,
            COALESCE(AVG(CASE WHEN correct THEN 1 ELSE 0 END) * 100, 0)
        FROM spelling_attempts
        WHERE user_id = %s
        """,
        (user_id,),
    )
    spelling_row = cur.fetchone() or (0, 0)
    spelling_attempts = spelling_row[0] or 0
    spelling_accuracy = round(spelling_row[1] or 0, 2)

    words_summary = get_synonym_attempt_summary(user_id)

    math_attempts, math_accuracy = _fetch_attempt_accuracy(
        cur,
        "math_attempts",
        user_id,
        user_columns=["student_id", "user_id"],
        correct_columns=["is_correct", "correct"],
    )

    cur.execute(
        """
        SELECT
            COUNT(*) as attempts,
            COALESCE(AVG(CASE WHEN correct THEN 1 ELSE 0 END) * 100, 0)
        FROM comprehension_attempts
        WHERE user_id = %s
        """,
        (user_id,),
    )
    comprehension_row = cur.fetchone() or (0, 0)
    comprehension_attempts = comprehension_row[0] or 0
    comprehension_accuracy = round(comprehension_row[1] or 0, 2)

    return {
        "spelling": {"attempts": spelling_attempts, "accuracy": spelling_accuracy},
        "words": {
            "attempts": words_summary.get("attempts", 0) or 0,
            "accuracy": round(words_summary.get("accuracy", 0) or 0, 2),
        },
        "math": {"attempts": math_attempts or 0, "accuracy": round(math_accuracy or 0, 2)},
        "comprehension": {"attempts": comprehension_attempts, "accuracy": comprehension_accuracy},
    }


def get_available_app_catalog():
    return AVAILABLE_APP_CATALOG


def get_valid_app_codes():
    return {item["app_code"] for item in AVAILABLE_APP_CATALOG}


def _normalize_admin_app_codes(raw_apps) -> list[str]:
    if isinstance(raw_apps, dict):
        raw_apps = list(raw_apps.values())
    elif isinstance(raw_apps, str):
        raw_apps = [raw_apps]
    elif raw_apps is None:
        raw_apps = []
    elif not isinstance(raw_apps, list):
        raise HTTPException(status_code=400, detail="Apps must be a list")

    valid_codes = get_valid_app_codes()
    normalized_apps = sorted(
        {
            app_code
            for app_code in (
                str(app.get("app_code") if isinstance(app, dict) else app).strip().lower()
                for app in raw_apps
                if (app.get("app_code") if isinstance(app, dict) else app) is not None
            )
            if app_code and app_code not in {"none", "null"}
        }
    )
    invalid_codes = [app for app in normalized_apps if app not in valid_codes]
    if invalid_codes:
        raise HTTPException(status_code=400, detail=f"Invalid apps: {', '.join(invalid_codes)}")

    return normalized_apps


def _resolve_member_for_admin_update(cur, *, raw_member_id: str, raw_email: str):
    if not raw_email and not raw_member_id:
        raise HTTPException(status_code=400, detail="Email or member_id is required")

    cur.execute(
        """
        SELECT id, email
        FROM kiaro_membership.members
        WHERE (%s <> '' AND id::text = %s)
           OR (%s <> '' AND LOWER(email) = LOWER(%s))
        ORDER BY id DESC
        LIMIT 1
        """,
        (raw_member_id, raw_member_id, raw_email, raw_email),
    )
    member = cur.fetchone()
    if not member:
        raise HTTPException(status_code=404, detail="User not found")
    return member


def _replace_member_apps(cur, member_id: int, normalized_apps: list[str]):
    cur.execute(
        """
        DELETE FROM kiaro_membership.member_apps
        WHERE member_id = %s
        """,
        (member_id,),
    )

    for app_code in normalized_apps:
        cur.execute(
            """
            INSERT INTO kiaro_membership.member_apps (member_id, app_code)
            VALUES (%s, %s)
            """,
            (member_id, app_code),
        )


# =========================
# Health
# =========================
@app.get("/")
def root():
    return {
    "service": "kiarolabs-membership",
    "version": "v6",
    "status": "running"
}


@app.get("/health")
def health_check():
    release_sha = os.getenv("RENDER_GIT_COMMIT", "") or os.getenv("GIT_SHA", "")
    try:
        conn = get_connection()
        conn.close()
        return {
            "database": "connected",
            "release_sha": release_sha,
        }
    except Exception as e:
        return {
            "database": "error",
            "details": str(e),
            "release_sha": release_sha,
        }


# =========================
# Auth: Register
# =========================
@app.post("/register")
def register(req: RegisterRequest):
    name = req.name
    email = req.email.strip().lower()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT email FROM kiaro_membership.members WHERE email = %s",
        (email,),
    )

    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail="Email already registered")

    pw_hash = hash_password(req.password)

    print(f"REGISTER DEBUG: email={email}, hash={pw_hash}")

    try:
        cur.execute(
            """
            INSERT INTO kiaro_membership.members
            (name, email, password_hash, subscription_status, account_type, auth_provider, created_at, updated_at)
            VALUES (%s, %s, %s, 'inactive', 'free', 'email', NOW(), NOW())
            RETURNING email
            """,
            (name, email, pw_hash),
        )

        inserted = cur.fetchone()

        if not inserted:
            raise Exception("Insert failed - no row returned")

        conn.commit()  # ✅ CRITICAL FIX
    except Exception as e:
        conn.rollback()
        print("REGISTER ERROR:", str(e))
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Members insert failed: {str(e)}")

    print(f"USER PROVISIONING: email={email}")

    try:
        cur.execute(
            """
            INSERT INTO users (
                email,
                name,
                password_hash,
                role,
                is_active,
                created_at
            )
            VALUES (%s, %s, %s, 'student', TRUE, NOW())
            ON CONFLICT (email)
            DO UPDATE SET
                name = COALESCE(users.name, EXCLUDED.name),
                password_hash = COALESCE(users.password_hash, EXCLUDED.password_hash)
            """,
            (email, name if name else "Student", pw_hash),
        )
        conn.commit()
    except Exception as e:
        print("USER PROVISIONING ERROR:", str(e))
        conn.rollback()

    print(f"REGISTER SUCCESS: email={email}")
    cur.close()
    conn.close()

    token, exp = create_access_token(email, "free")

    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_at": exp.isoformat(),
    }


# =========================
# Auth: Login
# =========================
@app.post("/login")
async def login(request: Request):
    data = {}

    try:
        data = await request.json()
    except Exception:
        data = {}

    if not data:
        form = await request.form()
        data = dict(form)

    email = (data.get("email") or data.get("username") or "").strip().lower()
    password = data.get("password")

    print("LOGIN DEBUG:", {"email": email, "has_password": bool(password)})

    if not email or not password:
        raise HTTPException(status_code=400, detail="Missing credentials")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # 1) Auth ONLY against membership table
        cur.execute(
            """
            SELECT email, password_hash, account_type
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (email,),
        )
        member_row = cur.fetchone()

        if not member_row:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        member_email, member_password_hash, account_type = member_row

        valid = False
        if member_password_hash:
            try:
                valid = pwd_context.verify(password, member_password_hash)
            except Exception:
                valid = False

        if not valid:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # 2) Ensure user exists in users table
        cur.execute(
            """
            SELECT user_id, role, is_active
            FROM users
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        user_row = cur.fetchone()

        if not user_row:
            cur.execute(
                """
                INSERT INTO users (email, name, password_hash, role, is_active, created_at)
                VALUES (%s, %s, %s, 'student', TRUE, NOW())
                RETURNING user_id, role, is_active
                """,
                (email, email.split("@")[0], member_password_hash),
            )
            user_row = cur.fetchone()
            conn.commit()

        legacy_user_id, role, is_active = user_row

        if is_active is False:
            raise HTTPException(status_code=403, detail="User is inactive")

        # 3) Optional member_id from members.id via separate query
        member_id = None
        try:
            cur.execute(
                """
                SELECT id
                FROM kiaro_membership.members
                WHERE LOWER(email) = LOWER(%s)
                ORDER BY id DESC
                LIMIT 1
                """,
                (email,),
            )
            row = cur.fetchone()
            if row:
                member_id = row[0]
        except Exception:
            member_id = None

        token = jwt.encode(
            {
                "sub": member_email,
                "member_email": member_email,
                "user_id": legacy_user_id,
                "member_id": member_id,
                "role": role,
                "account_type": account_type or "free",
                "exp": datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
            },
            JWT_SECRET,
            algorithm=JWT_ALGO
        )

        return {
            "access_token": token,
            "token_type": "bearer",
            "user_id": legacy_user_id,
            "email": member_email,
            "role": role,
            "account_type": account_type or "free",
        }

    finally:
        cur.close()
        conn.close()


# =========================
# Current User
# =========================
@app.get("/me")
def me(user=Depends(get_current_user)):
    return {
        "email": user["sub"],
        "role": user.get("role", "student"),
        "account_type": user.get("account_type"),
        "user_id": user.get("user_id"),
        "member_id": user.get("member_id"),
    }

@app.get("/dashboard")
def dashboard(user=Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    # -------------------------
    # SAFE USER RESOLUTION
    # -------------------------
    user_id = resolve_verified_learning_user_id(cur, user)
    member_id = None
    if not user_id:
        cur.close()
        conn.close()
        return {
            "role": user.get("role", "student"),
            "modules": {
                "spelling": {"attempts": 0, "accuracy": 0, "unlocked": False},
                "words": {"attempts": 0, "accuracy": 0, "unlocked": False},
                "math": {"unlocked": False},
                "practice_papers": {"unlocked": False},
                "mock_exams": {"unlocked": False},
                "nvr": {"unlocked": False},
                "comprehension": {"unlocked": False}
            },
            "insights": {
                "strongest": None,
                "weakest": None
            }
        }

    # -------------------------
    # LEGACY USER CHECK
    # -------------------------
    is_admin = str(user.get("role", "")).strip().lower() == "admin"

    user_email = str(user.get("sub") or user.get("email") or "").strip().lower()
    if user_email:
        cur.execute(
            """
            SELECT id
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_email,),
        )
        member_row = cur.fetchone()
        member_id = member_row[0] if member_row else None

    if member_id:
        cur.execute("""
            SELECT created_at
            FROM kiaro_membership.members
            WHERE id = %s
        """, (member_id,))

        row = cur.fetchone()

        # created_at is still queried to preserve existing audit/debug expectations,
        # but unlocks for non-admin users are always sourced from member_apps.
        _ = row[0] if row else None

    # -------------------------
    # FETCH ENTITLEMENTS
    # -------------------------
    apps = []

    if member_id:
        cur.execute("""
            SELECT app_code
            FROM kiaro_membership.member_apps
            WHERE member_id = %s
        """, (member_id,))

        rows = cur.fetchall()
        apps = [r[0] for r in rows] if rows else []

    legacy_attempts = _fetch_legacy_dashboard_attempts(cur, user_id)
    s_total = legacy_attempts["spelling"]["attempts"]
    s_acc = legacy_attempts["spelling"]["accuracy"]
    w_total = legacy_attempts["words"]["attempts"]
    w_acc = legacy_attempts["words"]["accuracy"]
    m_total = legacy_attempts["math"]["attempts"]
    m_acc = legacy_attempts["math"]["accuracy"]
    c_total = legacy_attempts["comprehension"]["attempts"]
    c_acc = legacy_attempts["comprehension"]["accuracy"]

    s_completed, s_lessons_total, s_completion = _fetch_spelling_completion(cur, user_id)
    w_completed, w_lessons_total, w_completion = _fetch_words_completion(cur, user_id)
    m_completed, m_lessons_total, m_completion = _fetch_math_completion(cur, user_id)
    c_completed, c_lessons_total, c_completion = _fetch_comprehension_completion(cur, user_id)

    cur.close()
    conn.close()

    # -------------------------
    # MODULE ACCESS LOGIC
    # -------------------------
    modules = {
        # Learning modules (legacy unlocked)
        "spelling": {
            "attempts": s_total,
            "accuracy": round(s_acc, 2),
            "completed_lessons": s_completed,
            "total_lessons": s_lessons_total,
            "completion_percent": s_completion,
            "unlocked": is_admin or ("spelling" in apps)
        },
        "words": {
            "attempts": w_total,
            "accuracy": round(w_acc, 2),
            "completed_lessons": w_completed,
            "total_lessons": w_lessons_total,
            "completion_percent": w_completion,
            "unlocked": is_admin or ("general" in apps)
        },
        "math": {
            "attempts": m_total,
            "accuracy": round(m_acc, 2),
            "completed_lessons": m_completed,
            "total_lessons": m_lessons_total,
            "completion_percent": m_completion,
            "unlocked": is_admin or ("math" in apps)
        },

        # Monetised products (NEVER legacy unlocked)
        "practice_papers": {
            "unlocked": is_admin or ("practice" in apps or any(code in apps for code in ("vr_printables", "vr_single_paper", "vr_starter_pack", "vr_complete_pack")))
        },
        "vr_printables": {
            "unlocked": is_admin or any(code in apps for code in ("practice", "vr_printables", "vr_single_paper", "vr_starter_pack", "vr_complete_pack"))
        },
        "mock_exams": {
            "unlocked": is_admin or ("mock" in apps)
        },

        # Future modules
        "nvr": {
            "unlocked": is_admin or ("nvr" in apps)
        },
        "comprehension": {
            "attempts": c_total,
            "accuracy": round(c_acc, 2),
            "completed_lessons": c_completed,
            "total_lessons": c_lessons_total,
            "completion_percent": c_completion,
            "unlocked": is_admin or ("comprehension" in apps)
        }
    }

    # -------------------------
    # INSIGHTS (SAFE)
    # -------------------------
    metrics = {
        "spelling": s_acc,
        "words": w_acc,
        "math": m_acc,
        "comprehension": c_acc,
    }

    if s_total == 0 and w_total == 0 and m_total == 0 and c_total == 0:
        strongest = None
        weakest = None
    else:
        strongest = max(metrics, key=metrics.get)
        weakest = min(metrics, key=metrics.get)

    return {
        "role": user.get("role", "student"),
        "modules": modules,
        "insights": {
            "strongest": strongest,
            "weakest": weakest
        }
    }


@app.get("/dashboard/insights")
def dashboard_insights(user=Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    try:
        user_id = resolve_verified_learning_user_id(cur, user)
        if not user_id:
            return {
                "summary": {
                    "average_score": 0,
                    "best_score": 0,
                    "total_tests": 0,
                },
                "recent_attempts": [],
                "weak_areas": [],
                "recommended_action": None,
                "learning_path": [],
                "streak": {
                    "current": 0,
                    "current_streak": 0,
                    "longest": 0,
                    "last_active": None,
                    "active_days_last_7": [False] * 7,
                },
            }

        def _collect_activity_days(table_name: str, *, user_candidates: list[str], timestamp_candidates: list[str], collected_days: set):
            columns = _get_table_columns(cur, table_name)
            timestamp_column = _first_matching_column(columns, timestamp_candidates)
            matched_user_columns = [column for column in user_candidates if column in columns]

            if not timestamp_column or not matched_user_columns:
                return

            where_clause = " OR ".join(f"{column} = %s" for column in matched_user_columns)
            params = tuple(user_id for _ in matched_user_columns)

            try:
                cur.execute(
                    f"""
                    SELECT DISTINCT DATE({timestamp_column})
                    FROM {table_name}
                    WHERE {where_clause}
                    """,
                    params,
                )
                for activity_day, in cur.fetchall():
                    if not activity_day:
                        continue
                    collected_days.add(activity_day.date() if hasattr(activity_day, "date") else activity_day)
            except Exception as exc:
                print(f"dashboard_insights practice streak query failed for {table_name}: {exc}")

        practice_activity_days = set()

        _collect_activity_days(
            "spelling_attempts",
            user_candidates=["user_id"],
            timestamp_candidates=["created_at", "submitted_at"],
            collected_days=practice_activity_days,
        )

        words_table = None
        words_columns = _get_table_columns(cur, "synonym_attempts")
        if words_columns:
            words_table = "synonym_attempts"
        else:
            words_columns = _get_table_columns(cur, "words_attempts")
            if words_columns:
                words_table = "words_attempts"

        if words_table:
            _collect_activity_days(
                words_table,
                user_candidates=["user_id"],
                timestamp_candidates=["created_at", "submitted_at"],
                collected_days=practice_activity_days,
            )

        _collect_activity_days(
            "math_attempts",
            user_candidates=["student_id", "user_id"],
            timestamp_candidates=["created_at", "submitted_at"],
            collected_days=practice_activity_days,
        )

        _collect_activity_days(
            "comprehension_attempts",
            user_candidates=["user_id"],
            timestamp_candidates=["created_at", "submitted_at"],
            collected_days=practice_activity_days,
        )

        math_attempt_columns = _get_table_columns(cur, "math_attempts")
        has_paper_attempts = {"user_id", "paper_code", "score", "total", "created_at"}.issubset(math_attempt_columns)
        math_submission_columns = _get_table_columns(cur, "math_submission_attempts")
        attempts_table = "math_attempts" if has_paper_attempts else "math_submission_attempts" if math_submission_columns else None

        math_test_paper_columns = _get_table_columns(cur, "math_test_papers")
        ordered_mock_papers = []
        if math_test_paper_columns:
            try:
                is_active_clause = "WHERE is_active = TRUE" if "is_active" in math_test_paper_columns else ""
                sort_column = "sort_order" if "sort_order" in math_test_paper_columns else "paper_code"
                cur.execute(
                    f"""
                    SELECT paper_code
                    FROM math_test_papers
                    {is_active_clause}
                    ORDER BY {sort_column} ASC
                    """
                )
                ordered_mock_papers = [row[0] for row in cur.fetchall()]
            except Exception as exc:
                print(f"dashboard_insights mock paper lookup failed: {exc}")

        attempts = []
        if attempts_table:
            try:
                cur.execute(
                    f"""
                    SELECT paper_code, score, total, created_at
                    FROM {attempts_table}
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                attempts = cur.fetchall()
            except Exception as exc:
                print(f"dashboard_insights attempt query failed for {attempts_table}: {exc}")
                attempts = []

        today = datetime.utcnow().date()
        activity_days = sorted(practice_activity_days, reverse=True)
        current_streak = 0

        for i, day in enumerate(activity_days):
            if i == 0:
                if day == today or day == today - timedelta(days=1):
                    current_streak = 1
                else:
                    break
            elif activity_days[i - 1] - day == timedelta(days=1):
                current_streak += 1
            else:
                break

        longest_streak = 0
        streak_run = 0
        previous_day = None

        for day in activity_days:
            if previous_day is None:
                streak_run = 1
            elif previous_day - day == timedelta(days=1):
                streak_run += 1
            else:
                streak_run = 1

            longest_streak = max(longest_streak, streak_run)
            previous_day = day

        streak = {
            "current": current_streak,
            "current_streak": current_streak,
            "longest": longest_streak,
            "last_active": activity_days[0] if activity_days else None,
            "active_days_last_7": [
                (today - timedelta(days=offset)) in practice_activity_days
                for offset in range(6, -1, -1)
            ],
        }

        if not attempts:
            return {
                "summary": {
                    "average_score": 0,
                    "best_score": 0,
                    "total_tests": 0,
                },
                "recent_attempts": [],
                "weak_areas": [],
                "recommended_action": None,
                "learning_path": [
                    {
                        "step": 1,
                        "type": "new",
                        "paper_code": ordered_mock_papers[0],
                        "reason": "Start your first test",
                    }
                ] if ordered_mock_papers else [],
                "streak": streak,
            }

        percentages = [
            (score / total * 100)
            for _paper_code, score, total, _created_at in attempts
            if total
        ]

        total_tests = len(attempts)
        average_score = (sum(percentages) / len(percentages)) if percentages else 0
        best_score = max(percentages) if percentages else 0

        recent_rows = []
        try:
            cur.execute(
                f"""
                SELECT paper_code, score, total, created_at
                FROM {attempts_table}
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 5
                """,
                (user_id,),
            )
            recent_rows = cur.fetchall()
        except Exception as exc:
            print(f"dashboard_insights recent attempts query failed for {attempts_table}: {exc}")

        recent_attempts = [
            {
                "paper_code": paper_code,
                "percentage": (score / total * 100) if total else 0,
                "date": created_at,
            }
            for paper_code, score, total, created_at in recent_rows
        ]

        weak_rows = []
        try:
            cur.execute(
                f"""
                SELECT paper_code, AVG(score::float / NULLIF(total, 0)) AS avg_score
                FROM {attempts_table}
                WHERE user_id = %s
                GROUP BY paper_code
                HAVING AVG(score::float / NULLIF(total, 0)) < 0.7
                ORDER BY avg_score ASC
                LIMIT 3
                """,
                (user_id,),
            )
            weak_rows = cur.fetchall()
        except Exception as exc:
            print(f"dashboard_insights weak area query failed for {attempts_table}: {exc}")

        weak_areas = [
            {
                "paper_code": paper_code,
                "average_score": round((avg_score or 0) * 100, 2),
            }
            for paper_code, avg_score in weak_rows
        ]

        if weak_areas:
            recommended_action = {
                "type": "retry",
                "paper_code": weak_areas[0]["paper_code"],
            }
        else:
            attempted_papers = {paper_code for paper_code, _score, _total, _created_at in attempts}
            next_paper = None
            for paper_code in ordered_mock_papers:
                if paper_code not in attempted_papers:
                    next_paper = paper_code
                    break

            recommended_action = {
                "type": "new",
                "paper_code": next_paper,
            } if next_paper else None

        attempted_papers = {paper_code for paper_code, _score, _total, _created_at in attempts}
        next_unattempted_paper = None
        for paper_code in ordered_mock_papers:
            if paper_code not in attempted_papers:
                next_unattempted_paper = paper_code
                break

        learning_path = []
        step = 1

        for weak_area in weak_areas[:2]:
            learning_path.append(
                {
                    "step": step,
                    "type": "retry",
                    "paper_code": weak_area["paper_code"],
                    "reason": f"Low score ({int(weak_area['average_score'])}%)",
                }
            )
            step += 1

        if next_unattempted_paper:
            learning_path.append(
                {
                    "step": step,
                    "type": "new",
                    "paper_code": next_unattempted_paper,
                    "reason": "Next recommended test",
                }
            )

        return {
            "summary": {
                "average_score": round(average_score, 2),
                "best_score": round(best_score, 2),
                "total_tests": total_tests,
            },
            "recent_attempts": recent_attempts,
            "weak_areas": weak_areas,
            "recommended_action": recommended_action,
            "learning_path": learning_path,
            "streak": streak,
        }
    finally:
        cur.close()
        conn.close()


@app.get("/progress/weekly-improvement")
def legacy_weekly_improvement(user=Depends(get_current_user)):
    from app.practice.router import get_weekly_improvement

    return get_weekly_improvement(user)


@app.get("/admin/users")
def get_all_users(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                m.id,
                u.user_id,
                m.email,
                COALESCE(u.role, 'student') as role,
                m.account_type,
                m.created_at,
                COALESCE(array_agg(ma.app_code) FILTER (WHERE ma.app_code IS NOT NULL), '{}') as apps
            FROM kiaro_membership.members m
            LEFT JOIN users u
                ON LOWER(u.email) = LOWER(m.email)
            LEFT JOIN kiaro_membership.member_apps ma
                ON ma.member_id = m.id
            GROUP BY m.id, u.user_id, m.email, u.role, m.account_type, m.created_at
            ORDER BY m.created_at DESC
            """
        )

        rows = cur.fetchall()

        result = []
        for r in rows:
            account_type = r[4]
            apps = [app for app in (r[6] or []) if app]

            expected_apps = []

            if account_type != "free":
                expected_apps = ["math", "mock", "practice"]

            status = "ok" if set(expected_apps).issubset(set(apps)) else "missing_access"

            result.append(
                {
                    "member_id": str(r[0]),
                    "user_id": r[1],
                    "email": r[2],
                    "role": r[3] if r[3] else "student",
                    "account_type": r[4],
                    "created_at": str(r[5]),
                    "apps": apps,
                }
            )

        return result
    finally:
        cur.close()
        conn.close()


@app.get("/admin/app-catalog")
def get_admin_app_catalog(user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    return {"apps": get_available_app_catalog()}


@app.get("/admin/user-apps")
def get_admin_user_apps(email: str, user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT m.id, m.email, COALESCE(array_agg(ma.app_code ORDER BY ma.app_code) FILTER (WHERE ma.app_code IS NOT NULL), '{}') AS apps
            FROM kiaro_membership.members m
            LEFT JOIN kiaro_membership.member_apps ma
              ON ma.member_id = m.id
            WHERE LOWER(m.email) = LOWER(%s)
            GROUP BY m.id, m.email
            LIMIT 1
            """,
            (email,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "email": row[1],
            "member_id": row[0],
            "apps": [app for app in (row[2] or []) if app],
        }
    finally:
        cur.close()
        conn.close()


@app.post("/admin/set-user-apps")
def set_admin_user_apps(payload: dict = Body(...), user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    raw_email = str(payload.get("email", "")).strip().lower()
    raw_member_id = str(payload.get("member_id", "")).strip()
    normalized_apps = _normalize_admin_app_codes(payload.get("apps", []))

    conn = get_connection()
    cur = conn.cursor()

    try:
        member = _resolve_member_for_admin_update(
            cur,
            raw_member_id=raw_member_id,
            raw_email=raw_email,
        )
        member_id = member[0]
        _replace_member_apps(cur, member_id, normalized_apps)

        conn.commit()

        return {
            "status": "success",
            "email": member[1],
            "member_id": member_id,
            "apps": normalized_apps,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@app.post("/admin/set-user-apps-bulk")
def set_admin_user_apps_bulk(payload: dict = Body(...), user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    raw_targets = payload.get("users", [])
    if not isinstance(raw_targets, list) or not raw_targets:
        raise HTTPException(status_code=400, detail="At least one user is required")

    normalized_apps = _normalize_admin_app_codes(payload.get("apps", []))

    conn = get_connection()
    cur = conn.cursor()

    try:
        updated_users = []
        seen_member_ids: set[int] = set()

        for raw_target in raw_targets:
            if isinstance(raw_target, dict):
                raw_member_id = str(raw_target.get("member_id", "")).strip()
                raw_email = str(raw_target.get("email", "")).strip().lower()
            else:
                raw_member_id = str(raw_target).strip()
                raw_email = ""

            member = _resolve_member_for_admin_update(
                cur,
                raw_member_id=raw_member_id,
                raw_email=raw_email,
            )
            member_id = int(member[0])
            if member_id in seen_member_ids:
                continue

            _replace_member_apps(cur, member_id, normalized_apps)
            seen_member_ids.add(member_id)
            updated_users.append(
                {
                    "member_id": member_id,
                    "email": member[1],
                }
            )

        conn.commit()

        return {
            "status": "success",
            "updated_count": len(updated_users),
            "apps": normalized_apps,
            "users": updated_users,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@app.post("/admin/fix-user-access")
def fix_user_access(payload: dict, user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403)

    email = payload.get("email")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, account_type
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        member_id, account_type = row

        if account_type == "free":
            return {"status": "no_action_needed"}

        apps = ["math", "mock", "practice", "grammar"]

        for app_code in apps:
            cur.execute(
                """
                INSERT INTO kiaro_membership.member_apps (member_id, app_code)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (member_id, app_code),
            )

        conn.commit()

        return {"status": "fixed"}
    finally:
        cur.close()
        conn.close()


@app.get("/admin/user-detail")
def user_detail(email: str, user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT *
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        member = cur.fetchone()

        cur.execute(
            """
            SELECT *
            FROM kiaro_membership.member_apps
            WHERE member_id = (
                SELECT id
                FROM kiaro_membership.members
                WHERE LOWER(email) = LOWER(%s)
            )
            """,
            (email,),
        )
        apps = cur.fetchall()

        return {
            "member": member,
            "apps": apps,
        }
    finally:
        cur.close()
        conn.close()


@app.get("/admin/debug/member-access")
def admin_debug_member_access(email: str, limit: int = 20, user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    target_email = str(email or "").strip().lower()
    if not target_email:
        raise HTTPException(status_code=400, detail="email is required")

    safe_limit = max(1, min(int(limit or 20), 100))

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, email
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
            ORDER BY id DESC
            LIMIT 1
            """,
            (target_email,),
        )
        member_row = cur.fetchone()
        if not member_row:
            raise HTTPException(status_code=404, detail="User not found")

        member_id = int(member_row[0])
        member_email = str(member_row[1] or "").strip().lower()

        cur.execute(
            """
            SELECT app_code
            FROM kiaro_membership.member_apps
            WHERE member_id = %s
            ORDER BY app_code
            """,
            (member_id,),
        )
        member_apps = [str(row[0]) for row in (cur.fetchall() or []) if row and row[0]]

        cur.execute(
            """
            SELECT test_id
            FROM math_user_test_access
            WHERE member_id = %s
            ORDER BY test_id
            """,
            (member_id,),
        )
        mock_test_ids = [str(row[0]) for row in (cur.fetchall() or []) if row and row[0]]

        cur.execute(
            """
            SELECT id, event_type, product_name, test_id, processed
            FROM math_gumroad_events
            WHERE LOWER(email) = LOWER(%s)
            ORDER BY id DESC
            LIMIT %s
            """,
            (member_email, safe_limit),
        )
        raw_events = cur.fetchall() or []
        last_gumroad_events = [
            {
                "id": int(row[0]),
                "event_type": str(row[1] or ""),
                "product_name": str(row[2] or ""),
                "test_id": str(row[3] or ""),
                "processed": bool(row[4]),
            }
            for row in raw_events
        ]
    finally:
        cur.close()
        conn.close()

    purchased_keys, purchased_permalinks = get_printable_purchase_state_for_email(member_email)

    return {
        "email": member_email,
        "member_id": member_id,
        "member_apps": member_apps,
        "mock_test_ids": mock_test_ids,
        "printable_purchase_keys": sorted(purchased_keys),
        "printable_permalinks": sorted(purchased_permalinks),
        "last_gumroad_events": last_gumroad_events,
    }


@app.post("/admin/set-role")
def set_user_role(payload: dict, user=Depends(get_current_user)):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    target_email = payload.get("email")
    new_role = payload.get("role")

    if new_role not in ["admin", "student"]:
        raise HTTPException(status_code=400, detail="Invalid role")

    conn = get_connection()
    cur = conn.cursor()

    try:
        def grant_all_apps_to_member(conn, member_id):
            query = """
            INSERT INTO kiaro_membership.member_apps (member_id, app_code)
            SELECT %s, app_code
            FROM (VALUES
                ('comprehension'),
                ('general'),
                ('grammar'),
                ('math'),
                ('mock'),
                ('nvr'),
                ('practice'),
                ('spelling')
            ) AS apps(app_code)
            WHERE NOT EXISTS (
                SELECT 1
                FROM kiaro_membership.member_apps ma
                WHERE ma.member_id = %s
                  AND ma.app_code = apps.app_code
            );
            """
            with conn.cursor() as grant_cur:
                grant_cur.execute(query, (member_id, member_id))

        cur.execute(
            """
            UPDATE users
            SET role = %s
            WHERE LOWER(email) = LOWER(%s)
            """,
            (new_role, target_email),
        )

        if new_role == "admin":
            cur.execute(
                """
                SELECT id FROM kiaro_membership.members
                WHERE LOWER(email) = LOWER(%s)
                ORDER BY id DESC
                LIMIT 1
                """,
                (target_email,),
            )
            member = cur.fetchone()

            if member:
                member_id = member[0]
                grant_all_apps_to_member(conn, member_id)

        conn.commit()
        return {"status": "success"}
    finally:
        cur.close()
        conn.close()


# =========================
# Gumroad Webhook
# =========================
MOCK_PACK_IDENTIFIERS_V1: set[str] = {
    normalize_gumroad_identifier(token)
    for token in DISABLED_OR_IGNORED_PERMALINKS
}


def _collect_gumroad_identifiers(form) -> set[str]:
    identifiers: set[str] = set()
    for field_name in (
        "product_permalink",
        "short_product_id",
        "product_id",
        "product_url",
        "sale[product_permalink]",
        "sale[short_product_id]",
        "sale[product_id]",
        "sale[product_url]",
    ):
        raw_value = form.get(field_name)
        if raw_value is None:
            continue
        value = normalize_gumroad_identifier(str(raw_value))
        if value:
            identifiers.add(value)

    return identifiers


def _extract_webhook_permalink(form) -> str:
    for field_name in (
        "product_permalink",
        "sale[product_permalink]",
        "short_product_id",
        "sale[short_product_id]",
        "product_url",
        "sale[product_url]",
    ):
        token = normalize_gumroad_identifier(form.get(field_name))
        if token:
            return token
    return ""


def _resolve_gumroad_app_code(identifiers: set[str], product_name: str = "") -> str | None:
    for identifier in identifiers:
        app_code = ACTIVE_ONLINE_PRACTICE_PERMALINK_APP_CODE.get(identifier)
        if app_code:
            return app_code

    # Identifier-bearing payloads are authoritative; unknown identifiers must not
    # fall back to name-based module unlocks (prevents printable/mock name drift).
    if identifiers:
        return None

    normalized_name = re.sub(r"[^a-z0-9]+", "", (product_name or "").strip().lower())
    if normalized_name == "wordsprint":
        return "general"
    if normalized_name == "spellingsprint":
        return "spelling"
    if normalized_name in {"mathsprint", "mathsprintmodule", "mathsprintaccess"}:
        return "math"
    if normalized_name == "comprehensionsprint":
        return "comprehension"

    return None


def _resolve_mock_test_id(product_name: str, identifiers: set[str]) -> str | None:
    for identifier in identifiers:
        mapped_test_id = ACTIVE_MATH_MOCK_PERMALINK_TEST_ID.get(identifier)
        if mapped_test_id:
            return mapped_test_id

    # Identifier-bearing payloads are authoritative; do not infer a mock test
    # from product name when identifiers do not map to an active mock permalink.
    if identifiers:
        return None

    source_values = [(product_name or "").lower(), *identifiers]

    for value in source_values:
        if value in MOCK_PACK_IDENTIFIERS_V1:
            return None

        explicit_match = re.search(r"(?:math|maths)[-_ ]?(?:mock|exam)[-_ ]*(\d{1,2})", value)
        if explicit_match:
            return f"MATH_MOCK_{int(explicit_match.group(1))}"

        if "mock" in value:
            trailing_match = re.search(r"(?:^|[_-])(\d{1,2})(?:$|[^0-9])", value)
            if trailing_match:
                return f"MATH_MOCK_{int(trailing_match.group(1))}"

    return None


def _resolve_gumroad_product_key(identifiers: set[str], product_name: str = "") -> str | None:
    """
    Backward-compatible resolver contract used by legacy validation checks.
    """
    app_code = _resolve_gumroad_app_code(identifiers, product_name=product_name)
    if app_code:
        return app_code
    return _resolve_mock_test_id(product_name, identifiers)


def _is_purchase_event(event_type: str) -> bool:
    normalized = (event_type or "").strip().lower()
    return normalized in {"sale", "purchase", "sale.created", "purchase.created"}


def _is_refund_event(event_type: str) -> bool:
    normalized = (event_type or "").strip().lower()
    return normalized in {"refund", "chargeback", "refund.created", "chargeback.created"}


def _legacy_product_code_from_entitlement(app_code: str | None, mock_test_id: str | None) -> str | None:
    app_map = {
        "math": "MSM",
        "spelling": "SSM",
        "general": "WSM",
        "comprehension": "CHM",
        "grammar": "GSM",
    }
    normalized_app_code = str(app_code or "").strip().lower()
    if normalized_app_code in app_map:
        return app_map[normalized_app_code]

    normalized_mock_test_id = str(mock_test_id or "").strip().upper()
    if normalized_mock_test_id.startswith("MATH_MOCK_"):
        suffix = normalized_mock_test_id.removeprefix("MATH_MOCK_")
        if suffix.isdigit():
            return f"MME{int(suffix)}"
    return None

@app.post("/webhook/gumroad")
async def gumroad_webhook(request: Request):
    try:
        form = await request.form()

        email = (form.get("email") or "").strip().lower()
        product_name = (form.get("product_name") or "").strip()
        event_type = (form.get("event") or "").strip()
        sale_id = (form.get("sale_id") or form.get("sale[id]") or "").strip()
        identifiers = _collect_gumroad_identifiers(form)
        webhook_permalink = _extract_webhook_permalink(form)
        resolved_catalog_product = resolve_product_by_provider_identifier(identifiers)
        resolved_app_code = _resolve_gumroad_app_code(identifiers, product_name=product_name)
        resolved_mock_test_id = _resolve_mock_test_id(product_name, identifiers)
        if resolved_catalog_product:
            if resolved_catalog_product.get("entitlement_type") == "member_app":
                resolved_app_code = resolved_catalog_product.get("entitlement_value") or resolved_app_code
            elif resolved_catalog_product.get("entitlement_type") == "mock_test_access":
                resolved_mock_test_id = resolved_catalog_product.get("entitlement_value") or resolved_mock_test_id
        else:
            legacy_product_code = _legacy_product_code_from_entitlement(resolved_app_code, resolved_mock_test_id)
            if legacy_product_code:
                resolved_catalog_product = {
                    "product_code": legacy_product_code,
                    "provider_product_key": webhook_permalink,
                }
        event_product_payload = (
            f"{product_name} | permalink={webhook_permalink}"
            if webhook_permalink
            else product_name
        )

        # Embed the product_permalink into the stored product_name so that
        # get_printable_purchase_state_for_email can recover it for per-paper
        # purchased-state tracking (no schema change required).
        _raw_permalink = normalize_gumroad_identifier(str(form.get("product_permalink") or ""))
        if _raw_permalink:
            product_name = f"{product_name}|permalink={_raw_permalink}"

        print(
            "GUMROAD EVENT:",
            email,
            product_name,
            event_type,
            {
                "identifiers": sorted(identifiers),
                "product_code": (resolved_catalog_product or {}).get("product_code"),
                "app_code": resolved_app_code,
                "test_id": resolved_mock_test_id,
            },
        )

        if not email or not product_name:
            return {"status": "ignored"}

        conn = get_connection()
        cur = conn.cursor()

        try:
            # Log incoming event (always)
            cur.execute(
                """
                INSERT INTO math_gumroad_events (email, product_name, event_type, test_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (email, event_product_payload, event_type, resolved_mock_test_id),
            )
            event_id = cur.fetchone()[0]

            # Find user
            cur.execute(
                """
                SELECT id
                FROM kiaro_membership.members
                WHERE LOWER(email) = LOWER(%s)
                ORDER BY id DESC
                LIMIT 1
                """,
                (email,),
            )

            row = cur.fetchone()

            if not row:
                print("❌ USER NOT FOUND:", email)
                conn.commit()
                return {"status": "user_not_found"}

            member_id = row[0]

            is_purchase_event = _is_purchase_event(event_type)
            is_refund_event = _is_refund_event(event_type)

            # Bundle/packs stay disabled for V1 and must never unlock individual mock entitlements.
            if is_purchase_event and any(identifier in MOCK_PACK_IDENTIFIERS_V1 for identifier in identifiers):
                print(f"ℹ️ IGNORED DISABLED PACK PURCHASE → {email} → {sorted(identifiers)}")
                conn.commit()
                return {"status": "disabled_pack_ignored"}

            # Handle bundle purchases (inactive placeholder; retained for backward-compatible structure)
            if is_purchase_event and any(identifier in MOCK_PACK_IDENTIFIERS_V1 for identifier in identifiers):
                for i in range(1, 7):
                    bundle_test_id = f"MATH_MOCK_{i}"
                    cur.execute(
                        """
                        INSERT INTO math_user_test_access (member_id, test_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (member_id, bundle_test_id),
                    )
                print(f"✅ ACCESS GRANTED → {email} → 6-pack")
                conn.commit()
                return {"status": "6_pack_unlocked"}

            if is_purchase_event and any(identifier in MOCK_PACK_IDENTIFIERS_V1 for identifier in identifiers):
                for i in range(1, 13):
                    bundle_test_id = f"MATH_MOCK_{i}"
                    cur.execute(
                        """
                        INSERT INTO math_user_test_access (member_id, test_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (member_id, bundle_test_id),
                    )
                print(f"✅ ACCESS GRANTED → {email} → full-pack")
                conn.commit()
                return {"status": "full_pack_unlocked"}

            if is_purchase_event and resolved_app_code:
                cur.execute(
                    """
                    INSERT INTO kiaro_membership.member_apps (member_id, app_code)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (member_id, resolved_app_code),
                )

            if resolved_catalog_product:
                upsert_member_product_access(
                    member_id=member_id,
                    purchase_email=email,
                    product_code=resolved_catalog_product["product_code"],
                    provider_product_key=resolved_catalog_product.get("provider_product_key"),
                    sale_id=sale_id or None,
                    status="active" if is_purchase_event else "refunded" if is_refund_event else "active",
                    conn=conn,
                )

            # Handle single-test purchase
            if is_purchase_event and resolved_mock_test_id:
                cur.execute(
                    """
                    INSERT INTO math_user_test_access (member_id, test_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (member_id, resolved_mock_test_id),
                )
                print(f"✅ ACCESS GRANTED → {email} → {resolved_mock_test_id}")

            # Handle refund
            if is_refund_event and resolved_mock_test_id:
                cur.execute(
                    """
                    DELETE FROM math_user_test_access
                    WHERE member_id = %s
                    AND test_id = %s
                    """,
                    (member_id, resolved_mock_test_id),
                )
                print(f"❌ ACCESS REVOKED → {email} → {resolved_mock_test_id}")

            if is_refund_event and resolved_app_code:
                cur.execute(
                    """
                    DELETE FROM kiaro_membership.member_apps
                    WHERE member_id = %s
                      AND app_code = %s
                    """,
                    (member_id, resolved_app_code),
                )

            # Mark event processed
            cur.execute(
                """
                UPDATE math_gumroad_events
                SET processed = TRUE
                WHERE id = %s
                """,
                (event_id,),
            )

            conn.commit()
            return {"status": "ok"}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print("❌ WEBHOOK ERROR:", str(e))
        return {"status": "error"}


# =========================
# Purchases: Printables
# =========================
@app.get("/purchases/printables")
def get_printable_purchases(user: dict = Depends(get_current_user)):
    """Return the set of printable paper permalinks purchased by the authenticated user."""
    user_email = (
        (user.get("sub") or user.get("member_email") or user.get("email") or "")
        .strip()
        .lower()
    )
    _, purchased_permalinks = get_printable_purchase_state_for_email(user_email)
    owned_product_codes = get_owned_product_codes_for_email(
        user_email,
        families={"printable_paper"},
    )
    return {
        "purchased_permalinks": sorted(purchased_permalinks),
        "owned_product_codes": sorted(owned_product_codes),
    }


@app.get("/catalog/printables")
def get_printable_catalog():
    return {"products": get_current_printable_catalog()}


# =========================
# Validate User
# =========================
@app.get("/validate-user")
def validate_user(email: str):
    if not email:
        return {"active": False, "reason": "email_required"}

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT subscription_status, subscription_end, account_type
        FROM kiaro_membership.members
        WHERE email = %s
        """,
        (email.strip().lower(),),
    )

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return {"active": False, "reason": "user_not_found"}

    subscription_status, subscription_end, account_type = row
    active, reason = derive_subscription_state(subscription_status, subscription_end)

    return {
        "active": active,
        "reason": reason,
        "account_type": account_type,
        "subscription_status": subscription_status,
        "subscription_end": subscription_end.isoformat() if subscription_end else None,
    }
# =========================
# Practice Engine Routes
# =========================
app.include_router(practice_router, prefix="")
app.include_router(practice_admin_router)
app.include_router(comprehension_router)
app.include_router(auth_reset_router)
app.include_router(admin_ingestion_router)
app.include_router(admin_branding_router)
app.include_router(admin_curriculum_router)
app.include_router(printable_router)
