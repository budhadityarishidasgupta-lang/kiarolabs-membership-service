import os
import re
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException, Depends, Body
#from fastapi.security import OAuth2PasswordBearer
from app.auth import get_current_user
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
from app.ingestion.verbal_reasoning.service import init_verbal_reasoning_printable_tables
from typing import Optional
from app.comprehension.router import router as comprehension_router
from app.auth_reset import init_password_reset_tables, router as auth_reset_router


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


def get_available_app_catalog():
    return AVAILABLE_APP_CATALOG


def get_valid_app_codes():
    return {item["app_code"] for item in AVAILABLE_APP_CATALOG}


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
    try:
        conn = get_connection()
        conn.close()
        return {"database": "connected"}
    except Exception as e:
        return {"database": "error", "details": str(e)}


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
    user_id = user.get("user_id")
    member_id = user.get("member_id")
    email = user.get("sub")

    # Validate user_id belongs to users table
    if user_id:
        cur.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
        if not cur.fetchone():
            user_id = None

    # Fallback via email
    if not user_id:
        cur.execute(
            "SELECT user_id FROM users WHERE LOWER(email) = LOWER(%s)",
            (email,)
        )
        row = cur.fetchone()
        if not row:
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
        user_id = row[0]

    # -------------------------
    # LEGACY USER CHECK
    # -------------------------
    created_at = None
    is_legacy = False

    if member_id:
        cur.execute("""
            SELECT created_at
            FROM kiaro_membership.members
            WHERE id = %s
        """, (member_id,))

        row = cur.fetchone()

        if row and row[0]:
            created_at = row[0]
            from datetime import datetime
            cutoff_date = datetime(2026, 4, 3)
            is_legacy = created_at < cutoff_date

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

    s_total, s_acc = _fetch_attempt_accuracy(
        cur,
        "spelling_attempts",
        user_id,
        user_columns=["user_id"],
        correct_columns=["correct"],
    )

    w_total, w_acc = _fetch_attempt_accuracy(
        cur,
        "words_attempts",
        user_id,
        user_columns=["user_id"],
        correct_columns=["correct"],
    )

    m_total, m_acc = _fetch_attempt_accuracy(
        cur,
        "math_attempts",
        user_id,
        user_columns=["student_id", "user_id"],
        correct_columns=["is_correct", "correct"],
    )

    c_total, c_acc = _fetch_attempt_accuracy(
        cur,
        "comprehension_attempts",
        user_id,
        user_columns=["user_id"],
        correct_columns=["correct"],
    )

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
            "unlocked": True if is_legacy else ("spelling" in apps or "general" in apps)
        },
        "words": {
            "attempts": w_total,
            "accuracy": round(w_acc, 2),
            "unlocked": True if is_legacy else ("general" in apps)
        },
        "math": {
            "attempts": m_total,
            "accuracy": round(m_acc, 2),
            "unlocked": True if is_legacy else ("math" in apps)
        },

        # Monetised products (NEVER legacy unlocked)
        "practice_papers": {
            "unlocked": "practice" in apps
        },
        "mock_exams": {
            "unlocked": "mock" in apps
        },

        # Future modules
        "nvr": {
            "unlocked": "nvr" in apps
        },
        "comprehension": {
            "attempts": c_total,
            "accuracy": round(c_acc, 2),
            "unlocked": "comprehension" in apps
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
    user_id = user.get("user_id")

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
                "longest": 0,
                "last_active": None,
                "active_days_last_7": 0,
            },
        }

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'math_attempts'
            """
        )
        math_attempt_columns = {row[0] for row in cur.fetchall()}
        has_paper_attempts = {"user_id", "paper_code", "score", "total", "created_at"}.issubset(math_attempt_columns)
        attempts_table = "math_attempts" if has_paper_attempts else "math_submission_attempts"

        cur.execute(
            """
            SELECT paper_code
            FROM math_test_papers
            WHERE is_active = TRUE
            ORDER BY sort_order ASC
            """
        )
        ordered_mock_papers = [row[0] for row in cur.fetchall()]

        cur.execute(
            f"""
            SELECT paper_code, score, total, created_at
            FROM {attempts_table}
            WHERE user_id = %s
            """,
            (user_id,),
        )
        attempts = cur.fetchall()

        cur.execute(
            f"""
            SELECT DISTINCT DATE(created_at)
            FROM {attempts_table}
            WHERE user_id = %s
            ORDER BY DATE(created_at) DESC
            """,
            (user_id,),
        )
        activity_days = [
            row[0].date() if hasattr(row[0], "date") else row[0]
            for row in cur.fetchall()
            if row[0]
        ]

        today = datetime.utcnow().date()
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
            "longest": longest_streak,
            "last_active": activity_days[0] if activity_days else None,
            "active_days_last_7": sum(
                1 for day in activity_days if today - timedelta(days=6) <= day <= today
            ),
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
        recent_attempts = [
            {
                "paper_code": paper_code,
                "percentage": (score / total * 100) if total else 0,
                "date": created_at,
            }
            for paper_code, score, total, created_at in recent_rows
        ]

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
            GROUP BY m.id, m.email, u.role, m.account_type, m.created_at
            ORDER BY m.created_at DESC
            """
        )

        rows = cur.fetchall()

        result = []
        for r in rows:
            account_type = r[3]
            apps = [app for app in (r[5] or []) if app]

            expected_apps = []

            if account_type != "free":
                expected_apps = ["math", "mock", "practice"]

            status = "ok" if set(expected_apps).issubset(set(apps)) else "missing_access"

            result.append(
                {
                    "member_id": str(r[0]),
                    "email": r[1],
                    "role": r[2] if r[2] else "student",
                    "account_type": r[3],
                    "created_at": str(r[4]),
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
    raw_apps = payload.get("apps", [])

    if isinstance(raw_apps, dict):
        raw_apps = list(raw_apps.values())
    elif isinstance(raw_apps, str):
        raw_apps = [raw_apps]
    elif not isinstance(raw_apps, list):
        raise HTTPException(status_code=400, detail="Apps must be a list")

    if not raw_email and not raw_member_id:
        raise HTTPException(status_code=400, detail="Email or member_id is required")

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

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, email
            FROM kiaro_membership.members
            WHERE (%s <> '' AND id::text = %s)
               OR (%s <> '' AND LOWER(email) = LOWER(%s))
            ORDER BY id
            LIMIT 1
            """,
            (raw_member_id, raw_member_id, raw_email, raw_email),
        )
        member = cur.fetchone()
        if not member:
            raise HTTPException(status_code=404, detail="User not found")

        member_id = member[0]

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

        apps = ["math", "mock", "practice"]

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
@app.post("/webhook/gumroad")
async def gumroad_webhook(request: Request):
    try:
        form = await request.form()

        email = (form.get("email") or "").strip().lower()
        product_name = (form.get("product_name") or "").strip()
        event_type = (form.get("event") or "").strip()

        print("GUMROAD EVENT:", email, product_name, event_type)

        if not email or not product_name:
            return {"status": "ignored"}

        # Extract test number
        match = re.search(r"Maths Mock Exam (\d+)", product_name)

        test_id = None
        if match:
            test_number = match.group(1)
            test_id = f"MATH_MOCK_{test_number}"

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
                (email, product_name, event_type, test_id),
            )
            event_id = cur.fetchone()[0]

            # Find user
            cur.execute(
                """
                SELECT id
                FROM kiaro_membership.members
                WHERE LOWER(email) = LOWER(%s)
                """,
                (email,),
            )

            row = cur.fetchone()

            if not row:
                print("❌ USER NOT FOUND:", email)
                conn.commit()
                return {"status": "user_not_found"}

            member_id = row[0]

            # Handle bundle purchases
            if event_type in ["sale", "purchase"] and product_name == "Maths Mock Pack (6 Tests)":
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

            if event_type in ["sale", "purchase"] and product_name == "Maths Complete Pack (12 Tests)":
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

            # Handle single-test purchase
            if event_type in ["sale", "purchase"] and test_id:
                cur.execute(
                    """
                    INSERT INTO math_user_test_access (member_id, test_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (member_id, test_id),
                )
                print(f"✅ ACCESS GRANTED → {email} → {test_id}")

            # Handle refund
            if event_type in ["refund", "chargeback"] and test_id:
                cur.execute(
                    """
                    DELETE FROM math_user_test_access
                    WHERE member_id = %s
                    AND test_id = %s
                    """,
                    (member_id, test_id),
                )
                print(f"❌ ACCESS REVOKED → {email} → {test_id}")

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
