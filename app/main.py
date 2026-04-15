import os
import re
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException, Depends
#from fastapi.security import OAuth2PasswordBearer
from app.auth import get_current_user
from pydantic import BaseModel, EmailStr
from app.database import get_connection
from app.database_init_words import init_words_tables
from datetime import datetime, timedelta
from passlib.context import CryptContext
from jose import jwt, JWTError
from app.practice.router import router as practice_router
from typing import Optional
from app.comprehension.router import router as comprehension_router


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

        # 2) Optional enrichment from users table
        cur.execute(
            """
            SELECT user_id, role, is_active
            FROM users
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        user_row = cur.fetchone()

        legacy_user_id = None
        role = "student"

        if user_row:
            legacy_user_id, role, is_active = user_row
            if is_active is False:
                raise HTTPException(status_code=403, detail="User is inactive")

        # 3) Optional member_id if column exists in members table via separate query
        member_id = None
        try:
            cur.execute(
                """
                SELECT member_id
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
            "token_type": "bearer"
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

    # -------------------------
    # SPELLING STATS
    # -------------------------
    cur.execute("""
        SELECT COUNT(*), SUM(CASE WHEN correct THEN 1 ELSE 0 END)
        FROM spelling_attempts
        WHERE user_id = %s
    """, (user_id,))
    s_total, s_correct = cur.fetchone()
    s_total = s_total or 0
    s_correct = s_correct or 0
    s_acc = (s_correct / s_total * 100) if s_total > 0 else 0

    # -------------------------
    # WORD STATS
    # -------------------------
    cur.execute("""
        SELECT COUNT(*), SUM(CASE WHEN correct THEN 1 ELSE 0 END)
        FROM words_attempts
        WHERE user_id = %s
    """, (user_id,))
    w_total, w_correct = cur.fetchone()
    w_total = w_total or 0
    w_correct = w_correct or 0
    w_acc = (w_correct / w_total * 100) if w_total > 0 else 0

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
            "unlocked": "comprehension" in apps
        }
    }

    # -------------------------
    # INSIGHTS (SAFE)
    # -------------------------
    metrics = {
        "spelling": s_acc,
        "words": w_acc
    }

    if s_total == 0 and w_total == 0:
        strongest = None
        weakest = None
    else:
        strongest = max(metrics, key=metrics.get)
        weakest = min(metrics, key=metrics.get)

    return {
        "modules": modules,
        "insights": {
            "strongest": strongest,
            "weakest": weakest
        }
    }


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
                COALESCE(array_agg(ma.app_code), '{}') as apps
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
            apps = r[5] or []

            expected_apps = []

            if account_type != "free":
                expected_apps = ["math", "mock", "practice"]

            status = "ok" if set(expected_apps).issubset(set(apps)) else "missing_access"

            result.append(
                {
                    "email": r[1],
                    "role": r[2] if r[2] else "student",
                    "account_type": r[3],
                    "created_at": str(r[4]),
                    "apps": r[5] if r[5] else [],
                }
            )

        return result
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
            SELECT member_id, account_type
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
                SELECT member_id
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
        cur.execute(
            """
            UPDATE users
            SET role = %s
            WHERE email = %s
            """,
            (new_role, target_email),
        )
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
        # Gumroad sends FORM data (NOT JSON)
        form = await request.form()

        email = (form.get("email") or "").strip().lower()
        product_name = (form.get("product_name") or "").strip()
        match = re.search(r"Maths Mock Exam (\d+)", product_name)

        print("GUMROAD WEBHOOK:", email, product_name)

        if not email or not product_name:
            return {"status": "ignored"}

        # Exact product mapping
        product_map = {
            "MathsSprint": "math",
            "SpellingSprint": "spelling",
            "WordSprint": "words",
            "ComprehensionSprint": "comprehension",
        }

        app_code = product_map.get(product_name)

        if not app_code and not match:
            print("UNKNOWN PRODUCT:", product_name)
            return {"status": "unknown_product"}

        conn = get_connection()
        cur = conn.cursor()

        try:
            # Get member_id
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
                print("USER NOT FOUND:", email)
                return {"status": "user_not_found"}

            member_id = row[0]

            if match:
                test_number = match.group(1)
                test_id = f"MATH_MOCK_{test_number}"

                print("TEST PURCHASE:", email, test_id)

                cur.execute(
                    """
                    INSERT INTO math_user_test_access (member_id, test_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (member_id, test_id),
                )

                conn.commit()

                print(f"TEST ACCESS GRANTED -> {email} -> {test_id}")
                return {"status": "test_unlocked"}

            # Grant access (idempotent)
            cur.execute(
                """
                INSERT INTO kiaro_membership.member_apps (member_id, app_code)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (member_id, app_code),
            )

            conn.commit()
            print(f"ACCESS GRANTED -> {email} -> {app_code}")
            return {"status": "success"}
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print("WEBHOOK ERROR:", str(e))
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
app.include_router(comprehension_router)
