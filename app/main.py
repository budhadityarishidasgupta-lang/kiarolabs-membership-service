import os
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
        "https://preview--growth-leap-studio.lovable.app",
        "https://growth-leap-studio.lovable.app",
        "http://localhost:3000",
        "http://localhost:5173",
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
        "SELECT id FROM kiaro_membership.members WHERE email = %s",
        (email,),
    )

    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=409, detail="Email already registered")

    pw_hash = hash_password(req.password)

    cur.execute(
        """
        INSERT INTO kiaro_membership.members
        (name, email, password_hash, subscription_status, account_type, auth_provider, created_at, updated_at)
        VALUES (%s, %s, %s, 'inactive', 'free', 'email', NOW(), NOW())
        """,
        (name, email, pw_hash),
    )

    print(f"USER PROVISIONING: email={email}")
    try:
        cur.execute(
            """
            SELECT user_id
            FROM users
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )

        row = cur.fetchone()

        if not row:
            cur.execute(
                """
                INSERT INTO users (name, email, password_hash, role, is_active)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    name,
                    email,
                    "membership_managed_user",
                    "student",
                    True,
                ),
            )
            print("USER PROVISIONING: created users row")
        else:
            print("USER PROVISIONING: already exists")
    except Exception as provision_err:
        print(f"USER PROVISIONING ERROR: {provision_err}")

    conn.commit()
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
        # 1) Membership-first login
        cur.execute(
            """
            SELECT id, email, password_hash, account_type
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        member_row = cur.fetchone()

        if member_row:
            member_id, member_email, member_password_hash, account_type = member_row

            valid = False
            if member_password_hash:
                try:
                    valid = pwd_context.verify(password, member_password_hash)
                except Exception:
                    valid = False

            if valid:
                # Try to preserve legacy user_id if a users-row exists
                cur.execute(
                    """
                    SELECT user_id
                    FROM users
                    WHERE LOWER(email) = LOWER(%s)
                    """,
                    (email,),
                )
                legacy_row = cur.fetchone()
                legacy_user_id = legacy_row[0] if legacy_row else None

                token = jwt.encode(
                    {
                        "sub": member_email,
                        "user_id": legacy_user_id if legacy_user_id is not None else member_id,
                        "member_id": member_id,
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

        # 2) Fallback to legacy users login
        cur.execute(
            """
            SELECT user_id, email, password_hash
            FROM users
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        user_id, user_email, password_hash = row

        if password_hash == "membership_managed_user":
            # This sentinel should not be used for direct password login
            raise HTTPException(status_code=401, detail="Invalid credentials")

        try:
            valid = pwd_context.verify(password, password_hash)
        except Exception:
            valid = False

        if not valid:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # Try to enrich token with member/account info if available
        cur.execute(
            """
            SELECT id, account_type
            FROM kiaro_membership.members
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )
        member_info = cur.fetchone()
        member_id = member_info[0] if member_info else None
        account_type = member_info[1] if member_info else "free"

        token = jwt.encode(
            {
                "sub": user_email,
                "user_id": user_id,
                "member_id": member_id,
                "account_type": account_type,
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
        "account_type": user.get("account_type"),
    }

@app.get("/dashboard")
def dashboard(user=Depends(get_current_user)):
    conn = get_connection()
    cur = conn.cursor()

    user_id = user.get("user_id")
    email = user.get("sub")

    # Validate that user_id belongs to users table (important)
    if user_id:
        cur.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
        if not cur.fetchone():
            user_id = None

    # Fallback: resolve user_id via email
    if not user_id:
        cur.execute(
            "SELECT user_id FROM users WHERE LOWER(email) = LOWER(%s)",
            (email,)
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return {"error": "user not found"}
        user_id = row[0]

    # -------------------------
    # USER TYPE (LEGACY vs NEW)
    # -------------------------
    cur.execute("""
        SELECT created_at
        FROM kiaro_membership.members
        WHERE id = %s
    """, (user.get("member_id"),))

    row = cur.fetchone()
    created_at = row[0] if row else None

    cutoff_date = datetime(2026, 4, 3)
    is_legacy = created_at and created_at < cutoff_date

    # -------------------------
    # FETCH ENTITLEMENTS
    # -------------------------
    cur.execute("""
        SELECT app_code
        FROM kiaro_membership.member_apps
        WHERE member_id = %s
    """, (user.get("member_id"),))

    apps = [row[0] for row in cur.fetchall()]

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

    modules = {
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
            "unlocked": ("math" in apps) if not is_legacy else False
        },
        "nvr": {
            "unlocked": "nvr" in apps
        },
        "comprehension": {
            "unlocked": "comprehension" in apps
        }
    }

    module_scores = {
        "spelling": s_acc,
        "words": w_acc
    }

    # Handle empty data correctly
    if s_total == 0 and w_total == 0:
        strongest = None
        weakest = None
    else:
        strongest = max(module_scores, key=module_scores.get)
        weakest = min(module_scores, key=module_scores.get)

    return {
        "modules": modules,
        "insights": {
            "strongest": strongest,
            "weakest": weakest
        }
    }


# =========================
# Gumroad Webhook
# =========================
@app.post("/webhook/gumroad")
async def gumroad_webhook(request: Request):
    data = await request.form()

    email = (data.get("email") or "").strip().lower()
    name = data.get("full_name")
    subscription_id = data.get("subscription_id")
    cancelled_at_payload = data.get("subscription_cancelled_at")

    if not email:
        return {"error": "missing email"}

    now = datetime.utcnow()

    subscription_status = "active"
    cancelled_at = None

    if cancelled_at_payload:
        subscription_status = "cancelled"
        cancelled_at = now

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT subscription_end FROM kiaro_membership.members WHERE email = %s",
        (email,),
    )
    existing = cur.fetchone()

    if subscription_status == "active" and existing and existing[0] and existing[0] > now:
        end_date = existing[0] + timedelta(days=30)
    else:
        end_date = now + timedelta(days=30)

    account_type = "paid" if subscription_status in ("active", "cancelled") else "free"

    cur.execute(
        """
        INSERT INTO kiaro_membership.members
        (name, email, password_hash, subscription_status,
         subscription_start, subscription_end,
         gumroad_subscription_id,
         subscription_event,
         cancelled_at,
         account_type,
         auth_provider,
         updated_at)
        VALUES (%s, %s, NULL, %s,
                %s, %s, %s,
                %s, %s, %s,
                COALESCE((SELECT auth_provider FROM kiaro_membership.members WHERE email=%s), 'gumroad'),
                NOW())
        ON CONFLICT (email) DO UPDATE
        SET name = COALESCE(EXCLUDED.name, kiaro_membership.members.name),
            subscription_status = EXCLUDED.subscription_status,
            subscription_start = EXCLUDED.subscription_start,
            subscription_end = EXCLUDED.subscription_end,
            gumroad_subscription_id = EXCLUDED.gumroad_subscription_id,
            subscription_event = EXCLUDED.subscription_event,
            cancelled_at = EXCLUDED.cancelled_at,
            account_type = EXCLUDED.account_type,
            updated_at = NOW()
        """,
        (
            name,
            email,
            subscription_status,
            now,
            end_date,
            subscription_id,
            "gumroad_webhook",
            cancelled_at,
            account_type,
            email,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "webhook processed"}


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
