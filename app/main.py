import os
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException, Depends, Form
#from fastapi.security import OAuth2PasswordBearer
from app.auth import get_current_user
from pydantic import BaseModel, EmailStr
from app.database import get_connection
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

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
#oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

app = FastAPI()

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
def login(
    email: Optional[str] = Form(None),
    username: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    request: Optional[LoginRequest] = None
):

    # Normalize input
    if request:
        email = request.email
        password = request.password
    else:
        # Swagger sends username instead of email
        if not email and username:
            email = username

    if not email or not password:
        raise HTTPException(status_code=400, detail="Missing credentials")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT user_id, password_hash
        FROM users
        WHERE email = %s
        """,
        (email,),
    )

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user_id, password_hash = row

    # Handle membership-managed users
    if password_hash == "membership_managed_user":
        # trust external auth (Lovable already validated)
        valid = True
    else:
        try:
            valid = pwd_context.verify(password, password_hash)
        except:
            valid = False

    if not valid:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = jwt.encode(
        {
            "sub": email,
            "user_id": user_id
        },
        JWT_SECRET,
        algorithm=JWT_ALGO
    )

    return {"access_token": token}


# =========================
# Current User
# =========================
@app.get("/me")
def me(user=Depends(get_current_user)):
    return {
        "email": user["sub"],
        "account_type": user.get("account_type"),
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
