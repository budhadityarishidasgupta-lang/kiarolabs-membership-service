from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, EmailStr
from app.database import get_connection
from datetime import datetime, timedelta
from passlib.context import CryptContext
from jose import jwt, JWTError

# =========================
# Config (keep simple)
# =========================
JWT_SECRET = "CHANGE_ME_IN_RENDER_ENV"  # put in Render env var later
JWT_ALGO = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 2

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

app = FastAPI()

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
def create_access_token(email: str, account_type: str):
    expire = datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    payload = {"sub": email, "account_type": account_type, "exp": expire}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
    return token, expire

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

def derive_subscription_state(subscription_status: str | None, subscription_end):
    """
    Keeps behavior predictable for your apps:
    - active + end>=now -> active
    - cancelled + end>=now -> still treated as active access (but reason=cancelled)
    - end<now -> expired
    - otherwise -> inactive
    """
    now = datetime.utcnow()

    if subscription_end and subscription_end < now:
        return False, "subscription_expired"

    if subscription_status == "active":
        return True, "active"

    if subscription_status == "cancelled":
        # Still allow access until end date (common subscription behavior)
        return True, "cancelled"

    return False, subscription_status or "inactive"

# =========================
# Health
# =========================
@app.get("/")
def root():
    return {"status": "membership service running v6"}

@app.get("/health")
def health_check():
    try:
        conn = get_connection()
        conn.close()
        return {"database": "connected"}
    except Exception as e:
        return {"database": "error", "details": str(e)}

# =========================
# Auth: Register + Login
# =========================
@app.post("/register")
def register(req: RegisterRequest):
    conn = get_connection()
    cur = conn.cursor()

    # Check existing
    cur.execute("SELECT id FROM kiaro_membership.members WHERE email = %s", (req.email,))
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
        (req.name, req.email, pw_hash),
    )

    conn.commit()
    cur.close()
    conn.close()

    # Auto-login after register
    token, exp = create_access_token(req.email, "free")
    return {"access_token": token, "token_type": "bearer", "expires_at": exp.isoformat()}

@app.post("/login")
def login(req: LoginRequest):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT password_hash, account_type
        FROM kiaro_membership.members
        WHERE email = %s
        """,
        (req.email,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    password_hash, account_type = row

    if not password_hash:
        # e.g. created via Gumroad webhook before setting password
        raise HTTPException(status_code=401, detail="Password not set. Please register or set password.")

    if not verify_password(req.password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token, exp = create_access_token(req.email, account_type or "free")
    return {"access_token": token, "token_type": "bearer", "expires_at": exp.isoformat()}

@app.get("/me")
def me(user=Depends(get_current_user)):
    # user contains sub + account_type
    return {"email": user["sub"], "account_type": user.get("account_type")}

# =========================
# Gumroad webhook
# =========================
@app.post("/webhook/gumroad")
async def gumroad_webhook(request: Request):
    """
    Gumroad sends form-encoded payloads.
    Needs python-multipart installed.
    """
    data = await request.form()

    email = (data.get("email") or "").strip().lower()
    name = data.get("full_name")
    subscription_id = data.get("subscription_id")
    cancelled_at_payload = data.get("subscription_cancelled_at")

    if not email:
        return {"error": "missing email"}

    now = datetime.utcnow()

    # Basic status handling
    subscription_status = "active"
    cancelled_at = None
    if cancelled_at_payload:
        subscription_status = "cancelled"
        cancelled_at = now

    # Extend logic
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

    # If user doesn't exist yet: create user WITHOUT password (they can register later)
    # If user exists: upgrade to paid
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
            email,  # for COALESCE subquery
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "webhook processed"}

# =========================
# Validate user (keep for apps)
# =========================
@app.get("/validate-user")
def validate_user(email: str):
    """
    Keep this endpoint for now (simple integration for your apps/website).
    Later we can switch to JWT-only access checks.
    """
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
