from fastapi import FastAPI, Request
from app.database import get_connection
from datetime import datetime, timedelta
import hashlib

app = FastAPI()


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


@app.post("/webhook/gumroad")
async def gumroad_webhook(request: Request):
    data = await request.form()

    email = data.get("email")
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

    account_type = "paid" if subscription_status == "active" else "free"

    conn = get_connection()
    cur = conn.cursor()

    # Check existing subscription_end
    cur.execute(
        "SELECT subscription_end FROM kiaro_membership.members WHERE email = %s",
        (email,)
    )
    existing = cur.fetchone()

    if existing and existing[0] and subscription_status == "active":
        end_date = existing[0] + timedelta(days=30)
    else:
        end_date = now + timedelta(days=30)

    # Only generate password if new user
    cur.execute(
        "SELECT password_hash FROM kiaro_membership.members WHERE email = %s",
        (email,)
    )
    existing_user = cur.fetchone()

    if not existing_user:
        temp_password = hashlib.sha256(email.encode()).hexdigest()[:10]
    else:
        temp_password = existing_user[0]

    cur.execute(
        """
        INSERT INTO kiaro_membership.members
        (name, email, password_hash, subscription_status,
         subscription_start, subscription_end,
         gumroad_subscription_id,
         subscription_event,
         cancelled_at,
         account_type,
         updated_at)
        VALUES (%s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, NOW())
        ON CONFLICT (email) DO UPDATE
        SET subscription_status = %s,
            subscription_end = %s,
            gumroad_subscription_id = %s,
            subscription_event = %s,
            cancelled_at = %s,
            account_type = %s,
            updated_at = NOW()
        """,
        (
            name,
            email,
            temp_password,
            subscription_status,
            now,
            end_date,
            subscription_id,
            "gumroad_webhook",
            cancelled_at,
            account_type,
            subscription_status,
            end_date,
            subscription_id,
            "gumroad_webhook",
            cancelled_at,
            account_type
        )
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "webhook processed"}


@app.get("/validate-user")
def validate_user(email: str):
    if not email:
        return {"active": False, "reason": "email_required"}

    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT subscription_status, subscription_end
            FROM kiaro_membership.members
            WHERE email = %s
            """,
            (email,)
        )

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {"active": False, "reason": "user_not_found"}

        subscription_status, subscription_end = row
        now = datetime.utcnow()

        if subscription_status != "active":
            return {"active": False, "reason": subscription_status}

        if subscription_end and subscription_end < now:
            return {"active": False, "reason": "subscription_expired"}

        return {
            "active": True,
            "subscription_status": subscription_status,
            "subscription_end": subscription_end.isoformat() if subscription_end else None
        }

    except Exception as e:
        return {"error": str(e)}
