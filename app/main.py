from fastapi import FastAPI, Request
from app.database import get_connection
from datetime import datetime, timedelta
import hashlib

app = FastAPI()


@app.get("/")
def root():
    return {"status": "membership service running"}


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

    if not email:
        return {"error": "missing email"}

    # simple password generation (temporary)
    temp_password = hashlib.sha256(email.encode()).hexdigest()[:10]

    conn = get_connection()
    cur = conn.cursor()

    # Check if user already exists
    cur.execute(
        "SELECT id FROM kiaro_membership.members WHERE email = %s",
        (email,)
    )
    existing_user = cur.fetchone()

    if existing_user:
        cur.execute(
            """
            UPDATE kiaro_membership.members
            SET subscription_status = 'active',
                subscription_start = %s,
                subscription_end = %s,
                gumroad_subscription_id = %s
            WHERE email = %s
            """,
            (
                datetime.utcnow(),
                datetime.utcnow() + timedelta(days=30),
                subscription_id,
                email
            )
        )
    else:
        cur.execute(
            """
            INSERT INTO kiaro_membership.members
            (name, email, password_hash, subscription_status,
             subscription_start, subscription_end,
             gumroad_subscription_id)
            VALUES (%s, %s, %s, 'active', %s, %s, %s)
            """,
            (
                name,
                email,
                temp_password,
                datetime.utcnow(),
                datetime.utcnow() + timedelta(days=30),
                subscription_id
            )
        )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "membership updated"}
