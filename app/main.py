from fastapi import FastAPI, Request
from app.database import get_connection
from datetime import datetime, timedelta
import hashlib

app = FastAPI()


@app.get("/")
def root():
    return {"status": "membership service running v4"}


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
    end_date = now + timedelta(days=30)

    subscription_status = "active"
    cancelled_at = None

    if cancelled_at_payload:
        subscription_status = "cancelled"
        cancelled_at = now

    temp_password = hashlib.sha256(email.encode()).hexdigest()[:10]

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO kiaro_membership.members
        (name, email, password_hash, subscription_status,
         subscription_start, subscription_end,
         gumroad_subscription_id,
         subscription_event,
         cancelled_at,
         updated_at)
        VALUES (%s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, NOW())
        ON CONFLICT (email) DO UPDATE
        SET subscription_status = %s,
            subscription_start = %s,
            subscription_end = %s,
            gumroad_subscription_id = %s,
            subscription_event = %s,
            cancelled_at = %s,
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
            subscription_status,
            now,
            end_date,
            subscription_id,
            "gumroad_webhook",
            cancelled_at
        )
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "webhook processed"}


@app.get("/system-check")
def system_check():
    results = {}

    try:
        conn = get_connection()
        cur = conn.cursor()
        results["database_connection"] = "OK"

        test_email = "systemcheck@test.com"

        cur.execute(
            """
            INSERT INTO kiaro_membership.members
            (name, email, password_hash, subscription_status,
             subscription_start, subscription_end,
             gumroad_subscription_id,
             subscription_event,
             updated_at)
            VALUES (%s, %s, %s, 'active',
                    NOW(),
                    NOW() + INTERVAL '30 days',
                    %s,
                    'system_check',
                    NOW())
            ON CONFLICT (email) DO UPDATE
            SET subscription_status = 'active',
                updated_at = NOW()
            """,
            ("System Check", test_email, "testpass", "sys-check")
        )
        conn.commit()
        results["write_test"] = "OK"

        cur.execute(
            "SELECT subscription_status FROM kiaro_membership.members WHERE email = %s",
            (test_email,)
        )
        row = cur.fetchone()

        if row and row[0] == "active":
            results["read_test"] = "OK"
        else:
            results["read_test"] = "FAILED"

        cur.close()
        conn.close()

    except Exception as e:
        results["error"] = str(e)

    return results
