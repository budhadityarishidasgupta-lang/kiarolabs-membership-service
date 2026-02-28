from fastapi import FastAPI, Request
from app.database import get_connection
from datetime import datetime, timedelta
import hashlib

app = FastAPI()


@app.get("/")
def root():
    return {"status": "membership service running v2"}


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

    temp_password = hashlib.sha256(email.encode()).hexdigest()[:10]

    conn = get_connection()
    cur = conn.cursor()

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


@app.get("/test-webhook")
def test_webhook():
    email = "testuser@email.com"
    name = "Test User"
    subscription_id = "test-sub-123"

    temp_password = hashlib.sha256(email.encode()).hexdigest()[:10]

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO kiaro_membership.members
        (name, email, password_hash, subscription_status,
         subscription_start, subscription_end,
         gumroad_subscription_id)
        VALUES (%s, %s, %s, 'active', %s, %s, %s)
        ON CONFLICT (email) DO UPDATE
        SET subscription_status = 'active',
            subscription_start = %s,
            subscription_end = %s,
            gumroad_subscription_id = %s
        """,
        (
            name,
            email,
            temp_password,
            datetime.utcnow(),
            datetime.utcnow() + timedelta(days=30),
            subscription_id,
            datetime.utcnow(),
            datetime.utcnow() + timedelta(days=30),
            subscription_id
        )
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "test membership created"}
    
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
                 gumroad_subscription_id)
                VALUES (%s, %s, %s, 'active',
                        NOW(),
                        NOW() + INTERVAL '30 days',
                        %s)
                ON CONFLICT (email) DO UPDATE
                SET subscription_status = 'active'
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
