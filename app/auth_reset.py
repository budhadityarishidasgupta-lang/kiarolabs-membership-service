import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr

from app.auth import get_current_user
from app.database import get_connection


router = APIRouter()

pwd_context = CryptContext(
    schemes=["bcrypt", "pbkdf2_sha256"],
    deprecated="auto",
)

RESET_MESSAGE = "If account exists, reset link sent"
RESET_TOKEN_MINUTES = 30


class RequestResetPayload(BaseModel):
    email: EmailStr


class ResetPasswordPayload(BaseModel):
    token: str
    new_password: str


class AdminResetPasswordPayload(BaseModel):
    user_id: int
    new_password: str


def init_password_reset_tables():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                token TEXT NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                used BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _hash_password(password: str) -> str:
    return pwd_context.hash(password)


def _update_password_hashes(cur, user_id: int, password_hash: str):
    cur.execute(
        """
        SELECT email
        FROM users
        WHERE user_id = %s
        """,
        (user_id,),
    )
    row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    email = row[0]

    cur.execute(
        """
        UPDATE users
        SET password_hash = %s
        WHERE user_id = %s
        """,
        (password_hash, user_id),
    )

    cur.execute(
        """
        UPDATE kiaro_membership.members
        SET password_hash = %s, updated_at = NOW()
        WHERE LOWER(email) = LOWER(%s)
        """,
        (password_hash, email),
    )


@router.post("/auth/request-reset")
def request_reset(payload: RequestResetPayload):
    email = payload.email.strip().lower()

    conn = get_connection()
    cur = conn.cursor()

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

        if row:
            token = secrets.token_urlsafe(32)
            expires_at = datetime.utcnow() + timedelta(minutes=RESET_TOKEN_MINUTES)

            cur.execute(
                """
                INSERT INTO password_reset_tokens (user_id, token, expires_at)
                VALUES (%s, %s, %s)
                """,
                (row[0], token, expires_at),
            )
            conn.commit()

        return {"message": RESET_MESSAGE}
    finally:
        cur.close()
        conn.close()


@router.post("/auth/reset-password")
def reset_password(payload: ResetPasswordPayload):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT id, user_id
            FROM password_reset_tokens
            WHERE token = %s
              AND used = FALSE
              AND expires_at > NOW()
            """,
            (payload.token,),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=400, detail="Invalid or expired reset token")

        token_id, user_id = row
        password_hash = _hash_password(payload.new_password)
        _update_password_hashes(cur, user_id, password_hash)

        cur.execute(
            """
            UPDATE password_reset_tokens
            SET used = TRUE
            WHERE id = %s
            """,
            (token_id,),
        )

        conn.commit()
        return {"message": "Password reset successful"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    finally:
        cur.close()
        conn.close()


@router.post("/admin/reset-password")
def admin_reset_password(
    payload: AdminResetPasswordPayload,
    user=Depends(get_current_user),
):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")

    conn = get_connection()
    cur = conn.cursor()

    try:
        password_hash = _hash_password(payload.new_password)
        _update_password_hashes(cur, payload.user_id, password_hash)
        conn.commit()

        return {"message": "Password updated by admin"}
    except HTTPException:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
