from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
import os
from app.database import get_connection

# OAuth2 token extractor
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGO = "HS256"


def resolve_verified_learning_user_id(cur, user) -> int | None:
    email = user.get("sub") or user.get("email")
    token_user_id = user.get("user_id")

    if not email:
        return None

    cur.execute(
        """
        SELECT user_id
        FROM users
        WHERE LOWER(email) = LOWER(%s)
        ORDER BY user_id ASC
        LIMIT 1
        """,
        (email,),
    )
    row = cur.fetchone()
    if not row:
        return None

    email_user_id = row[0]
    if token_user_id is None:
        return email_user_id

    if str(token_user_id) == str(email_user_id):
        return email_user_id

    return email_user_id


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])

        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        conn = get_connection()
        cur = conn.cursor()
        try:
            payload["user_id"] = resolve_verified_learning_user_id(cur, payload)
        finally:
            cur.close()
            conn.close()

        # Return full payload so role/user_id/account_type are preserved
        return payload

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
