from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
import os
from app.database import get_connection

# OAuth2 token extractor
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGO = "HS256"


def get_current_user(token: str = Depends(oauth2_scheme)):

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])

        email = payload.get("sub")
        user_id = payload.get("user_id")
        account_type = payload.get("account_type")

        print("AUTH PHASE2 user_id_from_token=", user_id)

        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        if user_id:
            return {
                "id": user_id,
                "user_id": user_id,
                "email": email,
                "sub": email,
                "account_type": account_type,
            }

        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT user_id
            FROM users
            WHERE LOWER(email) = LOWER(%s)
            """,
            (email,),
        )

        user_row = cur.fetchone()
        platform_user_id = user_row[0] if user_row else None

        print(f"AUTH DEBUG email={email}")
        print(f"AUTH DEBUG user_id={platform_user_id}")

        cur.close()
        conn.close()

        if not user_row:
            raise HTTPException(status_code=401, detail="User not found")

        return {
            "id": user_row[0],
            "user_id": user_row[0] if user_row else None,
            "email": email,
            "sub": email,
            "account_type": account_type,
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
