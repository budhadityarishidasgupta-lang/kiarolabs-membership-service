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
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        if not user_id:
            conn = get_connection()
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT user_id FROM users WHERE LOWER(email)=LOWER(%s)",
                    (email,),
                )
                row = cur.fetchone()
                if row:
                    user_id = row[0]
                    payload["user_id"] = user_id
            finally:
                cur.close()
                conn.close()

        # Return full payload so role/user_id/account_type are preserved
        return payload

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
