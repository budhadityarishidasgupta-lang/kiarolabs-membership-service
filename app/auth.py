from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
import os

# OAuth2 token extractor
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGO = "HS256"


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])

        email = payload.get("sub")
        user_id = payload.get("user_id")
        member_id = payload.get("member_id")
        account_type = payload.get("account_type")

        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        if not user_id and not member_id:
            raise HTTPException(status_code=401, detail="Invalid token: user_id missing")

        return {
            "id": member_id if member_id is not None else user_id,
            "user_id": user_id,
            "member_id": member_id,
            "email": email,
            "sub": email,
            "account_type": account_type,
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
