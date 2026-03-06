from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
import os

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"


def get_current_user(token: str = Depends(oauth2_scheme)):

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])

        user_id = payload.get("user_id")
        email = payload.get("email")

        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        return {
            "id": user_id,
            "email": email
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
