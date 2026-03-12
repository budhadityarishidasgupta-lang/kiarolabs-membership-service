from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
import os
from app.database import get_connection

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGO = "HS256"


from fastapi import Header

def get_current_user(Authorization: str = Header(None)):

if not Authorization:
    raise HTTPException(status_code=401, detail="Missing Authorization header")

if not Authorization.startswith("Bearer "):
    raise HTTPException(status_code=401, detail="Invalid Authorization header")

token = Authorization.split(" ")[1]

try:
    payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])

    email = payload.get("sub")
    account_type = payload.get("account_type")

    if not email:
        raise HTTPException(status_code=401, detail="Invalid token")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id
        FROM kiaro_membership.members
        WHERE email = %s
        """,
        (email,),
    )

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=401, detail="User not found")

    return {
        "id": row[0],
        "email": email,
        "sub": email,
        "account_type": account_type,
    }

except JWTError:
    raise HTTPException(status_code=401, detail="Invalid token")
