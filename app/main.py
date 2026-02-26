from fastapi import FastAPI
from app.database import get_connection

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
