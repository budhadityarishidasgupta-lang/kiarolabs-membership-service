import os
import psycopg2
from psycopg2.extras import RealDictCursor


def get_conn():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    conn.autocommit = False
    return conn
