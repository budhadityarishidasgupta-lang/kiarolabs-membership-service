import json
import sys
from pathlib import Path

from db import get_conn

FORBIDDEN_SQL = ["DROP ", "TRUNCATE ", "DELETE "]


def validate_plan(plan: dict):
    if "migration_name" not in plan:
        raise ValueError("Plan missing migration_name")
    if "sources" not in plan or not isinstance(plan["sources"], list):
        raise ValueError("Plan missing sources list")


def ensure_safe_sql(sql: str):
    upper_sql = sql.upper()
    for keyword in FORBIDDEN_SQL:
        if keyword in upper_sql:
            raise ValueError(f"Forbidden SQL detected: {keyword.strip()}")


def main():
    if len(sys.argv) != 3 or sys.argv[1] != "--plan":
        print("Usage: python migrations/schema_agent.py --plan migrations/plans/2026_03_unify_members.json")
        sys.exit(1)

    plan_path = Path(sys.argv[2])
    plan = json.loads(plan_path.read_text())
    validate_plan(plan)

    conn = get_conn()
    cur = conn.cursor()

    try:
        statements = [
            """
            CREATE SCHEMA IF NOT EXISTS kiaro_membership;
            """,
            """
            CREATE TABLE IF NOT EXISTS kiaro_membership.migration_runs (
                id SERIAL PRIMARY KEY,
                migration_name TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,
                started_at TIMESTAMP NULL,
                finished_at TIMESTAMP NULL,
                details JSONB NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS kiaro_membership.member_apps (
                id SERIAL PRIMARY KEY,
                member_id INTEGER NOT NULL REFERENCES kiaro_membership.members(id),
                app_code TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (member_id, app_code)
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_member_apps_member_id
            ON kiaro_membership.member_apps(member_id);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_member_apps_app_code
            ON kiaro_membership.member_apps(app_code);
            """
        ]

        for sql in statements:
            ensure_safe_sql(sql)
            cur.execute(sql)

        cur.execute(
            """
            INSERT INTO kiaro_membership.migration_runs (migration_name, status, started_at, details)
            VALUES (%s, 'schema_ready', CURRENT_TIMESTAMP, %s::jsonb)
            ON CONFLICT (migration_name)
            DO UPDATE SET
                status = EXCLUDED.status,
                started_at = CURRENT_TIMESTAMP,
                details = EXCLUDED.details
            """,
            (
                plan["migration_name"],
                json.dumps({"step": "schema_agent_completed"})
            )
        )

        conn.commit()
        print("Schema agent completed successfully.")
        print("Created/validated:")
        print("- kiaro_membership.migration_runs")
        print("- kiaro_membership.member_apps")

    except Exception as e:
        conn.rollback()
        print(f"Schema agent failed: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
