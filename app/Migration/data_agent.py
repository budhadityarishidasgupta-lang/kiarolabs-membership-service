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

    if "UPDATE " in upper_sql and "KIARO_MEMBERSHIP.MIGRATION_RUNS" not in upper_sql:
        raise ValueError("UPDATE is only allowed on kiaro_membership.migration_runs")


def get_migration_status(cur, migration_name: str):
    cur.execute(
        """
        SELECT status
        FROM kiaro_membership.migration_runs
        WHERE migration_name = %s
        """,
        (migration_name,)
    )
    row = cur.fetchone()
    return row["status"] if row else None


def build_member_insert_sql(source):
    schema = source["schema"]
    table = source["table"]
    email_col = source["email_col"]
    name_col = source["name_col"]
    password_hash_col = source["password_hash_col"]

    name_expr = name_col if name_col else "NULL"
    password_expr = password_hash_col if password_hash_col else "NULL"

    sql = f"""
    INSERT INTO kiaro_membership.members (email, name, password_hash)
    SELECT DISTINCT
        {email_col} AS email,
        {name_expr} AS name,
        {password_expr} AS password_hash
    FROM {schema}.{table}
    WHERE {email_col} IS NOT NULL
    ON CONFLICT (email) DO NOTHING
    """
    return sql


def build_member_apps_insert_sql(source):
    schema = source["schema"]
    table = source["table"]
    email_col = source["email_col"]
    app_code_col = source["app_code_col"]
    default_app_code = source["default_app_code"]

    if app_code_col:
        app_code_expr = app_code_col
        where_clause = f"{email_col} IS NOT NULL AND {app_code_col} IS NOT NULL"
    else:
        app_code_expr = f"'{default_app_code}'"
        where_clause = f"{email_col} IS NOT NULL"

    sql = f"""
    INSERT INTO kiaro_membership.member_apps (member_id, app_code)
    SELECT DISTINCT
        m.id,
        {app_code_expr} AS app_code
    FROM {schema}.{table} s
    JOIN kiaro_membership.members m
      ON LOWER(TRIM(s.{email_col})) = LOWER(TRIM(m.email))
    WHERE {where_clause}
    ON CONFLICT (member_id, app_code) DO NOTHING
    """
    return sql


def source_count_sql(source):
    schema = source["schema"]
    table = source["table"]
    email_col = source["email_col"]
    return f"""
    SELECT COUNT(*) AS cnt
    FROM {schema}.{table}
    WHERE {email_col} IS NOT NULL
    """


def main():
    args = sys.argv[1:]
    dry_run = "--dry-run" in args

    if "--plan" not in args:
        print("Usage: python migrations/data_agent.py --plan migrations/plans/2026_03_unify_members.json [--dry-run]")
        sys.exit(1)

    plan_index = args.index("--plan")
    plan_path = Path(args[plan_index + 1])
    plan = json.loads(plan_path.read_text())
    validate_plan(plan)

    conn = get_conn()
    cur = conn.cursor()

    try:
        status = get_migration_status(cur, plan["migration_name"])
        if status != "schema_ready" and status != "failed":
            raise RuntimeError(f"Migration must be schema_ready or failed before running. Current status: {status}")

        cur.execute(
            """
            UPDATE kiaro_membership.migration_runs
            SET status = 'running', started_at = CURRENT_TIMESTAMP
            WHERE migration_name = %s
            """,
            (plan["migration_name"],)
        )

        report = {"dry_run": dry_run, "sources": []}

        for source in plan["sources"]:
            src_sql = source_count_sql(source)
            ensure_safe_sql(src_sql)
            cur.execute(src_sql)
            source_count = cur.fetchone()["cnt"]

            member_sql = build_member_insert_sql(source)
            member_apps_sql = build_member_apps_insert_sql(source)

            ensure_safe_sql(member_sql)
            ensure_safe_sql(member_apps_sql)

            if not dry_run:
                cur.execute(member_sql)
                members_inserted = cur.rowcount

                cur.execute(member_apps_sql)
                member_apps_inserted = cur.rowcount
            else:
                members_inserted = None
                member_apps_inserted = None

            report["sources"].append(
                {
                    "source_name": source["source_name"],
                    "source_rows_with_email": source_count,
                    "members_inserted": members_inserted,
                    "member_apps_inserted": member_apps_inserted
                }
            )

        final_status = "schema_ready" if dry_run else "completed"

        cur.execute(
            """
            UPDATE kiaro_membership.migration_runs
            SET status = %s,
                finished_at = CURRENT_TIMESTAMP,
                details = %s::jsonb
            WHERE migration_name = %s
            """,
            (
                final_status,
                json.dumps(report),
                plan["migration_name"]
            )
        )

        conn.commit()
        if dry_run:
            print("Dry run completed successfully.")
        else:
            print("Data migration completed successfully.")
        print(json.dumps(report, indent=2))

    except Exception as e:
        conn.rollback()
        try:
            cur.execute(
                """
                UPDATE kiaro_membership.migration_runs
                SET status = 'failed',
                    finished_at = CURRENT_TIMESTAMP,
                    details = %s::jsonb
                WHERE migration_name = %s
                """,
                (
                    json.dumps({"error": str(e)}),
                    plan["migration_name"]
                )
            )
            conn.commit()
        except Exception:
            conn.rollback()

        print(f"Data agent failed: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
