from __future__ import annotations

from app.database import get_connection


def insert_vr_questions_bulk(rows: list[dict], conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    inserted = 0
    existing = 0
    cur = conn.cursor()

    try:
        for row in rows:
            cur.execute(
                """
                INSERT INTO verbal_reasoning_printable_questions
                (
                    paper_code,
                    question_number,
                    question_text,
                    notes,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (paper_code, question_number) DO NOTHING
                """,
                (
                    row["paper_code"],
                    row["question_number"],
                    row["question_text"],
                    row.get("notes"),
                ),
            )
            if cur.rowcount:
                inserted += 1
            else:
                existing += 1

        return {
            "inserted": inserted,
            "existing": existing,
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def insert_vr_answers_bulk(rows: list[dict], conn=None) -> dict:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    inserted = 0
    updated = 0
    unchanged = 0
    cur = conn.cursor()

    try:
        for row in rows:
            cur.execute(
                """
                SELECT correct_answer
                FROM verbal_reasoning_printable_answer_keys
                WHERE paper_code = %s
                  AND question_number = %s
                """,
                (row["paper_code"], row["question_number"]),
            )
            existing = cur.fetchone()

            if not existing:
                cur.execute(
                    """
                    INSERT INTO verbal_reasoning_printable_answer_keys
                    (paper_code, question_number, correct_answer, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    """,
                    (
                        row["paper_code"],
                        row["question_number"],
                        row["correct_answer"],
                    ),
                )
                inserted += 1
                continue

            if (existing[0] or "").strip() == row["correct_answer"]:
                unchanged += 1
                continue

            cur.execute(
                """
                UPDATE verbal_reasoning_printable_answer_keys
                SET correct_answer = %s,
                    updated_at = NOW()
                WHERE paper_code = %s
                  AND question_number = %s
                """,
                (
                    row["correct_answer"],
                    row["paper_code"],
                    row["question_number"],
                ),
            )
            updated += 1

        return {
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
        }
    finally:
        cur.close()
        if owns_connection:
            conn.close()


def get_vr_answers_by_paper(paper_code: str, conn=None) -> list[dict]:
    owns_connection = conn is None
    if owns_connection:
        conn = get_connection()

    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT question_number, correct_answer
            FROM verbal_reasoning_printable_answer_keys
            WHERE paper_code = %s
            ORDER BY question_number
            """,
            (paper_code,),
        )
        rows = cur.fetchall()
        return [
            {
                "question_number": row[0],
                "correct_answer": row[1],
            }
            for row in rows
        ]
    finally:
        cur.close()
        if owns_connection:
            conn.close()
